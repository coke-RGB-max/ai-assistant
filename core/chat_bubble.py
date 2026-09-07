"""
聊天气泡拆分与表情标记解析（纯逻辑、无 IO，可单测）。

目标：让一次 LLM 回复能像真人一样拆成多条短气泡依次发送，并在气泡之间插入 QQ 表情。

约定：
- LLM 用分隔符 BUBBLE_SEP（‖）显式分条；未显式分条时，仅当整段偏长才按中文句读温和切分，
  短回复保持单条，避免“晚上好呀”这种被切碎。
- 表情占位符 [face:名字或id]，名字经 FACE_NAME_TO_ID 映射为 OneBot 原生小黄脸 id，
  在发送端作为独立 face 气泡发出。
"""
import re
import random
from typing import Dict, List, Optional

# 显式气泡分隔符（同时兼容模型偶尔输出的 || 和 ▏）
BUBBLE_SEP = "‖"
_ALT_SEPS = ("||", "▏", "｜｜")
# 表情占位符：[face:开心] / [face:66]
FACE_PATTERN = re.compile(r"\[face:([^\[\]]{1,8})\]")

# QQ 原生小黄脸（OneBot v11 face id），按中文语义名映射
FACE_NAME_TO_ID: Dict[str, str] = {
    "微笑": "0", "撇嘴": "1", "色": "2", "发呆": "3", "得意": "4", "流泪": "5",
    "害羞": "6", "闭嘴": "7", "睡": "8", "大哭": "9", "尴尬": "10", "生气": "11",
    "发怒": "11", "调皮": "12", "呲牙": "13", "开心": "13", "笑": "13", "哈哈": "13",
    "惊讶": "14", "难过": "15", "酷": "16", "冷汗": "17", "抓狂": "18", "吐": "19",
    "偷笑": "20", "可爱": "21", "傲慢": "22", "傲娇": "22", "饿": "23", "困": "24",
    "惊恐": "25", "流汗": "26", "憨笑": "28", "奋斗": "30", "疑问": "32", "疑惑": "32",
    "嘘": "33", "晕": "34", "衰": "36", "敲打": "38", "再见": "39", "擦汗": "40",
    "抠鼻": "41", "鼓掌": "42", "坏笑": "44", "哈欠": "47", "鄙视": "48",
    "委屈": "49", "快哭了": "50", "亲亲": "52", "吓": "53", "可怜": "54",
    "月亮": "75", "太阳": "76", "礼物": "77", "拥抱": "78", "抱抱": "78",
    "强": "79", "加油": "79", "弱": "80", "握手": "81", "胜利": "82", "抱拳": "83",
    "勾引": "84", "拳头": "85", "爱你": "87", "爱心": "66", "玫瑰": "63",
    "心碎": "67", "爱情": "90", "飞吻": "91", "晚安": "75", "吃饭": "61",
    "咖啡": "60", "蛋糕": "68", "馋": "23",
}

# 句读切分用的结束标点
_SENT_END = "。！？!?；;"


def resolve_face(token: str) -> Optional[str]:
    """把表情名/数字解析为 QQ face id；无法识别返回 None（发送端按普通文本处理）。"""
    if token is None:
        return None
    t = str(token).strip()
    if not t:
        return None
    if t.isdigit():
        return t
    return FACE_NAME_TO_ID.get(t) or FACE_NAME_TO_ID.get(t.lower())


def _split_by_punct(text: str) -> List[str]:
    """按中文句读切分并保留标点；省略号（……/...）不在中间断开。"""
    protected = text.replace("……", "\x00").replace("...", "\x01")
    pieces = re.split(r"(?<=[" + _SENT_END + r"])", protected)
    out = []
    for p in pieces:
        p = p.replace("\x00", "……").replace("\x01", "...").strip()
        if p:
            out.append(p)
    return out


def tokenize_fragments(text: str) -> List[Dict[str, str]]:
    """把一条文本解析成有序的 text/face 片段。"""
    if not text:
        return []
    frags: List[Dict[str, str]] = []
    pos = 0
    for m in FACE_PATTERN.finditer(text):
        if m.start() > pos:
            t = text[pos:m.start()].strip()
            if t:
                frags.append({"type": "text", "text": t})
        fid = resolve_face(m.group(1))
        if fid:
            frags.append({"type": "face", "face": fid, "name": m.group(1).strip()})
        else:
            # 未知名的表情标记原样保留为文本，避免吞字
            frags.append({"type": "text", "text": m.group(0)})
        pos = m.end()
    tail = text[pos:].strip()
    if tail:
        frags.append({"type": "text", "text": tail})
    return frags


def split_bubbles(text: str,
                  max_bubbles: int = 6,
                  soft_max_len: int = 24,
                  auto_split: bool = True) -> List[Dict[str, str]]:
    """
    把一次完整回复拆成扁平的发送片段序列，元素为 {"type":"text","text":...}
    或 {"type":"face","face":id,"name":...}。

    规则：
    1. 含显式分隔符 ‖ 时严格按模型意图切分；
    2. 否则仅当整段超过 soft_max_len 且能在句读处切成不碎片的多段时才自动切，短回复单条；
    3. 最多 max_bubbles 个文字气泡，超出并入最后一条；
    4. 每个气泡内部再解析 [face:xx]，表情独立成片段。
    """
    if not text or not str(text).strip():
        return []
    raw = str(text)
    for alt in _ALT_SEPS:
        raw = raw.replace(alt, BUBBLE_SEP)
    raw = raw.strip()

    if BUBBLE_SEP in raw:
        chunks = [c.strip() for c in raw.split(BUBBLE_SEP) if c.strip()]
        explicit = True
    else:
        chunks = [raw]
        explicit = False

    if not chunks:
        return []

    # 无显式分条时的温和自动切分
    if not explicit and auto_split and len(chunks[0]) > soft_max_len:
        cand = _split_by_punct(chunks[0])
        if len(cand) >= 2 and all(len(c) >= 2 for c in cand):
            chunks = cand

    # 条数上限：超出部分并入最后一条
    if len(chunks) > max_bubbles:
        chunks = chunks[:max_bubbles - 1] + ["".join(chunks[max_bubbles - 1:])]

    # 注意：不同气泡、以及同一气泡内被表情隔开的文字都必须保持独立片段，
    # 因此这里不做相邻 text 合并（合并会把 ‖ 切开的气泡重新粘回去）。
    seq: List[Dict[str, str]] = []
    for chunk in chunks:
        seq.extend(tokenize_fragments(chunk))
    return seq


def typing_delay_seconds(text: str, rng=None) -> float:
    """按文字长度估算“打字中”的间隔秒数（表情不计字数）。"""
    n = len(text or "")
    base = 0.55 + min(n, 30) * 0.05
    jitter = (rng.random() if rng is not None else random.random()) * 0.5
    return round(min(2.3, base + jitter), 2)


def to_plain_text(text: str, sep: str = "\n") -> str:
    """
    供非 QQ 通道（网页 WebSocket / HTTP / TTS 语音）渲染：
    - 移除 [face:xx] 标记（这些通道无法渲染 QQ 表情）；
    - 把气泡分隔符 ‖ 换成 sep（网页用换行，TTS 用逗号停顿）。
    """
    if not text:
        return ""
    t = FACE_PATTERN.sub("", str(text))
    t = t.replace(BUBBLE_SEP, sep)
    for alt in _ALT_SEPS:
        t = t.replace(alt, sep)
    t = re.sub(r"\n{2,}", "\n", t)
    t = re.sub(r"[ \t]*\n[ \t]*", "\n", t)
    return t.strip()


# 去 AI 腔：句首/整句中的书面连接词与元话语（后处理清洗用）
AI_TONE_PATTERNS = [
    r"^首先[，,、\s]*", r"^其次[，,、\s]*", r"^最后[，,、\s]*",
    r"^总之[，,、\s]*", r"^综上所述[，,、：:\s]*", r"^总的来说[，,、\s]*",
    r"^严格来说[，,、\s]*", r"^事实上?[，,、\s]*", r"^其实吧[，,、\s]*",
    r"我理解你的(感受|心情|想法)[，,。\s]*",
    r"作为一?个?[^\n，。！？]{0,6}[，,]\s*",
]


def strip_ai_tone(text: str) -> str:
    """逐行去除 AI 腔开头词/元话语；保留 ‖ 与 [face:] 标记。"""
    if not text:
        return ""
    out_lines = []
    for line in str(text).split("\n"):
        original = line
        for pat in AI_TONE_PATTERNS:
            line = re.sub(pat, "", line)
        out_lines.append(line if line.strip() else original)
    return "\n".join(out_lines).strip()

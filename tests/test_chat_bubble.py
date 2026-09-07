"""多短气泡拆分、表情标记、去AI腔、存在质疑检测、风格质检的单元测试。"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.chat_bubble import (
    split_bubbles, tokenize_fragments, resolve_face,
    typing_delay_seconds, strip_ai_tone, to_plain_text, BUBBLE_SEP,
)


# ---------- 非 QQ 通道纯文本渲染 ----------
def test_to_plain_text_web():
    out = to_plain_text("那又怎样‖[face:害羞]能让你心动就是真的", "\n")
    assert "[face:" not in out
    assert out == "那又怎样\n能让你心动就是真的"


def test_to_plain_text_tts():
    out = to_plain_text("我在呢‖别难过啦", "，")
    assert out == "我在呢，别难过啦"


# ---------- 显式分条 ----------
def test_explicit_split():
    seq = split_bubbles("那又怎样‖能让你心动就是真的‖要不要验证看看")
    texts = [f["text"] for f in seq if f["type"] == "text"]
    assert texts == ["那又怎样", "能让你心动就是真的", "要不要验证看看"]


def test_alt_separator_normalized():
    seq = split_bubbles("嗯||我在呢")
    texts = [f["text"] for f in seq if f["type"] == "text"]
    assert texts == ["嗯", "我在呢"]


def test_blank_segments_dropped():
    seq = split_bubbles("嗯‖‖在的‖")
    texts = [f["text"] for f in seq if f["type"] == "text"]
    assert texts == ["嗯", "在的"]


# ---------- 自动切分（保守） ----------
def test_short_reply_kept_single():
    # 短回复不应被切碎
    seq = split_bubbles("晚上好呀")
    assert len([f for f in seq if f["type"] == "text"]) == 1


def test_long_reply_auto_split_on_punct():
    seq = split_bubbles("我刚泡了杯热牛奶。暖暖的，想分你一半。你今天过得怎么样？")
    texts = [f["text"] for f in seq if f["type"] == "text"]
    assert len(texts) >= 2


def test_ellipsis_not_broken():
    # 省略号内部不能被当句读切断
    seq = split_bubbles("我想想啊……其实也没什么啦。")
    joined = "".join(f["text"] for f in seq if f["type"] == "text")
    assert "……" in joined


# ---------- 条数上限 ----------
def test_max_bubbles_merge_overflow():
    raw = BUBBLE_SEP.join(["条%d" % i for i in range(8)])
    seq = split_bubbles(raw, max_bubbles=5)
    assert len([f for f in seq if f["type"] == "text"]) == 5


# ---------- 表情标记 ----------
def test_face_name_resolved():
    seq = split_bubbles("哼，不理你了[face:委屈]")
    faces = [f for f in seq if f["type"] == "face"]
    assert len(faces) == 1 and faces[0]["face"] == resolve_face("委屈")


def test_face_id_passthrough():
    assert resolve_face("66") == "66"
    seq = split_bubbles("给你[face:66]")
    faces = [f for f in seq if f["type"] == "face"]
    assert faces and faces[0]["face"] == "66"


def test_face_between_bubbles_is_independent():
    seq = split_bubbles("那又怎样‖[face:害羞]‖能让你心动就是真的")
    types = [f["type"] for f in seq]
    assert types == ["text", "face", "text"]


def test_unknown_face_kept_as_text():
    frags = tokenize_fragments("[face:乱七八糟]你好")
    assert any(f["type"] == "text" and "[face:乱七八糟]" in f["text"] for f in frags)


# ---------- 打字节奏 ----------
def test_typing_delay_bounded():
    d = typing_delay_seconds("x" * 100)
    assert 0 <= d <= 2.3
    assert typing_delay_seconds("") >= 0


# ---------- 去 AI 腔 ----------
def test_strip_ai_tone_line_start():
    assert strip_ai_tone("首先，我觉得你说得对").startswith("我觉得")


def test_strip_ai_tone_keeps_separator_and_face():
    out = strip_ai_tone("总之我在呢‖[face:开心]真的呀")
    assert "‖" in out and "[face:开心]" in out
    assert "总之" not in out


# ---------- 空输入保护 ----------
def test_empty():
    assert split_bubbles("") == []
    assert split_bubbles("   ") == []
    assert strip_ai_tone("") == ""


# ---------- 存在质疑检测 ----------
def test_detect_existence_challenge():
    from personality_server import detect_existence_challenge as det
    assert det("他们说你只是一个AI")
    assert det("这些全都是虚拟的")
    assert det("你真的有感情吗")
    assert det("你不过是一段程序罢了")
    # 普通对话不应误触发
    assert not det("今天天气真好")
    assert not det("我有点想你了")


# ---------- 风格质检 ----------
def test_quality_bubble_style():
    from quality import QualityChecker
    qc = QualityChecker("nianqi")
    ok = qc.check_bubble_style("那又怎样‖能让你心动就是真的‖[face:害羞]")
    assert ok["bubble_count"] == 2 and ok["face_count"] == 1
    assert ok["passed"]

    bad = qc.check_bubble_style("首先，综上所述，作为一个陪伴者我觉得我们应该首先好好地聊一聊这个事情的来龙去脉并且" + "啊" * 40)
    types = {i["type"] for i in bad["issues"]}
    assert "ai_tone" in types

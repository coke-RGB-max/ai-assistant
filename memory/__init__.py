"""
FlexiChrono memory 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- EventHistoryTracker
- MemorySystem
- MemoryAnalyzer
- UserProfileExtractor
- MemoryDecaySystem
- AssociativeMemory
"""
import logging
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
from collections import defaultdict
import asyncio, json, re, random, time, os, sqlite3, hashlib, datetime
import httpx
from fastapi import FastAPI, Request, HTTPException, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, ValidationError, field_validator

logger = logging.getLogger("memory")


# ============================================================
# EventHistoryTracker
# ============================================================
class EventHistoryTracker:
    PROFILES = {
        "positive": {"curve":[1.0,0.7,0.4,0.2,0.1,0.0,0.0,0.0,0.0], "decay_after":10, "decay_amount":3},
        "negative": {"curve":[1.0,0.95,0.9,0.85,0.8,0.75,0.7,0.65], "decay_after":25, "decay_amount":1},
        "trauma":   {"curve":[1.0,1.0,0.98,0.95,0.92,0.9,0.88,0.85], "decay_after":50, "decay_amount":1},
    }
    def __init__(self, history=None):
        self.history = {}
        if history:
            if isinstance(history, dict) and history and isinstance(list(history.values())[0], dict):
                self.history = history
            elif isinstance(history, dict):
                for k,v in history.items():
                    if isinstance(v, int):
                        self.history[k] = {"count": v, "last_turn": 0}
    def _eff(self, et, turn, p):
        if et not in self.history: return 0
        c = self.history[et].get("count", 0)
        gap = turn - self.history[et].get("last_turn", turn)
        return max(0, c - p["decay_amount"]) if gap > p["decay_after"] else c
    def calc(self, et, base, turn, cat="positive"):
        p = self.PROFILES.get(cat, self.PROFILES["positive"])
        n = self._eff(et, turn, p)
        m = 1.0 if n <= 0 else p["curve"][min(n-1, len(p["curve"])-1)]
        return int(base*m), m, n
    def record(self, et, turn):
        if et not in self.history:
            self.history[et] = {"count": 0, "last_turn": turn}
        self.history[et]["count"] += 1
        self.history[et]["last_turn"] = turn

# ============================================================
# RelationshipRepairSystem
# ============================================================

# ============================================================
# MemorySystem
# ============================================================
class MemorySystem:
    SC = ["喜欢","讨厌","爱","生日","名字","岁","住在","工作","学习","学校","专业","养","过敏"]
    EC = ["感动","温暖","伤心","开心","难过","幸福","孤独","被重视","心疼","后悔","珍惜"]
    PC = ["那天","上次","一起","第一次","昨天","记得","约定","答应"]
    def __init__(self, m=3): self.m = m
    def process(self, ctx, structured=None, llm_cands=None):
        if structured:
            r = {k: structured.get(k,[])[:self.m] for k in ("episodic","semantic","emotional")}
        else:
            ep, sem, emo = [], [], []
            if ctx and ctx.strip():
                for item in re.split(r"[。\n；;]", ctx):
                    item = item.strip()
                    if len(item) < 4: continue
                    e = {"content":item,"weight":50}
                    if sum(1 for w in self.EC if w in item): e["weight"]=90; emo.append(e)
                    elif sum(1 for w in self.SC if w in item): e["weight"]=70; sem.append(e)
                    elif sum(1 for w in self.PC if w in item): e["weight"]=60; ep.append(e)
                    else: e["weight"]=40; ep.append(e)
            for l in (ep,sem,emo): l.sort(key=lambda x:x["weight"], reverse=True)
            r = {"episodic":ep[:self.m],"semantic":sem[:self.m],"emotional":emo[:self.m]}
        if llm_cands:
            for c in llm_cands:
                if not c.get("remember"): continue
                t = c.get("type","episodic")
                if t in r:
                    r[t].insert(0, {"content":c.get("content",""),"weight":c.get("importance",70),
                                    "reason":c.get("reason","")})
                    r[t] = r[t][:self.m]
        # v11.0: MMR最大边际相关性重排 — 平衡相关性与多样性，避免5条全是同类记忆
        for t in ("episodic","semantic","emotional"):
            r[t] = self._mmr_rerank(r[t], top_k=self.m, lambda_param=0.5)
        return r
    @staticmethod
    def _text_similarity(a: str, b: str) -> float:
        """基于字符级Jaccard相似度。"""
        if not a or not b: return 0.0
        sa, sb = set(a), set(b)
        if not sa or not sb: return 0.0
        return len(sa & sb) / len(sa | sb)
    def _mmr_rerank(self, items: List[Dict], top_k: int = 5, lambda_param: float = 0.5) -> List[Dict]:
        """
        MMR (Maximal Marginal Relevance) 最大边际相关性重排。
        score = λ * 相关性 - (1-λ) * 与已选最大相似度
        避免 top-5 全是同一件事的不同版本（比如5条全是吵架）。
        """
        if not items: return items
        if len(items) <= top_k: return items
        selected = []
        remaining = items.copy()
        while len(selected) < top_k and remaining:
            best_idx = 0
            best_score = -1e9
            for i, cand in enumerate(remaining):
                relevance = cand.get("weight", 50) / 100.0
                max_cross_sim = 0.0
                for s in selected:
                    sim = self._text_similarity(cand.get("content",""), s.get("content",""))
                    if sim > max_cross_sim:
                        max_cross_sim = sim
                score = lambda_param * relevance - (1 - lambda_param) * max_cross_sim
                if score > best_score:
                    best_score = score
                    best_idx = i
            selected.append(remaining.pop(best_idx))
        return selected
    def build(self, mems):
        L = ["【过去经历】（自然融入，不要逐条罗列，不要说'根据记忆'）"]
        if mems["emotional"]:
            L.append("  ▸ 情感记忆（最深、最影响你对他的感觉）：")
            for m in mems["emotional"]:
                L.append(f"    - {m['content']}" + (f"（{m['reason']}）" if m.get("reason") else ""))
        if mems["semantic"]:
            L.append("  ▸ 你知道关于他的事：")
            for m in mems["semantic"]: L.append(f"    - {m['content']}")
        if mems["episodic"]:
            L.append("  ▸ 你们一起经历过：")
            for m in mems["episodic"]: L.append(f"    - {m['content']}")
        if not any(mems.values()): L.append("  （暂无共同记忆）")
        return "\n".join(L)


# ============================================================
# MemoryAnalyzer
# ============================================================
class MemoryAnalyzer:
    async def analyze(self, rname, pers, umsg, areply, history):
        recent = ""
        for h in history[-4:]:
            r = "用户" if h.get("role") == "user" else rname
            recent += f"{r}：{h.get('content','')[:60]}\n"
        prompt = (
            f"判断对话是否包含值得角色「{rname}」（{pers}）长期记住的内容。\n\n"
            f"近期：\n{recent}\n用户：{umsg}\n{rname}：{areply[:200]}\n\n"
            f"episodic=共同经历/约定；semantic=用户事实(喜好/生日)；emotional=情感冲击时刻(即使无情感词)。"
            f"importance>=60才值得记住。\n\n"
            f'返回JSON：不值得{{"remember":false}}；值得{{"remember":true,"type":"episodic/semantic/emotional","content":"...","importance":0-100,"reason":"..."}}'
        )
        content = await smart_llm_call([{"role":"user","content":prompt}], temperature=0,
                                       max_tokens=200, json_mode=True, timeout=15)
        if not content: return None
        r = safe_json_parse(content)
        if r and r.get("remember") and r.get("content"): return r
        return None

# ============================================================
# v11.0: 用户画像提取器（从对话中提取用户稳定事实，更新画像）
# ============================================================

# ============================================================
# UserProfileExtractor
# ============================================================
class UserProfileExtractor:
    """
    每N轮对话调用一次LLM，从近期对话中提取用户的稳定事实。
    提取结果与现有画像做去重和冲突检测，不直接覆盖矛盾信息。
    """
    EXTRACT_PROMPT = """从以下对话中提取关于用户的稳定事实。
只提取确定的信息（喜好、厌恶、性格、重要事件、基本信息），不确定的不要提取。
如果没有新信息，输出空JSON。
输出格式：{"likes":[],"dislikes":[],"traits":[],"events":[],"basic_info":{}}"""
    def __init__(self, existing_profile=None):
        self.profile = existing_profile or {"likes":[],"dislikes":[],"traits":[],"events":[],"basic_info":{}}
    async def extract(self, history: List[Dict], role_name: str = "") -> Optional[Dict]:
        """从历史对话中提取用户画像更新。"""
        if not history:
            return None
        recent = ""
        for h in history[-8:]:
            r = "用户" if h.get("role") == "user" else role_name
            recent += f"{r}：{h.get('content','')[:80]}\n"
        prompt = self.EXTRACT_PROMPT + f"\n\n【近期对话】\n{recent}"
        content = await smart_llm_call(
            [{"role":"user","content":prompt}],
            temperature=0, max_tokens=300, json_mode=True, timeout=20)
        if not content:
            return None
        extracted = safe_json_parse(content)
        if not extracted:
            return None
        merged, updates = self._merge(extracted)
        if merged is not None:
            self.profile = merged  # 原子替换，避免并发交叉修改
        return updates
    def _merge(self, extracted: Dict) -> Tuple[Optional[Dict], Optional[Dict]]:
        """合并新提取的信息到现有画像副本，处理冲突。
        返回 (merged_profile, updates)；无更新时返回 (None, None)。
        不修改 self.profile，由调用方决定是否原子替换，避免并发交叉修改。"""
        import copy
        merged = copy.deepcopy(self.profile)
        updates = {}
        for key in ("likes","dislikes","traits","events"):
            new_items = extracted.get(key, [])
            if not new_items: continue
            existing = set(merged.get(key, []))
            added = []
            for item in new_items:
                if item and item not in existing:
                    # 简单冲突检测：likes和dislikes不能同时包含相同项
                    if key == "likes" and item in merged.get("dislikes",[]):
                        continue  # 矛盾信息跳过，标记为待确认
                    if key == "dislikes" and item in merged.get("likes",[]):
                        continue
                    merged.setdefault(key, []).append(item)
                    added.append(item)
            if added:
                updates[key] = added
        # basic_info 合并
        new_basic = extracted.get("basic_info", {})
        if new_basic:
            for k, v in new_basic.items():
                old_v = merged.get("basic_info", {}).get(k)
                if old_v and old_v != v:
                    continue  # 矛盾信息不覆盖
                if not old_v:
                    merged.setdefault("basic_info", {})[k] = v
                    updates.setdefault("basic_info", {})[k] = v
        if not updates:
            return None, None
        return merged, updates
    def build_context(self) -> str:
        """生成用户画像的prompt上下文。"""
        p = self.profile
        lines = ["【关于用户】（你了解的关于他/她的信息，自然融入对话，不要逐条说出来）"]
        if p.get("basic_info"):
            info_str = "，".join(f"{k}:{v}" for k,v in p["basic_info"].items())
            lines.append(f"  - 基本信息：{info_str}")
        if p.get("likes"):
            lines.append(f"  - 喜欢：{'、'.join(p['likes'][-5:])}")
        if p.get("dislikes"):
            lines.append(f"  - 不喜欢：{'、'.join(p['dislikes'][-5:])}")
        if p.get("traits"):
            lines.append(f"  - 性格：{'、'.join(p['traits'][-3:])}")
        if p.get("events"):
            lines.append(f"  - 最近重要的事：{'、'.join(p['events'][-3:])}")
        if len(lines) == 1:
            return ""
        return "\n".join(lines)
    def to_dict(self):
        return self.profile

# ============================================================
# BehaviorPreference
# ============================================================

# ============================================================
# MemoryDecaySystem
# ============================================================
class MemoryDecaySystem:
    """
    情感记忆权重动态衰减：
    - 情感冲击越强，记忆越深
    - 反复被提及的记忆权重上升
    - 被"纠正"的记忆更新而非覆盖
    - 遗忘曲线：很久没被提起的记忆权重自然衰减
    """
    DECAY_RATE = 0.02  # 每轮衰减2%
    RECALL_BOOST = 5   # 被提及一次+5权重
    MAX_WEIGHT = 100
    MIN_WEIGHT = 0

    def __init__(self, memories=None):
        self.memories = memories or []  # [{content, weight, type, last_recalled, recall_count, created_turn}]

    def decay_all(self, current_turn: int):
        """对所有记忆应用遗忘曲线。"""
        for m in self.memories:
            gap = current_turn - m.get("last_recalled", current_turn)
            decay = self.DECAY_RATE * gap
            m["weight"] = max(self.MIN_WEIGHT, m["weight"] - decay)
        # 移除权重过低的记忆
        self.memories = [m for m in self.memories if m["weight"] > 5]

    def recall(self, content_keyword: str, current_turn: int):
        """记忆被提及，权重上升。"""
        for m in self.memories:
            if content_keyword in m.get("content", ""):
                m["weight"] = min(self.MAX_WEIGHT, m["weight"] + self.RECALL_BOOST)
                m["last_recalled"] = current_turn
                m["recall_count"] = m.get("recall_count", 0) + 1

    def add_memory(self, content: str, weight: int, mem_type: str, current_turn: int):
        """添加新记忆。"""
        # 检查是否是对已有记忆的纠正/更新
        for m in self.memories:
            if self._is_correction(m["content"], content):
                m["content"] = content  # 更新而非覆盖
                m["weight"] = max(m["weight"], weight)
                m["last_recalled"] = current_turn
                m["recall_count"] = m.get("recall_count", 0) + 1
                return
        self.memories.append({
            "content": content,
            "weight": weight,
            "type": mem_type,
            "last_recalled": current_turn,
            "recall_count": 1,
            "created_turn": current_turn
        })
        # 按权重排序，只保留 top 20
        self.memories.sort(key=lambda x: x["weight"], reverse=True)
        if len(self.memories) > 20:
            self.memories = self.memories[:20]

    def _is_correction(self, old: str, new: str) -> bool:
        """
        判断是否是对同一事实的纠正（如生日日期更正）。
        条件：两条记忆都包含数字且数字不同，且共享至少2个关键词（词语级，非字符级）。
        """
        old_nums = set(re.findall(r'\d+', old))
        new_nums = set(re.findall(r'\d+', new))
        if not (old_nums and new_nums and old_nums != new_nums):
            return False
        # 词语级分词：按标点、空格、常见停用词分割
        def tokenize(text: str) -> set:
            # 移除标点和数字
            cleaned = re.sub(r'[，。！？、；：""''（）\s\d]+', ' ', text).strip()
            if JIEBA_AVAILABLE:
                # 使用 jieba 精确分词，只保留长度>=2的词，避免2-gram噪音匹配
                words = set()
                for w in cleaned.split():
                    for seg in jieba.lcut(w):
                        if len(seg) >= 2:
                            words.add(seg)
                return words
            # 降级方案：2-gram 滑动窗口（jieba 未安装时使用）
            words = set()
            for w in cleaned.split():
                if len(w) >= 2:
                    words.add(w)
                for i in range(len(w)-1):
                    words.add(w[i:i+2])
            return words
        old_words = tokenize(old)
        new_words = tokenize(new)
        common = old_words & new_words
        # 共享至少2个关键词才视为同一事实的纠正
        return len(common) >= 2

    def get_top_memories(self, n=5) -> List[Dict]:
        """获取权重最高的n条记忆。"""
        return sorted(self.memories, key=lambda x: x["weight"], reverse=True)[:n]

# ============================================================
# v10.0: 关联记忆系统
# ============================================================

# ============================================================
# AssociativeMemory
# ============================================================
class AssociativeMemory:
    """
    关联记忆：用户说"今天吃了火锅" → 角色回忆起"上次你说喜欢吃辣"
    需要给记忆打标签，建立关联图谱。
    """
    TAG_CATEGORIES = {
        "food": ["吃","火锅","奶茶","饭","零食","好吃","美食","咖啡","蛋糕","辣","甜","酸"],
        "weather": ["下雨","晴天","热","冷","雪","风","阴天"],
        "activity": ["看电影","逛街","打游戏","学习","工作","运动","旅行","读书","听音乐"],
        "emotion": ["开心","难过","生气","感动","孤独","幸福","害怕","紧张"],
        "person": ["朋友","家人","同学","同事","前任","新认识的人"],
        "time": ["昨天","今天","明天","上周","下周","上次","下次"],
        "place": ["学校","家","公司","商场","公园","餐厅","电影院"],
    }

    def __init__(self):
        self.tag_map: Dict[str, List[Dict]] = {}  # tag -> [{content, weight, type}]

    def tag_content(self, content: str) -> List[str]:
        """给内容打标签。"""
        tags = []
        for category, keywords in self.TAG_CATEGORIES.items():
            for kw in keywords:
                if kw in content:
                    tags.append(f"{category}:{kw}")
                    break  # 每个类别只打一个标签
        return tags

    def add_memory(self, content: str, weight: int = 50, mem_type: str = "episodic"):
        """添加带标签的记忆。"""
        tags = self.tag_content(content)
        entry = {"content": content, "weight": weight, "type": mem_type, "tags": tags}
        for tag in tags:
            if tag not in self.tag_map:
                self.tag_map[tag] = []
            self.tag_map[tag].append(entry)
            # 每个标签只保留 top 5
            self.tag_map[tag].sort(key=lambda x: x["weight"], reverse=True)
            if len(self.tag_map[tag]) > 5:
                self.tag_map[tag] = self.tag_map[tag][:5]

    def recall_associated(self, user_message: str, top_n=3) -> List[Dict]:
        """根据用户消息，检索关联记忆。"""
        msg_tags = self.tag_content(user_message)
        if not msg_tags:
            return []
        candidates = []
        seen = set()
        for tag in msg_tags:
            if tag in self.tag_map:
                for entry in self.tag_map[tag]:
                    if entry["content"] not in seen:
                        # 关联度 = 共同标签数 * 记忆权重
                        common_tags = len(set(entry["tags"]) & set(msg_tags))
                        relevance = common_tags * entry["weight"]
                        candidates.append({**entry, "relevance": relevance, "matched_tag": tag})
                        seen.add(entry["content"])
        candidates.sort(key=lambda x: x["relevance"], reverse=True)
        return candidates[:top_n]

    def build(self, associated: List[Dict]) -> str:
        if not associated:
            return ""
        lines = ["【关联记忆】（对方的话让你想起了这些，自然融入，不要逐条罗列）"]
        for m in associated:
            lines.append(f"  - {m['content']}")
        return "\n".join(lines)

# ============================================================
# v10.0: 关系里程碑追踪器
# ============================================================

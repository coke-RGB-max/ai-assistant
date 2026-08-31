"""
FlexiChrono topic 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- IntentManager
- TopicInitiator
- CallbackEngine
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

logger = logging.getLogger("topic")


# ============================================================
# IntentManager
# ============================================================
class IntentManager:
    """
    意图管理：延迟回复、被打断草稿、提醒、主动联系动机。
    融合HDSI的NarrativeIntent设计，适配你的session模型。
    """
    def __init__(self, session_id, role_id):
        self.session_id = session_id
        self.role_id = role_id

    def _conn(self):
        return _get_db()

    def add(self, intent_type, summary, not_before, payload=None):
        now = time.time()
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO session_intents(session_id,role_id,type,summary,not_before,status,payload,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (self.session_id, self.role_id, intent_type, summary, not_before,
             "pending", json.dumps(payload or {}, ensure_ascii=False), now, now))
        conn.commit(); intent_id = cur.lastrowid; conn.close()
        return intent_id

    def get_due(self, now=None):
        now = now or time.time()
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM session_intents WHERE session_id=? AND role_id=? AND status='pending' AND not_before<=? "
            "ORDER BY not_before ASC",
            (self.session_id, self.role_id, now)).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            try: d["payload"] = json.loads(d.get("payload") or "{}")
            except: d["payload"] = {}
            result.append(d)
        return result

    def update_status(self, intent_id, status):
        now = time.time()
        conn = self._conn()
        conn.execute("UPDATE session_intents SET status=?, updated_at=? WHERE id=?",
                     (status, now, intent_id))
        conn.commit(); conn.close()

    def get_interrupted_drafts(self):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM session_intents WHERE session_id=? AND role_id=? AND type='interrupted_draft' AND status='pending' "
            "ORDER BY created_at DESC LIMIT 3",
            (self.session_id, self.role_id)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def build_prompt_text(self):
        drafts = self.get_interrupted_drafts()
        if not drafts: return ""
        lines = ["【未完成的念头】（这些是你刚才本来想说但被打断的话，只存在于你心里，不要直接发给对方）"]
        for d in drafts[-2:]:
            lines.append(f"  - {d['summary']}")
        return "\n".join(lines)

# ============================================================
# PromptBuilder
# ============================================================

# ============================================================
# TopicInitiator
# ============================================================
class TopicInitiator:
    """
    根据亲密度和当前情绪，有一定概率角色主动抛出话题而不是回答问题。
    """
    BASE_PROB = 0.15  # 基础15%概率主动引导
    HIGH_INTIMACY_BONUS = 0.10  # 高亲密时+10%
    LOW_EMOTION_BONUS = 0.08  # 低情绪强度时+8%（日常闲聊更可能主动）

    def __init__(self, role_id):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.topic_pool = self.role.get("topic_pool", [])
        self._last_topic = None

    def should_initiate(self, intimacy: int, emotion_intensity: int, conflict_sev: int) -> bool:
        """判断是否应该主动引导话题。"""
        if conflict_sev >= 2:
            return False  # 冲突中不主动引导
        if emotion_intensity > 60:
            return False  # 强情绪时专注回应
        prob = self.BASE_PROB
        if intimacy > 70:
            prob += self.HIGH_INTIMACY_BONUS
        if emotion_intensity < 30:
            prob += self.LOW_EMOTION_BONUS
        return random.random() < prob

    def pick_topic(self, intimacy: int) -> Optional[Dict]:
        """选择一个话题。"""
        if not self.topic_pool:
            return None
        available = [t for t in self.topic_pool if t != self._last_topic]
        if not available:
            available = self.topic_pool
        topic = random.choice(available)
        self._last_topic = topic
        return {"topic": topic, "intimacy": intimacy}

    def build(self, topic_info: Dict) -> str:
        if not topic_info:
            return ""
        return f"【话题引导】你可以主动提起：{topic_info['topic']}。自然地融入对话，不要太突兀，也可以选择不引导。"

# ============================================================
# v10.0: Call Back 引擎（记住3-5轮前说过的话，在合适时机回马枪）
# ============================================================

# ============================================================
# CallbackEngine
# ============================================================
class CallbackEngine:
    """
    角色应该记住 3-5 轮前说过的话，在合适的时机 Call Back。
    用户5轮前说"明天考试"，5轮后角色问"考试怎么样"。
    """
    CALLBACK_WINDOW = (3, 8)  # 回溯3-8轮
    CALLBACK_PROB = 0.12  # 12%概率触发
    KEYWORDS = ["明天","下次","以后","待会","等一下","约定","答应","考试","面试","约会","见面","回来","去","来"]

    def __init__(self):
        self.pending_callbacks = []  # [{turn, content, keyword, resolved}]

    def scan_for_pending(self, history: List[Dict], current_turn: int):
        """扫描历史消息，找出可能需要 Call Back 的内容。"""
        for h in history[-10:]:
            if h.get("role") != "user":
                continue
            content = h.get("content", "")
            for kw in self.KEYWORDS:
                if kw in content:
                    # 检查是否已经记录过
                    if not any(pc["content"] == content for pc in self.pending_callbacks):
                        self.pending_callbacks.append({
                            "turn": current_turn,
                            "content": content,
                            "keyword": kw,
                            "resolved": False
                        })
                    break
        # 只保留最近15条
        if len(self.pending_callbacks) > 15:
            self.pending_callbacks = self.pending_callbacks[-15:]

    def find_callback(self, current_turn: int, emotion_intensity: int) -> Optional[Dict]:
        """找到一个合适的 Call Back 机会。"""
        if emotion_intensity > 50:
            return None  # 强情绪时不Call Back
        if random.random() > self.CALLBACK_PROB:
            return None
        candidates = []
        for pc in self.pending_callbacks:
            if pc["resolved"]:
                continue
            gap = current_turn - pc["turn"]
            if self.CALLBACK_WINDOW[0] <= gap <= self.CALLBACK_WINDOW[1]:
                candidates.append(pc)
        if not candidates:
            return None
        chosen = random.choice(candidates)
        chosen["resolved"] = True
        return chosen

    def build(self, callback: Dict) -> str:
        if not callback:
            return ""
        content = callback["content"][:50]
        return f"【回马枪】你想起对方之前说过：「{content}」。你可以自然地问一句后续，比如'那件事怎么样了'，让对方觉得你真的在听在记。"

# ============================================================
# v10.0: 记忆权重动态衰减系统
# ============================================================

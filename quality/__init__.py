"""
FlexiChrono quality 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- QualityChecker
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

# ---- P4 修复：显式导入共享底座，不再依赖 personality_server 全局命名空间 ----
from core.config import *
from core.utils import *
from core.llm import smart_llm_call, kimi_search_call
from core.roles import (ROLES_DEFINITION, EVENT_CATEGORY, RELATIONSHIP_MILESTONES,
                        VIRTUAL_GIFTS, get_cached_persona, get_role_definition)

logger = logging.getLogger("quality")


# ============================================================
# QualityChecker
# ============================================================
class QualityChecker:
    """
    轻量在线检测角色回复是否符合人设：
    1. 禁用词检测（角色不能说的话）
    2. AI身份暴露检测
    3. 长度异常检测
    4. 情绪一致性检测（简单规则）
    """
    # 全局禁用模式（所有角色都不能说）
    GLOBAL_FORBIDDEN_PATTERNS = [
        r"作为(一个)?AI", r"作为人工智能", r"我是(一个)?AI", r"我是人工智能",
        r"我是语言模型", r"我是大语言模型", r"作为语言模型",
        r"根据我的(训练|算法|模型)", r"我(没有|无法)(情感|意识|感觉)",
        r"抱歉，我(不能|无法|不会)", r"作为(虚拟|数字)助手",
    ]
    def __init__(self, role_id: str):
        self.role_id = role_id
        self.role = ROLES_DEFINITION.get(role_id, {})
    def check(self, reply: str, expected_emotion: Optional[str] = None,
              expected_length: str = "medium") -> Dict:
        """检测回复质量，返回问题列表。"""
        issues = []
        if not reply:
            issues.append({"level":"error","type":"empty","msg":"回复为空"})
            return {"passed": False, "issues": issues, "score": 0}
        # 1. AI身份暴露检测
        for pattern in self.GLOBAL_FORBIDDEN_PATTERNS:
            if re.search(pattern, reply, re.IGNORECASE):
                issues.append({"level":"error","type":"ai_identity","msg":f"检测到AI身份暴露: {pattern}"})
                break
        # 2. 角色禁忌词检测
        taboos = self.role.get("taboos", [])
        for taboo in taboos:
            if taboo in reply and expected_emotion not in ("angry", "jealous"):
                # 生气时可能提到禁忌词（如"你才可爱"），所以只在非负面情绪时报警
                issues.append({"level":"warning","type":"taboo","msg":f"回复包含角色禁忌词: {taboo}"})
        # 3. 长度异常检测
        length_map = {"short": (0, 30), "medium": (20, 80), "medium-long": (40, 120)}
        lo, hi = length_map.get(expected_length, (10, 100))
        reply_len = len(reply)
        if reply_len > hi * 2:
            issues.append({"level":"warning","type":"too_long","msg":f"回复过长({reply_len}字)，预期{expected_length}({lo}-{hi}字)"})
        elif reply_len < lo // 2 and expected_length != "short":
            issues.append({"level":"info","type":"too_short","msg":f"回复偏短({reply_len}字)，预期{expected_length}"})
        # 4. 动作描写/舞台提示检测
        if re.search(r"[（(](动作|表情|心理|旁白|内心|OS|os)[：:]", reply):
            issues.append({"level":"warning","type":"stage_direction","msg":"回复包含动作描写/舞台提示"})
        # 计算分数
        score = 100
        for issue in issues:
            if issue["level"] == "error": score -= 40
            elif issue["level"] == "warning": score -= 15
            elif issue["level"] == "info": score -= 5
        score = max(0, score)
        return {"passed": score >= 60, "score": score, "issues": issues}

    # AI 腔/书面腔连接词（出现即提示）
    AI_TONE_WORDS = ["首先", "其次", "综上所述", "总的来说", "总之", "严格来说",
                     "作为一个", "作为一名", "我理解你的感受", "我明白你的感受",
                     "值得注意的是", "从某种意义上", "换句话说"]
    # 单个文字气泡建议上限（字）
    BUBBLE_HARD_LEN = 30
    # 一次回复建议的气泡上限（与 core.chat_bubble.split_bubbles 默认值对齐）
    BUBBLE_MAX_COUNT = 6

    def check_bubble_style(self, reply: str) -> Dict:
        """
        多气泡风格质检（诊断用，不阻断发送）：
        - AI 腔连接词；单气泡过长；气泡数过多；非法 [face:?] 标记。
        """
        issues = []
        if not reply:
            return {"passed": False, "issues": [{"level": "error", "type": "empty", "msg": "回复为空"}]}
        try:
            from core.chat_bubble import split_bubbles, resolve_face, FACE_PATTERN
        except Exception:
            return {"passed": True, "issues": []}  # 依赖缺失时不报错

        seq = split_bubbles(reply)
        text_bubbles = [f["text"] for f in seq if f.get("type") == "text"]
        face_bubbles = [f for f in seq if f.get("type") == "face"]

        # 1. AI 腔
        hit = [w for w in self.AI_TONE_WORDS if w in reply]
        if hit:
            issues.append({"level": "warning", "type": "ai_tone",
                           "msg": f"含AI腔/书面腔词汇: {'、'.join(hit)}"})
        # 2. 单气泡过长
        for b in text_bubbles:
            if len(b) > self.BUBBLE_HARD_LEN:
                issues.append({"level": "info", "type": "bubble_too_long",
                               "msg": f"单条气泡{len(b)}字偏长（建议≤{self.BUBBLE_HARD_LEN}）: {b[:12]}…"})
        # 3. 气泡数过多
        n_text = len(text_bubbles)
        if n_text > self.BUBBLE_MAX_COUNT:
            issues.append({"level": "info", "type": "too_many_bubbles",
                           "msg": f"气泡数{n_text}超过建议上限{self.BUBBLE_MAX_COUNT}"})
        # 4. 表情标记合法性（写了 [face:xx] 但映射不到 id，且不是数字）
        for raw in FACE_PATTERN.findall(reply):
            if not str(raw).strip().isdigit() and resolve_face(raw) is None:
                issues.append({"level": "info", "type": "unknown_face",
                               "msg": f"未识别的表情标记[face:{raw}]，将按原文发送"})
        # 5. 表情过多
        if len(face_bubbles) > 1:
            issues.append({"level": "info", "type": "too_many_faces",
                           "msg": f"一次回复出现{len(face_bubbles)}个表情，建议≤1"})

        score = 100
        for issue in issues:
            if issue["level"] == "error": score -= 40
            elif issue["level"] == "warning": score -= 15
            else: score -= 5
        return {"passed": score >= 60, "score": max(0, score), "issues": issues,
                "bubble_count": n_text, "face_count": len(face_bubbles)}

    @staticmethod
    def fallback_reply(role_id: str, emotion: str = "calm") -> str:
        """OOC重试失败后的规则模板降级回复，确保不崩人设、不暴露AI身份。"""
        templates = {
            "jingwen": {
                "happy": "哼，今天心情还算不错吧。",
                "shy": "……你、你说什么呢，笨蛋。",
                "angry": "……哼，不想说了。",
                "sad": "……没什么，别问了。",
                "jealous": "哦，是吗，随便你。",
                "worried": "……你没事吧？",
                "default": "……哼，算了。"
            },
            "qinghe": {
                "happy": "呵呵，和你聊天很开心呢。",
                "shy": "哎呀，你这样说我会不好意思的。",
                "angry": "……好了，别生气了。",
                "sad": "……我在呢，想说就说吧。",
                "default": "嗯，我在呢，慢慢说。"
            },
            "nianqi": {
                "happy": "嘿嘿，好开心～",
                "shy": "（脸红）你、你说什么呢…",
                "angry": "我现在有点生气，等会儿和你说好吗？",
                "sad": "…我有点难过，抱抱我好不好？",
                "default": "我在呢，怎么啦？"
            }
        }
        role_tpl = templates.get(role_id, {"default": "……嗯，我在。"})
        return role_tpl.get(emotion, role_tpl["default"])

# ============================================================
# HDSI-PORT: AlterSystem 情绪偏移追踪系统（移植自HDSI alter.ts）
# ============================================================

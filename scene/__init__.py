"""
FlexiChrono scene 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- SceneModeEngine
- VirtualGiftSystem
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

logger = logging.getLogger("scene")


# ============================================================
# SceneModeEngine
# ============================================================
class SceneModeEngine:
    """
    特殊场景模式：
    - 约会模式：语气更紧张/期待
    - 吵架模式：冲突系统升级，角色可能冷战
    - 深夜模式：角色更走心、更感性，更容易说真心话
    - 节日模式：生日、情人节、跨年，角色有特殊对话
    """
    SCENE_CONFIGS = {
        "date": {
            "mood_bias": +10,
            "style": "紧张/期待/心跳加速",
            "prompt_hint": "你们正在约会。你有点紧张，又很期待，会不自觉地注意自己的形象和言行，偶尔会脸红。",
            "length_bias": "medium",
        },
        "argument": {
            "mood_bias": -15,
            "style": "冲突升级/可能冷战",
            "prompt_hint": "你们正在吵架。冲突系统升级，你可能会说出更冲的话，也可能选择冷战不说话。",
            "length_bias": "short",
        },
        "late_night": {
            "mood_bias": +5,
            "style": "走心/感性/容易说真心话",
            "prompt_hint": "现在是深夜。你比平时更走心、更感性，防备心降低，更容易说出真心话，语气会更柔软。",
            "length_bias": "medium-long",
        },
        "festival": {
            "mood_bias": +8,
            "style": "节日氛围/特殊对话",
            "prompt_hint": "今天是个特别的节日。你会有符合节日氛围的特殊对话，可能会送上祝福或礼物。",
            "length_bias": "medium",
        },
        "birthday": {
            "mood_bias": +12,
            "style": "生日/庆祝",
            "prompt_hint": "今天是生日（你的或对方的）。你会有特别的生日对话，可能会唱生日歌、送祝福、或期待礼物。",
            "length_bias": "medium",
        },
        "valentine": {
            "mood_bias": +10,
            "style": "情人节/暧昧",
            "prompt_hint": "今天是情人节。空气中有暧昧的氛围，你可能会比平时更主动，也可能会害羞。",
            "length_bias": "medium",
        },
        "new_year": {
            "mood_bias": +8,
            "style": "跨年/新年祝福",
            "prompt_hint": "今天是新年/跨年。你会有新年祝福，可能会许下新年愿望，或回顾过去一年。",
            "length_bias": "medium",
        },
    }

    def __init__(self):
        self.current_scene = "normal"

    def set_scene(self, scene: str):
        if scene in self.SCENE_CONFIGS or scene == "normal":
            self.current_scene = scene

    def get_scene_config(self) -> Optional[Dict]:
        if self.current_scene == "normal":
            return None
        return self.SCENE_CONFIGS.get(self.current_scene)

    def build(self) -> str:
        cfg = self.get_scene_config()
        if not cfg:
            return ""
        return f"【场景模式】{cfg['prompt_hint']}"

    def get_mood_bias(self) -> int:
        cfg = self.get_scene_config()
        return cfg["mood_bias"] if cfg else 0

# ============================================================
# v10.0: 虚拟礼物系统
# ============================================================

# ============================================================
# VirtualGiftSystem
# ============================================================
class VirtualGiftSystem:
    """
    前端可以虚拟"送礼物"——角色收到虚拟礼物后有独特反应。
    送花 → 璟雯："谁、谁要你送啊…不过放这吧"
    送食物 → 清禾："谢谢你，我会好好吃完的～"
    """
    def __init__(self, role_id):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.role_id = role_id

    def get_reaction(self, gift_id: str) -> Optional[Dict]:
        """获取角色对礼物的反应。"""
        gift = VIRTUAL_GIFTS.get(gift_id)
        if not gift:
            return None
        reaction = gift.get(self.role_id, gift.get("jingwen", "谢谢你的礼物。"))
        return {
            "gift_id": gift_id,
            "gift_name": gift["name"],
            "reaction": reaction,
            "mood_bias": +5,
            "intimacy_delta": +2,
        }

    def build(self, gift_reaction: Dict) -> str:
        if not gift_reaction:
            return ""
        return (f"【收到礼物】对方送了你一个{gift_reaction['gift_name']}！"
                f"你的第一反应是：{gift_reaction['reaction']}。"
                f"这个礼物让你心情变好了，对对方的好感也增加了一点。")

# ============================================================
# v10.0: 知识路由判断模型（豆包判断"这件事我知道吗？"）
# ============================================================

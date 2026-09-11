"""
FlexiChrono knowledge 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- KnowledgeRouter
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

logger = logging.getLogger("knowledge")


# ============================================================
# KnowledgeRouter
# ============================================================
class KnowledgeRouter:
    """
    知识路由架构：
    用户消息 → 判断模型（豆包）分析"这件事我知道吗？"
      - 知道 → B线：直接用人格模型回复
      - 不知道 → A线：Kimi联网搜索（失败兜底豆包搜索）→ 整理搜索结果 → 人格模型回复
    v15.0 增强：
      - 天气/气温等"地点强相关"问题，搜索词自动拼上用户城市（如「武汉 今天天气」）
      - 地点强相关但还不知道用户城市时，不瞎搜，先让角色自然反问城市
      - 时效性内容（天气/新闻/价格/比赛等）即使是闲聊语气也判为需要联网
    """
    # 不需要联网的关键词（角色日常对话/情感交流）
    OFFLINE_KEYWORDS = ["我","你","喜欢","爱","想","难过","开心","生气","吃醋","晚安","早安",
        "在吗","干嘛","吃饭","睡觉","累","烦","无聊","陪","聊","约会","吵架","分手","复合",
        "生日","礼物","拥抱","牵手","亲吻","想念","孤独","寂寞","害怕","担心","安慰",
        "哼","笨蛋","白痴","可爱","帅","漂亮","好看","丑","胖","瘦","高","矮",
        "我们","咱们","一起","永远","承诺","约定","未来","以后","下次","昨天","今天",
        "璟雯","清禾","念琦","角色","人设","扮演","AI","机器人","程序","大模型",
    ]

    # 需要联网的关键词前缀（事实性问题）
    ONLINE_HINTS = ["什么是","是谁","在哪","什么时候","为什么","怎么","如何","多少","几",
        "最新","最近","新闻","价格","多少钱","配置","参数","发布","上市","版本",
        "天气","气温","下雨","下雪","台风","地震","比赛","比分","冠军","选举",
        "股票","股价","行情","基金","汇率","利率","政策","法律","规定","标准",
        # v15.0：天气/时效衍生说法（只放较无歧义的，单字"冷/热/晴/阴"不放，避免误伤性格形容）
        "降温","升温","寒潮","暴雨","雷阵雨","空气质量","雾霾","pm2.5","带伞",
        "冷不冷","热不热","多少度","几度","穿衣指数","紫外线",
    ]

    # v15.0：地点强相关词——这类问题搜索时必须带城市，否则结果毫无意义
    # 注意：本集合只在"已经判定需要联网"之后用于决定要不要拼城市，范围可以放宽
    WEATHER_KEYWORDS = ["天气","气温","温度","多少度","几度","下雨","下雪","降雨","降雪",
        "雷阵雨","暴雨","大雨","小雨","雨","雪","台风","寒潮","降温","升温","冷不冷","热不热",
        "带伞","穿什么","穿衣","空气质量","雾霾","pm2.5","紫外线","风大","刮风"]

    # 无城市需反问时，注入给人格引擎的特殊标记（build_search_context 据此换措辞）
    ASK_CITY_MARK = "[ASK_CITY]"

    def __init__(self):
        self.last_decision = None

    def _is_location_bound(self, msg: str) -> bool:
        """这条消息是否属于天气等强依赖地点的问题。"""
        low = msg.lower()
        return any(k.lower() in low for k in self.WEATHER_KEYWORDS)

    def _build_query(self, user_message: str, user_city: str) -> Tuple[str, bool]:
        """
        生成最终搜索词。
        返回 (query, missing_city)：
          - 非地点强相关：query=原消息，missing_city=False
          - 地点强相关且有城市：query="城市 原消息"，missing_city=False
          - 地点强相关但无城市：query=原消息，missing_city=True（调用方应改为反问城市）
        """
        msg = user_message.strip()
        if not self._is_location_bound(msg):
            return msg, False
        city = (user_city or "").strip()
        if city:
            if city in msg:
                return msg, False  # 用户自己已经说了城市，不重复拼
            return f"{city} {msg}", False
        return msg, True

    async def judge(self, user_message: str, role_name: str = "") -> Dict:
        """
        判断用户消息是否需要联网搜索。
        返回: {need_search: bool, reason: str, confidence: float}
        """
        msg = user_message.strip()

        # 太短的消息不走知识路由
        if len(msg) < KNOWLEDGE_ROUTER_MIN_LEN:
            return {"need_search": False, "reason": "消息太短，属于日常对话", "confidence": 0.9}

        # 规则快速判断：包含离线关键词且不包含在线提示
        has_offline = any(kw in msg for kw in self.OFFLINE_KEYWORDS)
        has_online_hint = any(msg.startswith(hint) or hint in msg for hint in self.ONLINE_HINTS)
        # v11.0: 疑问词优先级 — 以疑问词开头且含在线提示词，优先判为需要联网
        question_prefixes = ("什么是", "是谁", "在哪", "在哪里", "什么时候", "为什么", "怎么", "如何", "多少", "几", "最新", "最近")
        is_question = any(msg.startswith(q) for q in question_prefixes)

        if is_question and has_online_hint:
            self.last_decision = {"need_search": True, "reason": "疑问词开头且含事实性提示词", "confidence": 0.75}
            return self.last_decision
        # 含在线提示词且为疑问句（不以疑问词开头但带问号），直接判联网，避免额外LLM调用
        if has_online_hint and ("?" in msg or "？" in msg):
            self.last_decision = {"need_search": True, "reason": "包含事实性提示词且为疑问句", "confidence": 0.65}
            return self.last_decision
        # v15.0：即便不是疑问句，只要命中天气/时效关键词，也直接判联网（防止"明天要带伞吗"这类陈述式提问漏网）
        if self._is_location_bound(msg) or any(k in msg for k in ("新闻","最新","最近","价格","比赛","比分","股价","行情")):
            self.last_decision = {"need_search": True, "reason": "命中天气/时效类关键词", "confidence": 0.7}
            return self.last_decision
        if has_offline and not has_online_hint:
            self.last_decision = {"need_search": False, "reason": "包含情感/日常关键词，属于角色对话", "confidence": 0.85}
            return self.last_decision

        # 调用豆包做精准判断
        prompt = (
            f"判断以下用户消息是否需要联网搜索才能准确回答。\n\n"
            f"用户消息：{msg}\n\n"
            f"判断标准：\n"
            f"- 需要联网：事实性问题（新闻、天气、气温、价格、知识科普、最新事件、人物信息、比赛结果、政策规定等）。"
            f"尤其是天气、新闻、价格、比赛、行情这类时效性内容，即使用户用闲聊语气提到，也必须联网，不能凭印象回答。\n"
            f"- 不需要联网：情感交流、日常对话、角色扮演、个人感受、关于角色本身的问题\n\n"
            f'返回JSON：{{"need_search": true/false, "reason": "简短原因", "confidence": 0.0-1.0}}'
        )
        content = await smart_llm_call(
            [{"role": "user", "content": prompt}],
            temperature=0, max_tokens=100, json_mode=True, timeout=15
        )
        if content:
            result = safe_json_parse(content)
            if result and "need_search" in result:
                self.last_decision = {
                    "need_search": bool(result["need_search"]),
                    "reason": result.get("reason", ""),
                    "confidence": float(result.get("confidence", 0.5)),
                }
                return self.last_decision

        # 兜底：有在线提示则需要搜索
        self.last_decision = {
            "need_search": has_online_hint,
            "reason": "兜底判断" + ("（包含事实性问题提示）" if has_online_hint else "（默认不搜索）"),
            "confidence": 0.6,
        }
        return self.last_decision

    async def route_and_search(self, user_message: str, role_name: str = "", user_city: str = "") -> Dict:
        """
        完整路由流程：判断 → 拼城市/反问 → Kimi搜索（失败兜底豆包）→ 返回搜索结果
        参数 user_city：用户画像里记录的所在城市（basic_info.city），天气类搜索用
        返回: {need_search, search_result, route, reason, ask_city}
        """
        decision = await self.judge(user_message, role_name)
        if not decision["need_search"]:
            return {
                "need_search": False,
                "route": "B",
                "search_result": None,
                "ask_city": False,
                "reason": decision["reason"],
            }

        # v15.0：根据用户城市改写搜索词
        query, missing_city = self._build_query(user_message, user_city)

        # 天气等地点强相关、却还没有用户城市：不瞎搜，先让角色反问城市
        if missing_city:
            logger.info("[KnowledgeRouter] 地点强相关但缺少用户城市，改为先反问城市，不进行无效搜索")
            ask_text = (
                self.ASK_CITY_MARK +
                "用户问的是天气/气温这类必须先知道所在城市才能准确回答的问题，"
                "但目前并不知道用户在哪个城市。请你用角色的语气、自然地先问一句 TA 在哪个城市"
                "（例如「你在哪个城市呀？我帮你看看那边天气」）。"
                "在用户告诉你城市之前，绝对不允许编造任何天气、气温、会不会下雨/降温之类的具体内容。"
            )
            return {
                "need_search": True,
                "route": "ASK_CITY",
                "search_result": ask_text,
                "ask_city": True,
                "reason": "地点强相关但缺城市，先反问",
            }

        # A线：联网搜索（query 已拼好城市；kimi_search_call 内部已含豆包搜索兜底）
        search_result = await kimi_search_call(query)
        if search_result:
            return {
                "need_search": True,
                "route": "A",
                "search_result": search_result,
                "ask_city": False,
                "reason": decision["reason"],
                "query": query,
            }
        else:
            # 搜索失败，降级到B线
            logger.warning("[KnowledgeRouter] 联网搜索失败（Kimi与豆包兜底均失败），降级到B线直答")
            return {
                "need_search": True,
                "route": "B_fallback",
                "search_result": None,
                "ask_city": False,
                "reason": f"搜索失败降级: {decision['reason']}",
            }

    def build_search_context(self, search_result: str) -> str:
        """将搜索结果整理成Prompt上下文。"""
        if not search_result:
            return ""
        # v15.0：缺城市反问分支，用"对话引导"措辞而不是"搜索结果"措辞
        if search_result.startswith(self.ASK_CITY_MARK):
            body = search_result[len(self.ASK_CITY_MARK):].strip()
            return f"【对话引导要求】{body}"
        return (f"【联网搜索结果】（以下是刚刚搜索到的最新信息，请用角色的语气自然地融入回答，"
                f"不要说'根据搜索结果'或'我查了一下'，就像你本来就知道一样）：\n{search_result[:1000]}")

# ============================================================
# v10.0: PersonalityEngine（统一管线 + 全部新模块集成）
# ============================================================

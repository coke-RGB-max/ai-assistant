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

logger = logging.getLogger("knowledge")


# ============================================================
# KnowledgeRouter
# ============================================================
class KnowledgeRouter:
    """
    知识路由架构：
    用户消息 → 判断模型（豆包）分析"这件事我知道吗？"
      - 知道 → B线：直接用人格模型回复
      - 不知道 → A线：Kimi联网搜索 → 整理搜索结果 → 人格模型回复
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
    ]

    def __init__(self):
        self.last_decision = None

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
        if has_offline and not has_online_hint:
            self.last_decision = {"need_search": False, "reason": "包含情感/日常关键词，属于角色对话", "confidence": 0.85}
            return self.last_decision

        # 调用豆包做精准判断
        prompt = (
            f"判断以下用户消息是否需要联网搜索才能准确回答。\n\n"
            f"用户消息：{msg}\n\n"
            f"判断标准：\n"
            f"- 需要联网：事实性问题（新闻、天气、价格、知识科普、最新事件、人物信息、比赛结果等）\n"
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

    async def route_and_search(self, user_message: str, role_name: str = "") -> Dict:
        """
        完整路由流程：判断 → 如果需要则Kimi搜索 → 返回搜索结果
        返回: {need_search, search_result, route, reason}
        """
        decision = await self.judge(user_message, role_name)
        if not decision["need_search"]:
            return {
                "need_search": False,
                "route": "B",
                "search_result": None,
                "reason": decision["reason"],
            }

        # A线：Kimi联网搜索
        search_result = await kimi_search_call(user_message)
        if search_result:
            return {
                "need_search": True,
                "route": "A",
                "search_result": search_result,
                "reason": decision["reason"],
            }
        else:
            # 搜索失败，降级到B线
            logger.warning("[KnowledgeRouter] Kimi搜索失败，降级到B线直答")
            return {
                "need_search": True,
                "route": "B_fallback",
                "search_result": None,
                "reason": f"搜索失败降级: {decision['reason']}",
            }

    def build_search_context(self, search_result: str) -> str:
        """将搜索结果整理成Prompt上下文。"""
        if not search_result:
            return ""
        return (f"【联网搜索结果】（以下是刚刚搜索到的最新信息，请用角色的语气自然地融入回答，"
                f"不要说'根据搜索结果'或'我查了一下'，就像你本来就知道一样）：\n{search_result[:1000]}")

# ============================================================
# v10.0: PersonalityEngine（统一管线 + 全部新模块集成）
# ============================================================

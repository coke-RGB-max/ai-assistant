"""
讲笑话插件
P4 序号5：插件/技能系统示例

触发词（强意图）：笑话、段子、讲个笑话、joke、逗我笑
功能：随机讲一个笑话

注意：不再用“开心/无聊/搞笑”这类情绪宽词，避免把“我不开心/好无聊陪陪我”
这类需要情感陪伴的消息误判成要段子而绕过人格 LLM。
"""
import random
from typing import Any, Dict, Optional

from plugins.base import BasePlugin


class JokePlugin(BasePlugin):
    """讲笑话插件。"""

    name = "joke"
    description = "讲笑话：随机讲一个笑话逗你开心"
    version = "1.1.0"
    author = "FlexiChrono"
    commands = ["笑话", "讲个笑话", "段子", "joke", "逗我笑"]

    # 明确“要一个笑话/段子”的强意图词
    STRONG_KEYWORDS = ["笑话", "段子", "joke", "make me laugh", "逗我笑", "逗笑",
                       "讲个笑", "来个笑", "说个笑"]
    # 情绪低落时应交给人格 LLM 安抚，而不是抛段子
    NEGATIVE_PHRASES = ["不开心", "难过", "心情不好", "想哭", "郁闷", "emo", "心累", "好烦"]
    MAX_LEN = 18

    def can_handle(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        if not message:
            return False
        m = message.strip().lower()
        if not m or len(m) > self.MAX_LEN:
            return False
        if any(neg in m for neg in self.NEGATIVE_PHRASES):
            return False
        return any(kw in m for kw in self.STRONG_KEYWORDS)

    JOKES = [
        "程序员的读书方式：从入门到放弃，从放弃到重新入门，从重新入门到再次放弃。",
        "问：为什么程序员总是分不清万圣节和圣诞节？答：因为 Oct 31 == Dec 25。",
        "一个程序员走进酒吧，点了1.0000000000000001杯啤酒。",
        "老婆给程序员老公打电话:\"去买一斤包子，如果看到卖西瓜的，买一个。\" 老公回来只买了一个包子，因为他看到了卖西瓜的。",
        "程序员最讨厌的四件事：1.写注释 2.别人不写注释 3.写文档 4.别人不写文档",
        "问：程序员的孩子为什么不哭？答：因为他们有 try-catch。",
        "一个 SQL 查询走进酒吧，看到两张表，问:\"我可以 JOIN 你们吗?\"",
        "程序员的三大浪漫：编译原理、图形学、操作系统。程序员的三大现实：改bug、改bug、还是改bug。",
        "问：为什么程序员喜欢黑色？答：因为黑色会吸收所有光，包括bug的光。",
        "产品经理说:\"这个需求很简单，只要加个按钮就行。\" 程序员说:\"好的，需要改架构、重构数据库、重写前端、测试一周。\"",
        "问：如何判断一个程序员是真的累了？答：他开始写注释了。",
        "一个程序员在森林里迷路了，他看到一个路标，上面写着:\"前方有bug，请绕行。\" 于是他绕了三年。",
    ]

    async def handle(self, message: str, context: Optional[Dict[str, Any]] = None) -> str:
        """随机讲一个笑话。"""
        joke = random.choice(self.JOKES)
        # 随机加个开场白
        intros = [
            "好呀，给你讲个笑话～",
            "嘿嘿，这个超好笑的！",
            "来啦来啦，听好了哦～",
            "那我讲一个程序员专属笑话吧！",
            "保证让你笑出声！",
        ]
        intro = random.choice(intros)
        return f"{intro}\n\n{joke}\n\n哈哈哈哈，是不是超好笑！还要再听一个吗～"

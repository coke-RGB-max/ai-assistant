"""
报时与日期插件
P4 序号5：插件/技能系统示例

触发词（强意图，避免劫持日常对话）：几点、现在时间、现在几点、几号、星期几、what time 等
功能：报告当前时间、日期、星期，以及一些时间相关的贴心话

设计原则：陪伴产品中绝大多数消息都应走人格 LLM，插件只在用户【明确询问时间/日期】时接管。
因此：
1. 不再用“时间”这种过宽的子串（“时间不早了该睡了”不是在问时间）；
2. 时间一律取北京时间（Asia/Shanghai，回落固定 +8），不依赖容器是否配置 TZ。
"""
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from plugins.base import BasePlugin

_CST = timezone(timedelta(hours=8))


def now_beijing() -> datetime:
    """返回北京时间；优先用系统 tzdata，缺失时回落固定 UTC+8（容器内也正确）。"""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai"))
    except Exception:
        return datetime.now(_CST)


class TimePlugin(BasePlugin):
    """报时插件。"""

    name = "time"
    description = "报时：告诉你现在的时间、日期、星期"
    version = "1.1.0"
    author = "FlexiChrono"
    # 仅用于后台展示；真正的触发判定见重写后的 can_handle
    commands = ["几点", "现在时间", "现在几点", "几号", "星期几", "周几", "what time"]

    # 明确询问钟点/日期的强意图词
    STRONG_KEYWORDS = [
        "几点", "多少点", "现在时间", "现在几点", "什么时间", "报时",
        "几号", "多少号", "什么日期", "今天日期", "今天几号",
        "星期几", "周几", "礼拜几", "今天星期", "今天周",
        "what time", "what day", "current time", "what's the date", "date today",
    ]
    # 命中这些说明是在陈述/感叹，而不是询问时间（防止“时间不早了”被误判）
    NEGATIVE_PHRASES = [
        "不早了", "该睡", "很晚", "没时间", "时间过得", "时间到", "赶时间",
        "多长时间", "多久", "浪费时间", "省时间", "时间有限", "时间真快",
        "时间慢", "时间还", "有的是时间",
    ]
    # 问时间一般很简短，超过该长度视为正常长句，交给 LLM
    MAX_LEN = 16

    def can_handle(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """强意图判定：短消息 + 命中报时词 + 不含陈述性短语。"""
        if not message:
            return False
        m = message.strip().lower()
        if not m or len(m) > self.MAX_LEN:
            return False
        if any(neg in m for neg in self.NEGATIVE_PHRASES):
            return False
        return any(kw in m for kw in self.STRONG_KEYWORDS)

    def _get_time_greeting(self, hour: int) -> str:
        """根据时段返回问候语。"""
        if 5 <= hour < 8:
            return "早上好呀～这么早就醒了吗？"
        elif 8 <= hour < 11:
            return "上午好！今天也要元气满满哦～"
        elif 11 <= hour < 13:
            return "中午啦～该吃午饭了，别饿肚子哦！"
        elif 13 <= hour < 17:
            return "下午好～工作学习累了就休息一下吧。"
        elif 17 <= hour < 19:
            return "傍晚啦～今天过得怎么样呀？"
        elif 19 <= hour < 22:
            return "晚上好～吃完晚饭了吗？"
        elif 22 <= hour < 24:
            return "这么晚还没睡呀～别熬夜，早点休息哦！"
        else:
            return "凌晨啦～还没睡吗？要注意身体哦！"

    async def handle(self, message: str, context: Optional[Dict[str, Any]] = None) -> str:
        """报告当前时间（北京时间）。"""
        now = now_beijing()
        hour = now.hour
        minute = now.minute
        weekday_names = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
        weekday = weekday_names[now.weekday()]

        # 格式化时间
        time_str = f"{hour:02d}:{minute:02d}"
        date_str = f"{now.year}年{now.month}月{now.day}日"

        greeting = self._get_time_greeting(hour)

        # 判断用户问的是时间还是日期
        if any(kw in message for kw in ["日期", "几号", "星期", "周几", "号"]):
            return f"{greeting}\n\n今天是{date_str}，{weekday}，现在是{time_str}。"
        else:
            return f"{greeting}\n\n现在是{time_str}，{date_str}，{weekday}。"

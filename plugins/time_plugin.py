"""
示例插件：报时与日期
P4 序号5：插件/技能系统示例

触发词：几点、时间、现在时间、日期、今天几号、星期、time
功能：报告当前时间、日期、星期，以及一些时间相关的贴心话
"""
from datetime import datetime
from typing import Any, Dict, Optional

from plugins.base import BasePlugin


class TimePlugin(BasePlugin):
    """报时插件。"""

    name = "time"
    description = "报时：告诉你现在的时间、日期、星期"
    version = "1.0.0"
    author = "FlexiChrono"
    commands = ["几点", "时间", "现在时间", "日期", "今天几号", "星期", "what time", "time"]

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
        """报告当前时间。"""
        now = datetime.now()
        hour = now.hour
        minute = now.minute
        weekday_names = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
        weekday = weekday_names[now.weekday()]

        # 格式化时间
        time_str = f"{hour:02d}:{minute:02d}"
        date_str = f"{now.year}年{now.month}月{now.day}日"

        greeting = self._get_time_greeting(hour)

        # 判断用户问的是时间还是日期
        if any(kw in message for kw in ["日期", "几号", "星期"]):
            return f"{greeting}\n\n今天是{date_str}，{weekday}，现在是{time_str}。"
        else:
            return f"{greeting}\n\n现在是{time_str}，{date_str}，{weekday}。"

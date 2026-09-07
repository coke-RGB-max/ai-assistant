"""
天气查询插件
P4 序号5：插件/技能系统示例

触发词（强意图）：天气怎么/如何、气温、几度、多少度、会下雨吗、天气预报、weather 等
功能：查询指定城市的天气（模拟数据，实际可接入真实天气API）

注意：不再用“下雨/晴天/阴天”这类宽子串，避免把“我这边下雨了好想你”这类
情绪倾诉误判成天气查询而绕过人格 LLM。
"""
from typing import Any, Dict, Optional

from plugins.base import BasePlugin


class WeatherPlugin(BasePlugin):
    """天气查询插件。"""

    name = "weather"
    description = "天气查询：查询指定城市的天气情况"
    version = "1.1.0"
    author = "FlexiChrono"
    commands = ["天气", "weather", "气温", "温度", "几度", "天气预报"]

    # 明确的“查询天气”强意图词
    STRONG_KEYWORDS = [
        "气温", "天气预报", "几度", "多少度", "温度多少", "天气怎么", "天气如何",
        "天气多少", "会下雨吗", "下雨吗", "会不会下雨", "查天气", "看看天气",
        "weather", "forecast",
    ]
    # 陈述/抒情，不是查询（“我这边下雨了好想你”应交给人格 LLM）
    NEGATIVE_PHRASES = ["我这边", "外面在下雨", "下雨了", "好想", "想你", "心情",
                        "不错", "真好", "好舒服", "喜欢"]
    QUESTION_MARKS = ["吗", "？", "?", "怎么", "如何", "多少", "咋样", "怎么样"]
    MAX_LEN = 16

    def can_handle(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        if not message:
            return False
        m = message.strip().lower()
        if not m or len(m) > self.MAX_LEN:
            return False
        if any(neg in m for neg in self.NEGATIVE_PHRASES):
            return False
        if any(kw in m for kw in self.STRONG_KEYWORDS):
            return True
        # “XX天气？/ 天气怎么样”这类简短查询：含“天气”且带疑问语气
        return "天气" in m and any(q in m for q in self.QUESTION_MARKS)

    # 模拟天气数据（实际使用时可接入和风天气/OpenWeather等API）
    WEATHER_DATA = {
        "北京": {"temp": "26°C", "weather": "晴", "humidity": "45%", "wind": "东南风3级"},
        "上海": {"temp": "28°C", "weather": "多云", "humidity": "65%", "wind": "东风2级"},
        "广州": {"temp": "32°C", "weather": "雷阵雨", "humidity": "80%", "wind": "南风4级"},
        "深圳": {"temp": "31°C", "weather": "多云转晴", "humidity": "75%", "wind": "东南风3级"},
        "武汉": {"temp": "29°C", "weather": "阴", "humidity": "70%", "wind": "北风2级"},
        "成都": {"temp": "25°C", "weather": "小雨", "humidity": "85%", "wind": "微风"},
        "杭州": {"temp": "27°C", "weather": "晴转多云", "humidity": "60%", "wind": "东南风2级"},
        "南京": {"temp": "28°C", "weather": "多云", "humidity": "62%", "wind": "东风3级"},
    }

    def _extract_city(self, message: str) -> Optional[str]:
        """从消息中提取城市名。"""
        for city in self.WEATHER_DATA:
            if city in message:
                return city
        return None

    async def handle(self, message: str, context: Optional[Dict[str, Any]] = None) -> str:
        """处理天气查询。"""
        city = self._extract_city(message)

        if city:
            data = self.WEATHER_DATA[city]
            reply = (
                f"{city}今天的天气是{data['weather']}，"
                f"气温{data['temp']}，"
                f"湿度{data['humidity']}，"
                f"{data['wind']}。"
            )
            # 根据天气给点贴心建议
            if "雨" in data["weather"]:
                reply += "出门记得带伞哦～"
            elif int(data["temp"].replace("°C", "")) >= 30:
                reply += "天气很热，注意防暑降温，多喝水！"
            elif int(data["temp"].replace("°C", "")) <= 10:
                reply += "天气有点冷，注意保暖哦～"
            return reply
        else:
            # 没有指定城市，问用户想查哪个城市
            cities = "、".join(list(self.WEATHER_DATA.keys())[:6])
            return f"你想查哪个城市的天气呀？目前支持查询{cities}等城市～告诉我城市名就行！"

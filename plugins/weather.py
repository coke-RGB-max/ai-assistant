"""
示例插件：天气查询
P4 序号5：插件/技能系统示例

触发词：天气、weather、气温、下雨、温度
功能：查询指定城市的天气（模拟数据，实际可接入真实天气API）
"""
from typing import Any, Dict, Optional

from plugins.base import BasePlugin


class WeatherPlugin(BasePlugin):
    """天气查询插件。"""

    name = "weather"
    description = "天气查询：查询指定城市的天气情况"
    version = "1.0.0"
    author = "FlexiChrono"
    commands = ["天气", "weather", "气温", "下雨", "温度", "晴天", "阴天"]

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

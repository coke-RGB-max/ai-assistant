"""插件触发防回归：宽子串不得劫持日常/情绪对话，只有强意图才接管（回归“时间不早了”事故）。"""
import pytest

from plugins.time_plugin import TimePlugin, now_beijing
from plugins.weather import WeatherPlugin
from plugins.joke import JokePlugin


@pytest.fixture
def plugins():
    return TimePlugin(), WeatherPlugin(), JokePlugin()


@pytest.mark.parametrize("msg,should", [
    ("时间不早了，该睡觉了", False),   # 本次事故：陈述被误判为问时间
    ("晚上好呀", False),
    ("这个星期好累啊", False),
    ("没时间了快走吧", False),
    ("现在几点了", True),
    ("今天星期几", True),
    ("现在时间", True),
    ("what time is it", True),
])
def test_time_plugin_intent(plugins, msg, should):
    t = plugins[0]
    assert t.can_handle(msg) is should


@pytest.mark.parametrize("msg,should", [
    ("我这边下雨了，好想你", False),
    ("今天天气真好", False),
    ("武汉天气怎么样", True),
    ("现在几度", True),
    ("会下雨吗", True),
])
def test_weather_plugin_intent(plugins, msg, should):
    assert plugins[1].can_handle(msg) is should


@pytest.mark.parametrize("msg,should", [
    ("我不开心，陪陪我", False),
    ("好无聊啊", False),
    ("讲个笑话", True),
    ("来段段子", True),
])
def test_joke_plugin_intent(plugins, msg, should):
    assert plugins[2].can_handle(msg) is should


def test_now_beijing_is_utc8():
    now = now_beijing()
    # 必须带 +8 时区信息，且年月日时分可正常取到
    assert now.utcoffset() is not None
    assert now.utcoffset().total_seconds() == 8 * 3600

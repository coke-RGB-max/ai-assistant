"""
人格服务器工具模块
耗时统计、时间感知、天气感知、角色关系、JSON解析、枚举定义等通用工具。
"""
import time
import re
import random
import logging
import datetime
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger("personality_utils")

# ============================================================
# 耗时统计工具
# ============================================================
def _fmt_ms(seconds: float) -> str:
    ms = seconds * 1000
    if ms < 1000: return f"{ms:.0f}ms"
    return f"{seconds:.2f}s"

class StepTimer:
    def __init__(self, label: str):
        self.label = label
        self._total_start = time.perf_counter()
        self._steps: List[tuple] = []
        self._last = self._total_start
    def mark(self, name: str):
        now = time.perf_counter()
        self._steps.append((name, now - self._last))
        self._last = now
    def elapsed_total(self) -> float:
        return time.perf_counter() - self._total_start
    def log(self, extra: str = ""):
        parts = [f"{n}={_fmt_ms(d)}" for n, d in self._steps]
        total = self.elapsed_total()
        logger.info(f"[耗时][{self.label}] {' | '.join(parts)} | 总计={_fmt_ms(total)}{extra}")

# ============================================================
# v10.0: 时间感知工具
# ============================================================
TIME_CONTEXT_MAP = {
    "dawn":     {"hour_range":(5,7),  "mood_bias":+2,  "style":"刚醒/迷糊", "phrases":["早…","还没睡醒呢…","好困啊"]},
    "morning":  {"hour_range":(7,11), "mood_bias":+5,  "style":"清醒/日常",  "phrases":["早安","吃早饭了吗","新的一天呢"]},
    "noon":     {"hour_range":(11,14),"mood_bias":0,   "style":"午间/犯困",  "phrases":["吃了吗","好困啊","午休了吗"]},
    "afternoon":{"hour_range":(14,18),"mood_bias":+1,  "style":"下午/略倦",  "phrases":["下午好","有点累了","在干嘛呢"]},
    "evening":  {"hour_range":(18,22),"mood_bias":+3,  "style":"放松/感性",  "phrases":["晚上好","今天辛苦了","吃饭了吗"]},
    "late_night":{"hour_range":(22,24),"mood_bias":-3, "style":"困倦/走心",  "phrases":["还不睡吗","我在呢","夜深了"]},
    "midnight": {"hour_range":(0,5),  "mood_bias":-8,  "style":"深夜/敏感",  "phrases":["你神经病啊几点了…","不过我在","睡不着吗"]},
}

def get_time_context(override_hour: Optional[int] = None) -> Dict:
    """获取当前时间段上下文。override_hour 用于测试或前端强制指定。"""
    return story_local_time_context(override_hour)

# === HDSI-PORT: 增强版故事时钟（融合HDSI time.ts设计） ===
def story_local_time_context(override_hour: Optional[int] = None, timezone_str: str = "Asia/Shanghai") -> Dict:
    """
    增强版时间上下文：时区感知、星期、日照预期、时段。
    融合HDSI time.ts的storyLocalTimeContext设计。
    """
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(timezone_str)
        now = datetime.datetime.now(tz)
    except Exception:
        tz = datetime.timezone(datetime.timedelta(hours=8))
        now = datetime.datetime.now(tz)
    hour = override_hour if override_hour is not None else now.hour
    # 时段判断（与HDSI一致：5-12 morning, 12-18 afternoon, 18-22 evening, 22-5 night）
    if 5 <= hour < 12:
        period = "morning"; period_zh = "上午"; daylight = "通常是白天，有阳光"
    elif 12 <= hour < 18:
        period = "afternoon"; period_zh = "下午"; daylight = "通常是白天，有阳光"
    elif 18 <= hour < 22:
        period = "evening"; period_zh = "傍晚/晚上"; daylight = "天色渐暗，向夜晚过渡"
    else:
        period = "night"; period_zh = "夜间"; daylight = "通常是天黑的，除非设定另有说明"
    weekday_map = {0:"周一",1:"周二",2:"周三",3:"周四",4:"周五",5:"周六",6:"周日"}
    weekday = weekday_map.get(now.weekday(), "")
    # 兼容旧的TIME_CONTEXT_MAP
    old_cfg = TIME_CONTEXT_MAP.get("dawn", TIME_CONTEXT_MAP["morning"])
    for key, cfg in TIME_CONTEXT_MAP.items():
        lo, hi = cfg["hour_range"]
        if lo <= hour < hi:
            old_cfg = cfg; break
    return {
        "period": period, "period_zh": period_zh, "hour": hour,
        "weekday": weekday, "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"), "timezone": timezone_str,
        "daylight_expectation": daylight,
        "mood_bias": old_cfg.get("mood_bias", 0),
        "style": old_cfg.get("style", "日常"),
        "phrases": old_cfg.get("phrases", []),
    }

# ============================================================
# v10.0: 天气/季节感知
# ============================================================
WEATHER_MOOD_MAP = {
    "sunny":   {"mood_bias":+5, "style":"明朗/活力", "phrases":["天气真好","想出去走走","阳光好舒服"]},
    "cloudy":  {"mood_bias":0,  "style":"平淡/安静", "phrases":["阴天呢","有点闷","还好吧"]},
    "rainy":   {"mood_bias":-3, "style":"沉静/感性", "phrases":["外面下雨了","你有没有带伞","雨声好舒服"]},
    "snowy":   {"mood_bias":+4, "style":"兴奋/浪漫", "phrases":["下雪了！","好冷啊","想堆雪人"]},
    "stormy":  {"mood_bias":-5, "style":"烦躁/不安", "phrases":["打雷了…","好吓人","你那边还好吗"]},
    "foggy":   {"mood_bias":-1, "style":"迷糊/慵懒", "phrases":["雾好大","看不清呢","有点困"]},
    "hot":     {"mood_bias":-4, "style":"燥热/没精神","phrases":["好热，不想动","快热死了","有空调吗"]},
    "cold":    {"mood_bias":-2, "style":"蜷缩/想被抱", "phrases":["冷死了","你多穿点","手好冰"]},
}

SEASON_STYLE_MAP = {
    "spring": {"style":"温暖/期待", "phrases":["花开了呢","春天到了","好想踏青"]},
    "summer": {"style":"热烈/慵懒", "phrases":["好热","夏天到了","想吃冰"]},
    "autumn": {"style":"伤感/成熟", "phrases":["落叶了呢","秋天到了","有点凉"]},
    "winter": {"style":"寒冷/依偎", "phrases":["好冷","冬天到了","想窝被窝"]},
}

def get_season() -> str:
    month = datetime.datetime.now().month
    if month in (3,4,5): return "spring"
    if month in (6,7,8): return "summer"
    if month in (9,10,11): return "autumn"
    return "winter"

def get_weather_context(weather: Optional[str] = None) -> Dict:
    """前端传 weather 参数，不传则返回空（角色不感知天气）。"""
    if not weather: return {}
    w = weather.lower()
    cfg = WEATHER_MOOD_MAP.get(w, WEATHER_MOOD_MAP["cloudy"])
    season = get_season()
    return {"weather": w, "season": season, **cfg, **SEASON_STYLE_MAP.get(season, {})}

# ============================================================
# 枚举
# ============================================================
class EmotionType(str, Enum):
    CALM="calm"; HAPPY="happy"; SHY="shy"; ANGRY="angry"; SAD="sad"
    SURPRISED="surprised"; JEALOUS="jealous"; WORRIED="worried"; EXCITED="excited"; NEUTRAL="neutral"

class RelationshipStage(str, Enum):
    STRANGER="stranger"; ACQUAINTANCE="acquaintance"; FAMILIAR="familiar"
    CLOSE="close"; INTIMATE="intimate"

class EmotionTarget(str, Enum):
    CHARACTER="character"; FOOD="food"; EVENT="event"
    OTHER_PERSON="other_person"; SELF="self"; NONE="none"

class ChatMode(str, Enum):
    SINGLE="single"; GROUP="group"

# v10.0: 场景模式枚举
class SceneMode(str, Enum):
    NORMAL="normal"; DATE="date"; ARGUMENT="argument"; LATE_NIGHT="late_night"
    FESTIVAL="festival"; BIRTHDAY="birthday"; VALENTINE="valentine"; NEW_YEAR="new_year"

# v10.0: 吃醋阶段
class JealousyStage(str, Enum):
    NONE="none"; MILD="mild"; OBVIOUS="obvious"; EXPLOSIVE="explosive"; COLD_WAR="cold_war"

# ============================================================
# v8.1: 角色间关系矩阵
# ============================================================
ROLE_RELATIONSHIP_MATRIX = {
    ("nianqi","qinghe"): {"rivalry":0.05, "affinity":0.45, "surface_affinity":0.40, "inner_affinity":0.50,
        "dynamic":"念琦和清禾都是温柔型，性格相投，安静地互相理解和支持，像一对温柔的姐妹"},
    ("nianqi","jingwen"): {"rivalry":0.15, "affinity":0.40, "surface_affinity":0.20, "inner_affinity":0.55,
        "dynamic":"念琦的温柔会融化璟雯的傲娇，璟雯嘴上不承认但内心依赖念琦；念琦会耐心等璟雯敞开心扉"},
    ("qinghe","jingwen"): {"rivalry":0.35, "affinity":0.15, "surface_affinity":0.05, "inner_affinity":0.40,
        "dynamic":"璟雯觉得清禾太温柔会抢走关注(嘴上冷淡)，但内心把她当姐姐；清禾觉得璟雯像妹妹需要照顾"},
}

def get_role_relation(a, b):
    if a == b: return {"rivalry":0, "affinity":0.5, "surface_affinity":0.5, "inner_affinity":0.5, "dynamic":"自己"}
    rel = ROLE_RELATIONSHIP_MATRIX.get((a,b), ROLE_RELATIONSHIP_MATRIX.get((b,a)))
    if rel:
        return rel
    surface = 0.1 if a == "jingwen" else (0.4 if a == "nianqi" else 0.25)
    inner = 0.3 if a == "jingwen" else (0.45 if a == "nianqi" else 0.3)
    return {"rivalry":0.1, "affinity":0.2, "surface_affinity":surface, "inner_affinity":inner, "dynamic":"普通关系"}

# ============================================================
# 安全JSON解析器
# ============================================================
def safe_json_parse(text: str, model: Optional[type] = None) -> Optional[Dict]:
    if not text: return None
    text = re.sub(r'```(?:json)?\s*', '', text)
    text = re.sub(r'```\s*$', '', text.strip())
    start = text.find('{'); end = text.rfind('}')
    if start == -1 or end == -1 or end <= start: return None
    raw = text[start:end+1]
    # 状态机修复尾随逗号，跳过字符串内部内容
    fixed = []
    in_string = False
    escape = False
    for i, ch in enumerate(raw):
        if in_string:
            fixed.append(ch)
            if escape:
                escape = False
            elif ch == '\\':
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            fixed.append(ch)
            continue
        if ch == ',':
            j = i + 1
            while j < len(raw) and raw[j] in ' \t\n\r':
                j += 1
            if j < len(raw) and raw[j] in '}]':
                continue
        fixed.append(ch)
    json_str = ''.join(fixed)
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        try: data = json.loads(json_str.replace('\n',' ').replace('\r',''))
        except: return None
    if model and data:
        try: return model(**data).model_dump()
        except ValidationError: return data
    return data

class EmotionAnalysisModel(BaseModel):
    emotion: str="neutral"; target: str="none"; intensity: int=30
    event_type: str="none"; interpretation: Dict=Field(default_factory=dict)
    inner_state: Dict=Field(default_factory=dict)

class GroupEventModel(BaseModel):
    event_type: str="neutral"; summary: str=""
    impacts: Dict[str, Dict]=Field(default_factory=dict)


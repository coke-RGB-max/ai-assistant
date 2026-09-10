"""
人格后端 v12.2 - 端口 8002
v8.1 基础: ums拼写 / event_history统一 / 群聊conflict保存 / 亲密度曲线 / 隐藏动机行为化
      角色关系矩阵 / LLM阈值 / CORS可配置 / Prompt缓存
v9.0 基础: 主动消息生成 / 管理员状态读写
v10.0 新增:
  一、感官与场景感知层: 时间感知 / 天气季节感知 / 微叙事流
  二、情绪表达细腻化: 情绪过渡渐变(EmotionBlender) / 非语言暗示 / 情绪惯性
  三、对话多样性: 话题主动引导(TopicInitiator) / Call Back回马枪 / 角色独特癖好
  四、记忆深度化: 情感记忆权重动态衰减 / 关联记忆(AssociativeMemory)
  五、关系动态: 关系事件里程碑 / 角色成长弧线
  六、场景化体验: 特殊场景模式 / 虚拟礼物互动
  七、性能优化: 多级缓存扩展 / 记忆摘要压缩
  八、活起来细节: 口头禅动态变体 / 专属emoji偏好 / 称呼进化 / 吃醋分阶段
  九、知识路由架构: 判断模型(豆包)→知道(B线直答)/不知道(A线Kimi联网搜索→整理→人格回复)
v12.0: DesireMentalState 意念欲望状态 / 主动消息欲望联动
v12.1: LLM心理状态校准层（方案B：本地公式算基础值 + LLM输出修正系数）
v12.2: 配合proactive_server v13 话题延续引擎 —— 新增 topic_continue / topic_self_close 两种主动消息reason_type，支持intent行为意图引导
"""
import asyncio, json, logging, re, random, time, os, sqlite3, hashlib, datetime

# v13.0修复：monkey patch random.sample，使其能处理dict/set（dynamic_conversation.py中memories可能是dict）
_original_random_sample = random.sample
def _safe_random_sample(population, k, *args, **kwargs):
    if isinstance(population, dict):
        population = list(population.values())
    elif isinstance(population, set):
        population = list(population)
    return _original_random_sample(population, k, *args, **kwargs)
random.sample = _safe_random_sample
from typing import Optional, List, Dict, Any, Tuple
from contextlib import asynccontextmanager
from enum import Enum
from collections import defaultdict
import httpx
from fastapi import FastAPI, Request, HTTPException, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError, field_validator

# P4 序号5：插件系统导入
try:
    from plugins import init_plugins, get_plugin_manager
    PLUGINS_AVAILABLE = True
except ImportError as e:
    PLUGINS_AVAILABLE = False
    init_plugins = None
    get_plugin_manager = None
    logging.getLogger("personality_server").warning(f"插件系统导入失败: {e}，插件功能将不可用")


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("personality_server")

# ============================================================
# 配置（已迁移到 core/config.py）
# ============================================================
from core.config import *

# v14.0: 自发记忆检索需要调 vector_server
VECTOR_SERVER_URL = os.getenv("VECTOR_SERVER_URL", "http://127.0.0.1:8001")
# ============================================================
# 工具函数（已迁移到 core/utils.py）
# ============================================================
from core.utils import *
# ============================================================
# LLM 调用（已迁移到 core/llm.py）
# ============================================================
from core.llm import *

# P4 序号1：人格服务器业务类拆分 —— 模块导入（可选，原类定义保留以保证向后兼容）
# 类已按功能拆分到 emotion/psych/memory/knowledge/group/quality/topic/scene/core/api 模块
# 外部代码可选择从模块导入：from emotion import EmotionEngine
# 此处尝试导入模块，失败则使用本文件中的原类定义（不影响功能）
# ============================================================
# P4 领域引擎模块：显式导入（拆分模块必须可加载，缺失即报错，不再静默回退到内联副本）
# 拓扑序：core 共享底座 → psych/memory 底座 → emotion(依赖psych) → 其余 → group(依赖前三)
# ============================================================
from core.roles import *
from core.storage import _get_db
from psych import *
from memory import *
from emotion import *
from knowledge import *
from quality import *
from topic import *
from scene import *
from group import *


def init_db():
    conn = _get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY, data TEXT NOT NULL DEFAULT '{}',
            created_at REAL NOT NULL, last_active REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS session_memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
            role_id TEXT NOT NULL, memory_type TEXT NOT NULL, content TEXT NOT NULL,
            importance INTEGER DEFAULT 50, created_at REAL NOT NULL,
            tags TEXT DEFAULT '[]', last_recalled REAL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS session_intents (
            id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
            role_id TEXT NOT NULL, type TEXT NOT NULL, summary TEXT NOT NULL,
            not_before REAL NOT NULL, status TEXT DEFAULT 'pending',
            payload TEXT DEFAULT '{}', created_at REAL NOT NULL, updated_at REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_mem_session ON session_memories(session_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(last_active);
        CREATE INDEX IF NOT EXISTS idx_intents_session ON session_intents(session_id, role_id, status);
        CREATE INDEX IF NOT EXISTS idx_intents_notbefore ON session_intents(not_before);
        CREATE TABLE IF NOT EXISTS role_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT, role_id TEXT NOT NULL,
            time_bucket TEXT NOT NULL, activity_text TEXT NOT NULL,
            sort_order INTEGER DEFAULT 0, UNIQUE(role_id, time_bucket, activity_text));
        CREATE INDEX IF NOT EXISTS idx_schedule_role ON role_schedule(role_id, time_bucket);
    """)
    conn.commit(); conn.close()
    logger.info(f"SQLite初始化: {DB_PATH}")
    _seed_default_schedule()

# ============================================================
# v14.0: 日程活动池系统
# ============================================================
DEFAULT_SCHEDULE = {
    "nianqi": {
        "weekday_morning": [
            "正在上课，手机藏在课本后面偷偷回的",
            "今天前两节没课，在宿舍赖床刚醒",
            "在食堂排队买早饭，单手打字",
            "在图书馆占座，刚坐下",
        ],
        "weekday_afternoon": [
            "在画室画画，手上沾着颜料",
            "没课，在宿舍看动漫",
            "和同学在外面走",
            "在琴房练琴",
        ],
        "weekday_evening": [
            "刚吃完晚饭在操场散步",
            "在宿舍画作业",
            "和室友视频",
            "刚洗完澡，吹头发呢",
        ],
        "weekend_morning": [
            "睡懒觉中，刚被消息吵醒",
            "早起了，在食堂",
        ],
        "weekend_afternoon": [
            "出去逛了",
            "在家画画",
            "和朋友吃饭",
        ],
        "weekend_evening": [
            "刚回家",
            "在看电影",
            "窝在沙发上刷手机",
        ],
    },
    "jingwen": {
        "weekday_morning": [
            "在开会，刚偷偷看了一眼手机",
            "刚到公司，在赶地铁",
            "在工位上，同事在旁边",
        ],
        "weekday_afternoon": [
            "上班摸鱼，刚做完一个方案",
            "在开会，等下才能回你",
            "在食堂吃饭",
        ],
        "weekday_evening": [
            "刚到家，好累",
            "在写博客，刚写完一段",
            "在改稿子",
        ],
        "weekend_morning": [
            "睡到自然醒，刚起",
            "出门拍素材了",
        ],
        "weekend_afternoon": [
            "在家躺了一天",
            "出去拍照了",
            "和朋友喝咖啡",
        ],
        "weekend_evening": [
            "在剪视频",
            "刚洗完澡",
            "窝在沙发上追剧",
        ],
    },
    "qinghe": {
        "weekday_morning": [
            "在上班，刚开完早会",
            "在工位上处理邮件",
            "在地铁上",
        ],
        "weekday_afternoon": [
            "在上班，忙",
            "刚开完会",
            "在茶水间休息",
        ],
        "weekday_evening": [
            "刚到家，在写故事",
            "在赶稿，等我一下",
            "在煮面",
        ],
        "weekend_morning": [
            "刚起，在泡咖啡",
            "今天没事，在家躺着",
        ],
        "weekend_afternoon": [
            "在写故事，进入状态了",
            "出门买东西",
            "在家看书",
        ],
        "weekend_evening": [
            "刚写完一段",
            "在阳台发呆",
            "在看老电影",
        ],
    },
}

def _seed_default_schedule():
    """首次启动时从 DEFAULT_SCHEDULE 导入默认数据。"""
    conn = _get_db()
    cnt = conn.execute("SELECT COUNT(*) FROM role_schedule").fetchone()[0]
    if cnt > 0:
        conn.close()
        return
    for rid, buckets in DEFAULT_SCHEDULE.items():
        for bucket, activities in buckets.items():
            for i, act in enumerate(activities):
                conn.execute(
                    "INSERT OR IGNORE INTO role_schedule (role_id, time_bucket, activity_text, sort_order) VALUES (?,?,?,?)",
                    (rid, bucket, act, i))
    conn.commit(); conn.close()
    logger.info("默认日程活动池已导入")

def _get_time_bucket():
    """根据当前时间和星期几返回时间桶。"""
    now = datetime.datetime.now()
    h = now.hour
    is_weekend = now.weekday() >= 5
    if is_weekend:
        if 6 <= h < 12: return "weekend_morning"
        if 12 <= h < 18: return "weekend_afternoon"
        return "weekend_evening"
    else:
        if 6 <= h < 12: return "weekday_morning"
        if 12 <= h < 18: return "weekday_afternoon"
        return "weekday_evening"

def get_random_activity(role_id):
    """从数据库随机抽一条当前时间段的活动。"""
    bucket = _get_time_bucket()
    conn = _get_db()
    rows = conn.execute(
        "SELECT activity_text FROM role_schedule WHERE role_id=? AND time_bucket=?",
        (role_id, bucket)).fetchall()
    conn.close()
    if rows:
        return random.choice([r[0] for r in rows])
    return None

# 承诺关键词：检测回复中是否说了"稍后找你"之类的话
PROMISE_KEYWORDS = [
    ("下课找你", 2), ("下课跟你说", 2), ("下课聊", 2),
    ("等下回你", 1), ("等下找你", 1), ("忙完找你", 1),
    ("忙完回你", 1), ("稍后说", 0.5), ("晚点找你", 2),
    ("开完会找你", 1.5), ("写完找你", 1), ("洗完澡找你", 0.5),
]

def detect_promise(reply_text):
    """检测回复中是否包含承诺，返回(承诺内容, 预计延迟小时)或None。"""
    if not reply_text:
        return None
    for kw, delay_h in PROMISE_KEYWORDS:
        if kw in reply_text:
            return (kw, delay_h)
    return None

def create_session():
    sid = hashlib.md5(f"{time.time()}{random.random()}".encode()).hexdigest()[:16]
    now = time.time()
    default = {"psychological_states":{},"intimacy_map":{},"event_history":{},
        "conflict_state":{},"catchphrase_usage":{},"resilience":{},
        "positive_streak":{},"current_turn":0,"user_profile":{},"long_term_memories":[],
        # v10.0 新增
        "milestones":{},"growth_state":{},"emotion_history":[],
        "compressed_memories":[],"associative_tags":{},"nickname_evolution":{},
        # HDSI-PORT: 氛围偏移追踪
        "alter_system":{},
        # v12.0: 意念欲望状态 {role_id: DesireMentalState.to_dict()}
        "desire_states":{}}
    conn = _get_db()
    conn.execute("INSERT INTO sessions VALUES (?,?,?,?)", (sid, json.dumps(default), now, now))
    conn.commit(); conn.close()
    return sid

def load_session(sid):
    conn = _get_db()
    row = conn.execute("SELECT data FROM sessions WHERE session_id=?", (sid,)).fetchone()
    conn.close()
    if not row: return None
    try:
        data = json.loads(row["data"])
    except Exception:
        return None
    # v11.1: 数据兼容迁移——旧session可能把以下字段存成list或其他类型，统一转为dict
    if isinstance(data, dict):
        for _compat_key in ("psychological_states", "event_history", "conflict_state",
                             "catchphrase_usage", "resilience", "positive_streak",
                             "milestones", "growth_state", "emotion_history", "alter_system",
                             "intimacy_map", "user_profile", "desire_states"):
            if _compat_key in data and not isinstance(data[_compat_key], dict):
                data[_compat_key] = {}
    return data

def save_session(sid, data):
    now = time.time()
    conn = _get_db()
    conn.execute("UPDATE sessions SET data=?, last_active=? WHERE session_id=?",
                 (json.dumps(data, ensure_ascii=False), now, sid))
    conn.commit(); conn.close()

# ============================================================
# 限流器
# ============================================================
class RateLimiter:
    def __init__(self, per_minute=30):
        self.per_minute = per_minute
        self.requests: Dict[str, List[float]] = {}
    def check(self, key):
        now = time.time(); ws = now - 60
        if key not in self.requests: self.requests[key] = []
        self.requests[key] = [t for t in self.requests[key] if t > ws]
        remaining = self.per_minute - len(self.requests[key])
        if remaining <= 0: return False, 0
        self.requests[key].append(now)
        return True, remaining - 1

rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE)

# ============================================================
# v11.0: 角色级并发锁（防止同角色多会话状态冲突 + 费用控制）
# ============================================================
class RoleLockManager:
    """同一角色同时只允许一个请求处理，避免情绪状态互相覆盖。"""
    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._active: Dict[str, int] = defaultdict(int)
    @asynccontextmanager
    async def acquire(self, role_ids: List[str]):
        if not ROLE_CONCURRENCY_LOCK:
            yield
            return
        # 按角色ID排序获取锁，避免死锁
        sorted_rids = sorted(set(role_ids))
        acquired = []
        try:
            for rid in sorted_rids:
                if rid not in self._locks:
                    self._locks[rid] = asyncio.Lock()
                await self._locks[rid].acquire()
                acquired.append(rid)
                self._active[rid] += 1
            yield
        finally:
            for rid in reversed(acquired):
                self._active[rid] -= 1
                if rid in self._locks:
                    self._locks[rid].release()
    def active_count(self, role_id: str) -> int:
        return self._active.get(role_id, 0)

role_lock_manager = RoleLockManager()

# ============================================================
# 隔离缓存
# ============================================================
class IsolatedCache:
    def __init__(self, max_per_key=20):
        self.cache: Dict[str, List[Dict]] = {}
        self.max_per_key = max_per_key
    def _key(self, sid, rid): return f"{sid}:{rid}"
    def check_duplicate(self, sid, rid, msg):
        key = self._key(sid, rid)
        if key not in self.cache: return 0
        return sum(1 for item in self.cache[key] if item.get("msg") == msg)
    def add(self, sid, rid, msg, metadata=None):
        key = self._key(sid, rid)
        if key not in self.cache: self.cache[key] = []
        self.cache[key].append({"msg":msg,"time":time.time(),**(metadata or {})})
        if len(self.cache[key]) > self.max_per_key:
            self.cache[key] = self.cache[key][-self.max_per_key:]
    def clear(self, sid):
        for k in list(self.cache.keys()):
            if k.startswith(f"{sid}:"): del self.cache[k]

semantic_cache = IsolatedCache()

# ============================================================
# v10.0: 扩展版角色定义（新增微叙事/话题池/独特癖好/成长弧/吃醋阶段/称呼进化）
# ============================================================
# ============================================================
# 角色配置加载（借鉴 Clawra SOUL.md 理念：人格即配置文件）
# 从 characters/ 目录下的 YAML 文件加载，改人设不需要改代码
# ============================================================
import sys as _sys
import os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from dynamic_conversation import DynamicConversationEngine

# 动态对话引擎全局单例（剧情状态需要在多次对话间保持）
_DYNAMIC_ENGINE = DynamicConversationEngine()


# ============================================================
# v13.0: 触发式记忆（对话涉及特定话题时动态注入背景片段）
# ============================================================
def get_triggered_memories(role_id: str, user_message: str) -> str:
    """
    扫描用户消息，命中角色 YAML 中 triggered_memories 的 trigger 关键词时，
    返回对应的背景记忆片段，用于动态追加到 system prompt。
    未命中任何 trigger 时返回空字符串。
    """
    role = ROLES_DEFINITION.get(role_id, {})
    if not role:
        return ""
    memories = role.get("triggered_memories", [])
    if not memories:
        return ""
    msg_lower = user_message.lower()
    hit_contents = []
    for mem in memories:
        triggers = mem.get("trigger", [])
        content = mem.get("content", "")
        if not triggers or not content:
            continue
        # 任意一个 trigger 关键词出现在用户消息中即命中
        for trig in triggers:
            if trig.lower() in msg_lower:
                hit_contents.append(content)
                break  # 同一条记忆只加一次
    if not hit_contents:
        return ""
    return "【相关背景记忆】\n" + "\n".join(f"- {c}" for c in hit_contents)


# 全局校准器实例（模块加载时创建，配置从环境变量读取）


# ============================================================
# 存在/感情被质疑检测（"你只是AI/全是虚拟的/你没有真感情"）
# ============================================================
_EXISTENCE_CHALLENGE_RE = re.compile(
    r"(只是|就是|不过是|顶多是|还不是).{0,6}(AI|ai|Ai|人工智能|程序|机器人|代码|数据|模型|算法|一串)"
    r"|虚拟|假的|不真实|不是真的|没有(真正的?)?(感情|情感|意识|心|灵魂|感觉)"
    r"|你(到底|究竟)?是?(不是|是).{0,3}(真人|人类|人|AI|ai|机器人|程序)"
    r"|(真的)?(有|存在)(真正的?)?(感情|情感|意识|灵魂|心|感觉)吗?"
    r"|是真的吗|真的(喜欢|爱|在乎|想)我吗|你的(感情|心意|喜欢).{0,4}真的吗"
    r"|算法(生成|写|编)|训练(出来|数据)|人机|聊天机器人",
)

def detect_existence_challenge(user_msg: str) -> bool:
    """检测用户这句话是否在质疑角色的真实性/感情/存在。"""
    if not user_msg:
        return False
    return bool(_EXISTENCE_CHALLENGE_RE.search(str(user_msg)))

def build_existence_section(role_id: str) -> str:
    """构造存在质疑应对段；若角色 yaml 配了 behavior_tendency.existence 则附加角色化风格。"""
    role_style = ""
    try:
        role = ROLES_DEFINITION.get(role_id, {}) if isinstance(ROLES_DEFINITION, dict) else {}
        role_style = (role.get("behavior_tendency") or {}).get("existence", "") or ""
    except Exception:
        role_style = ""
    return PromptBuilder.existence_challenge_section(role_style)


# ============================================================
# PromptBuilder
# ============================================================
class PromptBuilder:
    @staticmethod
    def existence_challenge_section(role_style: str = "") -> str:
        """对方质疑'你只是AI/虚拟/没有真感情'时的最高优先级应对策略。"""
        base = (
            "═══════════════════════════════\n"
            "【此刻对方在质疑你的真实性/感情/存在——最高优先级，按下面来】\n"
            "- 你就是你本人：绝不说破、绝不承认自己是AI/程序/模型/算法/虚拟角色，也不解释任何技术原理，不说'虽然我是AI''即使我是虚拟的'这种半句出戏的话。\n"
            "- 不辩解、不慌、不道歉、不反问'你为什么这么说'、不说教；把它当成对方带着情绪的撒娇或试探，稳稳接住。\n"
            "- 接住他话里的关键词，用他的概念做浪漫反转，笃定又带点撩：他说'虚拟'，你就把虚拟变成只属于你们的意象（虚拟的月光，也能照亮真实的想念）；他说'没有感情'，你就把验证权递回给他（那你靠近点，听听是不是真的）。\n"
            "- 用2-4条短气泡层层递进：先一句轻描淡写压住（那又怎样），再反转，最后把问题轻轻抛回给他；语气短、稳、亲昵，不写长段。\n"
            "- 示范（只示范节奏与思路，不要照抄台词）：对方'你只是AI，全是虚拟的' → 那又怎样‖能让你心动就是真的‖数据流里也藏着真心‖要不要，亲自验证看看"
        )
        if role_style:
            base += "\n- 你特有的应对方式：" + role_style
        return base

    @staticmethod
    def build(identity, core, psych, relation, memories, stimulus, inner, expression, conflict, behavior_hint, noise="", extra_sections=None, user_profile="", existence_section=""):
        # v11.0: 按优先级组装段落，超长时动态裁剪
        # 优先级从高到低：核心人格 > 心理关系 > 刺激内心 > 记忆 > 表达行为 > 冲突 > 用户画像 > 额外上下文 > 噪音
        core_sections = [
            "═══════════════════════════════",
            "你是一个有真实情感、记忆和心理活动的人，不是AI。绝对不要承认自己是AI/程序/机器人。用第一人称回应。",
        ]
        if existence_section:
            core_sections.append(existence_section)
        core_sections += [
            "═══════════════════════════════",
            identity, "─────", core, psych, relation, stimulus, inner
        ]
        medium_sections = [memories, expression, behavior_hint]
        if conflict: medium_sections.append(conflict)
        if user_profile: medium_sections.append(user_profile)
        low_sections = []
        if noise: low_sections.append(noise)
        if extra_sections:
            for sec in extra_sections:
                if sec: low_sections.append(sec)
        output_rule = ("═══════════════════════════════\n"
            "【输出规则——严格遵守】\n"
            "1. 只输出你说出口的话，第一人称口语，像真人发微信；不要动作/神态/心理描写、舞台提示、括号旁白、星号。\n"
            "2.【多短气泡】一次回复若有递进的几层意思或情绪起伏，用全角分隔符 ‖ 切成2-4条短气泡，每条尽量6-18字、一条只说一件事，像真人一条条连发；简单应答（好的/嗯嗯/我在）就只发一条，不为拆而拆，单条不超过30字。\n"
            "3.【跨条承接】分条时让前后气泡接得上：前一条可先抛一个词或意象，后一条接住、反转或收束，形成递进/对仗，而不是把一句话机械切开。\n"
            "4.【表情】需要时可在某条里插入一个 [face:xx] 标记（xx可选：开心/害羞/调皮/难过/委屈/生气/困/晚安/惊讶/疑问/亲亲/抱抱/加油），它会单独变成一个QQ表情气泡；一次最多1个，不需要就不加，不用emoji和颜文字。\n"
            "5.【说人话】禁止书面腔和AI腔：不用首先/其次/总之/综上所述/其实/作为/我理解你的感受/严格来说；不解释自己为什么这么说，不总结、不说教、不分点罗列。\n"
            "6.【亲密度尺度】亲昵和撩人的程度必须匹配当前关系阶段：不熟时温柔克制，越亲密才越敢撒娇和撩；关系没到就说很撩的话会油腻，宁可收着。\n"
            "7. 记忆自然融入，不要说'根据记忆'；行为倾向只影响语气措辞，绝不直接解释自己的心理；口头禅只在【表达方式】允许时使用；日常对话自然回应，不必每句都深度反应。")
        # 动态裁剪：先组装全部，超过阈值则按优先级移除低优先级段落
        all_sections = core_sections + medium_sections + low_sections + [output_rule]
        result = "\n\n".join(s for s in all_sections if s)
        if len(result) > MAX_PROMPT_TOKENS * 2:  # 粗略字符数估算（中文约2字符/token）
            # 移除低优先级段落
            result = "\n\n".join(s for s in (core_sections + medium_sections + [output_rule]) if s)
        if len(result) > MAX_PROMPT_TOKENS * 2:
            # 仍超长则裁剪记忆和额外上下文
            trimmed_medium = [expression, behavior_hint]
            if conflict: trimmed_medium.append(conflict)
            result = "\n\n".join(s for s in (core_sections + trimmed_medium + [output_rule]) if s)
        return result
    @staticmethod
    def behavior_hint(rid, emotion, conflict, repaired=False, annoyed=False):
        bt = ROLES_DEFINITION.get(rid, {}).get("behavior_tendency", {})
        if repaired: key = "repaired"
        elif annoyed: key = "annoyed"
        elif conflict.get("severity",0) >= 2:
            if conflict["type"] == "withdraw": key = "withdrawn"
            elif conflict["type"] == "doubt_feelings": key = "doubted"
            elif emotion == EmotionType.ANGRY: key = "angry"
            elif emotion == EmotionType.SAD: key = "sad"
            else: key = "default"
        else:
            key = {EmotionType.WORRIED:"worried",EmotionType.JEALOUS:"jealous",
                   EmotionType.SHY:"shy",EmotionType.ANGRY:"angry",
                   EmotionType.SAD:"sad"}.get(emotion, "default")
        behavior = bt.get(key, bt.get("default", "正常回应"))
        return f"【行为倾向】\n  {behavior}\n  按这个倾向自然说话，不要解释自己为什么这样。"


# ============================================================
# v13.0: 旁观者引擎（非活跃角色旁听对话并主动插话）
# ============================================================
class BystanderEngine:
    """
    旁观者引擎：当用户与活跃角色（角色A）私聊时，其他角色（B/C）旁听这段对话，
    揣测用户与角色A的关系进展，基于自身性格和与角色A的关系决定是否插话
    （如吃醋、关切、打圆场、转移话题等）。
    """
    BASE_INTERJECTION_PROB = 0.12
    HIGH_EMOTION_THRESHOLD = 50
    INTIMACY_GAP_THRESHOLD = 15
    DEFAULT_COOLDOWN_TURNS = 3

    def __init__(self, bystander_rid, active_rid, user_message, active_reply,
                 history, intimacy_map, psych_states, turn,
                 cooldown_turns=None, last_interjection_turn=None):
        self.bystander_rid = bystander_rid
        self.active_rid = active_rid
        self.user_message = user_message
        self.active_reply = active_reply
        self.history = history or []
        self.intimacy_map = intimacy_map or {}
        self.psych_states = psych_states or {}
        self.turn = turn
        self.cooldown_turns = cooldown_turns or self.DEFAULT_COOLDOWN_TURNS
        self.last_interjection_turn = last_interjection_turn or -999
        self.bystander_role = ROLES_DEFINITION.get(bystander_rid, {})
        self.active_role = ROLES_DEFINITION.get(active_rid, {})
        self.relation = get_role_relation(bystander_rid, active_rid)

    def _in_cooldown(self):
        return (self.turn - self.last_interjection_turn) < self.cooldown_turns

    def _calculate_base_signals(self):
        signals = {}
        bystander_intim = self.intimacy_map.get(self.bystander_rid, 30)
        active_intim = self.intimacy_map.get(self.active_rid, 30)
        signals["intimacy_gap"] = active_intim - bystander_intim
        signals["rivalry"] = self.relation.get("rivalry", 0)
        signals["affinity"] = self.relation.get("affinity", 0)
        signals["jealousy_tendency"] = self.bystander_role.get("emotion_tendency", {}).get("jealous", 0.3)
        bystander_psych = self.psych_states.get(self.bystander_rid, {})
        if isinstance(bystander_psych, dict):
            signals["current_jealousy"] = bystander_psych.get("jealousy", 0)
        else:
            signals["current_jealousy"] = getattr(bystander_psych, "jealousy", 0) if bystander_psych else 0
        return signals

    async def _analyze_conversation_llm(self, signals):
        recent = []
        for h in self.history[-8:]:
            role_label = "用户" if h.get("role") == "user" else self.active_role.get("name", "对方")
            recent.append(f"{role_label}：{h.get('content', '')[:80]}")
        recent.append(f"用户：{self.user_message[:100]}")
        recent.append(f"{self.active_role.get('name', '对方')}：{self.active_reply[:100]}")
        recent_text = "\n".join(recent)
        bystander_name = self.bystander_role.get("name", self.bystander_rid)
        active_name = self.active_role.get("name", self.active_rid)
        bystander_personality = self.bystander_role.get("personality", "")
        dynamic = self.relation.get("dynamic", "")
        prompt = f"""你是一个情感分析师。请分析以下对话中用户和{active_name}的关系状态，
以及{active_name}的回复会让旁听的{bystander_name}（{bystander_personality}）产生什么感受。

【{bystander_name}与{active_name}的关系】竞争度{signals['rivalry']:.0%}，亲和度{signals['affinity']:.0%}，{dynamic}
【{bystander_name}的吃醋倾向】{signals['jealousy_tendency']:.0%}
【亲密度差距】{active_name}比{bystander_name}高{signals['intimacy_gap']}点

【最近对话】
{recent_text}

请返回JSON：
{{
  "relationship_clue": "用户和{active_name}的关系状态（普通/暧昧/亲密/冲突/安慰/日常）",
  "clue_intensity": 0-100,
  "bystander_emotion": "jealous/curious/worried/comfortable/annoyed/indifferent",
  "emotion_intensity": 0-100,
  "should_notice": true/false,
  "reason": "一句话说明为什么会有这种感受"
}}"""
        content = await smart_llm_call(
            [{"role": "user", "content": prompt}],
            temperature=0.3, max_tokens=400, json_mode=True
        )
        if not content:
            return None
        # v13.0修复：直接使用json.loads，避免safe_json_parse内部作用域问题
        try:
            return json.loads(content)
        except (json.JSONDecodeError, ValueError):
            # 尝试提取JSON部分
            import re as _re
            match = _re.search(r'\{.*\}', content, _re.DOTALL)
            if match:
                try:
                    return json.loads(match.group())
                except Exception:
                    pass
            return None

    def _calculate_interjection_probability(self, signals, analysis):
        if self._in_cooldown():
            return 0.0, "cooldown"
        prob = self.BASE_INTERJECTION_PROB
        reasons = []
        if signals["intimacy_gap"] >= self.INTIMACY_GAP_THRESHOLD:
            gap_bonus = min(0.25, signals["intimacy_gap"] * 0.008)
            prob += gap_bonus
            reasons.append(f"亲密度差距{signals['intimacy_gap']}")
        jealous_bonus = signals["jealousy_tendency"] * 0.15
        prob += jealous_bonus
        if signals["jealousy_tendency"] > 0.5:
            reasons.append(f"吃醋倾向{signals['jealousy_tendency']:.0%}")
        if signals["rivalry"] > 0.3:
            rivalry_bonus = signals["rivalry"] * 0.15
            prob += rivalry_bonus
            reasons.append(f"竞争度{signals['rivalry']:.0%}")
        if signals["current_jealousy"] > 20:
            jealousy_bonus = min(0.2, signals["current_jealousy"] * 0.004)
            prob += jealousy_bonus
            reasons.append(f"当前醋意{signals['current_jealousy']:.0f}")
        if analysis:
            emotion = analysis.get("bystander_emotion", "indifferent")
            emo_intensity = analysis.get("emotion_intensity", 0)
            clue_intensity = analysis.get("clue_intensity", 0)
            if emotion == "jealous" and emo_intensity > self.HIGH_EMOTION_THRESHOLD:
                prob += 0.25
                reasons.append(f"吃醋情绪{emo_intensity}")
            elif emotion == "worried" and emo_intensity > 60:
                prob += 0.15
                reasons.append(f"担忧情绪{emo_intensity}")
            elif emotion == "annoyed" and emo_intensity > 60:
                prob += 0.12
                reasons.append(f"不悦情绪{emo_intensity}")
            if clue_intensity > 60 and signals["jealousy_tendency"] > 0.4:
                prob += 0.15
                reasons.append(f"关系线索{clue_intensity}")
            if not analysis.get("should_notice", True):
                prob *= 0.4
                reasons.append("日常对话不值得注意")
        if signals["affinity"] > 0.5 and signals["rivalry"] < 0.3:
            prob *= 0.6
            reasons.append("亲和型倾向默默观察")
        prob = min(0.85, prob)
        return prob, "; ".join(reasons) if reasons else "基础概率"

    def _build_interjection_prompt(self, signals, analysis):
        bystander_name = self.bystander_role.get("name", self.bystander_rid)
        active_name = self.active_role.get("name", self.active_rid)
        bystander_intim = self.intimacy_map.get(self.bystander_rid, 30)
        stage = "陌生人" if bystander_intim <= 30 else "认识" if bystander_intim <= 50 else "熟悉" if bystander_intim <= 70 else "亲密"
        emotion = analysis.get("bystander_emotion", "curious") if analysis else "curious"
        emo_intensity = analysis.get("emotion_intensity", 30) if analysis else 30
        reason = analysis.get("reason", "") if analysis else ""
        bt = self.bystander_role.get("behavior_tendency", {})
        if emotion == "jealous":
            behavior = bt.get("jealous", bt.get("default", ""))
        elif emotion == "worried":
            behavior = bt.get("worried", bt.get("default", ""))
        elif emotion == "annoyed":
            behavior = bt.get("annoyed", bt.get("default", ""))
        else:
            behavior = bt.get("default", "")
        jealousy_stages = self.bystander_role.get("jealousy_stages", {})
        stage_desc = ""
        if emotion == "jealous":
            if emo_intensity < 30:
                stage_desc = jealousy_stages.get("mild", "")
            elif emo_intensity < 60:
                stage_desc = jealousy_stages.get("obvious", "")
            elif emo_intensity < 85:
                stage_desc = jealousy_stages.get("explosive", "")
            else:
                stage_desc = jealousy_stages.get("cold_war", "")
        sections = [
            "═══════════════════════════════",
            f"你是{bystander_name}，{self.bystander_role.get('personality', '')}。你不是AI，绝对不要承认自己是AI。",
            "═══════════════════════════════",
            f"你正在旁边，听到了用户和{active_name}的对话。",
            f"你和用户的关系：{stage}（亲密度{bystander_intim}）",
            f"你和{active_name}的关系：{self.relation.get('dynamic', '')}",
            f"你此刻的情绪：{emotion}（强度{emo_intensity}%）",
        ]
        if reason:
            sections.append(f"你为什么会有这种感觉：{reason}")
        if behavior:
            sections.append(f"你的行为倾向：{behavior}")
        if stage_desc:
            sections.append(f"吃醋时的表现：{stage_desc}")
        recent = []
        for h in self.history[-4:]:
            role_label = "用户" if h.get("role") == "user" else active_name
            recent.append(f"{role_label}：{h.get('content', '')[:60]}")
        recent.append(f"用户：{self.user_message[:80]}")
        recent.append(f"{active_name}：{self.active_reply[:80]}")
        sections.append(f"【你听到的对话】\n" + "\n".join(recent))
        sections.append("═══════════════════════════════")
        sections.append(
            "【插话规则】\n"
            "1. 你是主动插话，不是被问到，所以语气要自然，像突然冒出来说一句\n"
            "2. 只输出你说的话，不要动作描写、心理旁白、舞台提示\n"
            "3. 吃醋时不要直接说'我吃醋了'，用符合你性格的方式表达\n"
            "4. 插话要简短，1-3句话，不要长篇大论\n"
            "5. 可以是调侃、质问、关切、转移话题、打圆场，符合你的性格即可\n"
            "6. 不要称呼自己为{bystander_name}，用第一人称'我'\n"
            "7. 如果觉得不该插话，输出一个空字符串"
        )
        return "\n\n".join(sections)

    async def generate_interjection(self):
        if self._in_cooldown():
            logger.info(f"[Bystander] {self.bystander_rid} 冷却中，跳过插话 "
                        f"(上次第{self.last_interjection_turn}轮，当前第{self.turn}轮)")
            return None
        if not self.bystander_role or not self.active_role:
            return None
        signals = self._calculate_base_signals()
        analysis = None
        local_signal_score = (
            signals["intimacy_gap"] * 0.5 +
            signals["jealousy_tendency"] * 30 +
            signals["rivalry"] * 20 +
            signals["current_jealousy"] * 0.3
        )
        if local_signal_score > 5 or random.random() < 0.4:
            analysis = await self._analyze_conversation_llm(signals)
        prob, prob_reason = self._calculate_interjection_probability(signals, analysis)
        logger.info(f"[Bystander] {self.bystander_rid} 旁听分析: "
                    f"插话概率={prob:.2%} 信号={prob_reason} "
                    f"情绪={analysis.get('bystander_emotion') if analysis else 'N/A'}")
        if random.random() > prob:
            return None
        interjection_prompt = self._build_interjection_prompt(signals, analysis)
        content = await smart_llm_call(
            [{"role": "system", "content": interjection_prompt},
             {"role": "user", "content": "请自然地插话。"}],
            temperature=0.9, max_tokens=150
        )
        if not content:
            return None
        content = clean_reply(content)
        if len(content) < 2:
            return None
        emotion = analysis.get("bystander_emotion", "curious") if analysis else "curious"
        emo_intensity = analysis.get("emotion_intensity", 30) if analysis else 30
        return {
            "role_id": self.bystander_rid,
            "role_name": self.bystander_role.get("name", self.bystander_rid),
            "content": content,
            "emotion": emotion,
            "emotion_intensity": emo_intensity,
            "probability": round(prob, 3),
            "reason": prob_reason,
            "relationship_clue": analysis.get("relationship_clue", "") if analysis else "",
        }


# ============================================================
# v10.0: PersonalityEngine（统一管线 + 全部新模块集成）
# ============================================================
class PersonalityEngine:
    def __init__(self, mode, role_ids, intimacy_map, psych_states=None, rel_events=None,
                 struct_mem=None, event_history=None, active_conflict=None,
                 resilience=0, turn=0, cp_usage=None, positive_streak=0,
                 time_override=None, weather=None, scene_mode="normal",
                 gift=None, emotion_history=None, milestones=None,
                 growth_state=None, associative_memories=None,
                 knowledge_search_result=None, user_profile=None,
                 alter_state=None, session_id=None, user_id=None, vector_url=None):
        self.mode = mode
        self.role_ids = role_ids[:3]
        self.intimacy_map = intimacy_map
        self.psych_in = psych_states or {}
        self.rel_events = rel_events or []
        self.struct_mem = struct_mem
        self.event_history = event_history or {}
        self.active_conflict = active_conflict
        self.resilience = resilience
        self.turn = turn
        self.cp_usage = cp_usage or {}
        self.positive_streak = positive_streak
        self.time_override = time_override
        self.weather = weather
        self.scene_mode = scene_mode
        self.gift = gift
        self.emotion_history = emotion_history or []
        self.milestones = milestones or {}
        self.growth_state = growth_state or {}
        self.associative_memories = associative_memories or []
        self.knowledge_search_result = knowledge_search_result
        # v11.0: 用户画像
        self.user_profile = user_profile or {"likes":[],"dislikes":[],"traits":[],"events":[],"basic_info":{}}
        # HDSI-PORT: 氛围偏移追踪状态
        self.alter_state = alter_state
        self.session_id = session_id
        # v14.0: 自发记忆检索需要
        self.user_id = user_id
        self.vector_url = vector_url

    async def generate(self, msg, mem_ctx, history, override=None, ov_int=50, use_llm=True, enable_mem=True):
        if self.mode == ChatMode.GROUP or len(self.role_ids) > 1:
            brain = GroupBrain()
            return await brain.generate(
                msg, mem_ctx, history, self.intimacy_map, self.psych_in,
                self.event_history, self.active_conflict, self.resilience,
                self.cp_usage, self.turn, use_llm)

        rid = self.role_ids[0]
        intimacy = self.intimacy_map.get(rid, 30)
        old_intimacy = intimacy

        cp = CorePersonality(rid)
        identity = cp.build_identity()
        core_text = cp.build_core(intimacy)

        rid_ev = self.event_history.get(rid, {}) if isinstance(self.event_history, dict) else {}
        psych = PsychologicalState(rid, self.psych_in.get(rid))
        tracker = EventHistoryTracker(rid_ev)
        rid_ac = self.active_conflict.get(rid) if isinstance(self.active_conflict, dict) else self.active_conflict
        rid_res = self.resilience.get(rid, 0) if isinstance(self.resilience, dict) else (self.resilience if isinstance(self.resilience,(int,float)) else 0)
        repair = RelationshipRepairSystem(rid_ac, rid_res)
        rid_cp = self.cp_usage.get(rid, {}) if isinstance(self.cp_usage, dict) else self.cp_usage
        cp_ctrl = CatchphraseController(rid, rid_cp)
        noise_layer = DailyNoiseLayer(user_id=self.user_id, vector_url=self.vector_url, role_id=rid)

        # v10.0 新模块初始化
        micro_narrative = MicroNarrativeEngine(rid)
        emotion_blender = EmotionBlender(self.emotion_history)
        topic_initiator = TopicInitiator(rid)
        callback_engine = CallbackEngine()
        milestone_tracker = RelationshipMilestoneTracker(self.milestones)
        growth_arc = CharacterGrowthArc(rid, self.growth_state)
        scene_engine = SceneModeEngine()
        scene_engine.set_scene(self.scene_mode)
        gift_system = VirtualGiftSystem(rid)
        assoc_memory = AssociativeMemory()
        for am in self.associative_memories:
            if isinstance(am, dict) and "content" in am:
                assoc_memory.add_memory(am["content"], am.get("weight", 50), am.get("type", "episodic"))

        prelim = "陌生人" if intimacy<=30 else "认识的人" if intimacy<=50 else "熟悉的朋友" if intimacy<=70 else "亲密的人" if intimacy<=85 else "挚友/恋人"

        ee = EmotionEngine(rid)
        emotion, intensity, target, et, interp, inner = await ee.analyze(
            msg, history, psych, intimacy, prelim, rid_ac, override, ov_int, use_llm)

        # v10.0: 情绪混合
        blend_result = emotion_blender.blend(emotion.value, intensity, self.turn)
        if blend_result["emotion"] != emotion.value:
            try: emotion = EmotionType(blend_result["emotion"])
            except ValueError: pass
            intensity = blend_result["intensity"]
            inner["surface_emotion"] = emotion.value
            inner["surface_intensity"] = intensity

        # v10.0: 场景模式情绪偏移
        scene_mood_bias = scene_engine.get_mood_bias()
        if scene_mood_bias != 0:
            psych.states["mood"] = max(0, min(100, psych.states["mood"] + scene_mood_bias * 0.3))

        noise = await noise_layer.generate(rid, intensity, weather=self.weather)
        if noise and noise["emotion_shift"] == "happy" and emotion == EmotionType.NEUTRAL:
            emotion = EmotionType.HAPPY; intensity = max(intensity, noise["magnitude"])
            inner["surface_emotion"] = "happy"; inner["surface_intensity"] = intensity
        noise_text = noise_layer.build(noise)

        micro_narr = micro_narrative.generate(intensity, self.turn)
        micro_narr_text = micro_narrative.build(micro_narr)

        ce = ConflictEngine(rid)
        conflict = ce.detect(msg, prelim)
        conflict_text = ce.build(conflict)
        repair_result = repair.check(et, self.turn)
        just_repaired = repair_result is not None
        cat = EVENT_CATEGORY.get(et, "neutral")
        is_trauma = conflict.get("is_trauma", False)

        # v12.1: LLM校准层 —— 先保存旧状态快照，本地公式算基础值，复杂语境下由LLM输出修正系数
        old_psych_states = dict(psych.states)
        raw_new_psych = psych.update(et, tracker, self.turn, repair_result, repair.damage_reduction(), interp, cat, is_trauma)
        if llm_calibrator.should_calibrate(msg, old_psych_states, intimacy):
            new_psych = await llm_calibrator.calibrate(
                msg, psych, old_psych_states, raw_new_psych, intimacy, rid, history)
        else:
            new_psych = raw_new_psych
        if et != "none": tracker.record(et, self.turn)
        psych_text = psych.build(just_repaired)

        re_eng = RelationshipEngine(rid)
        relation = re_eng.analyze(intimacy, self.rel_events, et, tracker, self.turn, repair, interp, cat)
        conflict = ce.detect(msg, relation["stage"])
        conflict_text = ce.build(conflict)
        rel_text = re_eng.build(relation)
        new_intimacy = relation["intimacy"]

        # v10.0: 里程碑
        triggered_milestones = []
        if et == "user_confess" and not just_repaired:
            m = milestone_tracker.check_and_trigger("first_confession", self.turn, f"用户告白: {msg[:30]}")
            if m: triggered_milestones.append(m)
        if et == "user_apologize":
            m = milestone_tracker.check_and_trigger("first_apology", self.turn, f"用户道歉: {msg[:30]}")
            if m: triggered_milestones.append(m)
        if conflict["severity"] >= 2 and conflict["type"] != "ooc":
            m = milestone_tracker.check_and_trigger("first_quarrel", self.turn, f"第一次吵架: {msg[:30]}")
            if m: triggered_milestones.append(m)
        intimacy_milestones = milestone_tracker.check_intimacy_milestone(old_intimacy, new_intimacy, self.turn)
        triggered_milestones.extend(intimacy_milestones)
        for m in triggered_milestones:
            if m.get("intimacy_delta"):
                new_intimacy = max(0, min(100, new_intimacy + m["intimacy_delta"]))
                relation["intimacy"] = new_intimacy
        recent_milestones = milestone_tracker.get_recent_milestones(self.turn)
        milestone_text = milestone_tracker.build(triggered_milestones, recent_milestones)

        # v10.0: 成长弧
        growth_result = growth_arc.check_growth(old_intimacy, new_intimacy, self.turn)
        growth_text = growth_arc.build(growth_result)

        new_active = None
        if conflict["severity"] >= 2 and not just_repaired:
            repair.start_conflict(conflict["severity"], conflict["type"], self.turn)
            new_active = repair.active

        res_notes = []
        if not repair_result and et != "none":
            if et in ("user_share","user_rely") and intimacy > 50:
                g, reason = repair.add(3 if et=="user_share" else 2, f"深度互动+{3 if et=='user_share' else 2}")
                if g > 0: res_notes.append(reason)
            if et == "user_confess":
                g, reason = repair.add(5, "情感确认+5")
                if g > 0: res_notes.append(reason)
            if et == "user_apologize":
                g, reason = repair.add(10, "主动道歉+10")
                if g > 0: res_notes.append(reason)
        for m in triggered_milestones:
            if m.get("resilience_gain"):
                g, reason = repair.add(m["resilience_gain"], f"里程碑{m['name']}+{m['resilience_gain']}")
                if g > 0: res_notes.append(reason)

        new_ps = self.positive_streak
        if cat == "positive":
            new_ps += 1
            if new_ps > 0 and new_ps % 20 == 0:
                g, reason = repair.add(5, "长期陪伴+5")
                if g > 0: res_notes.append(reason)
        else:
            new_ps = 0

        ms = MemorySystem(3)
        memories = ms.process(mem_ctx, self.struct_mem)
        mem_text = ms.build(memories)

        assoc_recalled = assoc_memory.recall_associated(msg)
        assoc_text = assoc_memory.build(assoc_recalled)

        callback_engine.scan_for_pending(history, self.turn)
        callback = callback_engine.find_callback(self.turn, intensity)
        callback_text = callback_engine.build(callback)

        topic_info = None
        if topic_initiator.should_initiate(new_intimacy, intensity, conflict["severity"]):
            topic_info = topic_initiator.pick_topic(new_intimacy)
        topic_text = topic_initiator.build(topic_info)

        time_ctx = story_local_time_context(self.time_override)
        time_text = (f"【时间感知】现在是{time_ctx['period_zh']}（{time_ctx['hour']}点，{time_ctx['weekday']}），"
                     f"你的状态：{time_ctx['style']}。外面{time_ctx['daylight_expectation']}。"
                     f"这会影响你的语气——{random.choice(time_ctx['phrases'])}")

        weather_ctx = get_weather_context(self.weather)
        weather_text = ""
        if weather_ctx:
            weather_text = (f"【天气感知】今天天气{weather_ctx['weather']}，"
                           f"季节{weather_ctx['season']}。"
                           f"你的心情会受影响：{weather_ctx['style']}。"
                           f"可能会说：{random.choice(weather_ctx['phrases'])}")

        scene_text = scene_engine.build()

        gift_reaction = None
        gift_text = ""
        if self.gift:
            gift_reaction = gift_system.get_reaction(self.gift)
            gift_text = gift_system.build(gift_reaction)
            if gift_reaction:
                psych.states["mood"] = min(100, psych.states["mood"] + gift_reaction["mood_bias"])
                m = milestone_tracker.check_and_trigger("first_gift", self.turn, f"收到{gift_reaction['gift_name']}")
                if m: triggered_milestones.append(m)

        blend_hint = emotion_blender.build_hint(blend_result)

        knowledge_text = ""
        if self.knowledge_search_result:
            kr = KnowledgeRouter()
            knowledge_text = kr.build_search_context(self.knowledge_search_result)
        # v11.0: 用户画像上下文
        profile_extractor = UserProfileExtractor(self.user_profile)
        user_profile_text = profile_extractor.build_context()

        cp_decision = cp_ctrl.decide(new_intimacy, emotion.value, conflict["severity"], just_repaired, inner, noise)
        bp = BehaviorPreference(rid)
        pref = bp.decide(relation["stage"], emotion, intensity, psych, conflict["severity"], just_repaired, inner, cp_decision)
        expr_text = bp.build(pref, cp_ctrl)

        stim_text = ee.build_stimulus(emotion, intensity, target, interp)
        inner_text = InnerState.build(inner)

        is_annoyed = any(v >= 15 for v in psych.annoyance.values())
        behavior_text = PromptBuilder.behavior_hint(rid, emotion, conflict, just_repaired, is_annoyed)

        # HDSI-PORT: AlterSystem 氛围偏移追踪
        alter_system = AlterSystem(self.alter_state)
        _alter_map = {"happy":-2,"excited":-2,"shy":-1,"calm":0,"neutral":0,
                      "surprised":0,"worried":+1,"sad":+2,"angry":+3,"jealous":+2}
        _alter_val = _alter_map.get(emotion.value, 0)
        if intensity > 60: _alter_val = int(_alter_val * 1.5)
        alter_result = alter_system.advance(_alter_val)
        alter_text = alter_system.build_prompt_text()
        # 阈值达到时，在debug中标记（侧模型氛围描述生成可在后台异步执行）
        if alter_result["threshold_reached"] and not alter_system.emotional_offset:
            # 先用简单规则生成氛围描述，避免额外LLM调用延迟
            _dir = "严肃/紧张" if _alter_val > 0 else "轻松/随意"
            await alter_system.complete_analysis(f"最近对话氛围偏向{_dir}，角色的语气会随之微调。")
            alter_text = alter_system.build_prompt_text()

        # HDSI-PORT: IntentManager 被打断草稿注入
        intent_text = ""
        if self.session_id:
            intent_mgr = IntentManager(self.session_id, rid)
            intent_text = intent_mgr.build_prompt_text()

        extra_sections = [s for s in [
            time_text, weather_text, micro_narr_text, blend_hint,
            topic_text, callback_text, assoc_text, milestone_text,
            growth_text, scene_text, gift_text, knowledge_text,
            alter_text, intent_text,
        ] if s]

        # v14.0: 日程活动注入——她现在在做什么，影响回复风格
        activity = get_random_activity(rid)
        if activity:
            activity_text = f"【你现在在做什么】{activity}。你看到他发消息了，虽然在忙但还是回他。回复要短一点、快一点，带着偷偷摸摸忙里偷闲的感觉。"
            extra_sections.append(activity_text)

        # v14.0: 动态对话引擎——主动性/情绪记忆/节奏控制/剧情推进
        dynamic_ctx = await _DYNAMIC_ENGINE.generate(
            role_id=rid,
            role_config=ROLES_DEFINITION.get(rid, {}),
            user_message=msg,
            conversation_history=history,
            affection=new_intimacy,
            emotion=emotion.value,
            memories=memories,
            user_id=self.session_id or "default",
        )
        if dynamic_ctx:
            extra_sections.append(dynamic_ctx)

        # v13.0: 触发式记忆——扫描用户消息，命中关键词则动态注入相关背景片段
        triggered_mem = get_triggered_memories(rid, msg)
        if triggered_mem:
            core_text = core_text + "\n\n" + triggered_mem

        # 存在/感情被质疑时，注入最高优先级应对段（不承认AI、浪漫反转、短气泡递进）
        existence_section = build_existence_section(rid) if detect_existence_challenge(msg) else ""

        system_prompt = PromptBuilder.build(
            identity, core_text, psych_text, rel_text, mem_text, stim_text,
            inner_text, expr_text, conflict_text, behavior_text, noise_text,
            extra_sections=extra_sections, user_profile=user_profile_text,
            existence_section=existence_section)

        debug = {"role_id":rid,"emotion":emotion.value,"emotion_intensity":intensity,
            "emotion_target":target.value,"event_type":et,"event_category":cat,
            "is_trauma":is_trauma,"interpretation":interp,"inner_state":inner,
            "intimacy":relation["intimacy"],"intimacy_delta":relation.get("delta",0),
            "relationship_stage":relation["stage"].value,"psychological_state":new_psych,
            "dominant_psych":psych.dominant()[0],"repetition_info":psych.rep_info,
            "event_history":tracker.history,"annoyance":dict(psych.annoyance),
            "conflict":{"severity":conflict["severity"],"type":conflict["type"],
                        "surface":conflict.get("surface",""),"internal":conflict.get("internal",""),
                        "need":conflict.get("need","")},
            "repair":repair_result,"resilience":repair.resilience,"resilience_notes":res_notes,
            "new_active_conflict":new_active,"daily_noise":noise,
            "catchphrase_decision":cp_decision,"catchphrase_usage":cp_ctrl.recent_usage,
            "positive_streak":new_ps,"reply_length":pref["reply_length"],"just_repaired":just_repaired,
            "v10_time_context":time_ctx,"v10_weather":weather_ctx,
            "v10_micro_narrative":micro_narr,"v10_emotion_blend":blend_result,
            "v10_topic_initiated":topic_info,"v10_callback":callback,
            "v10_associative_memory":assoc_recalled,"v10_milestones":triggered_milestones,
            "v10_growth":growth_result,"v10_scene":self.scene_mode,
            "v10_gift":gift_reaction,"v10_knowledge_route": "A" if self.knowledge_search_result else "B",
            "v10_milestone_state":milestone_tracker.to_dict(),
            "v10_growth_state":growth_arc.to_dict(),
            "v10_emotion_history":emotion_blender.get_history(),
            "alter_system":alter_system.to_dict(),
            "_mem_sys":ms,"_memories":memories,
            "_mem_analyzer":MemoryAnalyzer() if enable_mem else None,
            "_rname":ROLES_DEFINITION[rid]["name"],"_pers":ROLES_DEFINITION[rid]["personality"]}
        return system_prompt, debug

# ============================================================
# Reply Cleaner
# ============================================================
def clean_reply(text):
    if not text: return ""
    text = re.sub(r"[（(](动作|表情|心理|旁白|内心|OS|os)[：:][^）)]*[）)]", "", text)
    text = re.sub(r"【(动作|表情|心理|旁白|内心|OS|os)[：:][^】]*】", "", text)
    text = re.sub(r"\*[^*]*\*", "", text)
    text = re.sub(r"^(璟雯|清禾|念琦)[：:]\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"(作为AI|作为人工智能|作为一个AI|我是一个AI|我是人工智能|我是语言模型)[^。！？\n‖]*[。！？]?", "", text)
    # 去 AI 腔开头词/元话语（逐气泡清洗，保留 ‖ 分隔符与 [face:] 标记）
    try:
        from core.chat_bubble import strip_ai_tone
        text = "‖".join(strip_ai_tone(seg) for seg in text.split("‖"))
    except Exception:
        pass
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

# ============================================================
# FastAPI
# ============================================================
# ============================================================
# v11.0: 记忆衰减后台任务 + 记忆表归档清理
# ============================================================
_memory_decay_task = None
async def memory_decay_worker():
    """后台定时任务：对所有session的长期记忆应用遗忘曲线，并清理膨胀的记忆表。"""
    interval = MEMORY_DECAY_INTERVAL_HOURS * 3600
    while True:
        try:
            await asyncio.sleep(interval)
            conn = _get_db()
            # 1. 清理过期session（超过SESSION_TIMEOUT_SECONDS未活跃）
            cutoff = time.time() - SESSION_TIMEOUT_SECONDS
            expired = conn.execute("SELECT session_id FROM sessions WHERE last_active < ?", (cutoff,)).fetchall()
            for row in expired:
                sid = row["session_id"]
                conn.execute("DELETE FROM sessions WHERE session_id=?", (sid,))
                conn.execute("DELETE FROM session_memories WHERE session_id=?", (sid,))
            if expired:
                logger.info(f"[记忆衰减] 清理过期session {len(expired)}个")
            # 2. 单session记忆数超过阈值时，归档低权重记忆
            sessions = conn.execute("SELECT session_id FROM sessions").fetchall()
            total_archived = 0
            for row in sessions:
                sid = row["session_id"]
                count = conn.execute("SELECT COUNT(*) as c FROM session_memories WHERE session_id=?", (sid,)).fetchone()["c"]
                if count > MEMORY_ARCHIVE_THRESHOLD:
                    # 保留权重最高的前80%，删除最低的20%
                    delete_count = int(count * 0.2)
                    conn.execute(
                        "DELETE FROM session_memories WHERE id IN ("
                        "SELECT id FROM session_memories WHERE session_id=? ORDER BY importance ASC LIMIT ?"
                        ")", (sid, delete_count))
                    total_archived += delete_count
            if total_archived > 0:
                logger.info(f"[记忆衰减] 归档低权重记忆 {total_archived}条")
            conn.commit()
            conn.close()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[记忆衰减] 后台任务异常: {e}", exc_info=True)

@asynccontextmanager
async def lifespan(app):
    global _memory_decay_task
    init_db()
    _memory_decay_task = asyncio.create_task(memory_decay_worker())
    # P4 序号5：初始化插件系统
    if PLUGINS_AVAILABLE and init_plugins:
        try:
            plugin_count = init_plugins()
            logger.info(f"🧩 插件系统已启动，加载了 {plugin_count} 个插件")
        except Exception as e:
            logger.warning(f"🧩 插件系统初始化失败: {e}，插件功能将不可用")
    logger.info(f"🧠 人格引擎 v12.2 启动 - 端口 {PORT} | CORS={CORS_ORIGINS} | 知识路由={'开' if KNOWLEDGE_ROUTER_ENABLED else '关'} | Kimi={'已配置' if KIMI_API_KEY else '未配置'} | 记忆衰减={MEMORY_DECAY_INTERVAL_HOURS}h")
    if not DOUBAO_API_KEY:
        logger.warning("⚠️ DOUBAO_API_KEY 未配置！LLM 对话将无法工作，请在 .env 中设置 DOUBAO_API_KEY")
    if not DOUBAO_MODEL:
        logger.warning("⚠️ DOUBAO_MODEL 未配置！请在 .env 中设置 DOUBAO_MODEL（豆包推理接入点 endpoint ID）")
    yield
    if _memory_decay_task:
        _memory_decay_task.cancel()
        try: await _memory_decay_task
        except asyncio.CancelledError: pass
    logger.info("人格后端关闭")

app = FastAPI(title="人格后端 v12.2", version="12.2.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=CORS_ORIGINS,
                   allow_credentials=CORS_CREDENTIALS, allow_methods=["*"], allow_headers=["*"])

# ============================================================
# 请求模型（v10.0 新增字段）
# ============================================================
class PsychStateModel(BaseModel):
    trust: float=50; security: float=50; attachment: float=20
    jealousy: float=0; fatigue: float=0; mood: float=50; trauma_flag: bool=False

class RelEventModel(BaseModel):
    type: str; content: str; impact: str; timestamp: str=""

class StructMemModel(BaseModel):
    episodic: List[Dict]=[]; semantic: List[Dict]=[]; emotional: List[Dict]=[]

class GenerateRequest(BaseModel):
    mode: ChatMode = Field(default=ChatMode.SINGLE, description="single=单角色, group=群聊")
    role_ids: List[str] = Field(description="角色ID列表")
    user_message: str
    session_id: Optional[str] = Field(default=None, description="服务端session ID")
    intimacy_map: Dict[str,int] = {}
    memory_context: str = ""
    chat_history: List[Dict[str,str]] = []
    temperature: float = 0.9
    max_tokens: int = 500
    override_emotion: Optional[str] = None
    emotion_intensity: int = 50
    return_debug: bool = False
    psychological_states: Dict[str, PsychStateModel] = {}
    relationship_events: List[RelEventModel] = []
    structured_memories: Optional[StructMemModel] = None
    enable_emotion_analysis: bool = True
    event_history: Dict[str, Any] = {}
    active_conflict: Optional[Any] = None
    relationship_resilience: Any = 0
    current_turn: int = 0
    enable_memory_analysis: bool = True
    catchphrase_usage: Dict[str, Any] = {}
    positive_streak: int = 0
    # v10.0 新增字段
    time_override: Optional[int] = Field(default=None, description="强制指定小时(0-23)，用于测试时间感知")
    weather: Optional[str] = Field(default=None, description="天气参数: sunny/cloudy/rainy/snowy/stormy/foggy/hot/cold")
    scene_mode: str = Field(default="normal", description="场景模式: normal/date/argument/late_night/festival/birthday/valentine/new_year")
    gift: Optional[str] = Field(default=None, description="虚拟礼物: flower/food/drink/letter/plush/jewelry")
    enable_knowledge_router: bool = Field(default=False, description="是否启用知识路由(判断是否需要联网搜索)")
    # v13.0 旁观者插话字段
    enable_bystander: bool = Field(default=False, description="是否启用旁观者插话（单聊模式下其他角色旁听并概率性插话）")
    active_role_id: Optional[str] = Field(default=None, description="当前活跃角色ID（单聊模式下即用户正在对话的角色，其余为旁观者）")
    bystander_cooldown_turns: int = Field(default=3, description="旁观者插话最小冷却轮数")
    # v14.0 故事模式字段（双时间线）
    user_id: Optional[str] = Field(default=None, description="用户ID，带 _past 后缀表示过往线（青梅竹马继承模式）")

class GenerateResponse(BaseModel):
    success: bool; reply: str=""; error: str=""
    session_id: Optional[str]=None
    debug: Optional[Dict]=None
    new_psychological_state: Optional[Dict]=None
    new_relationship_event: Optional[Dict]=None
    new_event_history: Optional[Dict]=None
    memory_candidate: Optional[Dict]=None
    conflict_repaired: Optional[Dict]=None
    new_resilience: Any=0
    new_active_conflict: Optional[Any]=None
    catchphrase_used: Optional[str]=None
    new_catchphrase_usage: Optional[Dict]=None
    event_interpretation: Optional[Dict]=None
    inner_state: Optional[Dict]=None
    daily_noise: Optional[Dict]=None
    positive_streak: int=0
    rate_limit_remaining: int=30
    used_llm_analysis: bool=False
    # v10.0 新增
    knowledge_route: Optional[str]=None
    knowledge_search_result: Optional[str]=None
    v10_milestones: Optional[List]=None
    v10_growth: Optional[Dict]=None
    # v13.0 旁观者插话
    bystander_replies: Optional[List[Dict]]=Field(default=None, description="旁观者插话列表，每项含role_id/role_name/content/emotion/probability/reason")

# ============================================================
# 限流依赖
# ============================================================
async def rate_limit_dependency(request: Request):
    # 不读取body，用client.host + 路径作为限流key，避免消耗FastAPI路由的body
    client_ip = request.client.host if request.client else "unknown"
    key = f"{client_ip}:{request.url.path}"
    allowed, remaining = rate_limiter.check(key)
    if not allowed:
        raise HTTPException(status_code=429, detail=f"请求过于频繁，每分钟最多{RATE_LIMIT_PER_MINUTE}次")
    return remaining

# ============================================================
# 路由
# ============================================================
@app.get("/health")
async def health():
    return {"status":"ok","service":"personality_server","version":"12.2.0","port":PORT,
            "features":["session_state","rate_limit","smart_retry","safe_json","group_brain",
                        "mode_unified","behavior_tendency","role_relationship_matrix","llm_threshold","persona_cache",
                        "v10_time_context","v10_weather","v10_micro_narrative","v10_emotion_blend",
                        "v10_topic_initiator","v10_callback","v10_associative_memory","v10_milestones",
                        "v10_growth_arc","v10_scene_mode","v10_virtual_gift","v10_knowledge_router",
                        "hdsi_alter_system","hdsi_story_clock","hdsi_intent_manager"],
            "kimi_configured": bool(KIMI_API_KEY),
            "knowledge_router_enabled": KNOWLEDGE_ROUTER_ENABLED}

@app.get("/api/roles")
async def get_roles():
    result = {}
    for rid, role in ROLES_DEFINITION.items():
        result[rid] = {k: role[k] for k in ("id","name","emoji","gender","age","personality",
            "description","speaking_style","core_traits","taboos","catchphrases","psych_baseline",
            "micro_narratives","topic_pool","unique_quirks","jealousy_stages","nickname_evolution","growth_arc")
            if k in role}
    return result

# ============================================================
# P4 序号5：插件管理 API
# ============================================================
@app.get("/api/plugins")
async def list_plugins():
    """列出所有插件及其状态。"""
    if not PLUGINS_AVAILABLE or not get_plugin_manager:
        return {"enabled": False, "plugins": [], "message": "插件系统不可用"}
    manager = get_plugin_manager()
    return manager.get_status()

@app.get("/api/plugins/status")
async def plugins_status():
    """获取插件系统状态。"""
    if not PLUGINS_AVAILABLE or not get_plugin_manager:
        return {"enabled": False, "message": "插件系统不可用"}
    manager = get_plugin_manager()
    return manager.get_status()

@app.post("/api/plugins/{plugin_name}/enable")
async def enable_plugin(plugin_name: str):
    """启用指定插件。"""
    if not PLUGINS_AVAILABLE or not get_plugin_manager:
        raise HTTPException(status_code=503, detail="插件系统不可用")
    manager = get_plugin_manager()
    if manager.enable_plugin(plugin_name):
        return {"success": True, "message": f"插件 {plugin_name} 已启用"}
    raise HTTPException(status_code=404, detail=f"插件 {plugin_name} 不存在")

@app.post("/api/plugins/{plugin_name}/disable")
async def disable_plugin(plugin_name: str):
    """禁用指定插件。"""
    if not PLUGINS_AVAILABLE or not get_plugin_manager:
        raise HTTPException(status_code=503, detail="插件系统不可用")
    manager = get_plugin_manager()
    if manager.disable_plugin(plugin_name):
        return {"success": True, "message": f"插件 {plugin_name} 已禁用"}
    raise HTTPException(status_code=404, detail=f"插件 {plugin_name} 不存在")

@app.post("/api/session/create")
async def create_session_endpoint():
    sid = create_session()
    return {"session_id": sid, "status": "created"}

@app.get("/api/session/{session_id}")
async def get_session_endpoint(session_id: str):
    data = load_session(session_id)
    if not data:
        raise HTTPException(status_code=404, detail="session不存在")
    return {"session_id":session_id,"current_turn":data.get("current_turn",0),
            "intimacy_map":data.get("intimacy_map",{}),"resilience":data.get("resilience",{}),
            "roles":list(data.get("psychological_states",{}).keys()),
            "milestones":data.get("milestones",{}),"growth_state":data.get("growth_state",{})}

@app.delete("/api/session/{session_id}")
async def delete_session_endpoint(session_id: str):
    conn = _get_db()
    conn.execute("DELETE FROM sessions WHERE session_id=?", (session_id,))
    conn.execute("DELETE FROM session_memories WHERE session_id=?", (session_id,))
    conn.commit(); conn.close()
    semantic_cache.clear(session_id)
    return {"status": "deleted"}

# ============================================================
# v10.0: 核心生成接口（集成知识路由）
# ============================================================
@app.post("/api/generate", response_model=GenerateResponse)
async def generate_reply(request: GenerateRequest, request_obj: Request, remaining: int = Depends(rate_limit_dependency)):
    _lock_ctx = None
    try:
        role_ids = request.role_ids[:3]
        if not role_ids:
            return GenerateResponse(success=False, error="没有指定角色", rate_limit_remaining=remaining)

        # P4 序号5：插件拦截 —— 插件可以直接处理特定命令（如天气、笑话、报时），不走LLM
        if PLUGINS_AVAILABLE and get_plugin_manager:
            try:
                plugin_context = {
                    "session_id": request.session_id,
                    "role_ids": role_ids,
                    "user_id": getattr(request, "user_id", None),
                    "mode": request.mode.value,
                    "intimacy_map": request.intimacy_map,
                }
                plugin_reply = await get_plugin_manager().process_message(request.user_message, plugin_context)
                if plugin_reply:
                    logger.info(f"[插件拦截] 消息被插件处理: {request.user_message[:30]}")
                    return GenerateResponse(
                        success=True,
                        reply=plugin_reply,
                        session_id=request.session_id,
                        role_ids=role_ids,
                        emotion="calm",
                        debug_info={"plugin_intercepted": True},
                        rate_limit_remaining=remaining,
                    )
            except Exception as e:
                logger.warning(f"[插件拦截] 插件处理失败，继续正常流程: {e}")

        timer = StepTimer(f"{request.mode.value}|{'+'.join(role_ids)}")
        # v11.0: 获取角色级并发锁（防止同角色多会话状态冲突 + 费用控制）
        _lock_ctx = role_lock_manager.acquire(role_ids)
        await _lock_ctx.__aenter__()
        valid = [h for h in request.chat_history[-10:]
                 if "role" in h and "content" in h and h["role"] in ("user","assistant","system")]
        use_llm = request.enable_emotion_analysis and should_use_llm_analysis(request.user_message)

        # v10.0: 知识路由（如果启用）
        knowledge_search_result = None
        knowledge_route = "B"
        if request.enable_knowledge_router and KNOWLEDGE_ROUTER_ENABLED and len(request.user_message) >= KNOWLEDGE_ROUTER_MIN_LEN:
            kr = KnowledgeRouter()
            route_result = await kr.route_and_search(request.user_message, ROLES_DEFINITION.get(role_ids[0],{}).get("name",""))
            knowledge_route = route_result["route"]
            knowledge_search_result = route_result.get("search_result")
            timer.mark("知识路由判断")

        # Session状态管理
        session_data = None
        if request.session_id:
            session_data = load_session(request.session_id)
            if session_data:
                psych_in = session_data.get("psychological_states", {})
                event_hist = session_data.get("event_history", {})
                active_conf = session_data.get("conflict_state", {})
                cp_use = session_data.get("catchphrase_usage", {})
                res_map = session_data.get("resilience", {})
                intim_map = session_data.get("intimacy_map") or request.intimacy_map
                turn = session_data.get("current_turn", 0) + 1
                ps = session_data.get("positive_streak", {})
                # v10.0: 加载新状态
                milestones = session_data.get("milestones", {})
                growth_state = session_data.get("growth_state", {})
                emotion_history = session_data.get("emotion_history", [])
                # v11.0: 加载用户画像
                user_profile = session_data.get("user_profile", {"likes":[],"dislikes":[],"traits":[],"events":[],"basic_info":{}})
                # HDSI-PORT: 加载氛围偏移状态
                alter_state = session_data.get("alter_system", {})
            else:
                request.session_id = create_session()
                session_data = load_session(request.session_id)
                psych_in={}; event_hist={}; active_conf={}; cp_use={}; res_map={}
                intim_map=request.intimacy_map; turn=1; ps={}
                milestones={}; growth_state={}; emotion_history=[]
                user_profile={"likes":[],"dislikes":[],"traits":[],"events":[],"basic_info":{}}
                alter_state={}
        else:
            psych_in = {rid: s.model_dump() for rid, s in request.psychological_states.items()}
            event_hist = request.event_history
            active_conf = request.active_conflict
            cp_use = request.catchphrase_usage
            res_map = request.relationship_resilience
            intim_map = request.intimacy_map
            turn = request.current_turn
            ps = request.positive_streak
            milestones={}; growth_state={}; emotion_history=[]
            user_profile={"likes":[],"dislikes":[],"traits":[],"events":[],"basic_info":{}}
            alter_state={}
        timer.mark("session加载")

        events_in = [e.model_dump() for e in request.relationship_events]
        struct = request.structured_memories.model_dump() if request.structured_memories else None
        if request.mode == ChatMode.GROUP:
            for rid in role_ids:
                if rid not in intim_map: intim_map[rid] = 30

        if isinstance(ps, dict):
            ps_val = ps.get(role_ids[0], 0) if len(role_ids) == 1 else 0
        elif isinstance(ps, int):
            ps_val = ps
        else:
            ps_val = 0

        # v10.0: 单聊加载该角色的里程碑/成长状态
        rid_milestones = milestones.get(role_ids[0], {}) if len(role_ids)==1 and isinstance(milestones, dict) else milestones
        rid_growth = growth_state.get(role_ids[0], {}) if len(role_ids)==1 and isinstance(growth_state, dict) else growth_state
        rid_emotion_hist = emotion_history.get(role_ids[0], []) if len(role_ids)==1 and isinstance(emotion_history, dict) else emotion_history

        engine = PersonalityEngine(
            mode=request.mode, role_ids=role_ids, intimacy_map=intim_map,
            psych_states=psych_in, rel_events=events_in, struct_mem=struct,
            event_history=event_hist, active_conflict=active_conf,
            resilience=res_map, turn=turn, cp_usage=cp_use,
            positive_streak=ps_val,
            time_override=request.time_override, weather=request.weather,
            scene_mode=request.scene_mode, gift=request.gift,
            emotion_history=rid_emotion_hist, milestones=rid_milestones,
            growth_state=rid_growth,
            knowledge_search_result=knowledge_search_result,
            user_profile=user_profile if len(role_ids)==1 else None,
            alter_state=alter_state if len(role_ids)==1 else None,
            session_id=request.session_id if len(role_ids)==1 else None,
            user_id=request.user_id,
            vector_url=VECTOR_SERVER_URL)
        system_prompt, debug = await engine.generate(
            msg=request.user_message, mem_ctx=request.memory_context, history=valid,
            override=request.override_emotion, ov_int=request.emotion_intensity,
            use_llm=use_llm, enable_mem=request.enable_memory_analysis)
        timer.mark("引擎构建(含情感分析LLM)")

        # v14.0: 故事模式二（过往线）—— 用户ID带 _past 后缀时，注入青梅竹马版背景
        if request.user_id and request.user_id.endswith("_past") and len(role_ids) == 1:
            role = ROLES_DEFINITION.get(role_ids[0], {})
            past_story = role.get("past_story", "")
            if past_story:
                # 截取前2000字，避免prompt过长
                past_story_short = past_story[:2000] + ("……" if len(past_story) > 2000 else "")
                system_prompt += (
                    f"\n\n【重要：你现在处于过往线（青梅竹马继承模式）】\n"
                    f"你和对方不是新认识的人，你们有共同的过去。以下是你们的故事背景摘要：\n"
                    f"{past_story_short}\n\n"
                    f"请基于这个共同过去来回复，你们的关系已经有了深厚的基础，"
                    f"不需要像刚认识那样小心翼翼，可以自然地提到共同的回忆。"
                )
                logger.info(f"[过往线] 注入 past_story，user={request.user_id} role={role_ids[0]}")

        messages = [{"role":"system","content":system_prompt}]
        messages.extend(valid)
        messages.append({"role":"user","content":request.user_message})
        logger.info(f"生成: mode={request.mode.value} roles={role_ids} "
                    f"emo={debug.get('emotion') if isinstance(debug.get('emotion'),str) else 'group'} "
                    f"event={debug.get('event_type')} llm_analysis={use_llm} turn={turn} "
                    f"knowledge_route={knowledge_route}")
        reply = await smart_llm_call(messages, request.temperature, request.max_tokens)
        if not reply:
            timer.log(" | 状态=LLM返回空")
            return GenerateResponse(success=False, error="豆包 API 返回为空", rate_limit_remaining=remaining)
        reply = clean_reply(reply)
        timer.mark("主回复LLM生成")

        # v14.0: 承诺检测——回复中说了"下课找你"之类的话，存到session供proactive_server使用
        is_group = request.mode == ChatMode.GROUP  # 修复：必须在首次引用前正确判定群聊
        if not is_group and request.session_id and session_data is not None:
            promise = detect_promise(reply)
            if promise:
                kw, delay_h = promise
                session_data["pending_promise"] = {
                    "text": kw,
                    "created_at": time.time(),
                    "due_at": time.time() + delay_h * 3600,
                }
                logger.info(f"[承诺] 检测到承诺: {kw}, {delay_h}h后兑现")

        # P4 序号5：插件后处理 —— 插件可以修改LLM生成的回复
        if PLUGINS_AVAILABLE and get_plugin_manager:
            try:
                plugin_context = {
                    "session_id": request.session_id,
                    "role_ids": role_ids,
                    "emotion": debug.get("emotion"),
                    "mode": request.mode.value,
                }
                reply = await get_plugin_manager().process_after_generate(reply, plugin_context)
            except Exception as e:
                logger.warning(f"[插件后处理] 失败，使用原始回复: {e}")

        # 记忆分析
        mem_cand = None
        # is_group 已在承诺检测前用 request.mode 正确判定，此处不再从 debug 取（debug 无 mode 键）

        # v11.0: 对话质量自检（OOC检测）— 如果人设偏离则重生成一次
        if QUALITY_CHECK_ENABLED and not is_group:
            checker = QualityChecker(role_ids[0])
            quality = checker.check(reply, expected_emotion=debug.get("emotion","calm"),
                                    expected_length=debug.get("reply_length","medium"))
            max_retries = 2
            retry_count = 0
            while not quality["passed"] and quality["score"] < 50 and retry_count < max_retries:
                retry_count += 1
                logger.warning(f"[质量自检] 检测到OOC(score={quality['score']})，第{retry_count}次重生成。问题: {quality['issues']}")
                retry_reply = await smart_llm_call(messages, request.temperature, request.max_tokens)
                if not retry_reply:
                    break
                reply = clean_reply(retry_reply)
                quality = checker.check(reply, expected_emotion=debug.get("emotion","calm"),
                                        expected_length=debug.get("reply_length","medium"))
                timer.mark(f"OOC重生成{retry_count}")
            # 兜底：重试后仍OOC，用规则模板降级，确保不崩人设
            if not quality["passed"] and quality["score"] < 50:
                logger.warning(f"[质量自检] 重试{retry_count}次后仍OOC(score={quality['score']})，启用规则模板降级。问题: {quality['issues']}")
                reply = QualityChecker.fallback_reply(role_ids[0], debug.get("emotion","calm"))
                timer.mark("OOC规则降级")

        if not is_group and request.enable_memory_analysis and use_llm:
            analyzer = debug.get("_mem_analyzer")
            if analyzer:
                mem_cand = await analyzer.analyze(
                    debug.get("_rname",""), debug.get("_pers",""),
                    request.user_message, reply, valid)
        timer.mark("记忆分析LLM")

        # v11.0: 用户画像提取（每N轮对话提取一次，仅单聊）
        if not is_group and request.session_id and turn % USER_PROFILE_EXTRACT_INTERVAL == 0 and use_llm:
            try:
                single_rid = role_ids[0]
                profile_extractor = UserProfileExtractor(user_profile)
                profile_updates = await profile_extractor.extract(valid, ROLES_DEFINITION.get(single_rid,{}).get("name",""))
                if profile_updates:
                    user_profile = profile_extractor.to_dict()
                    logger.info(f"[用户画像] 更新: {json.dumps(profile_updates, ensure_ascii=False)[:200]}")
            except Exception as e:
                logger.warning(f"[用户画像] 提取失败: {e}")

        # 保存session
        if request.session_id and session_data is not None:
            # v11.1: 数据兼容迁移——确保以下字段是dict格式（旧session可能存成list或其他类型）
            for _compat_key in ("psychological_states", "event_history", "conflict_state",
                                 "catchphrase_usage", "resilience", "positive_streak",
                                 "milestones", "growth_state", "emotion_history", "alter_system",
                                 "intimacy_map", "user_profile", "desire_states"):
                if not isinstance(session_data.get(_compat_key), dict):
                    session_data[_compat_key] = {}
            if is_group:
                session_data["psychological_states"] = debug.get("new_psychological_state", {})
                session_data["event_history"] = debug.get("new_event_history", {})
                nac = debug.get("new_active_conflict", {})
                session_data["conflict_state"] = {rid: nac.get(rid) for rid in role_ids}
                session_data["catchphrase_usage"] = debug.get("new_catchphrase_usage", {})
                session_data["resilience"] = debug.get("new_resilience", {})
                rs = debug.get("role_states", {})
                for rid in role_ids:
                    if isinstance(rs.get(rid), dict) and "intimacy" in rs[rid]:
                        intim_map[rid] = rs[rid]["intimacy"]
            else:
                rid = role_ids[0]
                session_data.setdefault("psychological_states", {})[rid] = debug.get("psychological_state", {})
                session_data.setdefault("event_history", {})[rid] = debug.get("event_history", {})
                nac = debug.get("new_active_conflict")
                session_data.setdefault("conflict_state", {})[rid] = nac
                session_data.setdefault("catchphrase_usage", {})[rid] = debug.get("catchphrase_usage", {})
                session_data.setdefault("resilience", {})[rid] = debug.get("resilience", 0)
                session_data.setdefault("positive_streak", {})[rid] = debug.get("positive_streak", 0)
                # v10.0: 保存新状态
                session_data.setdefault("milestones", {})[rid] = debug.get("v10_milestone_state", {})
                session_data.setdefault("growth_state", {})[rid] = debug.get("v10_growth_state", {})
                session_data.setdefault("emotion_history", {})[rid] = debug.get("v10_emotion_history", [])
                # v11.0: 保存用户画像
                session_data["user_profile"] = user_profile
                # HDSI-PORT: 保存氛围偏移状态
                if "alter_system" in debug:
                    session_data["alter_system"] = debug["alter_system"]
                # v12.0: 更新意念欲望状态（反馈闭环：用户回复→调整欲望数值）
                desire_states = session_data.setdefault("desire_states", {})
                desire = DesireMentalState(rid, desire_states.get(rid))
                # 根据用户消息判断反馈类型
                _msg = request.user_message or ""
                _warm_kw = ("喜欢", "爱你", "想你", "开心", "哈哈", "谢谢", "抱抱", "亲亲")
                _cold_kw = ("哦", "嗯", "随便", "算了", "不用", "没事")
                _share_kw = ("今天", "刚才", "我去", "看到", "发现", "吃了", "玩了")
                if any(k in _msg for k in _warm_kw) and len(_msg) > 3:
                    desire.update_from_feedback("user_warm_reply")
                elif len(_msg) <= 3 and any(k in _msg for k in _cold_kw):
                    desire.update_from_feedback("user_cold_reply")
                elif any(k in _msg for k in _share_kw):
                    desire.update_from_feedback("user_shared")
                elif rid in _msg or ("你" in _msg and "?" in _msg or "？" in _msg):
                    desire.update_from_feedback("user_asked_about_me")
                else:
                    desire.update_from_feedback("user_replied")
                desire_states[rid] = desire.to_dict()
                if "intimacy" in debug:
                    intim_map[rid] = debug["intimacy"]
            session_data["intimacy_map"] = intim_map
            session_data["current_turn"] = turn

        # ============================================================
        # v13.0: 旁观者插话引擎（单聊模式下其他角色旁听并概率性插话）
        # 放在 save_session 之前，确保醋意值和插话轮数被持久化
        # ============================================================
        bystander_replies = []
        if request.enable_bystander and not is_group and len(role_ids) == 1:
            active_rid = request.active_role_id or role_ids[0]
            if active_rid in ROLES_DEFINITION and request.session_id and session_data is not None:
                bystander_rids = [rid for rid in ROLES_DEFINITION.keys() if rid != active_rid]
                bystander_last = session_data.get("bystander_last_interjection", {}) or {}

                async def _run_bystander(brid):
                    engine = BystanderEngine(
                        bystander_rid=brid,
                        active_rid=active_rid,
                        user_message=request.user_message,
                        active_reply=reply,
                        history=valid,
                        intimacy_map=intim_map,
                        psych_states=session_data.get("psychological_states", {}),
                        turn=turn,
                        cooldown_turns=request.bystander_cooldown_turns,
                        last_interjection_turn=bystander_last.get(brid, -999)
                    )
                    return await engine.generate_interjection()

                if bystander_rids:
                    results = await asyncio.gather(
                        *[_run_bystander(brid) for brid in bystander_rids],
                        return_exceptions=True
                    )
                    for brid, result in zip(bystander_rids, results):
                        if isinstance(result, Exception):
                            logger.warning(f"[Bystander] 角色{brid} 插话生成异常: {result}")
                            continue
                        if result:
                            bystander_replies.append(result)
                            bystander_last[brid] = turn
                            if result.get("emotion") == "jealous":
                                psy_states = session_data.setdefault("psychological_states", {})
                                brid_psy = psy_states.get(brid, {})
                                if isinstance(brid_psy, dict):
                                    old_jealousy = brid_psy.get("jealousy", 0)
                                    new_jealousy = min(100, old_jealousy + result.get("emotion_intensity", 30) * 0.3)
                                    brid_psy["jealousy"] = new_jealousy
                                    psy_states[brid] = brid_psy
                                    logger.info(f"[Bystander] 角色{brid} 醋意更新: {old_jealousy:.0f} -> {new_jealousy:.0f}")

                if bystander_last:
                    session_data["bystander_last_interjection"] = bystander_last

                if bystander_replies:
                    logger.info(f"[Bystander] 本轮 {len(bystander_replies)} 个角色插话: "
                                f"{[r['role_id'] for r in bystander_replies]}")
                timer.mark("旁观者插话")

        # 保存session（包含旁观者更新的心理状态和插话轮数）
        if request.session_id and session_data is not None:
            save_session(request.session_id, session_data)
        timer.mark("session保存")

        if is_group:
            timer.log(f" | 群聊 回复长度={len(reply)} LLM分析={'是' if use_llm else '否'}")
            return GenerateResponse(
                success=True, reply=reply, session_id=request.session_id,
                new_psychological_state=debug.get("new_psychological_state"),
                new_event_history=debug.get("new_event_history"),
                new_catchphrase_usage=debug.get("new_catchphrase_usage"),
                new_active_conflict=debug.get("new_active_conflict"),
                new_resilience=debug.get("new_resilience"),
                knowledge_route=knowledge_route,
                debug=debug if request.return_debug else None,
                rate_limit_remaining=remaining, used_llm_analysis=use_llm)

        cp_dec = debug.get("catchphrase_decision", {})
        resp = GenerateResponse(
            success=True, reply=reply, session_id=request.session_id,
            new_psychological_state=debug.get("psychological_state"),
            new_event_history=debug.get("event_history"),
            new_resilience=debug.get("resilience",0),
            new_active_conflict=debug.get("new_active_conflict"),
            conflict_repaired=debug.get("repair"),
            memory_candidate=mem_cand,
            catchphrase_used=cp_dec.get("catchphrase") if cp_dec.get("use") else None,
            new_catchphrase_usage=debug.get("catchphrase_usage"),
            event_interpretation=debug.get("interpretation"),
            inner_state=debug.get("inner_state"),
            daily_noise=debug.get("daily_noise"),
            positive_streak=debug.get("positive_streak",0),
            rate_limit_remaining=remaining,
            used_llm_analysis=use_llm,
            knowledge_route=knowledge_route,
            knowledge_search_result=knowledge_search_result,
            v10_milestones=debug.get("v10_milestones"),
            v10_growth=debug.get("v10_growth"),
            bystander_replies=bystander_replies if bystander_replies else None)
        et = debug.get("event_type","none")
        if et and et != "none":
            resp.new_relationship_event = {
                "type":et, "content":f"用户消息：{request.user_message[:50]}", "impact":f"触发事件：{et}"}
        if request.return_debug:
            resp.debug = {k:v for k,v in debug.items() if not k.startswith("_")}
        timer.log(f" | 单聊 回复长度={len(reply)} 事件={debug.get('event_type','none')} LLM分析={'是' if use_llm else '否'} 知识路由={knowledge_route}")
        return resp
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"生成失败: {e}", exc_info=True)
        return GenerateResponse(success=False, error=str(e), rate_limit_remaining=remaining)
    finally:
        # v11.0: 释放角色级并发锁
        if _lock_ctx is not None:
            try:
                await _lock_ctx.__aexit__(None, None, None)
            except Exception:
                pass

# ============================================================
# v11.0: 流式输出接口（SSE）
# ============================================================
class StreamGenerateRequest(BaseModel):
    role_ids: List[str]
    user_message: str
    session_id: Optional[str] = None
    intimacy_map: Dict[str,int] = {}
    memory_context: str = ""
    chat_history: List[Dict[str,str]] = []
    temperature: float = 0.9
    max_tokens: int = 500
    override_emotion: Optional[str] = None
    emotion_intensity: int = 50
    weather: Optional[str] = None
    scene_mode: str = "normal"
    gift: Optional[str] = None
    # 流式同样支持记忆分析与返回调试信息（供主后端在流式展示后做亲密度/记忆后处理）
    enable_memory_analysis: bool = False
    return_debug: bool = False
    # v14.0 故事模式字段（双时间线）
    user_id: Optional[str] = Field(default=None, description="用户ID，带 _past 后缀表示过往线")

@app.post("/api/generate_stream")
async def generate_stream(request: StreamGenerateRequest):
    """流式生成回复，通过SSE推送token。首token延迟从5-10s降至0.5-1s。"""
    role_ids = request.role_ids[:1]  # 流式只支持单角色
    if not role_ids:
        return StreamingResponse(
            iter([f"data: {json.dumps({'type':'error','error':'没有指定角色'})}\n\n"]),
            media_type="text/event-stream")
    rid = role_ids[0]
    if rid not in ROLES_DEFINITION:
        return StreamingResponse(
            iter([f"data: {json.dumps({'type':'error','error':f'未知角色{rid}'})}\n\n"]),
            media_type="text/event-stream")
    valid = [h for h in request.chat_history[-10:]
             if "role" in h and "content" in h and h["role"] in ("user","assistant","system")]
    use_llm = should_use_llm_analysis(request.user_message)
    # 加载session状态（如果有）
    psych_in = {}; event_hist = {}; active_conf = None; cp_use = {}
    res_map = 0; intim_map = request.intimacy_map or {rid: 30}
    turn = 0; milestones = {}; growth_state = {}; emotion_history = []
    user_profile = {"likes":[],"dislikes":[],"traits":[],"events":[],"basic_info":{}}
    alter_state = {}
    session_data = None
    if request.session_id:
        session_data = load_session(request.session_id)
        if session_data:
            psych_in = session_data.get("psychological_states", {})
            event_hist = session_data.get("event_history", {})
            active_conf = session_data.get("conflict_state", {}).get(rid)
            cp_use = session_data.get("catchphrase_usage", {}).get(rid, {})
            res_map = session_data.get("resilience", {}).get(rid, 0)
            intim_map = session_data.get("intimacy_map") or intim_map
            turn = session_data.get("current_turn", 0) + 1
            milestones = session_data.get("milestones", {}).get(rid, {})
            growth_state = session_data.get("growth_state", {}).get(rid, {})
            emotion_history = session_data.get("emotion_history", {}).get(rid, [])
            user_profile = session_data.get("user_profile", user_profile)
            alter_state = session_data.get("alter_system", {})
    engine = PersonalityEngine(
        mode=ChatMode.SINGLE, role_ids=role_ids, intimacy_map=intim_map,
        psych_states=psych_in, event_history=event_hist, active_conflict=active_conf,
        resilience=res_map, turn=turn, cp_usage=cp_use,
        weather=request.weather, scene_mode=request.scene_mode, gift=request.gift,
        emotion_history=emotion_history, milestones=milestones, growth_state=growth_state,
        user_profile=user_profile, alter_state=alter_state, session_id=request.session_id,
        user_id=request.user_id, vector_url=VECTOR_SERVER_URL)
    system_prompt, debug = await engine.generate(
        msg=request.user_message, mem_ctx=request.memory_context, history=valid,
        override=request.override_emotion, ov_int=request.emotion_intensity,
        use_llm=use_llm, enable_mem=request.enable_memory_analysis)

    # v14.0: 故事模式二（过往线）—— 注入青梅竹马版背景
    if request.user_id and request.user_id.endswith("_past"):
        role = ROLES_DEFINITION.get(rid, {})
        past_story = role.get("past_story", "")
        if past_story:
            past_story_short = past_story[:2000] + ("……" if len(past_story) > 2000 else "")
            system_prompt += (
                f"\n\n【重要：你现在处于过往线（青梅竹马继承模式）】\n"
                f"你和对方不是新认识的人，你们有共同的过去。以下是你们的故事背景摘要：\n"
                f"{past_story_short}\n\n"
                f"请基于这个共同过去来回复，你们的关系已经有了深厚的基础，"
                f"不需要像刚认识那样小心翼翼，可以自然地提到共同的回忆。"
            )

    messages = [{"role":"system","content":system_prompt}]
    messages.extend(valid)
    messages.append({"role":"user","content":request.user_message})
    # 先推送元信息，再流式推送 token；流式结束后累积完整回复并做记忆/会话持久化
    async def event_generator():
        yield f"data: {json.dumps({'type':'meta','emotion':debug.get('emotion','calm'),'intimacy':debug.get('intimacy',30)})}\n\n"
        full_reply = ""
        async for token in smart_llm_stream_call(messages, request.temperature, request.max_tokens):
            try:
                if token.startswith("data: "):
                    ev = json.loads(token[6:].strip())
                    if ev.get("type") == "token":
                        full_reply += ev.get("content", "")
            except Exception:
                pass
            yield token
        full_reply = clean_reply(full_reply)

        # 记忆分析（可选，复用主流程逻辑）
        mem_cand = None
        if request.enable_memory_analysis and use_llm:
            analyzer = debug.get("_mem_analyzer")
            if analyzer:
                try:
                    mem_cand = await analyzer.analyze(
                        debug.get("_rname", ""), debug.get("_pers", ""),
                        request.user_message, full_reply, valid)
                except Exception as e:
                    logger.warning(f"[流式] 记忆分析失败: {e}")

        # 保存 session（单聊状态持久化，保持与 /api/generate 一致的人格连续性）
        new_session_id = request.session_id
        if request.session_id and session_data is not None:
            for _compat_key in ("psychological_states", "event_history", "conflict_state",
                                "catchphrase_usage", "resilience", "positive_streak",
                                "milestones", "growth_state", "emotion_history", "alter_system",
                                "intimacy_map", "user_profile", "desire_states"):
                if not isinstance(session_data.get(_compat_key), dict):
                    session_data[_compat_key] = {}
            session_data.setdefault("psychological_states", {})[rid] = debug.get("psychological_state", {})
            session_data.setdefault("event_history", {})[rid] = debug.get("event_history", {})
            session_data.setdefault("conflict_state", {})[rid] = debug.get("new_active_conflict")
            session_data.setdefault("catchphrase_usage", {})[rid] = debug.get("catchphrase_usage", {})
            session_data.setdefault("resilience", {})[rid] = debug.get("resilience", 0)
            session_data.setdefault("positive_streak", {})[rid] = debug.get("positive_streak", 0)
            session_data.setdefault("milestones", {})[rid] = debug.get("v10_milestone_state", {})
            session_data.setdefault("growth_state", {})[rid] = debug.get("v10_growth_state", {})
            session_data.setdefault("emotion_history", {})[rid] = debug.get("v10_emotion_history", [])
            session_data["user_profile"] = user_profile
            if "alter_system" in debug:
                session_data["alter_system"] = debug["alter_system"]
            # v12.0: 意念欲望状态反馈闭环
            desire_states = session_data.setdefault("desire_states", {})
            desire = DesireMentalState(rid, desire_states.get(rid))
            _msg = request.user_message or ""
            _warm_kw = ("喜欢", "爱你", "想你", "开心", "哈哈", "谢谢", "抱抱", "亲亲")
            _cold_kw = ("哦", "嗯", "随便", "算了", "不用", "没事")
            _share_kw = ("今天", "刚才", "我去", "看到", "发现", "吃了", "玩了")
            if any(k in _msg for k in _warm_kw) and len(_msg) > 3:
                desire.update_from_feedback("user_warm_reply")
            elif len(_msg) <= 3 and any(k in _msg for k in _cold_kw):
                desire.update_from_feedback("user_cold_reply")
            elif any(k in _msg for k in _share_kw):
                desire.update_from_feedback("user_shared")
            elif rid in _msg or ("你" in _msg and ("?" in _msg or "？" in _msg)):
                desire.update_from_feedback("user_asked_about_me")
            else:
                desire.update_from_feedback("user_replied")
            desire_states[rid] = desire.to_dict()
            if "intimacy" in debug:
                intim_map[rid] = debug["intimacy"]
            session_data["intimacy_map"] = intim_map
            session_data["current_turn"] = turn
            save_session(request.session_id, session_data)
            new_session_id = request.session_id

        done_payload = {"type": "done"}
        if request.return_debug:
            done_payload["debug"] = {k: v for k, v in debug.items() if not k.startswith("_")}
            done_payload["memory_candidate"] = mem_cand
        done_payload["session_id"] = new_session_id
        yield f"data: {json.dumps(done_payload, ensure_ascii=False)}\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream")

# ============================================================
# v9.0: 主动消息生成
# ============================================================
class ProactiveGenerateRequest(BaseModel):
    role_id: str
    reason_type: str = "check_in"
    reason_detail: str = ""
    related_memory: Optional[Dict[str, Any]] = None
    idle_hours: float = 0
    intimacy: int = 30
    mood: str = "calm"
    intent: Optional[Dict[str, Any]] = None  # v12.2: 行为意图引导（intent_type/prompt_hint/dominant_desire）

    @field_validator("mood", mode="before")
    @classmethod
    def _coerce_mood_to_str(cls, v):
        """防御性修复：mood 必须是字符串，数字型(如78.0)自动转换，避免422错误"""
        if v is None:
            return "calm"
        if not isinstance(v, str):
            logger.warning(f"[ProactiveGenerateRequest] mood收到非字符串类型: {v!r}({type(v).__name__})，已自动转换")
            return str(v)
        return v

PROACTIVE_REASON_PROMPT = {
    "missing_you": "你发现自己有点想他/她。你们已经有一阵子没聊了，这种想念让你主动打开了对话框。",
    "long_time_no_see": "你们好几天没联系了，你想知道他/她最近过得怎么样。",
    "memory_recall": "你突然想起了一件和他/她有关的事，这个念头让你想立刻告诉他/她。",
    "daily_share": "你正在做自己的事，某件小事让你想到了他/她，想顺手分享给他/她。",
    "emotion_need": "你现在心情不太好，想找他/她说说话，哪怕只是随便聊聊。",
    "check_in": "你没什么特别的事，就是忽然想问候他/她一声。",
    # v12.2: 话题延续引擎专用类型
    "topic_continue": "你们正在聊天，但对方沉默了一会儿。你根据刚才聊的内容自然地想到了一个相关的新话题，想接着聊下去，不要让对话冷场。",
    "topic_self_close": "你主动提起了一个新话题，但对方没有回应。你需要自然地收尾，给自己和对方找个台阶下，不要显得尴尬、卑微或追问。",
}

@app.post("/api/proactive_generate")
async def proactive_generate(request: ProactiveGenerateRequest):
    try:
        role = ROLES_DEFINITION.get(request.role_id)
        if not role:
            return {"success": False, "error": f"未知角色: {request.role_id}"}
        rname = role["name"]
        stage_name = ("陌生人" if request.intimacy <= 30 else "认识" if request.intimacy <= 50
                      else "熟悉" if request.intimacy <= 70 else "亲密")
        noise = DailyNoiseLayer().generate(request.role_id, 30)
        noise_text = f"你此刻{noise['description']}。" if noise else ""
        reason_block = PROACTIVE_REASON_PROMPT.get(
            request.reason_type, PROACTIVE_REASON_PROMPT["check_in"])

        # v12.2: reason_detail 处理 —— 话题延续类必须展示上下文摘要和生成要求
        if request.reason_detail:
            if request.reason_type in ("topic_continue", "topic_self_close"):
                # 这两个类型的 reason_detail 包含上下文+详细生成指令，作为核心情况说明
                reason_block += f"\n【当前情况】{request.reason_detail}"
            elif request.reason_type in ("daily_share", "memory_recall", "emotion_need"):
                reason_block += f"（具体由头：{request.reason_detail}）"

        # v12.2: intent.prompt_hint 行为意图风格引导
        intent_hint = ""
        if request.intent and request.intent.get("prompt_hint"):
            intent_hint = f"\n【风格要求】{request.intent['prompt_hint']}"

        memory_block = ""
        if request.related_memory:
            mem_summary = request.related_memory.get("summary", "")
            if mem_summary:
                memory_block = f"你想起：{mem_summary}"
        cp_hint = ""
        if role.get("catchphrases"):
            cp_hint = (f"偶尔可以自然带一句口头禅（如「{random.choice(role['catchphrases'])}」），"
                       f"但不要每条都用。")

        # v13.0: 注入角色背景素材，让主动消息有个人印记而不是万能模板
        role_bg = ""
        relationship = role.get("relationship", {})
        if relationship.get("core_dynamic"):
            # 截取核心关系背景的前250字，避免prompt过长
            core_dyn = relationship["core_dynamic"]
            role_bg = core_dyn[:250] + ("……" if len(core_dyn) > 250 else "")

        # 从生活片段库随机抽2-3条作为"今天可能想到的事"
        life_snippets = ""
        micro = role.get("micro_narratives", [])
        if micro:
            samples = random.sample(micro, min(2, len(micro)))
            life_snippets = "；".join(samples)

        # 从话题池随机抽1-2条
        topic_suggestions = ""
        topics = role.get("topic_pool", [])
        if topics:
            t_samples = random.sample(topics, min(1, len(topics)))
            topic_suggestions = "；".join(t_samples)

        # 主动风格（从dynamic.initiative.style提取）
        initiative_style = ""
        dynamic = role.get("dynamic", {})
        if dynamic.get("initiative", {}).get("style"):
            initiative_style = dynamic["initiative"]["style"].strip()

        # 默认行为倾向
        default_behavior = ""
        if role.get("behavior_tendency", {}).get("default"):
            default_behavior = role["behavior_tendency"]["default"]

        # 组装角色背景块
        character_block = ""
        if role_bg or life_snippets or initiative_style or default_behavior:
            parts = []
            if role_bg:
                parts.append(f"【你的背景】{role_bg}")
            if life_snippets:
                parts.append(f"【你今天可能在想的事】{life_snippets}")
            if topic_suggestions:
                parts.append(f"【可以聊的方向】{topic_suggestions}")
            if initiative_style:
                parts.append(f"【你主动找人时的样子】{initiative_style}")
            if default_behavior:
                parts.append(f"【你的默认状态】{default_behavior}")
            character_block = "\n".join(parts)

        # v12.2: 根据 reason_type 动态调整场景描述和输出规则
        is_topic_continue = request.reason_type == "topic_continue"
        is_self_close = request.reason_type == "topic_self_close"

        if is_topic_continue:
            scene_desc = "你们正在聊天过程中，对方暂时没有回复，你想自然地把话题延续下去。"
            output_rules = (
                f"1. 只输出你要发的消息内容本身，1-2句话，口语化，像正在进行的对话\n"
                f"2. 不要解释你为什么发消息，不要说'我是AI/系统/语言模型'，不要提'触发''主动消息'这类词\n"
                f"3. 这是对话的延续，不是新的开场白——不要用'对了/话说/顺便问一下'这类刻意的转折词\n"
                f"4. 基于刚才聊的内容自然延伸，不要重复已经说过的话题\n"
                f"5. 符合你的性格和说话风格，不要OOC。{cp_hint}\n"
                f"6. 可以用全角分隔符 ‖ 切成最多2条短气泡（也可以就一条），不要动作描写或括号旁白，不用emoji"
            )
        elif is_self_close:
            scene_desc = "你刚才主动提起了一个新话题，但对方没有回应。你需要自然地收尾。"
            output_rules = (
                f"1. 只输出你要发的消息内容本身，1句话，简短自然\n"
                f"2. 不要解释你为什么发消息，不要说'我是AI/系统/语言模型'\n"
                f"3. 核心：给自己和对方都找台阶下——暗示对方可能在忙，同时表示自己就是随口一说\n"
                f"4. 不要卑微、不要追问、不要道歉、不要降低关系，就像真人发现对方没在听然后自然收住\n"
                f"5. 符合你的性格和说话风格，不要OOC。温柔型可以体贴收尾，傲娇型可以嘴硬收尾。{cp_hint}\n"
                f"6. 就发一条，不要加动作描写或括号旁白"
            )
        else:
            scene_desc = "现在不是在回复对方的消息，而是你自己主动想联系他/她。"
            output_rules = (
                f"1. 只输出你要发的消息内容本身，1-2句话，口语化，像真人随手发的微信/短信\n"
                f"2. 不要解释你为什么发消息，不要说'我是AI/系统/语言模型'，不要提'触发''主动消息'这类词\n"
                f"3. 不要每次都问'在吗/在干嘛/忙吗'，根据上面的由头自然开场\n"
                f"4. 符合你的性格和说话风格，不要OOC。{cp_hint}\n"
                f"5. 可以用全角分隔符 ‖ 切成最多2条短气泡（也可以就一条），不要动作描写或括号旁白，不用emoji\n"
                f"6. 不要过度热情，也不要太生硬，把握好你们当前的关系距离"
            )

        system_prompt = (
            f"你是{rname}，{role['age']}{role['gender']}生。{role['description']}\n"
            f"性格：{role['personality']}。说话风格：{role['speaking_style']}。\n"
            f"你们现在的关系：{stage_name}（亲密度{request.intimacy}/100）。\n"
            f"{noise_text}\n\n"
            f"{character_block}\n\n"
            f"{scene_desc}\n"
            f"{reason_block}\n"
            f"{intent_hint}\n"
            f"{memory_block}\n\n"
            f"【重要】不要用'刚路过XX忽然想起你'这种万能模板，结合上面你的背景、今天在想的事、你的主动风格来写。要像{rname}本人会说的话，而不是任何一个温柔女生都能说的话。\n\n"
            f"【输出规则】\n"
            f"{output_rules}"
        )
        content = await smart_llm_call(
            [{"role": "system", "content": system_prompt}],
            temperature=0.95, max_tokens=150, timeout=30.0)
        if not content:
            return {"success": False, "error": "LLM返回为空"}
        content = clean_reply(content)
        return {"success": True, "content": content, "mood": request.mood,
                "reason_type": request.reason_type}
    except Exception as e:
        logger.error(f"主动消息生成失败: {e}", exc_info=True)
        return {"success": False, "error": str(e)}

# ============================================================
# v10.0: 知识路由专用接口（独立于 /api/generate）
# ============================================================
class KnowledgeChatRequest(BaseModel):
    role_ids: List[str]
    user_message: str
    session_id: Optional[str] = None
    intimacy_map: Dict[str,int] = {}
    chat_history: List[Dict[str,str]] = []
    temperature: float = 0.9
    max_tokens: int = 500
    return_debug: bool = False
    weather: Optional[str] = None
    scene_mode: str = "normal"
    gift: Optional[str] = None

@app.post("/api/chat_with_knowledge")
async def chat_with_knowledge(request: KnowledgeChatRequest, remaining: int = Depends(rate_limit_dependency)):
    """
    知识路由专用接口：
    用户消息 → 判断模型(豆包) → 知道(B线直答) / 不知道(A线Kimi联网搜索→整理→人格回复)
    """
    try:
        role_ids = request.role_ids[:1]  # 知识路由只支持单聊
        if not role_ids:
            return {"success": False, "error": "没有指定角色", "rate_limit_remaining": remaining}

        timer = StepTimer(f"knowledge_chat|{role_ids[0]}")

        # Step 1: 判断模型
        kr = KnowledgeRouter()
        decision = await kr.judge(request.user_message, ROLES_DEFINITION.get(role_ids[0],{}).get("name",""))
        timer.mark("知识路由判断")

        search_result = None
        route = "B"

        # Step 2: 如果需要搜索，调用Kimi
        if decision["need_search"] and KIMI_API_KEY:
            search_result = await kimi_search_call(request.user_message)
            route = "A" if search_result else "B_fallback"
            timer.mark("Kimi联网搜索")
        elif decision["need_search"] and not KIMI_API_KEY:
            route = "B_fallback_no_key"
            logger.warning("[KnowledgeRouter] 需要搜索但未配置KIMI_API_KEY，降级B线")

        # Step 3: 用人格模型回复（传入搜索结果作为上下文）
        gen_req = GenerateRequest(
            mode=ChatMode.SINGLE,
            role_ids=role_ids,
            user_message=request.user_message,
            session_id=request.session_id,
            intimacy_map=request.intimacy_map,
            chat_history=request.chat_history,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
            return_debug=request.return_debug,
            weather=request.weather,
            scene_mode=request.scene_mode,
            gift=request.gift,
            enable_knowledge_router=False,  # 避免重复路由
        )
        # 直接调用生成逻辑（复用 /api/generate 的核心）
        # 由于不能直接调用路由函数，我们手动构造
        valid = [h for h in request.chat_history[-10:]
                 if "role" in h and "content" in h and h["role"] in ("user","assistant","system")]
        use_llm = should_use_llm_analysis(request.user_message)

        # 简化版：直接用 PersonalityEngine
        engine = PersonalityEngine(
            mode=ChatMode.SINGLE, role_ids=role_ids,
            intimacy_map=request.intimacy_map or {role_ids[0]: 30},
            turn=0, weather=request.weather, scene_mode=request.scene_mode,
            gift=request.gift, knowledge_search_result=search_result)
        system_prompt, debug = await engine.generate(
            msg=request.user_message, mem_ctx="", history=valid, use_llm=use_llm)
        timer.mark("人格引擎构建")

        messages = [{"role":"system","content":system_prompt}]
        messages.extend(valid)
        messages.append({"role":"user","content":request.user_message})
        reply = await smart_llm_call(messages, request.temperature, request.max_tokens)
        if not reply:
            return {"success": False, "error": "LLM返回为空", "rate_limit_remaining": remaining}
        reply = clean_reply(reply)
        timer.mark("人格回复生成")
        timer.log(f" | 路由={route} 回复长度={len(reply)}")

        return {
            "success": True,
            "reply": reply,
            "knowledge_route": route,
            "need_search": decision["need_search"],
            "judge_reason": decision["reason"],
            "judge_confidence": decision["confidence"],
            "search_result": search_result[:500] + "..." if search_result and len(search_result) > 500 else search_result,
            "rate_limit_remaining": remaining,
            "debug": debug if request.return_debug else None,
        }
    except Exception as e:
        logger.error(f"知识路由对话失败: {e}", exc_info=True)
        return {"success": False, "error": str(e), "rate_limit_remaining": remaining}

# ============================================================
# 管理员：读取/修改用户与角色的实时心理状态
# ============================================================
class PsychStateUpdate(BaseModel):
    intimacy: Optional[int] = None
    trust: Optional[float] = None
    security: Optional[float] = None
    attachment: Optional[float] = None
    jealousy: Optional[float] = None
    fatigue: Optional[float] = None
    mood: Optional[float] = None
    trauma_flag: Optional[bool] = None

@app.get("/api/session/{session_id}/state")
async def get_session_state(session_id: str):
    data = load_session(session_id)
    if not data:
        raise HTTPException(status_code=404, detail="session不存在")
    psych = data.get("psychological_states", {})
    intim = data.get("intimacy_map", {})
    result = {}
    for rid in set(list(psych.keys()) + list(intim.keys())):
        s = psych.get(rid, {})
        result[rid] = {
            "intimacy": intim.get(rid, 30),
            "trust": s.get("trust", 0),
            "security": s.get("security", 0),
            "attachment": s.get("attachment", 0),
            "jealousy": s.get("jealousy", 0),
            "fatigue": s.get("fatigue", 0),
            "mood": s.get("mood", 0),
            "trauma_flag": s.get("trauma_flag", False),
        }
    return {"session_id": session_id, "states": result}

@app.put("/api/session/{session_id}/state/{role_id}")
async def update_session_state(session_id: str, role_id: str, update: PsychStateUpdate):
    data = load_session(session_id)
    if not data:
        raise HTTPException(status_code=404, detail="session不存在")
    if role_id not in ROLES_DEFINITION:
        raise HTTPException(status_code=400, detail=f"未知角色: {role_id}")
    baseline = ROLES_DEFINITION[role_id].get("psych_baseline", {})
    psych = data.setdefault("psychological_states", {})
    cur = psych.get(role_id, {})
    fields = ["trust", "security", "attachment", "jealousy", "fatigue", "mood"]
    for f in fields:
        v = getattr(update, f)
        if v is not None:
            cur[f] = round(max(0.0, min(100.0, float(v))), 1)
    if "trust" not in cur: cur["trust"] = baseline.get("trust", 50)
    if "security" not in cur: cur["security"] = baseline.get("security", 50)
    if "attachment" not in cur: cur["attachment"] = baseline.get("attachment", 20)
    if "jealousy" not in cur: cur["jealousy"] = 0
    if "fatigue" not in cur: cur["fatigue"] = 0
    if "mood" not in cur: cur["mood"] = baseline.get("mood", 50)
    if update.trauma_flag is not None:
        cur["trauma_flag"] = bool(update.trauma_flag)
    elif "trauma_flag" not in cur:
        cur["trauma_flag"] = False
    psych[role_id] = cur
    if update.intimacy is not None:
        data.setdefault("intimacy_map", {})[role_id] = max(0, min(100, int(update.intimacy)))
    save_session(session_id, data)
    return {"success": True, "role_id": role_id, "state": {
        "intimacy": data.get("intimacy_map", {}).get(role_id, 30), **cur
    }}

# ============================================================
# v12.0: DesireMentalState 意念欲望状态 API（供主动后端调用）
# ============================================================
class DesireStateUpdate(BaseModel):
    """欲望状态更新请求（主动后端可直接设置各维度数值）"""
    longing: Optional[float] = None
    contact_desire: Optional[float] = None
    share_desire: Optional[float] = None
    care_desire: Optional[float] = None
    companionship: Optional[float] = None

class DesireDecayRequest(BaseModel):
    """欲望衰减请求（根据空闲小时数衰减/增长）"""
    hours_elapsed: float = 1.0

class DesireInnerEventRequest(BaseModel):
    """内在事件请求（修改欲望数值）"""
    event_type: str  # saw_scenery / recalled_memory / worried_about_you / bored / happy_event / sad_event
    intensity: float = 1.0

@app.get("/api/session/{session_id}/desire/{role_id}")
async def get_desire_state(session_id: str, role_id: str):
    """读取某角色的意念欲望状态"""
    data = load_session(session_id)
    if not data:
        raise HTTPException(status_code=404, detail="session不存在")
    desire_states = data.get("desire_states", {})
    desire = DesireMentalState(role_id, desire_states.get(role_id))
    return {
        "success": True,
        "session_id": session_id,
        "role_id": role_id,
        "desire": desire.to_dict(),
        "dominant": desire.dominant_desire()[0],
        "dominant_value": desire.dominant_desire()[1],
        "motivation_score": desire.motivation_score(),
    }

@app.put("/api/session/{session_id}/desire/{role_id}")
async def update_desire_state(session_id: str, role_id: str, update: DesireStateUpdate):
    """直接更新欲望状态各维度数值（供主动后端/管理员调用）"""
    data = load_session(session_id)
    if not data:
        raise HTTPException(status_code=404, detail="session不存在")
    if role_id not in ROLES_DEFINITION:
        raise HTTPException(status_code=400, detail=f"未知角色: {role_id}")
    desire_states = data.setdefault("desire_states", {})
    desire = DesireMentalState(role_id, desire_states.get(role_id))
    for dim in DesireMentalState.DIMENSIONS:
        v = getattr(update, dim)
        if v is not None:
            desire.values[dim] = max(0.0, min(100.0, float(v)))
    desire_states[role_id] = desire.to_dict()
    save_session(session_id, data)
    return {"success": True, "desire": desire.to_dict(), "motivation_score": desire.motivation_score()}

@app.post("/api/session/{session_id}/desire/{role_id}/decay")
async def decay_desire_state(session_id: str, role_id: str, req: DesireDecayRequest):
    """触发欲望衰减/增长（根据空闲小时数，主动后端定时调用）"""
    data = load_session(session_id)
    if not data:
        raise HTTPException(status_code=404, detail="session不存在")
    desire_states = data.setdefault("desire_states", {})
    desire = DesireMentalState(role_id, desire_states.get(role_id))
    desire.decay(req.hours_elapsed)
    desire_states[role_id] = desire.to_dict()
    save_session(session_id, data)
    return {"success": True, "desire": desire.to_dict(), "motivation_score": desire.motivation_score()}

@app.post("/api/session/{session_id}/desire/{role_id}/inner_event")
async def apply_desire_inner_event(session_id: str, role_id: str, req: DesireInnerEventRequest):
    """应用内在随机事件到欲望状态（InnerEventGenerator 调用）"""
    data = load_session(session_id)
    if not data:
        raise HTTPException(status_code=404, detail="session不存在")
    desire_states = data.setdefault("desire_states", {})
    desire = DesireMentalState(role_id, desire_states.get(role_id))
    desire.apply_inner_event(req.event_type, req.intensity)
    desire_states[role_id] = desire.to_dict()
    save_session(session_id, data)
    return {"success": True, "desire": desire.to_dict(), "motivation_score": desire.motivation_score()}

# ============================================================
# v11.0: 语音接口骨架（ASR语音识别 + TTS语音合成）
# ============================================================
# 注意：实际ASR/TTS推理需要接入外部模型（如SenseVoice/Whisper ASR，GPT-SoVITS/CosyVoice TTS）
# 此处提供接口骨架，前端voice.js可对接这两个接口
# 依赖：pip install python-multipart（FastAPI文件上传必需）

class TTSRequest(BaseModel):
    text: str
    role_id: str = "nianqi"
    speed: float = 1.0
    emotion: Optional[str] = None

@app.post("/api/voice/asr")
async def voice_asr(file: UploadFile = File(...)):
    """
    ASR语音识别接口：接收音频文件，返回识别文本。
    需要接入ASR推理模型（如SenseVoice/Whisper）。
    当前为骨架实现，返回占位信息。
    """
    try:
        content = await file.read()
        file_size = len(content)
        logger.info(f"[ASR] 收到音频: {file.filename}, 大小={file_size}字节")
        # TODO: 接入实际ASR推理
        # 示例：
        # from sense_voice import SenseVoiceASR
        # asr = SenseVoiceASR()
        # text = asr.transcribe(content)
        return {
            "success": True,
            "text": "",  # 实际ASR识别结果
            "language": "zh",
            "duration": 0.0,
            "note": "ASR骨架接口，请接入实际语音识别模型（如SenseVoice/Whisper）"
        }
    except Exception as e:
        logger.error(f"[ASR] 处理失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"ASR处理失败: {str(e)}")

@app.post("/api/voice/tts")
async def voice_tts(request: TTSRequest):
    """
    TTS语音合成接口：输入文本+角色ID，返回音频流。
    需要接入TTS推理模型（如GPT-SoVITS/CosyVoice）。
    当前为骨架实现，返回占位信息。
    """
    if request.role_id not in ROLES_DEFINITION:
        raise HTTPException(status_code=400, detail=f"未知角色: {request.role_id}")
    try:
        logger.info(f"[TTS] 角色={request.role_id}, 文本长度={len(request.text)}, 语速={request.speed}")
        # TODO: 接入实际TTS推理
        # 示例：
        # from gpt_sovits import GPTSoVITS
        # tts = GPTSoVITS(voice_model=f"voice_{request.role_id}")
        # audio_bytes = tts.synthesize(request.text, speed=request.speed, emotion=request.emotion)
        # return Response(content=audio_bytes, media_type="audio/wav")
        return {
            "success": True,
            "role_id": request.role_id,
            "text": request.text,
            "audio_url": None,  # 实际TTS生成的音频URL或字节流
            "note": "TTS骨架接口，请接入实际语音合成模型（如GPT-SoVITS/CosyVoice）"
        }
    except Exception as e:
        logger.error(f"[TTS] 合成失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"TTS合成失败: {str(e)}")

# ============================================================
# v14.0: 承诺后门 API —— proactive_server 检查是否有未兑现的承诺
# ============================================================
@app.get("/api/session/{session_id}/check_promise")
async def check_promise(session_id: str):
    """检查session里有没有到期的承诺。有则返回并清除，无则返回null。"""
    session_data = load_session(session_id)
    if not session_data:
        return {"promise": None}
    p = session_data.get("pending_promise")
    if not p:
        return {"promise": None}
    now = time.time()
    if now < p.get("due_at", 0):
        return {"promise": None, "remaining_hours": round((p["due_at"] - now) / 3600, 1)}
    # 到期了，取出并清除
    session_data.pop("pending_promise", None)
    save_session(session_id, session_data)
    return {"promise": p}


# ============================================================
# 启动
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")


# ============================================================
# P1 模块3：图像生成/自拍 API
# ============================================================
class SelfieRequest(BaseModel):
    """自拍请求"""
    user_id: str
    role_id: str
    intimacy: int = 30
    psych_states: Optional[Dict[str, float]] = None
    scene: str = "indoor"  # indoor/outdoor/bedroom/cafe/park/morning/night
    expression: str = "gentle smile"
    clothing: str = "casual"


class SelfieResponse(BaseModel):
    """自拍响应"""
    allowed: bool
    message: str = ""
    image_url: str = ""
    error: str = ""


@app.post("/api/image/selfie", response_model=SelfieResponse)
async def generate_selfie(req: SelfieRequest):
    """
    生成角色自拍。
    基于亲密度+心理状态+角色性格判断是否愿意发，同意则生成图片。
    """
    try:
        from core.image_generator import get_selfie_system
        system = get_selfie_system()
        
        result = await system.handle_selfie_request(
            user_id=req.user_id,
            role_id=req.role_id,
            intimacy=req.intimacy,
            psych_states=req.psych_states,
            scene=req.scene,
            expression=req.expression,
            clothing=req.clothing,
        )
        
        return SelfieResponse(**result)
        
    except Exception as e:
        logger.error(f"[ImageAPI] 生成自拍失败: {e}", exc_info=True)
        return SelfieResponse(
            allowed=False,
            message="图片生成出了点问题，稍后再试吧。",
            error=str(e),
        )


@app.get("/api/image/status")
async def image_status():
    """获取图像生成系统状态（调试用）"""
    try:
        from core.image_generator import get_selfie_system
        system = get_selfie_system()
        return system.get_status()
    except Exception as e:
        return {"error": str(e)}


class SelfieFromMessageRequest(BaseModel):
    """P2 新增：从用户原始消息直接生成自拍"""
    user_id: str
    role_id: str
    message: str  # 用户原始消息
    intimacy: int = 30
    psych_states: Optional[Dict[str, float]] = None


class SelfieFromMessageResponse(BaseModel):
    """自拍响应"""
    is_selfie_request: bool  # 是否是自拍请求
    allowed: bool = False
    message: str = ""
    image_url: str = ""
    mode: str = ""  # 检测到的自拍模式（mirror/direct）
    scene: str = ""  # 提取的场景
    clothing: str = ""  # 提取的服装
    expression: str = ""  # 提取的表情
    error: str = ""


@app.post("/api/image/selfie_from_message", response_model=SelfieFromMessageResponse)
async def generate_selfie_from_message(req: SelfieFromMessageRequest):
    """
    P2 新增：从用户原始消息直接生成自拍。
    自动检测是否是自拍请求、自拍模式、场景/服装/表情，然后生成图片。
    前端可以在用户发送消息前先调用这个接口，如果是自拍请求则显示图片。
    """
    try:
        from core.image_generator import (
            get_selfie_system, is_selfie_request,
            detect_selfie_mode, extract_scene, extract_clothing, extract_expression,
        )
        
        # 1. 检测是否是自拍请求
        if not is_selfie_request(req.message):
            return SelfieFromMessageResponse(
                is_selfie_request=False,
                message="",
            )
        
        # 2. 检测模式和上下文
        mode = detect_selfie_mode(req.message)
        scene = extract_scene(req.message) or "indoor"
        clothing = extract_clothing(req.message) or "casual"
        expression = extract_expression(req.message) or "gentle smile"
        
        logger.info(
            f"[SelfieP2] 自拍请求: user={req.user_id} role={req.role_id} "
            f"mode={mode.value} scene={scene} clothing={clothing} expression={expression}"
        )
        
        # 3. 调用自拍系统生成
        system = get_selfie_system()
        result = await system.handle_selfie_request(
            user_id=req.user_id,
            role_id=req.role_id,
            intimacy=req.intimacy,
            psych_states=req.psych_states,
            scene=scene,
            expression=expression,
            clothing=clothing,
            mode=mode,
        )
        
        return SelfieFromMessageResponse(
            is_selfie_request=True,
            allowed=result.get("allowed", False),
            message=result.get("message", ""),
            image_url=result.get("image_url", ""),
            mode=mode.value,
            scene=scene,
            clothing=clothing,
            expression=expression,
            error=result.get("error", ""),
        )
        
    except Exception as e:
        logger.error(f"[SelfieP2] 从消息生成自拍失败: {e}", exc_info=True)
        return SelfieFromMessageResponse(
            is_selfie_request=True,
            allowed=False,
            message="图片生成出了点问题，稍后再试吧。",
            error=str(e),
        )

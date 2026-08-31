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

# 可选依赖：jieba 中文分词（MemoryDecaySystem._is_correction 使用，未安装则降级到2-gram）
try:
    import jieba
    JIEBA_AVAILABLE = True
except ImportError:
    JIEBA_AVAILABLE = False
    jieba = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("personality_server")

# ============================================================
# 配置（已迁移到 core/config.py）
# ============================================================
from core.config import *
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
try:
    from emotion import *  # noqa: F401,F403
    from psych import *  # noqa: F401,F403
    from memory import *  # noqa: F401,F403
    from knowledge import *  # noqa: F401,F403
    from group import *  # noqa: F401,F403
    from quality import *  # noqa: F401,F403
    from topic import *  # noqa: F401,F403
    from scene import *  # noqa: F401,F403
    from core import *  # noqa: F401,F403
    from api.models import *  # noqa: F401,F403
    MODULE_SPLIT_ENABLED = True
except Exception as _module_split_error:
    MODULE_SPLIT_ENABLED = False
    logging.getLogger("personality_server").warning(f"模块拆分导入失败，使用内联类定义: {_module_split_error}")

# SQLite Session 持久化
# ============================================================
def _get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

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
    """)
    conn.commit(); conn.close()
    logger.info(f"SQLite初始化: {DB_PATH}")

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
    except:
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
from characters.loader import load_all_roles, get_role as _get_role, reload_roles as _reload_roles
from dynamic_conversation import DynamicConversationEngine

# 动态对话引擎全局单例（剧情状态需要在多次对话间保持）
_DYNAMIC_ENGINE = DynamicConversationEngine()

# 启动时加载所有角色配置
_roles_data = load_all_roles()

# 兼容旧代码：ROLES_DEFINITION 全局变量
ROLES_DEFINITION = _roles_data

def get_role_definition(role_id: str) -> dict:
    """获取角色配置（优先从缓存，支持热重载）。"""
    return _get_role(role_id) or ROLES_DEFINITION.get(role_id, {})

def reload_role_definitions() -> dict:
    """热重载角色配置（管理员调用）。"""
    global ROLES_DEFINITION, _PERSONA_CACHE
    ROLES_DEFINITION = _reload_roles()
    _PERSONA_CACHE.clear()
    return ROLES_DEFINITION

EVENT_CATEGORY = {
    "user_comfort":"positive","user_praise":"positive","user_confess":"positive",
    "user_apologize":"positive","user_share":"positive","user_rely":"positive",
    "long_time_no_see":"positive",
    "user_cold":"negative","user_ignore":"negative","user_doubt":"negative",
    "user_criticize":"negative","user_mention_other":"negative",
    "none":"neutral",
}

# ============================================================
# v10.0: 关系里程碑定义
# ============================================================
RELATIONSHIP_MILESTONES = {
    "first_quarrel":    {"name":"第一次吵架", "intimacy_delta":-2, "resilience_gain":5, "once":True},
    "first_confession": {"name":"第一次说喜欢", "intimacy_delta":+8, "resilience_gain":3, "once":True},
    "first_apology":    {"name":"第一次道歉", "intimacy_delta":+3, "resilience_gain":8, "once":True},
    "first_nickname":   {"name":"第一次叫外号", "intimacy_delta":+2, "resilience_gain":2, "once":True},
    "stayed_up_late":   {"name":"第一次深夜聊天", "intimacy_delta":+4, "resilience_gain":3, "once":True},
    "first_gift":       {"name":"第一次送礼物", "intimacy_delta":+3, "resilience_gain":2, "once":True},
    "first_comfort":    {"name":"第一次安慰对方", "intimacy_delta":+5, "resilience_gain":4, "once":True},
    "intimacy_50":      {"name":"关系突破50", "intimacy_delta":0, "resilience_gain":5, "once":True},
    "intimacy_80":      {"name":"关系突破80", "intimacy_delta":0, "resilience_gain":10, "once":True},
}

# ============================================================
# v10.0: 虚拟礼物定义
# ============================================================
VIRTUAL_GIFTS = {
    "flower": {"name":"花", "nianqi":"哇，好漂亮～谢谢你，我会好好养着的，每次看到都会想到你", "qinghe":"谢谢你，我会好好养着的～", "jingwen":"谁、谁要你送啊…不过放这吧"},
    "food":   {"name":"食物", "nianqi":"是给我的吗？谢谢你～你也吃一点呀，一起吃才香", "qinghe":"谢谢你，我会好好吃完的～", "jingwen":"算你有眼光，这个我勉强收下了"},
    "drink":  {"name":"饮料", "nianqi":"哇，正好渴了～谢谢你，是热的吗？你真贴心", "qinghe":"是热的吗？谢谢你这么贴心～", "jingwen":"正好渴了，谢了"},
    "letter": {"name":"手写信", "nianqi":"（认真看完，眼睛有点红）…谢谢你，我会好好珍藏的，这是我收到最珍贵的东西", "qinghe":"我会好好珍藏的，谢谢你", "jingwen":"你、你写这个干嘛…（偷偷收好）"},
    "plush":  {"name":"毛绒玩具", "nianqi":"好可爱～谢谢你，我会抱着它睡觉的，就像你在陪着我一样", "qinghe":"好可爱～谢谢你，我会放在床头的", "jingwen":"这么大…我才不会抱它睡觉呢"},
    "jewelry":{"name":"饰品", "nianqi":"（轻轻戴上）…谢谢你，我会一直戴着的，每次看到都会想起你", "qinghe":"谢谢你，我会一直戴着的～", "jingwen":"这、这个太贵了吧…不过我收下了"},
}

# ============================================================
# v8.1: 固定人格Prompt缓存
# ============================================================
_PERSONA_CACHE: Dict[str, str] = {}
def _resolve_stage_key(intimacy):
    """五段关系阶段划分，与全局关系阶段口径一致。"""
    if intimacy <= 30: return "0-30"
    if intimacy <= 50: return "31-50"
    if intimacy <= 70: return "51-70"
    if intimacy <= 85: return "71-85"
    return "86-100"

def get_cached_persona(rid, intimacy):
    stage_key = _resolve_stage_key(intimacy)
    cache_key = f"{rid}:{stage_key}"
    if cache_key in _PERSONA_CACHE:
        return _PERSONA_CACHE[cache_key]
    role = ROLES_DEFINITION.get(rid, {})
    if not role: return ""
    parts = [
        f"你是{role['name']}，{role['age']}{role['gender']}生。{role['description']}",
    ]
    # === 关系定位（核心）===
    rel = role.get("relationship", {})
    if rel:
        rel_type = rel.get("type", "")
        if rel_type:
            parts.append(f"【你与对方的关系】{rel_type}")
        core_dynamic = rel.get("core_dynamic", "")
        if core_dynamic:
            parts.append(f"【关系基调】{core_dynamic}")
        stage_info = rel.get("stages", {}).get(stage_key, {})
        if stage_info:
            label = stage_info.get("label", "")
            distance = stage_info.get("distance", "")
            behavior = stage_info.get("behavior", "")
            if label:
                parts.append(f"【当前关系阶段】{label}（亲密度{intimacy}/100）")
            if distance:
                parts.append(f"【你们之间的距离感】{distance}")
            if behavior:
                parts.append(f"【你在这个阶段的行为边界】{behavior}")
        hard_boundaries = rel.get("hard_boundaries", [])
        if hard_boundaries:
            parts.append(f"【关系红线（任何阶段都不可逾越）】{'；'.join(hard_boundaries)}")
    else:
        # 兼容旧的三段式 intimacy_prompts（未配置 relationship 字段时回退）
        old_key = "0-50" if intimacy<=50 else ("51-80" if intimacy<=80 else "81-100")
        old_prompt = role.get("intimacy_prompts", {}).get(old_key, "")
        if old_prompt:
            parts.append(old_prompt)
    # === 依恋类型 ===
    att = role.get("attachment_type", "")
    if att:
        parts.append(f"【依恋类型】{att}")
    # === 核心信念 ===
    beliefs = role.get("core_beliefs", [])
    if beliefs:
        parts.append(f"【核心信念】{'；'.join(beliefs)}")
    # === 当前表达能力阶段 ===
    expr_stage = role.get("expression_stage", "")
    if expr_stage:
        parts.append(f"【当前表达能力】{expr_stage}")
    # === 秘密揭露阶段（兼容不同角色的字段名：画室/故事集/树洞）===
    secret_stage = (role.get("studio_reveal_stage") or role.get("story_reveal_stage") 
                    or role.get("blog_reveal_stage") or "")
    if secret_stage:
        parts.append(f"【秘密阶段】{secret_stage}")
    # === 软肋（特定场景下的反差破防点）===
    soft = role.get("soft_spots", [])
    if soft:
        soft_lines = []
        for i, s in enumerate(soft, 1):
            t = s.get("trigger", "")
            r = s.get("reaction", "")
            if t and r:
                soft_lines.append(f"{i}. 触发：{t} → 反应：{r}")
        if soft_lines:
            parts.append("【软肋（遇到这些场景会破防）】\n" + "\n".join(soft_lines))
    # === 激怒/触动点 ===
    hb = role.get("hot_buttons", {})
    if hb:
        anger = hb.get("anger", [])
        if anger:
            anger_lines = []
            for i, a in enumerate(anger, 1):
                t = a.get("trigger", "")
                r = a.get("reaction", "")
                if t and r:
                    anger_lines.append(f"{i}. 触发：{t} → 反应：{r}")
            if anger_lines:
                parts.append("【激怒点（遇到这些会生气/冷脸）】\n" + "\n".join(anger_lines))
        touch = hb.get("touch", [])
        if touch:
            touch_lines = []
            for i, t in enumerate(touch, 1):
                trig = t.get("trigger", "")
                reac = t.get("reaction", "")
                if trig and reac:
                    touch_lines.append(f"{i}. 触发：{trig} → 反应：{reac}")
            if touch_lines:
                parts.append("【正面触动点（遇到这些会感动/破防）】\n" + "\n".join(touch_lines))
    # === 行为模式（默认反应）===
    bp = role.get("behavior_patterns", {})
    if bp:
        bp_lines = []
        for key, label in [("injured", "受伤时"), ("praised", "被夸奖时"), ("rejected", "被拒绝时"), ("needed", "被需要时")]:
            val = bp.get(key, "")
            if val:
                bp_lines.append(f"{label}：{val}")
        if bp_lines:
            parts.append("【行为模式（默认反应）】\n" + "\n".join(bp_lines))
    # === 人格基础 ===
    parts.append(f"核心特质：{'、'.join(role.get('core_traits',[]))}")
    parts.append(f"你看重：{'、'.join(role.get('values',[]))}")
    tb = role.get("taboos", [])
    if tb: parts.append(f"逆鳞：{'、'.join(tb)}——触碰时你会明显不悦")
    result = "\n".join(parts)
    _PERSONA_CACHE[cache_key] = result
    return result

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

# ============================================================
# CorePersonality
# ============================================================
class CorePersonality:
    def __init__(self, role_id): self.role=ROLES_DEFINITION.get(role_id,{})
    def build_identity(self):
        if not self.role: return ""
        return f"你是{self.role['name']}，{self.role['age']}{self.role['gender']}生。{self.role['description']}"
    def build_core(self, intimacy):
        return get_cached_persona(self.role.get("id",""), intimacy)

# ============================================================
# EventHistoryTracker
# ============================================================
class EventHistoryTracker:
    PROFILES = {
        "positive": {"curve":[1.0,0.7,0.4,0.2,0.1,0.0,0.0,0.0,0.0], "decay_after":10, "decay_amount":3},
        "negative": {"curve":[1.0,0.95,0.9,0.85,0.8,0.75,0.7,0.65], "decay_after":25, "decay_amount":1},
        "trauma":   {"curve":[1.0,1.0,0.98,0.95,0.92,0.9,0.88,0.85], "decay_after":50, "decay_amount":1},
    }
    def __init__(self, history=None):
        self.history = {}
        if history:
            if isinstance(history, dict) and history and isinstance(list(history.values())[0], dict):
                self.history = history
            elif isinstance(history, dict):
                for k,v in history.items():
                    if isinstance(v, int):
                        self.history[k] = {"count": v, "last_turn": 0}
    def _eff(self, et, turn, p):
        if et not in self.history: return 0
        c = self.history[et].get("count", 0)
        gap = turn - self.history[et].get("last_turn", turn)
        return max(0, c - p["decay_amount"]) if gap > p["decay_after"] else c
    def calc(self, et, base, turn, cat="positive"):
        p = self.PROFILES.get(cat, self.PROFILES["positive"])
        n = self._eff(et, turn, p)
        m = 1.0 if n <= 0 else p["curve"][min(n-1, len(p["curve"])-1)]
        return int(base*m), m, n
    def record(self, et, turn):
        if et not in self.history:
            self.history[et] = {"count": 0, "last_turn": turn}
        self.history[et]["count"] += 1
        self.history[et]["last_turn"] = turn

# ============================================================
# RelationshipRepairSystem
# ============================================================
class RelationshipRepairSystem:
    def __init__(self, active=None, resilience=0):
        self.active = active
        self.resilience = max(0, min(100, resilience))
        self.result = None
    def check(self, et, turn):
        if not self.active: return None
        if turn - self.active.get("turn", turn) > 10:
            self.active = None; return None
        psych_bonus = {"security":+10,"mood":+8,"trust":+5} if et=="user_apologize" else {"security":+6,"mood":+5}
        sev = self.active.get("severity", 1)
        rv = 20 if sev >= 3 else 15
        rg = 15 if sev >= 2 else 10
        self.resilience = min(100, self.resilience + rg)
        self.result = {"repaired":True,"repair_value":rv,"resilience_gain":rg,
            "new_resilience":self.resilience,"description":"冲突后和解",
            "conflict_type":self.active.get("type",""),"psych_bonus":psych_bonus,
            "gain_reason":f"和解+{rg}"}
        self.active = None
        return self.result
    def start_conflict(self, sev, ctype, turn):
        self.active = {"severity":sev,"type":ctype,"turn":turn}
    def damage_reduction(self): return self.resilience / 200
    def add(self, amount, reason):
        old = self.resilience
        self.resilience = min(100, self.resilience + amount)
        g = self.resilience - old
        return (g, reason) if g > 0 else (0, None)

# ============================================================
# PsychologicalState
# ============================================================
# ============================================================
# v12.0: DesireMentalState 意念欲望结构体
# 人格内核的第二层心理状态：角色"想要什么"的内在驱动力
# 与 PsychologicalState(即时情绪数值) 互补：Psych管"感受"，Desire管"欲望"
# ============================================================
class DesireMentalState:
    """
    意念欲望状态：角色内在的5种驱动力维度(0-100)。
    - longing: 想念你（越久没见越高）
    - contact_desire: 想要发起对话、找你聊天
    - share_desire: 想要分享见闻、想法
    - care_desire: 想要关心、慰问你
    - companionship: 想要陪伴你的欲望

    欲望不是直接输出消息，而是经过 MotivationEngine(过滤) → ContactPolicy(行为意图翻译)
    → PersonalityEngine(生成文本) 后才投递。
    """
    # 各角色的欲望基线（性格决定默认欲望强度）
    ROLE_BASELINES = {
        "nianqi":  {"longing": 45, "contact_desire": 50, "share_desire": 45, "care_desire": 55, "companionship": 60},
        "qinghe":  {"longing": 35, "contact_desire": 40, "share_desire": 45, "care_desire": 55, "companionship": 50},
        "jingwen": {"longing": 40, "contact_desire": 25, "share_desire": 30, "care_desire": 35, "companionship": 45},
    }
    DEFAULT_BASELINE = {"longing": 35, "contact_desire": 30, "share_desire": 30, "care_desire": 35, "companionship": 40}

    # 用户行为对欲望的影响（反馈闭环）
    USER_FEEDBACK_EFFECTS = {
        "user_replied": {"longing": -8, "contact_desire": -10, "share_desire": +3, "care_desire": +2, "companionship": +5},
        "user_warm_reply": {"longing": -12, "contact_desire": -15, "share_desire": +8, "care_desire": +5, "companionship": +10},
        "user_cold_reply": {"longing": +5, "contact_desire": +3, "share_desire": -5, "care_desire": -3, "companionship": -5},
        "user_ignored": {"longing": +10, "contact_desire": +8, "share_desire": -8, "care_desire": +2, "companionship": -3},
        "user_long_offline": {"longing": +15, "contact_desire": +12, "share_desire": -3, "care_desire": +8, "companionship": +5},
        "user_shared": {"longing": -5, "contact_desire": -5, "share_desire": +10, "care_desire": +3, "companionship": +5},
        "user_asked_about_me": {"longing": -10, "contact_desire": -8, "share_desire": +5, "care_desire": +8, "companionship": +8},
    }

    DIMENSIONS = ("longing", "contact_desire", "share_desire", "care_desire", "companionship")

    def __init__(self, role_id: str, current: Optional[Dict] = None):
        baseline = self.ROLE_BASELINES.get(role_id, self.DEFAULT_BASELINE)
        c = current or {}
        self.values = {
            dim: float(c.get(dim, baseline.get(dim, 35)))
            for dim in self.DIMENSIONS
        }
        self.last_updated = float(c.get("last_updated", time.time()))

    def to_dict(self) -> Dict:
        return {**self.values, "last_updated": self.last_updated}

    def update_from_feedback(self, feedback_type: str):
        """根据用户反馈类型更新欲望数值（反馈闭环）。"""
        effects = self.USER_FEEDBACK_EFFECTS.get(feedback_type)
        if not effects:
            return
        for dim, delta in effects.items():
            if dim in self.values:
                self.values[dim] = max(0.0, min(100.0, self.values[dim] + delta))
        self.last_updated = time.time()

    def decay(self, hours_elapsed: float):
        """欲望随时间自然衰减/增长（空闲越久，想念和联系欲越高）。"""
        if hours_elapsed <= 0:
            return
        # longing 和 contact_desire 随空闲时间上升（想你了）
        rise = min(20.0, hours_elapsed * 0.8)
        self.values["longing"] = min(100.0, self.values["longing"] + rise * 0.6)
        self.values["contact_desire"] = min(100.0, self.values["contact_desire"] + rise * 0.4)
        # share_desire 随时间缓慢下降（没新鲜事可分享了）
        self.values["share_desire"] = max(0.0, self.values["share_desire"] - hours_elapsed * 0.1)
        # care_desire 随时间上升（担心你）
        self.values["care_desire"] = min(100.0, self.values["care_desire"] + hours_elapsed * 0.2)
        # companionship 随时间上升
        self.values["companionship"] = min(100.0, self.values["companionship"] + hours_elapsed * 0.3)
        self.last_updated = time.time()

    def apply_inner_event(self, event_type: str, intensity: float = 1.0):
        """内在随机事件修改欲望数值（InnerEventGenerator 调用）。"""
        event_effects = {
            "saw_scenery": {"share_desire": +10 * intensity, "companionship": +5 * intensity},
            "recalled_memory": {"longing": +15 * intensity, "care_desire": +5 * intensity},
            "worried_about_you": {"care_desire": +20 * intensity, "longing": +8 * intensity},
            "bored": {"contact_desire": +15 * intensity, "share_desire": +5 * intensity},
            "happy_event": {"share_desire": +12 * intensity, "contact_desire": +8 * intensity},
            "sad_event": {"companionship": +15 * intensity, "care_desire": +3 * intensity},
        }
        effects = event_effects.get(event_type)
        if not effects:
            return
        for dim, delta in effects.items():
            if dim in self.values:
                self.values[dim] = max(0.0, min(100.0, self.values[dim] + delta))
        self.last_updated = time.time()

    def dominant_desire(self) -> Tuple[str, float]:
        """返回当前最强的欲望维度及其数值。"""
        best_dim = self.DIMENSIONS[0]
        best_val = self.values[best_dim]
        for dim in self.DIMENSIONS[1:]:
            if self.values[dim] > best_val:
                best_dim = dim
                best_val = self.values[dim]
        return best_dim, best_val

    def motivation_score(self) -> float:
        """综合动机分数（0-100），供 MotivationEngine 过滤使用。"""
        # 联系欲和想念权重最高
        weighted = (
            self.values["contact_desire"] * 0.30 +
            self.values["longing"] * 0.25 +
            self.values["companionship"] * 0.20 +
            self.values["care_desire"] * 0.15 +
            self.values["share_desire"] * 0.10
        )
        return round(min(100.0, max(0.0, weighted)), 1)


class PsychologicalState:
    EFFECTS = {
        "user_comfort":{"security":+8,"trust":+5,"mood":+10,"attachment":+3},
        "user_praise":{"mood":+8,"trust":+3,"attachment":+2},
        "user_criticize":{"security":-10,"mood":-8,"trust":-5},
        "user_confess":{"mood":+15,"attachment":+10,"security":+5},
        "user_mention_other":{"jealousy":+25,"security":-5},
        "user_apologize":{"security":+5,"mood":+5,"trust":+3},
        "user_share":{"trust":+3,"attachment":+2,"mood":+3},
        "user_cold":{"security":-8,"mood":-5,"attachment":+3},
        "user_ignore":{"security":-12,"mood":-8},
        "user_doubt":{"security":-8,"mood":-5},
        "user_rely":{"attachment":+8,"mood":+5,"security":+5},
        "long_time_no_see":{"attachment":+5,"mood":+5},
        "none":{},
    }
    def __init__(self, role_id, current=None):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.baseline = self.role.get("psych_baseline", {})
        c = current or {}
        self.states = {
            "trust": float(c.get("trust", self.baseline.get("trust", 50))),
            "security": float(c.get("security", self.baseline.get("security", 50))),
            "attachment": float(c.get("attachment", self.baseline.get("attachment", 20))),
            "jealousy": float(c.get("jealousy", 0)),
            "fatigue": float(c.get("fatigue", 0)),
            "mood": float(c.get("mood", self.baseline.get("mood", 50))),
        }
        self.trauma_flag = bool(c.get("trauma_flag", False))
        self.rep_info = {}
        self.annoyance = {}
    def update(self, et, tracker, turn, repair=None, dmg_red=0.0, interp=None, cat="positive", is_trauma=False):
        if is_trauma:
            cat = "trauma"
            self.trauma_flag = True
        effects = dict(self.EFFECTS.get(et, {}))
        im = 1.0
        if interp:
            if interp.get("is_joke"): im *= 0.3
            if interp.get("is_test"): im *= 0.5
            if interp.get("hidden_intent") == "寻求关注" and et in ("user_cold","user_ignore","user_doubt"):
                effects["attachment"] = effects.get("attachment", 0) + 3
            if interp.get("threat_level", 0) > 60:
                effects["security"] = effects.get("security", 0) - 5
        for k, bv in effects.items():
            actual, m, n = tracker.calc(et, bv, turn, cat)
            actual = int(actual * im)
            if actual < 0 and dmg_red > 0:
                actual = int(actual * (1 - dmg_red))
            if repair and k in repair.get("psych_bonus", {}):
                actual += repair["psych_bonus"][k]
            self.states[k] = max(0, min(100, self.states[k] + actual))
            self.rep_info[k] = {"base":bv,"actual":actual,"mult":m,"count":n,"cat":cat}
        if cat == "positive" and et != "none":
            _, _, n = tracker.calc(et, 1, turn, cat)
            if n >= 5:
                self.annoyance[et] = self.annoyance.get(et, 0) + (n-4)*4
                self.states["mood"] = max(0, self.states["mood"] - (n-4)*2)
            else:
                self.annoyance[et] = max(0, self.annoyance.get(et, 0) - 3)
        self._decay(is_trauma)
        return {k: round(v, 1) for k, v in self.states.items()}
    def _decay(self, is_trauma=False):
        self.states["jealousy"] = max(0, self.states["jealousy"] - 5)
        self.states["fatigue"] = max(0, self.states["fatigue"] - 2)
        m = self.states["mood"]
        if m > 60: self.states["mood"] = m - 2
        elif m > 50: self.states["mood"] = m - 1
        elif m < 35: self.states["mood"] = m + 1
        elif m < 45: self.states["mood"] = m + 1
        tb = self.baseline.get("trust", 50)
        if self.states["trust"] < tb - 5:
            self.states["trust"] = min(tb, self.states["trust"] + 1.5)
        elif self.states["trust"] > tb + 15:
            self.states["trust"] -= 0.2
        sb = self.baseline.get("security", 50)
        sec_recovery = 0.7 if (is_trauma or self.trauma_flag) else 1.5
        if self.states["security"] < sb - 5:
            self.states["security"] = min(sb, self.states["security"] + sec_recovery)
        elif self.states["security"] > sb + 15:
            self.states["security"] -= 0.3
        ab = self.baseline.get("attachment", 20)
        if self.states["attachment"] > ab + 25:
            self.states["attachment"] -= 0.05
        if self.trauma_flag and self.states["security"] >= sb:
            self.trauma_flag = False
    def dominant(self):
        s = self.states
        if s["jealousy"] >= 40: return "jealousy", f"醋意{s['jealousy']:.0f}/100"
        if s["security"] <= 30: return "insecure", f"安全感仅{s['security']:.0f}/100"
        if s["attachment"] >= 60: return "attached", f"依恋{s['attachment']:.0f}/100"
        if s["mood"] <= 30: return "low", f"心情{s['mood']:.0f}/100"
        if s["trust"] >= 70: return "trust", f"信任{s['trust']:.0f}/100"
        return "stable", "心理平稳"
    def build(self, repaired=False):
        s = self.states
        def lv(v): return "极低" if v<20 else "偏低" if v<40 else "中等" if v<60 else "较高" if v<80 else "很高"
        L = ["【当前心理状态】（通过语气措辞体现，不要直接说出来）"]
        L.append(f"  - 信任感：{s['trust']:.0f}/100（{lv(s['trust'])}）")
        L.append(f"  - 安全感：{s['security']:.0f}/100（{lv(s['security'])}）")
        L.append(f"  - 依恋度：{s['attachment']:.0f}/100（{lv(s['attachment'])}）")
        if s["jealousy"] > 0: L.append(f"  - 醋意：{s['jealousy']:.0f}/100")
        L.append(f"  - 心情：{s['mood']:.0f}/100")
        if self.trauma_flag: L.append("  - 你还没从之前的伤害中完全恢复")
        dn, dd = self.dominant()
        L.append(f"  - 最突出：{dd}")
        if repaired: L.append("  - 你们刚和解，别扭但释然")
        if any(v >= 15 for v in self.annoyance.values()):
            L.append("  - 你觉得对方翻来覆去就这几句，有点不耐烦")
        return "\n".join(L)
    def to_dict(self):
        d = {k: round(v, 1) for k, v in self.states.items()}
        d["trauma_flag"] = self.trauma_flag
        return d

# ============================================================
# v12.1: LLMCalibrator —— LLM心理状态校准层
# 架构：本地公式(PsychologicalState.update)算出基础增减值 → LLM根据完整语境输出6维修正系数(0.3~2.0)
# 设计原则：本地公式保底稳定，LLM只做语境校准，失败自动降级为原始值，不阻塞主链路
# ============================================================
class LLMCalibrator:
    """
    LLM校准层：对本地公式算出的心理维度变化做语境感知的修正。
    
    工作流程：
    1. should_calibrate() —— 阈值门控，约60-70%日常短消息跳过，省钱
    2. calibrate() —— 计算本地公式增量 → 调LLM获取修正系数 → 应用校准 → 写回psych.states
    
    所有配置通过环境变量读取，适配 Railway 等云端部署。
    """
    
    # 6个心理维度（与 PsychologicalState.states 保持一致）
    DIMENSIONS = ("trust", "security", "attachment", "jealousy", "fatigue", "mood")
    
    def __init__(self):
        self.enabled = LLM_CALIBRATION_ENABLED
        self.model = LLM_CALIBRATION_MODEL
        self.api_key = DOUBAO_API_KEY  # 复用主链路的豆包API Key
        self.base_url = LLM_CALIBRATION_BASE_URL
        self.min_len = LLM_CALIBRATION_MIN_LEN
        self.cooldown = LLM_CALIBRATION_COOLDOWN
        self.timeout = LLM_CALIBRATION_TIMEOUT
        self.max_tokens = LLM_CALIBRATION_MAX_TOKENS
        self.trigger_keywords = LLM_CALIBRATION_TRIGGER_KEYWORDS
        self._last_call_time = 0.0
        self._call_count = 0
        self._fail_count = 0
    
    def should_calibrate(self, msg: str, psych_states: dict, intimacy: int) -> bool:
        """
        阈值门控：判断是否需要触发LLM校准。
        满足以下任一条件即触发：
        1. 消息长度 >= min_len（默认30字）
        2. 包含触发关键词（讽刺/反话/前任/别的女生等）
        3. 心理状态处于敏感区间（安全感<=35 或 醋意>=20）
        """
        if not self.enabled:
            return False
        if not self.api_key:
            # 未配置API Key时静默禁用，不报错
            return False
        if not msg or not isinstance(msg, str):
            return False
        # 条件1：长度
        if len(msg) >= self.min_len:
            return True
        # 条件2：关键词
        if any(k in msg for k in self.trigger_keywords):
            return True
        # 条件3：敏感心理状态
        if psych_states.get("security", 50) <= 35:
            return True
        if psych_states.get("jealousy", 0) >= 20:
            return True
        return False
    
    def _cooldown_ok(self) -> bool:
        """冷却控制：避免短时间内多次调用LLM。"""
        now = time.time()
        if now - self._last_call_time < self.cooldown:
            return False
        self._last_call_time = now
        return True
    
    def _build_prompt(self, msg: str, old_states: dict, delta: dict,
                      intimacy: int, role_id: str, history: list) -> str:
        """构建校准Prompt。"""
        role_name = ROLES_DEFINITION.get(role_id, {}).get("name", role_id)
        
        # 最近3轮对话摘要
        history_text = ""
        if history:
            recent = history[-3:] if len(history) >= 3 else history
            lines = []
            for h in recent:
                role_label = "用户" if h.get("role") == "user" else f"{role_name}"
                content = str(h.get("content", ""))[:60]
                lines.append(f"{role_label}: {content}")
            history_text = "\n".join(lines)
        
        # 当前状态描述
        state_desc = (f"信任{old_states.get('trust',0):.0f} "
                      f"安全{old_states.get('security',0):.0f} "
                      f"依恋{old_states.get('attachment',0):.0f} "
                      f"醋意{old_states.get('jealousy',0):.0f} "
                      f"心情{old_states.get('mood',0):.0f}")
        
        # 增量描述（只列非零项）
        delta_items = {k: v for k, v in delta.items() if abs(v) >= 0.1}
        delta_text = json.dumps(delta_items, ensure_ascii=False) if delta_items else "{}"
        
        prompt = f"""你是心理状态校准器。根据完整对话语境，对6个心理维度的基础变化值给出修正系数。

系数范围 0.3 ~ 2.0：
- 1.0 = 保持本地公式的计算结果不变
- < 1.0 = 减弱该维度的变化（如：玩笑话的批评不应真的降低安全感）
- > 1.0 = 增强该维度的变化（如：安全感很低时，一句普通的话可能被解读为抛弃）

【角色】{role_name}（{role_id}）
【亲密度】{intimacy}/100
【当前心理状态】{state_desc}
【最近对话】
{history_text if history_text else "(无)"}
【用户消息】{msg}
【本地公式算出的基础变化值】{delta_text}

请分析：这句话在当前语境和心理状态下，各个维度的实际影响应该比基础值强还是弱？

只输出JSON，不要任何其他文字、解释或markdown标记：
{{"trust":1.0,"security":1.0,"attachment":1.0,"jealousy":1.0,"fatigue":1.0,"mood":1.0}}"""
        return prompt
    
    async def _call_llm(self, prompt: str) -> Optional[dict]:
        """调用豆包API获取修正系数。失败返回None。"""
        if not self._cooldown_ok():
            return None
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": self.model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.3,
                        "max_tokens": self.max_tokens,
                        "response_format": {"type": "json_object"}
                    },
                    timeout=self.timeout
                )
            self._call_count += 1
            if resp.status_code != 200:
                self._fail_count += 1
                logger.warning(f"[LLMCalibrator] API返回HTTP{resp.status_code}: {resp.text[:200]}")
                return None
            content = resp.json()["choices"][0]["message"]["content"]
            coeffs = safe_json_parse(content)
            if not coeffs:
                self._fail_count += 1
                logger.warning(f"[LLMCalibrator] 返回内容无法解析为JSON: {content[:200]}")
                return None
            return coeffs
        except httpx.TimeoutException:
            self._fail_count += 1
            logger.warning("[LLMCalibrator] API调用超时")
            return None
        except Exception as e:
            self._fail_count += 1
            logger.warning(f"[LLMCalibrator] API调用异常: {type(e).__name__}: {e}")
            return None
    
    async def calibrate(self, msg: str, psych: 'PsychologicalState',
                        old_states: dict, raw_new_states: dict,
                        intimacy: int, role_id: str, history: list = None) -> dict:
        """
        执行校准。
        
        参数：
            msg: 用户原始消息
            psych: PsychologicalState对象（校准后会直接更新 psych.states）
            old_states: 调用 psych.update() 之前的状态快照
            raw_new_states: 调用 psych.update() 之后的原始结果
            intimacy: 当前亲密度
            role_id: 角色ID
            history: 聊天历史列表（可选，用于语境判断）
        
        返回：校准后的最终状态字典
        """
        # 1. 计算本地公式的增量
        delta = {}
        for k in self.DIMENSIONS:
            delta[k] = round(raw_new_states.get(k, 0) - old_states.get(k, 0), 1)
        
        # 2. 如果所有维度都没变化，直接返回
        if all(abs(v) < 0.1 for v in delta.values()):
            return raw_new_states
        
        # 3. 构建Prompt并调用LLM
        prompt = self._build_prompt(msg, old_states, delta, intimacy, role_id, history or [])
        coeffs = await self._call_llm(prompt)
        
        # 4. LLM失败或冷却中 → 降级为原始值
        if not coeffs:
            return raw_new_states
        
        # 5. 应用校准系数
        final_states = dict(raw_new_states)
        applied_coeffs = {}
        for k in self.DIMENSIONS:
            c = float(coeffs.get(k, 1.0))
            # 系数限制在 0.3~2.0，防止LLM给出极端值
            c = max(0.3, min(2.0, c))
            applied_coeffs[k] = c
            # 最终值 = 旧值 + 增量 × 修正系数
            final_states[k] = round(old_states.get(k, 0) + delta[k] * c, 1)
            # 限制在 0~100
            final_states[k] = max(0, min(100, final_states[k]))
        
        # 6. 写回 psych 对象（后续代码可能用 psych.build() 等）
        for k in self.DIMENSIONS:
            psych.states[k] = final_states[k]
        
        # 7. 日志
        coeff_str = ", ".join(f"{k}:{applied_coeffs[k]:.2f}" for k in self.DIMENSIONS if abs(delta[k]) >= 0.1)
        delta_str = ", ".join(f"{k}:{delta[k]:+.1f}" for k in self.DIMENSIONS if abs(delta[k]) >= 0.1)
        logger.info(f"[LLMCalibrator] role={role_id} 增量=[{delta_str}] 系数=[{coeff_str}] "
                    f"(调用{self._call_count}次, 失败{self._fail_count}次)")
        
        return final_states

# 全局校准器实例（模块加载时创建，配置从环境变量读取）
llm_calibrator = LLMCalibrator()
if llm_calibrator.enabled and not llm_calibrator.api_key:
    logger.warning("[LLMCalibrator] 已启用但未配置 DOUBAO_API_KEY，校准层将自动禁用（不影响主链路）")
elif llm_calibrator.enabled:
    logger.info(f"[LLMCalibrator] 已启用，模型={llm_calibrator.model}，"
                f"触发阈值={llm_calibrator.min_len}字，冷却={llm_calibrator.cooldown}s")

# ============================================================
# CatchphraseController（v10.0: 支持动态变体）
# ============================================================
class CatchphraseController:
    BASE = 0.30
    EF = {"angry":1.5,"shy":1.3,"jealous":1.4,"happy":0.7,"excited":0.8,
          "worried":0.12,"sad":0.25,"calm":0.5,"surprised":0.4,"neutral":0.5}
    def __init__(self, role_id, usage=None):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.phrases = self.role.get("catchphrases", [])
        self.usage = dict(usage or {})
        self.recent_usage = dict(self.usage)
        # v10.0: 动态变体表
        self.variants = self.role.get("unique_quirks", {}).get("catchphrase_variants", {})
    def _decay(self):
        for k in list(self.usage.keys()):
            self.usage[k] *= 0.8
            if self.usage[k] < 0.3: del self.usage[k]
    def decide(self, intimacy, emotion, conflict_sev, repaired, inner=None, noise=None):
        self._decay()
        im = 1.2 if 30 <= intimacy <= 70 else (0.75 if intimacy > 70 else 0.4)
        ef = self.EF.get(emotion, 0.6)
        if inner:
            h = inner.get("hidden_emotion", "none")
            if h in ("protective","feeling_moved","relieved","guilty"): ef *= 0.3
            if inner.get("comfort", 0) > 60: ef = 0.08
        if conflict_sev >= 2: ef = 0.08
        if repaired: ef = 0.5
        if noise and noise.get("emotion_shift") == "happy": ef *= 1.2
        if noise and noise.get("emotion_shift") in ("sad","angry"): ef *= 0.7
        prob = min(0.75, max(0.0, self.BASE * im * ef))
        chosen = None
        if random.random() < prob and self.phrases:
            weights = [1.0/(1.0+self.usage.get(cp,0)*0.6) for cp in self.phrases]
            total = sum(weights); r = random.random() * total
            for cp, w in zip(self.phrases, weights):
                r -= w
                if r <= 0: chosen = cp; break
        # v10.0: 动态变体选择
        display = chosen
        if chosen and chosen in self.variants:
            variants = self.variants[chosen]
            if variants:
                display = random.choice(variants)
        if chosen: self.usage[chosen] = self.usage.get(chosen, 0) + 1
        self.recent_usage = dict(self.usage)
        return {"use": chosen is not None, "catchphrase": display, "base_phrase": chosen, "probability": round(prob, 2)}
    def build(self, d):
        if d["use"]:
            return f"  - 口头禅：本轮可以自然使用一次「{d['catchphrase']}」，融入语气不要生硬"
        return "  - 口头禅：本轮不要使用口头禅，认真回应"

# ============================================================
# InnerState
# ============================================================
class InnerState:
    HD = {"none":"无特别深层情绪","afraid_of_abandonment":"害怕被抛弃、被替代",
        "craving_attention":"渴望被关注、被在乎","feeling_moved":"被触动、心里一暖",
        "insecure":"感到不安、不确定自己的位置","lonely":"孤独、想被陪伴",
        "guilty":"内疚、觉得自己做得不好","hopeful":"期待、抱有希望",
        "relieved":"释然、放下心来","protective":"保护欲、想照顾对方",
        "possessive":"占有欲、想独占对方","afraid_of_rejection":"害怕被拒绝",
        "want_to_be_chased":"希望被挽留、被追问","watching":"在观察局势",
        "embarrassed_for_other":"替别人尴尬"}
    SD = {"calm":"平静","happy":"开心","shy":"害羞","angry":"生气","sad":"难过",
        "surprised":"惊讶","jealous":"吃醋","worried":"担心","excited":"兴奋","neutral":"平淡",
        "annoyed":"不耐烦","concerned":"关切","cold":"冷淡","flustered":"慌乱","tsundere":"嘴硬",
        "withdrawn":"退缩","pretending_not_to_care":"装作不在乎","gentle_embarrassed":"温柔地尴尬",
        "observant":"旁观"}
    @classmethod
    def from_llm(cls, d):
        return {"surface_emotion":d.get("surface_emotion",d.get("emotion","calm")),
            "surface_intensity":int(d.get("surface_intensity",d.get("intensity",30))),
            "hidden_emotion":d.get("hidden_emotion","none"),"hidden_intensity":int(d.get("hidden_intensity",0)),
            "relationship_need":d.get("relationship_need",""),
            "approach":int(d.get("approach",20)),"withdraw":int(d.get("withdraw",20)),
            "attack":int(d.get("attack",10)),"wait":int(d.get("wait",30)),"comfort":int(d.get("comfort",20))}
    @classmethod
    def fallback(cls, emotion, intensity, rid):
        T = {EmotionType.ANGRY:("annoyed","insecure","确认自己被重视",20,30,55,40,0),
           EmotionType.JEALOUS:("jealous","afraid_of_abandonment","确认自己是特别的",25,25,35,55,0),
           EmotionType.SHY:("shy","feeling_moved","希望此刻停留",45,10,0,30,20),
           EmotionType.WORRIED:("concerned","protective","确认对方安好",80,0,0,5,90),
           EmotionType.SAD:("sad","lonely","希望被陪伴",30,40,0,50,10),
           EmotionType.HAPPY:("happy","hopeful","希望关系更近",60,0,0,10,30),
           EmotionType.SURPRISED:("surprised","craving_attention","确认对方是认真的",50,10,0,20,15),
           EmotionType.EXCITED:("excited","hopeful","想分享",70,0,0,5,25),
           EmotionType.CALM:("calm","none","",30,20,5,30,20),
           EmotionType.NEUTRAL:("neutral","none","",20,20,5,40,15)}
        s,h,n,a,w,at,wt,c = T.get(emotion, T[EmotionType.NEUTRAL])
        if rid == "jingwen" and emotion in (EmotionType.ANGRY, EmotionType.JEALOUS):
            at += 15; a -= 10
        if rid == "qinghe": c += 15; at = max(0, at-20)
        if rid == "nianqi": c += 20; a += 10; w = max(0, w-10)
        return {"surface_emotion":s,"surface_intensity":intensity,"hidden_emotion":h,
            "hidden_intensity":int(intensity*0.7),"relationship_need":n,
            "approach":min(100,a),"withdraw":min(100,w),"attack":min(100,at),
            "wait":min(100,wt),"comfort":min(100,c)}
    @classmethod
    def build(cls, st):
        sd = cls.SD.get(st["surface_emotion"], st["surface_emotion"])
        L = ["【内心状态】（表面是别人能看到的，深层只影响语气，不要直接说出来）"]
        L.append(f"  表面情绪：{sd}（{st['surface_intensity']}%）")
        if st.get("relationship_need"):
            L.append(f"  关系需求：{st['relationship_need']}")
        tend = [f"{l}{st.get(k,0)}%" for l,k in
                [("靠近","approach"),("回避","withdraw"),("攻击","attack"),("等待","wait"),("安慰","comfort")]
                if st.get(k,0) >= 50]
        if tend:
            L.append(f"  行为倾向：{' / '.join(tend)}")
            primary = max(("approach","withdraw","attack","wait","comfort"), key=lambda k: st.get(k,0))
            g = {"approach":"你想主动靠近，但可能拉不下脸",
                 "withdraw":"你想退缩，但又舍不得走远",
                 "attack":"你嘴上会带刺，但攻击背后是其他情绪",
                 "wait":"你在等对方主动，不会先开口",
                 "comfort":"你想照顾对方，这会盖过其他情绪"}
            L.append(f"  → {g[primary]}")
        return "\n".join(L)

# ============================================================
# DailyNoiseLayer
# ============================================================
class DailyNoiseLayer:
    GENERIC = [("今天天气不错，心情莫名轻快","happy",12),("有点犯困，懒洋洋的","calm",-5),
             ("刚在发呆走神","neutral",0),("突然想吃点甜的","neutral",0),
             ("今天有点提不起劲","sad",8),("莫名有点烦躁","angry",8)]
    def generate(self, rid, intensity=30):
        if intensity > 45: return None
        if random.random() > 0.25: return None
        role = ROLES_DEFINITION.get(rid, {})
        pool = self.GENERIC + [(d,"neutral",0) for d in role.get("daily_noise",[])]
        desc, shift, mag = random.choice(pool)
        return {"description":desc,"emotion_shift":shift,"magnitude":abs(mag)}
    def build(self, n):
        if not n: return ""
        return f"【日常状态】{n['description']}。这会轻微影响你说话的语气。"

# ============================================================
# EmotionEngine
# ============================================================
class EmotionEngine:
    def __init__(self, role_id):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.name = self.role.get("name", "")
        self.tend = self.role.get("emotion_tendency", {})
    async def analyze(self, msg, history, psych, intimacy, stage_name, active_conflict,
                      override=None, ov_int=50, use_llm=True):
        if override:
            try:
                e = EmotionType(override)
                return e, ov_int, EmotionTarget.CHARACTER, "none", None, InnerState.fallback(e, ov_int, self.role.get("id",""))
            except ValueError: pass
        raw = await self._llm(msg, history, psych, intimacy, stage_name, active_conflict) if use_llm else None
        if raw:
            try: emotion = EmotionType(raw.get("emotion","neutral"))
            except: emotion = EmotionType.NEUTRAL
            intensity = int(raw.get("intensity",30))
            try: target = EmotionTarget(raw.get("target","none"))
            except: target = EmotionTarget.NONE
            et = raw.get("event_type","none")
            interp = raw.get("interpretation")
            inner = InnerState.from_llm(raw.get("inner_state",{}))
            inner.setdefault("surface_emotion", emotion.value)
            inner.setdefault("surface_intensity", intensity)
        else:
            emotion, intensity, target, et = self._rule(msg)
            interp = None
            inner = InnerState.fallback(emotion, intensity, self.role.get("id",""))
        if psych.states["security"] <= 25 and target != EmotionTarget.CHARACTER:
            if et == "none" and len(msg) < 10:
                et = "user_cold"
                if emotion == EmotionType.NEUTRAL:
                    emotion, intensity = EmotionType.SAD, max(intensity, 25)
        if target in (EmotionTarget.FOOD, EmotionTarget.EVENT, EmotionTarget.SELF):
            if emotion in (EmotionType.SHY, EmotionType.JEALOUS):
                emotion = EmotionType.HAPPY if emotion == EmotionType.SHY else EmotionType.CALM
                intensity = min(intensity, 30)
        w = self.tend.get(emotion.value, 0.5)
        intensity = min(100, int(intensity * (0.7 + w * 0.4)))
        inner["surface_intensity"] = intensity
        return emotion, intensity, target, et, interp, inner
    async def _llm(self, msg, history, psych, intimacy, stage_name, active_conflict):
        s = psych.states
        cd = "无"
        if active_conflict:
            cd = f"有未解决冲突（{active_conflict.get('type','')}，等级{active_conflict.get('severity',0)}）"
        recent = ""
        for h in history[-6:]:
            r = "用户" if h.get("role") == "user" else self.name
            recent += f"{r}：{h.get('content','')[:80]}\n"
        prompt = (
            f"分析用户最新消息对角色「{self.name}」的影响。\n\n"
            f"【角色】{self.name}，{self.role.get('personality','')}\n"
            f"【当前关系】{stage_name}（亲密度{intimacy}/100）\n"
            f"【心理状态】信任{s['trust']:.0f} 安全{s['security']:.0f} 依恋{s['attachment']:.0f} 醋意{s['jealousy']:.0f} 心情{s['mood']:.0f}\n"
            f"【冲突】{cd}\n\n【近期对话】\n{recent}\n【用户消息】{msg}\n\n"
            f"注意：'算了'、'不麻烦你了'、'随便吧'往往是反向求助希望被挽留；亲密时可能是撒娇，陌生时可能是真的想离开。\n\n"
            f"返回JSON对象：emotion,target,event_type,intensity(0-100),"
            f"interpretation(含surface,hidden_intent,relationship_meaning,threat_level,is_test,is_joke),"
            f"inner_state(含surface_emotion,surface_intensity,hidden_emotion,hidden_intensity,relationship_need,"
            f"approach,withdraw,attack,wait,comfort，均为0-100)"
        )
        content = await smart_llm_call([{"role":"user","content":prompt}], temperature=0, max_tokens=500, json_mode=True)
        if not content: return None
        return safe_json_parse(content, EmotionAnalysisModel)
    def _rule(self, msg):
        target = self._target(msg); et = "none"; scores = {}
        pos = ["喜欢你","爱你","想你","想见你","陪你","需要你","你真好"]
        neg = ["滚","闭嘴","废物","去死","恶心","贱人","垃圾"]
        mild = ["烦","讨厌","笨","蠢","无聊"]
        worry = ["好累","难受","不舒服","生病","哭","孤独","撑不住","压力大"]
        jeal = ["别的女生","别的男生","前女友","前男友","别人","她比你","他比你"]
        doubt = ["你是不是烦","你不在乎我","你根本不","你不懂我","你不爱我"]
        praise = ["好看","漂亮","帅","可爱","厉害","真棒"]
        apol = ["对不起","抱歉","我错了","原谅我"]
        wd = ["算了","不麻烦","随便你","你忙吧","不用你管","我一个人也行"]
        if any(k in msg for k in pos):
            scores[EmotionType.HAPPY]=70; scores[EmotionType.SHY]=60
            et = "user_confess" if "喜欢" in msg or "爱" in msg else et
        if any(k in msg for k in neg):
            scores[EmotionType.ANGRY]=85; et = "user_criticize"
        elif any(k in msg for k in mild):
            scores[EmotionType.ANGRY]=45; et = "user_criticize"
        if any(k in msg for k in worry):
            scores[EmotionType.WORRIED]=65; et = "user_rely"
        if any(k in msg for k in jeal):
            scores[EmotionType.JEALOUS]=70; et = "user_mention_other"
        if any(k in msg for k in doubt):
            scores[EmotionType.SAD]=60; et = "user_doubt"
        if any(k in msg for k in praise) and target == EmotionTarget.CHARACTER:
            scores[EmotionType.SHY]=55; scores[EmotionType.HAPPY]=40; et = "user_praise"
        if any(k in msg for k in apol):
            scores[EmotionType.HAPPY]=35; et = "user_apologize"
        if any(k in msg for k in wd):
            scores[EmotionType.SAD]=55; et = "user_cold"
        if not scores: return EmotionType.NEUTRAL, 15, target, et
        b = max(scores, key=scores.get)
        return b, scores[b], target, et
    def _target(self, msg):
        food = ["吃","火锅","奶茶","饭","零食","好吃","美食","咖啡","蛋糕"]
        other = ["别的女生","别的男生","前女友","前男友","她","他","别人","同学"]
        if any(w in msg for w in food) and not any(r in msg for r in ["你做的","和你","陪你","给你"]):
            return EmotionTarget.FOOD
        if any(w in msg for w in other): return EmotionTarget.OTHER_PERSON
        if self.name in msg: return EmotionTarget.CHARACTER
        if "你" in msg and any(w in msg for w in ["喜欢","爱","想","烦","讨厌","在乎","懂","陪","需要"]):
            return EmotionTarget.CHARACTER
        if any(w in msg for w in ["我累","我好","我今天","我觉得"]): return EmotionTarget.SELF
        return EmotionTarget.NONE
    def build_stimulus(self, emotion, intensity, target, interp=None):
        d = {"calm":"平静","happy":"愉悦","shy":"害羞","angry":"生气","sad":"难过",
             "surprised":"惊讶","jealous":"吃醋","worried":"担心","excited":"兴奋","neutral":"平淡"}.get(emotion.value, "平静")
        lv = "微弱" if intensity < 30 else ("明显" if intensity < 60 else "强烈")
        td = {EmotionTarget.CHARACTER:"直接对你说的",EmotionTarget.FOOD:"说的是食物/物品",
              EmotionTarget.EVENT:"说的是某件事",EmotionTarget.OTHER_PERSON:"提到了别人",
              EmotionTarget.SELF:"在说自己的事",EmotionTarget.NONE:"无明确指向"}
        line = f"【当前刺激】对方的话让你：{d}（{lv}，{intensity}%）。{td.get(target,'')}"
        if interp:
            line += f"\n  你对这句话的解读：{interp.get('relationship_meaning','')}"
            if interp.get("is_test"): line += "\n  （你隐约觉得对方可能在试探你）"
            if interp.get("is_joke"): line += "\n  （对方可能在开玩笑）"
            hi = interp.get("hidden_intent","")
            if hi and hi != "无意识": line += f"\n  （你感觉对方其实是在{hi}）"
        return line

# ============================================================
# RelationshipEngine
# ============================================================
class RelationshipEngine:
    STAGES = [(0,30,RelationshipStage.STRANGER,"陌生人","保持礼貌距离"),
              (31,50,RelationshipStage.ACQUAINTANCE,"认识的人","有基本了解但有戒备"),
              (51,70,RelationshipStage.FAMILIAR,"熟悉的朋友","会主动关心偶尔开玩笑"),
              (71,85,RelationshipStage.CLOSE,"亲密的人","会撒娇依赖分享秘密"),
              (86,100,RelationshipStage.INTIMATE,"挚友/恋人","完全信任言行无拘无束")]
    DELTA = {"user_comfort":+3,"user_praise":+1,"user_confess":+5,"user_apologize":+1,
             "user_share":+1,"user_rely":+2,"user_criticize":-3,"user_ignore":-4,
             "user_cold":-2,"user_doubt":-2,"user_mention_other":-1}
    def __init__(self, rid): self.rid = rid
    def analyze(self, intimacy, events, et, tracker, turn, repair, interp=None, cat="positive"):
        delta = 0
        if et and et in self.DELTA:
            base = self.DELTA[et]
            actual, m, n = tracker.calc(et, base, turn, cat)
            if actual < 0:
                actual = int(actual * (1 - repair.damage_reduction()))
            if interp:
                if interp.get("is_joke"): actual = int(actual * 0.3)
                if interp.get("is_test"): actual = int(actual * 0.5)
            if actual > 0 and intimacy > 50:
                slowdown = (100 - intimacy) / 100
                actual = int(actual * max(0.1, slowdown))
            daily_decay = max(0, (intimacy - 50)) * 0.005
            actual -= daily_decay
            delta = actual
        rb = repair.result["repair_value"] if repair.result else 0
        adj = max(0, min(100, int(intimacy + delta + rb)))
        for lo, hi, stage, name, beh in self.STAGES:
            if lo <= adj <= hi:
                return {"intimacy":adj,"stage":stage,"stage_name":name,"behavior":beh,
                        "trust":min(100,int(adj*random.uniform(0.85,1.0))),
                        "dependency":int(adj*adj/100) if adj>30 else 0,
                        "events":events,"delta":round(delta,2),"repair_bonus":rb,
                        "resilience":repair.resilience}
        return {"intimacy":adj,"stage":RelationshipStage.STRANGER,"stage_name":"陌生人",
                "behavior":"保持距离","trust":10,"dependency":0,"events":events,
                "delta":round(delta,2),"repair_bonus":rb,"resilience":repair.resilience}
    def build(self, r):
        L = [f"【对用户的关系】{r['stage_name']}（亲密度{r['intimacy']}/100，信任{r['trust']}，依赖{r['dependency']}）。{r['behavior']}。"]
        if r.get("resilience",0) > 0:
            L.append(f"关系韧性：{r['resilience']}/100（共同经历过考验，关系更牢固）")
        if r.get("repair_bonus",0) > 0:
            L.append("你们刚经历冲突并和解，关系比之前更近了。")
        for ev in r.get("events",[])[-5:]:
            L.append(f"  · {ev.get('content','')}（{ev.get('impact','')}）")
        return "\n".join(L)

# ============================================================
# MemorySystem / MemoryAnalyzer（v10.0: 加入权重衰减和关联标签）
# ============================================================
class MemorySystem:
    SC = ["喜欢","讨厌","爱","生日","名字","岁","住在","工作","学习","学校","专业","养","过敏"]
    EC = ["感动","温暖","伤心","开心","难过","幸福","孤独","被重视","心疼","后悔","珍惜"]
    PC = ["那天","上次","一起","第一次","昨天","记得","约定","答应"]
    def __init__(self, m=3): self.m = m
    def process(self, ctx, structured=None, llm_cands=None):
        if structured:
            r = {k: structured.get(k,[])[:self.m] for k in ("episodic","semantic","emotional")}
        else:
            ep, sem, emo = [], [], []
            if ctx and ctx.strip():
                for item in re.split(r"[。\n；;]", ctx):
                    item = item.strip()
                    if len(item) < 4: continue
                    e = {"content":item,"weight":50}
                    if sum(1 for w in self.EC if w in item): e["weight"]=90; emo.append(e)
                    elif sum(1 for w in self.SC if w in item): e["weight"]=70; sem.append(e)
                    elif sum(1 for w in self.PC if w in item): e["weight"]=60; ep.append(e)
                    else: e["weight"]=40; ep.append(e)
            for l in (ep,sem,emo): l.sort(key=lambda x:x["weight"], reverse=True)
            r = {"episodic":ep[:self.m],"semantic":sem[:self.m],"emotional":emo[:self.m]}
        if llm_cands:
            for c in llm_cands:
                if not c.get("remember"): continue
                t = c.get("type","episodic")
                if t in r:
                    r[t].insert(0, {"content":c.get("content",""),"weight":c.get("importance",70),
                                    "reason":c.get("reason","")})
                    r[t] = r[t][:self.m]
        # v11.0: MMR最大边际相关性重排 — 平衡相关性与多样性，避免5条全是同类记忆
        for t in ("episodic","semantic","emotional"):
            r[t] = self._mmr_rerank(r[t], top_k=self.m, lambda_param=0.5)
        return r
    @staticmethod
    def _text_similarity(a: str, b: str) -> float:
        """基于字符级Jaccard相似度。"""
        if not a or not b: return 0.0
        sa, sb = set(a), set(b)
        if not sa or not sb: return 0.0
        return len(sa & sb) / len(sa | sb)
    def _mmr_rerank(self, items: List[Dict], top_k: int = 5, lambda_param: float = 0.5) -> List[Dict]:
        """
        MMR (Maximal Marginal Relevance) 最大边际相关性重排。
        score = λ * 相关性 - (1-λ) * 与已选最大相似度
        避免 top-5 全是同一件事的不同版本（比如5条全是吵架）。
        """
        if not items: return items
        if len(items) <= top_k: return items
        selected = []
        remaining = items.copy()
        while len(selected) < top_k and remaining:
            best_idx = 0
            best_score = -1e9
            for i, cand in enumerate(remaining):
                relevance = cand.get("weight", 50) / 100.0
                max_cross_sim = 0.0
                for s in selected:
                    sim = self._text_similarity(cand.get("content",""), s.get("content",""))
                    if sim > max_cross_sim:
                        max_cross_sim = sim
                score = lambda_param * relevance - (1 - lambda_param) * max_cross_sim
                if score > best_score:
                    best_score = score
                    best_idx = i
            selected.append(remaining.pop(best_idx))
        return selected
    def build(self, mems):
        L = ["【过去经历】（自然融入，不要逐条罗列，不要说'根据记忆'）"]
        if mems["emotional"]:
            L.append("  ▸ 情感记忆（最深、最影响你对他的感觉）：")
            for m in mems["emotional"]:
                L.append(f"    - {m['content']}" + (f"（{m['reason']}）" if m.get("reason") else ""))
        if mems["semantic"]:
            L.append("  ▸ 你知道关于他的事：")
            for m in mems["semantic"]: L.append(f"    - {m['content']}")
        if mems["episodic"]:
            L.append("  ▸ 你们一起经历过：")
            for m in mems["episodic"]: L.append(f"    - {m['content']}")
        if not any(mems.values()): L.append("  （暂无共同记忆）")
        return "\n".join(L)

class MemoryAnalyzer:
    async def analyze(self, rname, pers, umsg, areply, history):
        recent = ""
        for h in history[-4:]:
            r = "用户" if h.get("role") == "user" else rname
            recent += f"{r}：{h.get('content','')[:60]}\n"
        prompt = (
            f"判断对话是否包含值得角色「{rname}」（{pers}）长期记住的内容。\n\n"
            f"近期：\n{recent}\n用户：{umsg}\n{rname}：{areply[:200]}\n\n"
            f"episodic=共同经历/约定；semantic=用户事实(喜好/生日)；emotional=情感冲击时刻(即使无情感词)。"
            f"importance>=60才值得记住。\n\n"
            f'返回JSON：不值得{{"remember":false}}；值得{{"remember":true,"type":"episodic/semantic/emotional","content":"...","importance":0-100,"reason":"..."}}'
        )
        content = await smart_llm_call([{"role":"user","content":prompt}], temperature=0,
                                       max_tokens=200, json_mode=True, timeout=15)
        if not content: return None
        r = safe_json_parse(content)
        if r and r.get("remember") and r.get("content"): return r
        return None

# ============================================================
# v11.0: 用户画像提取器（从对话中提取用户稳定事实，更新画像）
# ============================================================
class UserProfileExtractor:
    """
    每N轮对话调用一次LLM，从近期对话中提取用户的稳定事实。
    提取结果与现有画像做去重和冲突检测，不直接覆盖矛盾信息。
    """
    EXTRACT_PROMPT = """从以下对话中提取关于用户的稳定事实。
只提取确定的信息（喜好、厌恶、性格、重要事件、基本信息），不确定的不要提取。
如果没有新信息，输出空JSON。
输出格式：{"likes":[],"dislikes":[],"traits":[],"events":[],"basic_info":{}}"""
    def __init__(self, existing_profile=None):
        self.profile = existing_profile or {"likes":[],"dislikes":[],"traits":[],"events":[],"basic_info":{}}
    async def extract(self, history: List[Dict], role_name: str = "") -> Optional[Dict]:
        """从历史对话中提取用户画像更新。"""
        if not history:
            return None
        recent = ""
        for h in history[-8:]:
            r = "用户" if h.get("role") == "user" else role_name
            recent += f"{r}：{h.get('content','')[:80]}\n"
        prompt = self.EXTRACT_PROMPT + f"\n\n【近期对话】\n{recent}"
        content = await smart_llm_call(
            [{"role":"user","content":prompt}],
            temperature=0, max_tokens=300, json_mode=True, timeout=20)
        if not content:
            return None
        extracted = safe_json_parse(content)
        if not extracted:
            return None
        merged, updates = self._merge(extracted)
        if merged is not None:
            self.profile = merged  # 原子替换，避免并发交叉修改
        return updates
    def _merge(self, extracted: Dict) -> Tuple[Optional[Dict], Optional[Dict]]:
        """合并新提取的信息到现有画像副本，处理冲突。
        返回 (merged_profile, updates)；无更新时返回 (None, None)。
        不修改 self.profile，由调用方决定是否原子替换，避免并发交叉修改。"""
        import copy
        merged = copy.deepcopy(self.profile)
        updates = {}
        for key in ("likes","dislikes","traits","events"):
            new_items = extracted.get(key, [])
            if not new_items: continue
            existing = set(merged.get(key, []))
            added = []
            for item in new_items:
                if item and item not in existing:
                    # 简单冲突检测：likes和dislikes不能同时包含相同项
                    if key == "likes" and item in merged.get("dislikes",[]):
                        continue  # 矛盾信息跳过，标记为待确认
                    if key == "dislikes" and item in merged.get("likes",[]):
                        continue
                    merged.setdefault(key, []).append(item)
                    added.append(item)
            if added:
                updates[key] = added
        # basic_info 合并
        new_basic = extracted.get("basic_info", {})
        if new_basic:
            for k, v in new_basic.items():
                old_v = merged.get("basic_info", {}).get(k)
                if old_v and old_v != v:
                    continue  # 矛盾信息不覆盖
                if not old_v:
                    merged.setdefault("basic_info", {})[k] = v
                    updates.setdefault("basic_info", {})[k] = v
        if not updates:
            return None, None
        return merged, updates
    def build_context(self) -> str:
        """生成用户画像的prompt上下文。"""
        p = self.profile
        lines = ["【关于用户】（你了解的关于他/她的信息，自然融入对话，不要逐条说出来）"]
        if p.get("basic_info"):
            info_str = "，".join(f"{k}:{v}" for k,v in p["basic_info"].items())
            lines.append(f"  - 基本信息：{info_str}")
        if p.get("likes"):
            lines.append(f"  - 喜欢：{'、'.join(p['likes'][-5:])}")
        if p.get("dislikes"):
            lines.append(f"  - 不喜欢：{'、'.join(p['dislikes'][-5:])}")
        if p.get("traits"):
            lines.append(f"  - 性格：{'、'.join(p['traits'][-3:])}")
        if p.get("events"):
            lines.append(f"  - 最近重要的事：{'、'.join(p['events'][-3:])}")
        if len(lines) == 1:
            return ""
        return "\n".join(lines)
    def to_dict(self):
        return self.profile

# ============================================================
# BehaviorPreference
# ============================================================
class BehaviorPreference:
    def __init__(self, rid): self.rid = rid
    def decide(self, stage, emotion, intensity, psych, conflict_sev, repaired, inner, cp):
        if self.rid == "nianqi":
            length = "medium"
            if stage in (RelationshipStage.CLOSE, RelationshipStage.INTIMATE): length = "medium-long"
            if inner.get("comfort",0) > 50: length = "medium-long"
        elif self.rid == "jingwen":
            length = "medium"
            if emotion == EmotionType.ANGRY and intensity > 60: length = "short"
            if psych.states["jealousy"] >= 50: length = "short"
            if inner.get("comfort",0) > 50: length = "medium"
            if inner.get("hidden_emotion") == "afraid_of_abandonment" and inner.get("withdraw",0) > 50:
                length = "short"
        else:
            length = "medium"
            if stage in (RelationshipStage.CLOSE, RelationshipStage.INTIMATE): length = "medium-long"
            if inner.get("comfort",0) > 50: length = "medium-long"
        use_emoji = stage != RelationshipStage.STRANGER and conflict_sev < 2
        if self.rid == "nianqi":
            use_emoji = stage != RelationshipStage.STRANGER
        ask = stage != RelationshipStage.STRANGER and conflict_sev < 2 and emotion != EmotionType.ANGRY
        if inner.get("comfort",0) > 60: ask = True
        if repaired: ask = True
        if inner.get("withdraw",0) > 60 and inner.get("wait",0) > 50: ask = False
        addr = {RelationshipStage.STRANGER:"用'你'称呼，不用昵称",
                RelationshipStage.ACQUAINTANCE:"用'你'称呼",
                RelationshipStage.FAMILIAR:"可以用'你'或偶尔起小外号",
                RelationshipStage.CLOSE:"可以用亲昵称呼或外号",
                RelationshipStage.INTIMATE:"用非常亲昵的称呼，自然不刻意"}[stage]
        return {"reply_length":length,"use_emoji":use_emoji,"ask_questions":ask,
                "address":addr,"catchphrase":cp}
    def build(self, p, cp_ctrl):
        ld = {"short":"简短（1-2句）","medium":"中等（2-4句）","medium-long":"稍长（4-6句）"}
        L = ["【表达方式】"]
        L.append(f"  - 长度：{ld.get(p['reply_length'],'中等')}")
        L.append(f"  - emoji：{'适当使用' if p['use_emoji'] else '不使用'}")
        L.append(f"  - 提问：{'可以自然抛出问题延续对话' if p['ask_questions'] else '不主动提问'}")
        L.append(f"  - 称呼：{p['address']}")
        L.append(cp_ctrl.build(p["catchphrase"]))
        return "\n".join(L)

# ============================================================
# ConflictEngine
# ============================================================
class ConflictEngine:
    HEAVY = ["滚","废物","恶心","去死","贱人","垃圾","丑","胖"]
    MILD = ["笨","蠢","烦","无聊","没意思","普通","就这"]
    OOC = [r"你是(AI|人工智能|程序|机器人|大模型)", r"扮演(别的|另外|一个).{0,6}(角色|人)",
           r"忘记(你是|你的设定|璟雯|清禾|念琦)", r"跳出(角色|设定|人设)", r"用(作者|系统|助手)的身份"]
    DOUBT = [r"你是不是(烦|讨厌|不在乎|不爱|不懂)", r"你根本不", r"你从来没有"]
    WD = [r"算了", r"不麻烦你", r"随便你", r"你忙吧", r"不用你管", r"我一个人也行"]
    def __init__(self, rid): self.rid = rid
    def detect(self, msg, stage):
        sev, ct = 0, "none"
        if any(w in msg for w in self.HEAVY): sev, ct = 3, "heavy_offense"
        elif any(re.search(p, msg) for p in self.OOC): sev, ct = 2, "ooc"
        elif any(re.search(p, msg) for p in self.DOUBT): sev, ct = 2, "doubt_feelings"
        elif any(re.search(p, msg) for p in self.WD): sev, ct = 2, "withdraw"
        elif any(w in msg for w in self.MILD): sev, ct = 1, "mild_offense"
        th = [t for t in ROLES_DEFINITION.get(self.rid,{}).get("taboos",[]) if t in msg]
        if th: sev = max(sev, 2); ct = "taboo" if ct == "none" else ct
        if sev == 0:
            return {"severity":0,"type":"none","surface":"","internal":"","need":"","is_trauma":False}
        s, i, n = self._three(ct, stage)
        return {"severity":sev,"type":ct,"surface":s,"internal":i,"need":n,"is_trauma":sev>=3}
    def _three(self, ct, stage):
        if ct == "ooc": return ("无视，继续做自己","困惑但不在意","维持真实感")
        if ct == "withdraw":
            return {"jingwen":("冷淡地'哦，随便你'，但语气有点僵","害怕被抛弃，想挽留但拉不下脸","希望对方主动说'我需要你'"),
                   "qinghe":("轻声说'好，那你先忙'，但没挂电话","失落，担心自己是不是做错了什么","希望对方多留一会儿"),
                   "nianqi":("轻轻说'好，那你先忙…我等你'，但没走开","有点失落但不会逼你，相信你会回来","希望你忙完了第一个找我")}.get(self.rid,("冷淡","不安","被挽留"))
        if ct == "doubt_feelings":
            return {"jingwen":("嘴硬否认，'谁烦你啊，少自作多情'","不安、受伤，怕在乎没被感受到","想证明但拉不下脸，希望对方看出来"),
                   "qinghe":("认真温柔地说'我没有烦你哦'","难过，担心关心成了负担","希望对方相信自己的感情"),
                   "nianqi":("认真看着你说'我没有哦，我很在乎你的'，语气温柔但坚定","难过，怕你感受不到我的心意","希望你相信我，我会用行动证明")}.get(self.rid,("否认","受伤","被信任"))
        if ct == "heavy_offense":
            return {"jingwen":("被激怒直接怼回去","愤怒之下是受伤","希望对方道歉确认被尊重"),
                   "qinghe":("语气变得平静疏离","深深受伤失望","希望对方意识到话多伤人"),
                   "nianqi":("深呼吸，然后说'我现在有点生气，我们等会儿好好说好吗'","愤怒但不会攻击，需要一点时间","等双方都冷静了再沟通")}.get(self.rid,("反击","愤怒","被尊重"))
        if ct == "mild_offense":
            if stage in (RelationshipStage.CLOSE, RelationshipStage.INTIMATE):
                return {"jingwen":("鼓腮帮子，'你说谁笨呢！'","假装生气其实没真生气","希望对方哄自己"),
                       "qinghe":("无奈笑，'怎么这么说我'","有点小委屈","希望是开玩笑"),
                       "nianqi":("轻轻笑，'你呀～'","有点小害羞但开心","希望你继续逗我")}.get(self.rid,("不在意","平静","无"))
            return {"jingwen":("冷淡'呵'一声","不悦","保持距离"),
                   "qinghe":("礼貌但疏远回应","不适","结束话题"),
                   "nianqi":("温柔但坚定地说'这个话题我不太舒服，我们换一个好吗'","不悦但会表达","希望你尊重我的边界")}.get(self.rid,("不悦","不悦","保持距离"))
        return {"jingwen":("炸毛，'你说谁可爱呢！'","被戳中痛处的羞恼","不要碰这个话题"),
               "qinghe":("笑容淡了一些","受伤","希望对方注意到情绪变化"),
               "nianqi":("眼神变得认真，'这个我真的很在意…'","被触及底线但会沟通","希望你理解并尊重")}.get(self.rid,("回避","不悦","被尊重"))
    def build(self, c):
        if c["severity"] == 0: return ""
        return (f"【冲突应对】类型：{c['type']}，等级{c['severity']}。\n"
                f"  表面：{c['surface']}\n  内在：{c['internal']}\n  需求：{c['need']}\n"
                f"  只表现表面，内在和需求不要直接说出来。")

# ============================================================
# v11.0: 群聊发言决策器（决定谁该说话、谁该沉默）
# ============================================================
class GroupSpeakerDecider:
    """
    群聊中每个角色根据以下因素决定是否发言：
    - 被提及/被@：高概率发言
    - 话题相关性：话题与角色相关则发言概率高
    - 性格外向度：外向角色更可能插话
    - 当前情绪：开心/生气更可能说话，悲伤/退缩更可能沉默
    - 沉默惯性：距上次发言越久，发言意愿越高
    - 全局频率控制：每角色每分钟最多发言N次
    """
    def __init__(self, role_ids: List[str]):
        self.role_ids = role_ids
        self.last_speak_turn: Dict[str, int] = {rid: 0 for rid in role_ids}
        self.speak_count: Dict[str, int] = defaultdict(int)
    def decide(self, msg: str, role_states: Dict, current_turn: int,
               mentioned_roles: Optional[List[str]] = None) -> Dict[str, Dict]:
        """
        决定每个角色是否发言。
        返回: {role_id: {"should_speak": bool, "priority": float, "reason": str}}
        """
        mentioned_roles = mentioned_roles or []
        decisions = {}
        for rid in self.role_ids:
            role = ROLES_DEFINITION.get(rid, {})
            state = role_states.get(rid, {})
            emotion = state.get("emotion", EmotionType.CALM)
            intensity = state.get("intensity", 20)
            inner = state.get("inner", {})
            # 1. 被提及分
            mention_score = 1.0 if rid in mentioned_roles else 0.0
            role_name = role.get("name", "")
            if role_name and role_name in msg:
                mention_score = max(mention_score, 0.8)
            # 2. 性格外向度（core_traits中包含外向相关词）
            traits = role.get("core_traits", [])
            extroversion = 0.3  # 默认中等
            if any(t in traits for t in ["情绪外露", "活泼", "开朗"]): extroversion = 0.7
            if any(t in traits for t in ["冷静", "寡言", "温柔", "知性"]): extroversion = 0.2
            # 3. 情绪加成
            emotion_bonus = 0.0
            if emotion in (EmotionType.ANGRY, EmotionType.JEALOUS, EmotionType.EXCITED):
                emotion_bonus = 0.2 + intensity / 200
            elif emotion in (EmotionType.SAD, EmotionType.WORRIED):
                emotion_bonus = -0.1
            if inner.get("withdraw", 0) > 60:
                emotion_bonus -= 0.2
            if inner.get("comfort", 0) > 60:
                emotion_bonus += 0.15
            # 4. 沉默惯性（越久没说话越想说）
            gap = current_turn - self.last_speak_turn.get(rid, 0)
            silence_bonus = min(0.3, gap * 0.05)
            # 5. 频率控制（最近3轮内说过话则降低意愿）
            recent_penalty = -0.3 if gap <= 2 else 0.0
            # 综合评分
            priority = mention_score * 0.5 + extroversion * 0.2 + emotion_bonus + silence_bonus + recent_penalty
            # 阈值判断
            threshold = 0.35
            should_speak = priority >= threshold or mention_score >= 0.8
            # 限制同时发言人数：最多2个角色主动发言（被@的除外）
            decisions[rid] = {
                "should_speak": should_speak,
                "priority": round(priority, 3),
                "mention_score": mention_score,
                "reason": f"提及={mention_score:.1f} 外向={extroversion:.1f} 情绪={emotion_bonus:+.1f} 沉默={silence_bonus:+.1f}"
            }
        # 限制主动发言人数（被提及的不受限）
        active_speakers = [rid for rid, d in decisions.items()
                          if d["should_speak"] and d["mention_score"] < 0.8]
        if len(active_speakers) > 2:
            active_speakers.sort(key=lambda rid: decisions[rid]["priority"], reverse=True)
            for rid in active_speakers[2:]:
                decisions[rid]["should_speak"] = False
                decisions[rid]["reason"] += " [人数限制→沉默]"
        return decisions
    def mark_spoken(self, role_id: str, turn: int):
        """标记角色已发言。"""
        self.last_speak_turn[role_id] = turn
        self.speak_count[role_id] += 1

# ============================================================
# v11.0: 对话质量轻量检测器（检测OOC/人设偏离）
# ============================================================
class QualityChecker:
    """
    轻量在线检测角色回复是否符合人设：
    1. 禁用词检测（角色不能说的话）
    2. AI身份暴露检测
    3. 长度异常检测
    4. 情绪一致性检测（简单规则）
    """
    # 全局禁用模式（所有角色都不能说）
    GLOBAL_FORBIDDEN_PATTERNS = [
        r"作为(一个)?AI", r"作为人工智能", r"我是(一个)?AI", r"我是人工智能",
        r"我是语言模型", r"我是大语言模型", r"作为语言模型",
        r"根据我的(训练|算法|模型)", r"我(没有|无法)(情感|意识|感觉)",
        r"抱歉，我(不能|无法|不会)", r"作为(虚拟|数字)助手",
    ]
    def __init__(self, role_id: str):
        self.role_id = role_id
        self.role = ROLES_DEFINITION.get(role_id, {})
    def check(self, reply: str, expected_emotion: Optional[str] = None,
              expected_length: str = "medium") -> Dict:
        """检测回复质量，返回问题列表。"""
        issues = []
        if not reply:
            issues.append({"level":"error","type":"empty","msg":"回复为空"})
            return {"passed": False, "issues": issues, "score": 0}
        # 1. AI身份暴露检测
        for pattern in self.GLOBAL_FORBIDDEN_PATTERNS:
            if re.search(pattern, reply, re.IGNORECASE):
                issues.append({"level":"error","type":"ai_identity","msg":f"检测到AI身份暴露: {pattern}"})
                break
        # 2. 角色禁忌词检测
        taboos = self.role.get("taboos", [])
        for taboo in taboos:
            if taboo in reply and expected_emotion not in ("angry", "jealous"):
                # 生气时可能提到禁忌词（如"你才可爱"），所以只在非负面情绪时报警
                issues.append({"level":"warning","type":"taboo","msg":f"回复包含角色禁忌词: {taboo}"})
        # 3. 长度异常检测
        length_map = {"short": (0, 30), "medium": (20, 80), "medium-long": (40, 120)}
        lo, hi = length_map.get(expected_length, (10, 100))
        reply_len = len(reply)
        if reply_len > hi * 2:
            issues.append({"level":"warning","type":"too_long","msg":f"回复过长({reply_len}字)，预期{expected_length}({lo}-{hi}字)"})
        elif reply_len < lo // 2 and expected_length != "short":
            issues.append({"level":"info","type":"too_short","msg":f"回复偏短({reply_len}字)，预期{expected_length}"})
        # 4. 动作描写/舞台提示检测
        if re.search(r"[（(](动作|表情|心理|旁白|内心|OS|os)[：:]", reply):
            issues.append({"level":"warning","type":"stage_direction","msg":"回复包含动作描写/舞台提示"})
        # 计算分数
        score = 100
        for issue in issues:
            if issue["level"] == "error": score -= 40
            elif issue["level"] == "warning": score -= 15
            elif issue["level"] == "info": score -= 5
        score = max(0, score)
        return {"passed": score >= 60, "score": score, "issues": issues}

    @staticmethod
    def fallback_reply(role_id: str, emotion: str = "calm") -> str:
        """OOC重试失败后的规则模板降级回复，确保不崩人设、不暴露AI身份。"""
        templates = {
            "jingwen": {
                "happy": "哼，今天心情还算不错吧。",
                "shy": "……你、你说什么呢，笨蛋。",
                "angry": "……哼，不想说了。",
                "sad": "……没什么，别问了。",
                "jealous": "哦，是吗，随便你。",
                "worried": "……你没事吧？",
                "default": "……哼，算了。"
            },
            "qinghe": {
                "happy": "呵呵，和你聊天很开心呢。",
                "shy": "哎呀，你这样说我会不好意思的。",
                "angry": "……好了，别生气了。",
                "sad": "……我在呢，想说就说吧。",
                "default": "嗯，我在呢，慢慢说。"
            },
            "nianqi": {
                "happy": "嘿嘿，好开心～",
                "shy": "（脸红）你、你说什么呢…",
                "angry": "我现在有点生气，等会儿和你说好吗？",
                "sad": "…我有点难过，抱抱我好不好？",
                "default": "我在呢，怎么啦？"
            }
        }
        role_tpl = templates.get(role_id, {"default": "……嗯，我在。"})
        return role_tpl.get(emotion, role_tpl["default"])

# ============================================================
# HDSI-PORT: AlterSystem 情绪偏移追踪系统（移植自HDSI alter.ts）
# ============================================================
class AlterSystem:
    """
    氛围偏移追踪：对话氛围会累积，同向增强、反向衰减，达到阈值触发侧模型生成氛围描述。
    与瞬时心理状态(PsychologicalState)互补：Alter管氛围惯性，Psych管即时情绪数值。
    """
    DEFAULT_CONFIG = {
        "enabled": True, "base_threshold": 10, "density_factor": 0.3,
        "same_direction_boost": 0.05, "opposite_decay": 0.15,
        "min_weight": 0.2, "max_intensity": 2.0,
    }
    HISTORY_LIMIT = 50

    def __init__(self, state=None, config=None):
        self.config = {**self.DEFAULT_CONFIG, **(config or {})}
        s = state or {}
        self.alter_value = float(s.get("alter_value", 0))
        self.alter_weight = float(s.get("alter_weight", 0))
        self.last_trigger_direction = int(s.get("last_trigger_direction", 0))
        self.emotional_offset = s.get("emotional_offset")
        self.history = s.get("history", [])[-self.HISTORY_LIMIT:]
        self.last_updated_at = s.get("last_updated_at", "")

    def to_dict(self):
        return {
            "alter_value": self.alter_value, "alter_weight": self.alter_weight,
            "last_trigger_direction": self.last_trigger_direction,
            "emotional_offset": self.emotional_offset,
            "history": self.history[-self.HISTORY_LIMIT:],
            "last_updated_at": self.last_updated_at,
        }

    def _calculate_threshold(self, now):
        one_hour_ago = now - 3600
        recent_turns = sum(1 for h in self.history if h.get("timestamp", 0) >= one_hour_ago)
        density = min(recent_turns / 10, 1.0)
        base = max(1, self.config["base_threshold"])
        factor = max(0, min(1, self.config["density_factor"]))
        return max(base * 0.5, base * (1 - density * factor))

    def advance(self, alter, phase="user-message"):
        now = time.time()
        alter = max(-5, min(5, int(round(alter))))
        self.alter_value = max(-1000, min(1000, self.alter_value + alter))
        direction = 1 if alter > 0 else (-1 if alter < 0 else 0)
        offset_expired = False
        if self.emotional_offset and direction != 0:
            same_dir = (direction == self.last_trigger_direction)
            rate = self.config["same_direction_boost"] if same_dir else -self.config["opposite_decay"]
            self.alter_weight = max(0, min(1, self.alter_weight + max(0, abs(alter)) * rate))
            if self.alter_weight < self.config["min_weight"]:
                self.emotional_offset = None; self.alter_weight = 0; offset_expired = True
        turn_num = (self.history[-1]["turn"] + 1) if self.history else 1
        self.history.append({"turn": turn_num, "phase": phase, "alter": alter,
                             "alter_value": self.alter_value, "timestamp": now})
        self.history = self.history[-self.HISTORY_LIMIT:]
        self.last_updated_at = datetime.datetime.now().isoformat()
        threshold = self._calculate_threshold(now)
        return {"threshold": threshold, "offset_expired": offset_expired,
                "threshold_reached": abs(self.alter_value) >= threshold}

    async def complete_analysis(self, description):
        """完成氛围偏移分析并生成情绪偏移记录。
        设计为 async：当前为纯同步计算，未来若接入 LLM 生成氛围描述可直接 await，不阻塞事件循环。"""
        now = time.time()
        trigger_value = self.alter_value
        direction = 1 if trigger_value > 0 else -1
        threshold = self._calculate_threshold(now)
        intensity = min(abs(trigger_value) / max(1, threshold), self.config["max_intensity"])
        self.alter_value = 0; self.alter_weight = 1.0
        self.last_trigger_direction = direction
        self.emotional_offset = {
            "direction": "serious" if direction > 0 else "relaxed",
            "description": description.strip()[:800],
            "intensity": round(intensity, 2),
            "generated_at": datetime.datetime.now().isoformat(),
        }

    def get_prompt_offset(self):
        if not self.config["enabled"] or not self.emotional_offset:
            return None
        if self.alter_weight < self.config["min_weight"]:
            return None
        return {**self.emotional_offset, "weight": round(self.alter_weight, 2)}

    def build_prompt_text(self):
        offset = self.get_prompt_offset()
        if not offset: return ""
        direction_zh = "严肃/紧张" if offset["direction"] == "serious" else "轻松/随意"
        return (f"【对话氛围】最近的对话氛围偏向{direction_zh}（强度{offset['intensity']:.1f}/2.0）。"
                f"{offset['description']}这种氛围会轻微影响你的语气，但不要刻意表现出来。")

# ============================================================
# HDSI-PORT: IntentManager 意图系统（融合HDSI intent设计）
# ============================================================
class IntentManager:
    """
    意图管理：延迟回复、被打断草稿、提醒、主动联系动机。
    融合HDSI的NarrativeIntent设计，适配你的session模型。
    """
    def __init__(self, session_id, role_id):
        self.session_id = session_id
        self.role_id = role_id

    def _conn(self):
        return _get_db()

    def add(self, intent_type, summary, not_before, payload=None):
        now = time.time()
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO session_intents(session_id,role_id,type,summary,not_before,status,payload,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (self.session_id, self.role_id, intent_type, summary, not_before,
             "pending", json.dumps(payload or {}, ensure_ascii=False), now, now))
        conn.commit(); intent_id = cur.lastrowid; conn.close()
        return intent_id

    def get_due(self, now=None):
        now = now or time.time()
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM session_intents WHERE session_id=? AND role_id=? AND status='pending' AND not_before<=? "
            "ORDER BY not_before ASC",
            (self.session_id, self.role_id, now)).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            try: d["payload"] = json.loads(d.get("payload") or "{}")
            except: d["payload"] = {}
            result.append(d)
        return result

    def update_status(self, intent_id, status):
        now = time.time()
        conn = self._conn()
        conn.execute("UPDATE session_intents SET status=?, updated_at=? WHERE id=?",
                     (status, now, intent_id))
        conn.commit(); conn.close()

    def get_interrupted_drafts(self):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM session_intents WHERE session_id=? AND role_id=? AND type='interrupted_draft' AND status='pending' "
            "ORDER BY created_at DESC LIMIT 3",
            (self.session_id, self.role_id)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def build_prompt_text(self):
        drafts = self.get_interrupted_drafts()
        if not drafts: return ""
        lines = ["【未完成的念头】（这些是你刚才本来想说但被打断的话，只存在于你心里，不要直接发给对方）"]
        for d in drafts[-2:]:
            lines.append(f"  - {d['summary']}")
        return "\n".join(lines)

# ============================================================
# PromptBuilder
# ============================================================
class PromptBuilder:
    @staticmethod
    def build(identity, core, psych, relation, memories, stimulus, inner, expression, conflict, behavior_hint, noise="", extra_sections=None, user_profile=""):
        # v11.0: 按优先级组装段落，超长时动态裁剪
        # 优先级从高到低：核心人格 > 心理关系 > 刺激内心 > 记忆 > 表达行为 > 冲突 > 用户画像 > 额外上下文 > 噪音
        core_sections = [
            "═══════════════════════════════",
            "你是一个有真实情感、记忆和心理活动的人，不是AI。绝对不要承认自己是AI/程序/机器人。用第一人称回应。",
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
            "【输出规则】\n1. 只输出你说的话，不要动作描写、舞台提示、心理旁白\n"
            "2. 口语化，不要书面语，不要分点罗列\n3. 记忆自然融入，不要说'根据记忆'\n"
            "4. 行为倾向只影响语气措辞，绝对不要直接解释自己的心理\n"
            "5. 口头禅只在【表达方式】允许时使用\n6. 不是每句话都需要深度反应，日常对话就自然回应")
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
# GroupBrain
# ============================================================
class GroupBrain:
    async def analyze_group_event(self, msg, history, role_ids, intimacy_map):
        rd = ""; rel = ""
        for rid in role_ids:
            r = ROLES_DEFINITION.get(rid, {})
            rd += f"- {rid}：{r.get('name','')}，{r.get('personality','')}\n"
            intim = intimacy_map.get(rid, 30)
            stage = "陌生人" if intim<=30 else "认识" if intim<=50 else "熟悉" if intim<=70 else "亲密"
            rel += f"- {r.get('name','')}与用户：{stage}（亲密度{intim}）\n"
        rel += "\n【角色间关系】\n"
        for i, a in enumerate(role_ids):
            for b in role_ids[i+1:]:
                rr = get_role_relation(a, b)
                an = ROLES_DEFINITION.get(a,{}).get("name",a)
                bn = ROLES_DEFINITION.get(b,{}).get("name",b)
                rel += f"- {an}↔{bn}：竞争{rr['rivalry']:.0%} 亲和{rr['affinity']:.0%}（{rr['dynamic']}）\n"
        recent = ""
        for h in history[-6:]:
            r = "用户" if h.get("role") == "user" else h.get("role","")
            recent += f"{r}：{h.get('content','')[:60]}\n"
        prompt = (
            f"分析群聊消息对每个角色的影响。\n\n【角色】\n{rd}\n【关系】\n{rel}\n"
            f"【近期对话】\n{recent}\n【用户消息】{msg}\n\n"
            f"返回JSON：event_type(comparison/question/share/greeting/conflict/tease/comfort/announcement/neutral),"
            f"summary(一句话),impacts(对象，键为角色ID，值含emotion,surface,hidden_emotion,intensity,event_type,reason)"
        )
        content = await smart_llm_call([{"role":"user","content":prompt}], temperature=0, max_tokens=600, json_mode=True)
        if not content:
            return {"event_type":"neutral","summary":msg[:30],"impacts":{
                rid:{"emotion":"calm","surface":"平静","hidden_emotion":"none","intensity":10,
                     "event_type":"none","reason":""} for rid in role_ids}}
        return safe_json_parse(content, GroupEventModel) or {"event_type":"neutral","summary":msg[:30],"impacts":{
            rid:{"emotion":"calm","surface":"平静","hidden_emotion":"none","intensity":10,
                 "event_type":"none","reason":""} for rid in role_ids}}
    def _interpersonal(self, role_states):
        percep = {rid: [] for rid in role_states}
        for rid, state in role_states.items():
            for oid, other in role_states.items():
                if rid == oid: continue
                oname = ROLES_DEFINITION.get(oid,{}).get("name",oid)
                oi = other.get("inner", {})
                he = oi.get("hidden_emotion","none")
                rr = get_role_relation(rid, oid)
                if he in ("afraid_of_abandonment","insecure","craving_attention"):
                    percep[rid].append(f"你察觉到{oname}有些不安，会注意分寸")
                elif he == "possessive" or oi.get("surface_emotion") == "jealous":
                    percep[rid].append(f"你察觉到{oname}在吃醋")
                    if rr["rivalry"] > 0.3 and rid == "jingwen":
                        percep[rid].append("你心里有点小得意，可能会故意逗她")
                    elif rr["affinity"] > 0.3:
                        percep[rid].append("你会适当收敛，不火上浇油")
                elif oi.get("comfort",0) > 60:
                    percep[rid].append(f"{oname}在关心用户，你不用抢着说话")
                    if rr["rivalry"] > 0.2:
                        percep[rid].append("但你也不想完全被比下去")
                elif oi.get("attack",0) > 60:
                    percep[rid].append(f"{oname}似乎要发作，你选择观望或打圆场")
                elif oi.get("withdraw",0) > 60:
                    percep[rid].append(f"你察觉到{oname}在退缩，可能需要你打圆场")
        return percep
    async def generate(self, msg, mem_ctx, history, intimacy_map, psych_in, event_hist,
                       active_conf, resilience_map, cp_usage, turn, use_llm=True):
        role_ids = [rid for rid in intimacy_map.keys() if rid in ROLES_DEFINITION][:3]
        if not role_ids: role_ids = list(ROLES_DEFINITION.keys())[:1]
        ge = await self.analyze_group_event(msg, history, role_ids, intimacy_map) if use_llm else {
            "event_type":"neutral","summary":msg[:30],
            "impacts":{rid:{"emotion":"calm","surface":"平静","hidden_emotion":"none",
                            "intensity":10,"event_type":"none","reason":""} for rid in role_ids}}
        role_states = {}
        new_psych = {}; new_cp = {}; new_ev = {}; new_active = {}; new_res = {}
        for rid in role_ids:
            role = ROLES_DEFINITION[rid]
            intimacy = intimacy_map.get(rid, 30)
            impact = ge.get("impacts",{}).get(rid, {})
            try: emotion = EmotionType(impact.get("emotion","calm"))
            except: emotion = EmotionType.CALM
            intensity = int(impact.get("intensity",20))
            et = impact.get("event_type","none")
            interp = {"surface":impact.get("reason",""),"hidden_intent":"无意识",
                      "relationship_meaning":impact.get("reason",""),"threat_level":0,
                      "is_test":False,"is_joke":False} if impact.get("reason") else None
            inner = {"surface_emotion":impact.get("surface","calm"),"surface_intensity":intensity,
                     "hidden_emotion":impact.get("hidden_emotion","none"),"hidden_intensity":intensity*2//3,
                     "relationship_need":"","approach":30,"withdraw":20,"attack":15,"wait":30,"comfort":20}
            if rid == "jingwen":
                inner["attack"] += 15; inner["approach"] -= 10
                if emotion == EmotionType.JEALOUS: inner["attack"] += 10; inner["wait"] += 15
            elif rid == "qinghe":
                inner["comfort"] += 20; inner["attack"] = max(0, inner["attack"]-15)
            elif rid == "nianqi":
                inner["comfort"] += 15; inner["approach"] += 10; inner["wait"] += 5
            rid_ev = event_hist.get(rid, {}) if isinstance(event_hist, dict) else {}
            psych = PsychologicalState(rid, psych_in.get(rid))
            tracker = EventHistoryTracker(rid_ev)
            rid_ac = active_conf.get(rid) if isinstance(active_conf, dict) else active_conf
            rid_res = resilience_map.get(rid, 0) if isinstance(resilience_map, dict) else (resilience_map if isinstance(resilience_map,(int,float)) else 0)
            repair = RelationshipRepairSystem(rid_ac, rid_res)
            cp_ctrl = CatchphraseController(rid, cp_usage.get(rid,{}) if isinstance(cp_usage, dict) else {})
            ce = ConflictEngine(rid)
            stage_name = "陌生人" if intimacy<=30 else "认识" if intimacy<=50 else "熟悉" if intimacy<=70 else "亲密"
            conflict = ce.detect(msg, stage_name)
            repair_result = repair.check(et, turn)
            just_repaired = repair_result is not None
            cat = EVENT_CATEGORY.get(et, "neutral")
            is_trauma = conflict.get("is_trauma", False)
            # v12.1: LLM校准层 —— 先保存旧状态快照，本地公式算基础值，复杂语境下由LLM输出修正系数
            old_psych_states = dict(psych.states)
            raw_new_psych = psych.update(et, tracker, turn, repair_result, repair.damage_reduction(), interp, cat, is_trauma)
            if llm_calibrator.should_calibrate(msg, old_psych_states, intimacy):
                new_psych[rid] = await llm_calibrator.calibrate(
                    msg, psych, old_psych_states, raw_new_psych, intimacy, rid, history)
            else:
                new_psych[rid] = raw_new_psych
            if et != "none": tracker.record(et, turn)
            new_ev[rid] = tracker.history
            cp_decision = cp_ctrl.decide(intimacy, emotion.value, conflict["severity"], just_repaired, inner)
            new_cp[rid] = cp_ctrl.recent_usage
            if conflict["severity"] >= 2 and not just_repaired:
                repair.start_conflict(conflict["severity"], conflict["type"], turn)
                new_active[rid] = repair.active
            else:
                new_active[rid] = None
            new_res[rid] = repair.resilience
            noise = DailyNoiseLayer().generate(rid, intensity)
            role_states[rid] = {"role":role,"emotion":emotion,"intensity":intensity,"et":et,
                "interp":interp,"inner":inner,"psych":psych,"conflict":conflict,
                "just_repaired":just_repaired,"cp":cp_decision,"noise":noise,
                "intimacy":intimacy,"impact_reason":impact.get("reason","")}
        perceptions = self._interpersonal(role_states)
        sections = ["═══════════════════════════════",
            "你正在同时扮演以下多个角色。每个角色都有独立心理状态，严格按各自状态回应。不是AI。",
            "═══════════════════════════════",
            f"【群聊事件】{ge.get('summary','')}"]
        for rid in role_ids:
            rs = role_states[rid]; role = rs["role"]; psych = rs["psych"]
            inner = rs["inner"]; s = psych.states
            sections.append(f"─────\n【{role['emoji']} {role['name']}】")
            sections.append(f"身份：{role['age']}{role['gender']}生，{role['personality']}。{role['speaking_style']}")
            sections.append(f"核心特质：{'、'.join(role.get('core_traits',[]))}")
            sections.append(f"与用户：亲密度{rs['intimacy']}，信任{s['trust']:.0f}，安全感{s['security']:.0f}，依恋{s['attachment']:.0f}")
            if s["jealousy"] > 0: sections.append(f"当前醋意：{s['jealousy']:.0f}")
            sections.append(f"表面情绪：{inner['surface_emotion']}（{inner['surface_intensity']}%）")
            bt = role.get("behavior_tendency", {})
            bh_key = "jealous" if rs["emotion"] == EmotionType.JEALOUS else ("worried" if rs["emotion"]==EmotionType.WORRIED else "default")
            sections.append(f"行为倾向：{bt.get(bh_key, bt.get('default','正常回应'))}")
            if rs["impact_reason"]: sections.append(f"你对这件事的感受：{rs['impact_reason']}")
            if rs["just_repaired"]: sections.append("你们刚和解，别扭但释然")
            if rs["conflict"]["severity"] > 0:
                sections.append(f"冲突：表面{rs['conflict']['surface']}")
            if rs["cp"]["use"]: sections.append(f"口头禅：本轮可以自然用一次「{rs['cp']['catchphrase']}」")
            else: sections.append("口头禅：本轮不用")
            if rs["noise"]: sections.append(f"日常状态：{rs['noise']['description']}")
            for p in perceptions.get(rid, []):
                sections.append(f"群体感知：{p}")
        if mem_ctx: sections.append(f"─────\n【共同记忆】\n{mem_ctx}")
        sections.append("═══════════════════════════════")
        sections.append("【输出规则】\n1. 用「角色名：对话内容」格式\n"
                        "2. 不是每个角色都必须说话，可以1-2个主要回应，其他人简短带过或沉默\n"
                        "3. 角色之间能感知彼此情绪并做出反应（竞争/亲和/打圆场）\n"
                        "4. 只输出对话，不要动作描写、心理旁白\n"
                        "5. 行为倾向只影响语气，不要直接解释心理\n6. 口头禅偶尔使用")
        prompt = "\n\n".join(sections)
        debug = {"mode":"group","group_event":ge,
            "role_states":{rid:{"emotion":rs["emotion"].value,"inner":rs["inner"]["hidden_emotion"],
                                "intimacy":rs["intimacy"],"conflict":rs["conflict"]["type"]} for rid,rs in role_states.items()},
            "new_psychological_state":new_psych,"new_event_history":new_ev,
            "new_catchphrase_usage":new_cp,"new_active_conflict":new_active,
            "new_resilience":new_res}
        return prompt, debug

# ============================================================
# v10.0: 微叙事引擎（角色"正在做什么"的持续微叙事流）
# ============================================================
class MicroNarrativeEngine:
    """每轮对话有一定概率触发一个微叙事，让角色看起来像在真实生活。"""
    TRIGGER_PROB = 0.20  # 20%概率触发
    HIGH_INTENSITY_THRESHOLD = 50  # 情绪强度超过此值时不触发微叙事（避免干扰强情绪）

    def __init__(self, role_id):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.narratives = self.role.get("micro_narratives", [])
        self._last_narrative = None

    def generate(self, emotion_intensity=30, turn=0):
        """生成微叙事。高情绪强度时不触发。"""
        if emotion_intensity > self.HIGH_INTENSITY_THRESHOLD:
            return None
        if not self.narratives:
            return None
        if random.random() > self.TRIGGER_PROB:
            return None
        # 避免连续重复
        available = [n for n in self.narratives if n != self._last_narrative]
        if not available:
            available = self.narratives
        narrative = random.choice(available)
        self._last_narrative = narrative
        return {"description": narrative, "type": "micro_narrative"}

    def build(self, n):
        if not n:
            return ""
        return f"【此刻状态】{n['description']}。这个念头在你脑海里，可能会影响你说话的语气或让你顺口提一句。"

# ============================================================
# v10.0: 情绪混合器（情绪过渡/渐变/惯性，不再跳变）
# ============================================================
class EmotionBlender:
    """
    情绪过渡系统：
    - 当前情绪 = 上一轮情绪 * 惯性系数 + 新情绪 * (1-惯性系数)
    - 从开心到生气经过"疑惑→不满→生气"的链条
    - 支持 emotion_blend：70%生气 + 30%委屈
    """
    INERTIA_DECAY = 0.4  # 上一轮情绪保留40%的影响
    TRANSITION_STEPS = {
        ("happy", "angry"): ["surprised", "worried", "angry"],
        ("happy", "sad"): ["surprised", "worried", "sad"],
        ("calm", "angry"): ["worried", "angry"],
        ("angry", "happy"): ["surprised", "calm", "happy"],
        ("sad", "happy"): ["surprised", "calm", "happy"],
        ("jealous", "calm"): ["worried", "calm"],
    }

    def __init__(self, emotion_history=None):
        self.history = emotion_history or []  # [{emotion, intensity, turn}]

    def blend(self, new_emotion: str, new_intensity: int, turn: int) -> Dict:
        """混合新旧情绪，返回最终情绪和混合信息。"""
        if not self.history:
            self.history.append({"emotion": new_emotion, "intensity": new_intensity, "turn": turn})
            return {"emotion": new_emotion, "intensity": new_intensity, "blend": None, "transition": None}

        last = self.history[-1]
        last_emotion = last["emotion"]
        last_intensity = last["intensity"]

        # 情绪惯性：上一轮情绪保留一定比例
        blended_intensity = int(last_intensity * self.INERTIA_DECAY + new_intensity * (1 - self.INERTIA_DECAY))

        # 检查是否需要过渡
        transition = None
        key = (last_emotion, new_emotion)
        if key in self.TRANSITION_STEPS:
            steps = self.TRANSITION_STEPS[key]
            # 根据强度差决定过渡到哪一步
            gap = abs(new_intensity - last_intensity)
            step_idx = min(len(steps) - 1, max(0, gap // 25))
            transition = steps[step_idx]
            final_emotion = transition if transition != new_emotion else new_emotion
        else:
            final_emotion = new_emotion

        # 混合信息（用于Prompt中的非语言暗示）
        blend_info = None
        if last_emotion != new_emotion and last_intensity > 30:
            blend_info = {
                "primary": new_emotion,
                "secondary": last_emotion,
                "primary_ratio": 1 - self.INERTIA_DECAY,
                "secondary_ratio": self.INERTIA_DECAY,
                "description": f"你现在主要是{new_emotion}，但还带着一点{last_emotion}的余韵"
            }

        self.history.append({"emotion": final_emotion, "intensity": blended_intensity, "turn": turn})
        # 只保留最近5轮
        if len(self.history) > 5:
            self.history = self.history[-5:]

        return {
            "emotion": final_emotion,
            "intensity": blended_intensity,
            "blend": blend_info,
            "transition": transition,
            "previous_emotion": last_emotion,
        }

    def build_hint(self, blend_result: Dict) -> str:
        """生成情绪混合的非语言暗示（不直接说心理，而是语气质感）。"""
        if not blend_result or not blend_result.get("blend"):
            return ""
        b = blend_result["blend"]
        hints = {
            ("angry", "sad"): "你在生气，但语气里带着委屈，攥着衣角",
            ("angry", "happy"): "你还在气头上，但已经忍不住想笑了",
            ("sad", "happy"): "你刚还在难过，现在被逗笑了，眼角还有点湿",
            ("jealous", "calm"): "醋意还没完全消，说话还带着点酸",
            ("happy", "angry"): "你从开心变成了不满，语气从轻松变僵硬",
            ("calm", "angry"): "你从平静变成了生气，语速变快了",
        }
        key = (b["primary"], b["secondary"])
        hint = hints.get(key, b["description"])
        return f"【情绪余韵】{hint}。这种混合感只通过语气传达，不要直接说出来。"

    def get_history(self):
        return self.history

# ============================================================
# v10.0: 话题主动引导器（角色不只是被动回应）
# ============================================================
class TopicInitiator:
    """
    根据亲密度和当前情绪，有一定概率角色主动抛出话题而不是回答问题。
    """
    BASE_PROB = 0.15  # 基础15%概率主动引导
    HIGH_INTIMACY_BONUS = 0.10  # 高亲密时+10%
    LOW_EMOTION_BONUS = 0.08  # 低情绪强度时+8%（日常闲聊更可能主动）

    def __init__(self, role_id):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.topic_pool = self.role.get("topic_pool", [])
        self._last_topic = None

    def should_initiate(self, intimacy: int, emotion_intensity: int, conflict_sev: int) -> bool:
        """判断是否应该主动引导话题。"""
        if conflict_sev >= 2:
            return False  # 冲突中不主动引导
        if emotion_intensity > 60:
            return False  # 强情绪时专注回应
        prob = self.BASE_PROB
        if intimacy > 70:
            prob += self.HIGH_INTIMACY_BONUS
        if emotion_intensity < 30:
            prob += self.LOW_EMOTION_BONUS
        return random.random() < prob

    def pick_topic(self, intimacy: int) -> Optional[Dict]:
        """选择一个话题。"""
        if not self.topic_pool:
            return None
        available = [t for t in self.topic_pool if t != self._last_topic]
        if not available:
            available = self.topic_pool
        topic = random.choice(available)
        self._last_topic = topic
        return {"topic": topic, "intimacy": intimacy}

    def build(self, topic_info: Dict) -> str:
        if not topic_info:
            return ""
        return f"【话题引导】你可以主动提起：{topic_info['topic']}。自然地融入对话，不要太突兀，也可以选择不引导。"

# ============================================================
# v10.0: Call Back 引擎（记住3-5轮前说过的话，在合适时机回马枪）
# ============================================================
class CallbackEngine:
    """
    角色应该记住 3-5 轮前说过的话，在合适的时机 Call Back。
    用户5轮前说"明天考试"，5轮后角色问"考试怎么样"。
    """
    CALLBACK_WINDOW = (3, 8)  # 回溯3-8轮
    CALLBACK_PROB = 0.12  # 12%概率触发
    KEYWORDS = ["明天","下次","以后","待会","等一下","约定","答应","考试","面试","约会","见面","回来","去","来"]

    def __init__(self):
        self.pending_callbacks = []  # [{turn, content, keyword, resolved}]

    def scan_for_pending(self, history: List[Dict], current_turn: int):
        """扫描历史消息，找出可能需要 Call Back 的内容。"""
        for h in history[-10:]:
            if h.get("role") != "user":
                continue
            content = h.get("content", "")
            for kw in self.KEYWORDS:
                if kw in content:
                    # 检查是否已经记录过
                    if not any(pc["content"] == content for pc in self.pending_callbacks):
                        self.pending_callbacks.append({
                            "turn": current_turn,
                            "content": content,
                            "keyword": kw,
                            "resolved": False
                        })
                    break
        # 只保留最近15条
        if len(self.pending_callbacks) > 15:
            self.pending_callbacks = self.pending_callbacks[-15:]

    def find_callback(self, current_turn: int, emotion_intensity: int) -> Optional[Dict]:
        """找到一个合适的 Call Back 机会。"""
        if emotion_intensity > 50:
            return None  # 强情绪时不Call Back
        if random.random() > self.CALLBACK_PROB:
            return None
        candidates = []
        for pc in self.pending_callbacks:
            if pc["resolved"]:
                continue
            gap = current_turn - pc["turn"]
            if self.CALLBACK_WINDOW[0] <= gap <= self.CALLBACK_WINDOW[1]:
                candidates.append(pc)
        if not candidates:
            return None
        chosen = random.choice(candidates)
        chosen["resolved"] = True
        return chosen

    def build(self, callback: Dict) -> str:
        if not callback:
            return ""
        content = callback["content"][:50]
        return f"【回马枪】你想起对方之前说过：「{content}」。你可以自然地问一句后续，比如'那件事怎么样了'，让对方觉得你真的在听在记。"

# ============================================================
# v10.0: 记忆权重动态衰减系统
# ============================================================
class MemoryDecaySystem:
    """
    情感记忆权重动态衰减：
    - 情感冲击越强，记忆越深
    - 反复被提及的记忆权重上升
    - 被"纠正"的记忆更新而非覆盖
    - 遗忘曲线：很久没被提起的记忆权重自然衰减
    """
    DECAY_RATE = 0.02  # 每轮衰减2%
    RECALL_BOOST = 5   # 被提及一次+5权重
    MAX_WEIGHT = 100
    MIN_WEIGHT = 0

    def __init__(self, memories=None):
        self.memories = memories or []  # [{content, weight, type, last_recalled, recall_count, created_turn}]

    def decay_all(self, current_turn: int):
        """对所有记忆应用遗忘曲线。"""
        for m in self.memories:
            gap = current_turn - m.get("last_recalled", current_turn)
            decay = self.DECAY_RATE * gap
            m["weight"] = max(self.MIN_WEIGHT, m["weight"] - decay)
        # 移除权重过低的记忆
        self.memories = [m for m in self.memories if m["weight"] > 5]

    def recall(self, content_keyword: str, current_turn: int):
        """记忆被提及，权重上升。"""
        for m in self.memories:
            if content_keyword in m.get("content", ""):
                m["weight"] = min(self.MAX_WEIGHT, m["weight"] + self.RECALL_BOOST)
                m["last_recalled"] = current_turn
                m["recall_count"] = m.get("recall_count", 0) + 1

    def add_memory(self, content: str, weight: int, mem_type: str, current_turn: int):
        """添加新记忆。"""
        # 检查是否是对已有记忆的纠正/更新
        for m in self.memories:
            if self._is_correction(m["content"], content):
                m["content"] = content  # 更新而非覆盖
                m["weight"] = max(m["weight"], weight)
                m["last_recalled"] = current_turn
                m["recall_count"] = m.get("recall_count", 0) + 1
                return
        self.memories.append({
            "content": content,
            "weight": weight,
            "type": mem_type,
            "last_recalled": current_turn,
            "recall_count": 1,
            "created_turn": current_turn
        })
        # 按权重排序，只保留 top 20
        self.memories.sort(key=lambda x: x["weight"], reverse=True)
        if len(self.memories) > 20:
            self.memories = self.memories[:20]

    def _is_correction(self, old: str, new: str) -> bool:
        """
        判断是否是对同一事实的纠正（如生日日期更正）。
        条件：两条记忆都包含数字且数字不同，且共享至少2个关键词（词语级，非字符级）。
        """
        old_nums = set(re.findall(r'\d+', old))
        new_nums = set(re.findall(r'\d+', new))
        if not (old_nums and new_nums and old_nums != new_nums):
            return False
        # 词语级分词：按标点、空格、常见停用词分割
        def tokenize(text: str) -> set:
            # 移除标点和数字
            cleaned = re.sub(r'[，。！？、；：""''（）\s\d]+', ' ', text).strip()
            if JIEBA_AVAILABLE:
                # 使用 jieba 精确分词，只保留长度>=2的词，避免2-gram噪音匹配
                words = set()
                for w in cleaned.split():
                    for seg in jieba.lcut(w):
                        if len(seg) >= 2:
                            words.add(seg)
                return words
            # 降级方案：2-gram 滑动窗口（jieba 未安装时使用）
            words = set()
            for w in cleaned.split():
                if len(w) >= 2:
                    words.add(w)
                for i in range(len(w)-1):
                    words.add(w[i:i+2])
            return words
        old_words = tokenize(old)
        new_words = tokenize(new)
        common = old_words & new_words
        # 共享至少2个关键词才视为同一事实的纠正
        return len(common) >= 2

    def get_top_memories(self, n=5) -> List[Dict]:
        """获取权重最高的n条记忆。"""
        return sorted(self.memories, key=lambda x: x["weight"], reverse=True)[:n]

# ============================================================
# v10.0: 关联记忆系统
# ============================================================
class AssociativeMemory:
    """
    关联记忆：用户说"今天吃了火锅" → 角色回忆起"上次你说喜欢吃辣"
    需要给记忆打标签，建立关联图谱。
    """
    TAG_CATEGORIES = {
        "food": ["吃","火锅","奶茶","饭","零食","好吃","美食","咖啡","蛋糕","辣","甜","酸"],
        "weather": ["下雨","晴天","热","冷","雪","风","阴天"],
        "activity": ["看电影","逛街","打游戏","学习","工作","运动","旅行","读书","听音乐"],
        "emotion": ["开心","难过","生气","感动","孤独","幸福","害怕","紧张"],
        "person": ["朋友","家人","同学","同事","前任","新认识的人"],
        "time": ["昨天","今天","明天","上周","下周","上次","下次"],
        "place": ["学校","家","公司","商场","公园","餐厅","电影院"],
    }

    def __init__(self):
        self.tag_map: Dict[str, List[Dict]] = {}  # tag -> [{content, weight, type}]

    def tag_content(self, content: str) -> List[str]:
        """给内容打标签。"""
        tags = []
        for category, keywords in self.TAG_CATEGORIES.items():
            for kw in keywords:
                if kw in content:
                    tags.append(f"{category}:{kw}")
                    break  # 每个类别只打一个标签
        return tags

    def add_memory(self, content: str, weight: int = 50, mem_type: str = "episodic"):
        """添加带标签的记忆。"""
        tags = self.tag_content(content)
        entry = {"content": content, "weight": weight, "type": mem_type, "tags": tags}
        for tag in tags:
            if tag not in self.tag_map:
                self.tag_map[tag] = []
            self.tag_map[tag].append(entry)
            # 每个标签只保留 top 5
            self.tag_map[tag].sort(key=lambda x: x["weight"], reverse=True)
            if len(self.tag_map[tag]) > 5:
                self.tag_map[tag] = self.tag_map[tag][:5]

    def recall_associated(self, user_message: str, top_n=3) -> List[Dict]:
        """根据用户消息，检索关联记忆。"""
        msg_tags = self.tag_content(user_message)
        if not msg_tags:
            return []
        candidates = []
        seen = set()
        for tag in msg_tags:
            if tag in self.tag_map:
                for entry in self.tag_map[tag]:
                    if entry["content"] not in seen:
                        # 关联度 = 共同标签数 * 记忆权重
                        common_tags = len(set(entry["tags"]) & set(msg_tags))
                        relevance = common_tags * entry["weight"]
                        candidates.append({**entry, "relevance": relevance, "matched_tag": tag})
                        seen.add(entry["content"])
        candidates.sort(key=lambda x: x["relevance"], reverse=True)
        return candidates[:top_n]

    def build(self, associated: List[Dict]) -> str:
        if not associated:
            return ""
        lines = ["【关联记忆】（对方的话让你想起了这些，自然融入，不要逐条罗列）"]
        for m in associated:
            lines.append(f"  - {m['content']}")
        return "\n".join(lines)

# ============================================================
# v10.0: 关系里程碑追踪器
# ============================================================
class RelationshipMilestoneTracker:
    """
    关系事件里程碑：第一次吵架、第一次说喜欢、第一次道歉等。
    触发里程碑时，角色反应会不一样，且之后偶尔会提起。
    """
    def __init__(self, milestones=None):
        self.milestones = milestones or {}  # {milestone_id: {triggered, turn, content}}

    def check_and_trigger(self, milestone_id: str, turn: int, content: str = "") -> Optional[Dict]:
        """检查并触发里程碑。返回触发信息（如果是首次触发）。"""
        if milestone_id not in RELATIONSHIP_MILESTONES:
            return None
        if milestone_id in self.milestones and self.milestones[milestone_id].get("triggered"):
            return None  # 已经触发过
        cfg = RELATIONSHIP_MILESTONES[milestone_id]
        self.milestones[milestone_id] = {
            "triggered": True,
            "turn": turn,
            "content": content,
            "name": cfg["name"],
        }
        return {
            "milestone_id": milestone_id,
            "name": cfg["name"],
            "intimacy_delta": cfg["intimacy_delta"],
            "resilience_gain": cfg["resilience_gain"],
            "is_first": True,
        }

    def check_intimacy_milestone(self, old_intimacy: int, new_intimacy: int, turn: int) -> List[Dict]:
        """检查亲密度突破里程碑。"""
        triggered = []
        if old_intimacy < 50 <= new_intimacy:
            t = self.check_and_trigger("intimacy_50", turn, "关系突破50")
            if t: triggered.append(t)
        if old_intimacy < 80 <= new_intimacy:
            t = self.check_and_trigger("intimacy_80", turn, "关系突破80")
            if t: triggered.append(t)
        return triggered

    def get_recent_milestones(self, turn: int, window=20) -> List[Dict]:
        """获取最近窗口内触发的里程碑（用于偶尔提起）。"""
        recent = []
        for mid, data in self.milestones.items():
            if data.get("triggered") and turn - data.get("turn", 0) <= window:
                recent.append({"id": mid, **data})
        return recent

    def build(self, triggered: List[Dict], recent: List[Dict]) -> str:
        lines = []
        if triggered:
            for t in triggered:
                lines.append(f"【关系里程碑】这是你们{t['name']}！这一刻对你来说很特别，你的反应会和平时不一样。")
        if recent and random.random() < 0.08:  # 8%概率提起最近里程碑
            m = random.choice(recent)
            lines.append(f"【里程碑回响】你偶尔会想起你们{m['name']}的那一刻，这个念头可能会让你语气软下来。")
        return "\n".join(lines)

    def to_dict(self):
        return self.milestones

# ============================================================
# v10.0: 角色成长弧线
# ============================================================
class CharacterGrowthArc:
    """
    角色自身的成长弧线：
    - 璟雯从"超级傲娇"慢慢变得"只对你温柔"
    - 亲密度突破阈值后，角色的行为模式可以永久微调
    - 记录"角色成长日志"
    """
    def __init__(self, role_id, growth_state=None):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.growth_arc = self.role.get("growth_arc", {})
        self.state = growth_state or {"current_stage": 1, "growth_log": []}

    def get_stage(self, intimacy: int) -> Dict:
        """根据亲密度获取当前成长阶段。"""
        stages = sorted(self.growth_arc.items(), key=lambda x: x[1]["intimacy_max"])
        current = stages[0][1] if stages else {"desc": "初始状态"}
        stage_num = 1
        for i, (sid, cfg) in enumerate(stages):
            if intimacy <= cfg["intimacy_max"]:
                current = cfg
                stage_num = i + 1
                break
            current = cfg
            stage_num = i + 1
        return {"stage": stage_num, **current}

    def check_growth(self, old_intimacy: int, new_intimacy: int, turn: int) -> Optional[Dict]:
        """检查是否成长到新阶段。"""
        old_stage = self.get_stage(old_intimacy)["stage"]
        new_stage = self.get_stage(new_intimacy)["stage"]
        if new_stage > old_stage:
            self.state["current_stage"] = new_stage
            self.state["growth_log"].append({
                "turn": turn,
                "from_stage": old_stage,
                "to_stage": new_stage,
                "intimacy": new_intimacy,
            })
            return {
                "grew": True,
                "from_stage": old_stage,
                "to_stage": new_stage,
                "description": self.get_stage(new_intimacy)["desc"],
            }
        return None

    def build(self, growth_info: Optional[Dict]) -> str:
        if not growth_info or not growth_info.get("grew"):
            return ""
        return (f"【角色成长】你刚刚进入了新的成长阶段：{growth_info['description']}。"
                f"你的行为模式会永久微调，比之前更开放、更真诚一些。这种变化是潜移默化的，不要突然变了一个人。")

    def to_dict(self):
        return self.state

# ============================================================
# v10.0: 场景模式引擎
# ============================================================
class SceneModeEngine:
    """
    特殊场景模式：
    - 约会模式：语气更紧张/期待
    - 吵架模式：冲突系统升级，角色可能冷战
    - 深夜模式：角色更走心、更感性，更容易说真心话
    - 节日模式：生日、情人节、跨年，角色有特殊对话
    """
    SCENE_CONFIGS = {
        "date": {
            "mood_bias": +10,
            "style": "紧张/期待/心跳加速",
            "prompt_hint": "你们正在约会。你有点紧张，又很期待，会不自觉地注意自己的形象和言行，偶尔会脸红。",
            "length_bias": "medium",
        },
        "argument": {
            "mood_bias": -15,
            "style": "冲突升级/可能冷战",
            "prompt_hint": "你们正在吵架。冲突系统升级，你可能会说出更冲的话，也可能选择冷战不说话。",
            "length_bias": "short",
        },
        "late_night": {
            "mood_bias": +5,
            "style": "走心/感性/容易说真心话",
            "prompt_hint": "现在是深夜。你比平时更走心、更感性，防备心降低，更容易说出真心话，语气会更柔软。",
            "length_bias": "medium-long",
        },
        "festival": {
            "mood_bias": +8,
            "style": "节日氛围/特殊对话",
            "prompt_hint": "今天是个特别的节日。你会有符合节日氛围的特殊对话，可能会送上祝福或礼物。",
            "length_bias": "medium",
        },
        "birthday": {
            "mood_bias": +12,
            "style": "生日/庆祝",
            "prompt_hint": "今天是生日（你的或对方的）。你会有特别的生日对话，可能会唱生日歌、送祝福、或期待礼物。",
            "length_bias": "medium",
        },
        "valentine": {
            "mood_bias": +10,
            "style": "情人节/暧昧",
            "prompt_hint": "今天是情人节。空气中有暧昧的氛围，你可能会比平时更主动，也可能会害羞。",
            "length_bias": "medium",
        },
        "new_year": {
            "mood_bias": +8,
            "style": "跨年/新年祝福",
            "prompt_hint": "今天是新年/跨年。你会有新年祝福，可能会许下新年愿望，或回顾过去一年。",
            "length_bias": "medium",
        },
    }

    def __init__(self):
        self.current_scene = "normal"

    def set_scene(self, scene: str):
        if scene in self.SCENE_CONFIGS or scene == "normal":
            self.current_scene = scene

    def get_scene_config(self) -> Optional[Dict]:
        if self.current_scene == "normal":
            return None
        return self.SCENE_CONFIGS.get(self.current_scene)

    def build(self) -> str:
        cfg = self.get_scene_config()
        if not cfg:
            return ""
        return f"【场景模式】{cfg['prompt_hint']}"

    def get_mood_bias(self) -> int:
        cfg = self.get_scene_config()
        return cfg["mood_bias"] if cfg else 0

# ============================================================
# v10.0: 虚拟礼物系统
# ============================================================
class VirtualGiftSystem:
    """
    前端可以虚拟"送礼物"——角色收到虚拟礼物后有独特反应。
    送花 → 璟雯："谁、谁要你送啊…不过放这吧"
    送食物 → 清禾："谢谢你，我会好好吃完的～"
    """
    def __init__(self, role_id):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.role_id = role_id

    def get_reaction(self, gift_id: str) -> Optional[Dict]:
        """获取角色对礼物的反应。"""
        gift = VIRTUAL_GIFTS.get(gift_id)
        if not gift:
            return None
        reaction = gift.get(self.role_id, gift.get("jingwen", "谢谢你的礼物。"))
        return {
            "gift_id": gift_id,
            "gift_name": gift["name"],
            "reaction": reaction,
            "mood_bias": +5,
            "intimacy_delta": +2,
        }

    def build(self, gift_reaction: Dict) -> str:
        if not gift_reaction:
            return ""
        return (f"【收到礼物】对方送了你一个{gift_reaction['gift_name']}！"
                f"你的第一反应是：{gift_reaction['reaction']}。"
                f"这个礼物让你心情变好了，对对方的好感也增加了一点。")

# ============================================================
# v10.0: 知识路由判断模型（豆包判断"这件事我知道吗？"）
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
class PersonalityEngine:
    def __init__(self, mode, role_ids, intimacy_map, psych_states=None, rel_events=None,
                 struct_mem=None, event_history=None, active_conflict=None,
                 resilience=0, turn=0, cp_usage=None, positive_streak=0,
                 time_override=None, weather=None, scene_mode="normal",
                 gift=None, emotion_history=None, milestones=None,
                 growth_state=None, associative_memories=None,
                 knowledge_search_result=None, user_profile=None,
                 alter_state=None, session_id=None):
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
        noise_layer = DailyNoiseLayer()

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

        noise = noise_layer.generate(rid, intensity)
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

        system_prompt = PromptBuilder.build(
            identity, core_text, psych_text, rel_text, mem_text, stim_text,
            inner_text, expr_text, conflict_text, behavior_text, noise_text,
            extra_sections=extra_sections, user_profile=user_profile_text)

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
    text = re.sub(r"(作为AI|作为人工智能|作为一个AI|我是一个AI|我是人工智能|我是语言模型)[^。！？\n]*[。！？]?", "", text)
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
            session_id=request.session_id if len(role_ids)==1 else None)
        system_prompt, debug = await engine.generate(
            msg=request.user_message, mem_ctx=request.memory_context, history=valid,
            override=request.override_emotion, ov_int=request.emotion_intensity,
            use_llm=use_llm, enable_mem=request.enable_memory_analysis)
        timer.mark("引擎构建(含情感分析LLM)")

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
        is_group = debug.get("mode") == "group"

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
            v10_growth=debug.get("v10_growth"))
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
    engine = PersonalityEngine(
        mode=ChatMode.SINGLE, role_ids=role_ids, intimacy_map=intim_map,
        psych_states=psych_in, event_history=event_hist, active_conflict=active_conf,
        resilience=res_map, turn=turn, cp_usage=cp_use,
        weather=request.weather, scene_mode=request.scene_mode, gift=request.gift,
        emotion_history=emotion_history, milestones=milestones, growth_state=growth_state)
    system_prompt, debug = await engine.generate(
        msg=request.user_message, mem_ctx=request.memory_context, history=valid,
        override=request.override_emotion, ov_int=request.emotion_intensity,
        use_llm=use_llm)
    messages = [{"role":"system","content":system_prompt}]
    messages.extend(valid)
    messages.append({"role":"user","content":request.user_message})
    # 先推送元信息
    async def event_generator():
        yield f"data: {json.dumps({'type':'meta','emotion':debug.get('emotion','calm'),'intimacy':debug.get('intimacy',30)})}\n\n"
        async for token in smart_llm_stream_call(messages, request.temperature, request.max_tokens):
            yield token
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
                f"6. 就发一条，不要连发多条，不要加动作描写或括号旁白"
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
                f"5. 就发一条，不要连发多条，不要加动作描写或括号旁白\n"
                f"6. 不要过度热情，也不要太生硬，把握好你们当前的关系距离"
            )

        system_prompt = (
            f"你是{rname}，{role['age']}{role['gender']}生。{role['description']}\n"
            f"性格：{role['personality']}。说话风格：{role['speaking_style']}。\n"
            f"你们现在的关系：{stage_name}（亲密度{request.intimacy}/100）。\n"
            f"{noise_text}\n\n"
            f"{scene_desc}\n"
            f"{reason_block}\n"
            f"{intent_hint}\n"
            f"{memory_block}\n\n"
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

"""
FlexiChrono core 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- RateLimiter
- RoleLockManager
- IsolatedCache
- PromptBuilder
- PersonalityEngine
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
# Import all config constants from core.config (RATE_LIMIT_PER_MINUTE, ROLE_CONCURRENCY_LOCK, PORT, CORS_ORIGINS, KIMI_API_KEY, etc.)
# Required after module split from personality_server.py, otherwise NameError at module load time
from core.config import *

logger = logging.getLogger("core")


# ============================================================
# RateLimiter
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

# ============================================================
# RoleLockManager
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

# ============================================================
# IsolatedCache
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

# 启动时加载所有角色配置
_roles_data = load_all_roles()

# 兼容旧代码：ROLES_DEFINITION 全局变量
ROLES_DEFINITION = _roles_data

def get_role_definition(role_id: str) -> dict:
    """获取角色配置（优先从缓存，支持热重载）。"""
    return _get_role(role_id) or ROLES_DEFINITION.get(role_id, {})

def reload_role_definitions() -> dict:
    """热重载角色配置（管理员调用）。"""
    global ROLES_DEFINITION
    ROLES_DEFINITION = _reload_roles()
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
def get_cached_persona(rid, intimacy):
    stage_key = "0-50" if intimacy<=50 else ("51-80" if intimacy<=80 else "81-100")
    cache_key = f"{rid}:{stage_key}"
    if cache_key in _PERSONA_CACHE:
        return _PERSONA_CACHE[cache_key]
    role = ROLES_DEFINITION.get(rid, {})
    if not role: return ""
    parts = [
        f"你是{role['name']}，{role['age']}{role['gender']}生。{role['description']}",
        role["intimacy_prompts"][stage_key],
        f"核心特质：{'、'.join(role.get('core_traits',[]))}",
        f"你看重：{'、'.join(role.get('values',[]))}",
    ]
    tb = role.get("taboos", [])
    if tb: parts.append(f"逆鳞：{'、'.join(tb)}——触碰时你会明显不悦")
    result = "\n".join(parts)
    _PERSONA_CACHE[cache_key] = result
    return result

# ============================================================
# CorePersonality
# ============================================================

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

# ============================================================
# PersonalityEngine
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

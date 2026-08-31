"""
FlexiChrono api_models 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- PsychStateModel
- RelEventModel
- StructMemModel
- GenerateRequest
- GenerateResponse
- StreamGenerateRequest
- ProactiveGenerateRequest
- KnowledgeChatRequest
- PsychStateUpdate
- DesireStateUpdate
- DesireDecayRequest
- DesireInnerEventRequest
- TTSRequest
- SelfieRequest
- SelfieResponse
- SelfieFromMessageRequest
- SelfieFromMessageResponse
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

logger = logging.getLogger("api_models")


# ============================================================
# PsychStateModel
# ============================================================
class PsychStateModel(BaseModel):
    trust: float=50; security: float=50; attachment: float=20
    jealousy: float=0; fatigue: float=0; mood: float=50; trauma_flag: bool=False


# ============================================================
# RelEventModel
# ============================================================
class RelEventModel(BaseModel):
    type: str; content: str; impact: str; timestamp: str=""


# ============================================================
# StructMemModel
# ============================================================
class StructMemModel(BaseModel):
    episodic: List[Dict]=[]; semantic: List[Dict]=[]; emotional: List[Dict]=[]


# ============================================================
# GenerateRequest
# ============================================================
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


# ============================================================
# GenerateResponse
# ============================================================
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

# ============================================================
# StreamGenerateRequest
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

# ============================================================
# ProactiveGenerateRequest
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

# ============================================================
# KnowledgeChatRequest
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

# ============================================================
# PsychStateUpdate
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

# ============================================================
# DesireStateUpdate
# ============================================================
class DesireStateUpdate(BaseModel):
    """欲望状态更新请求（主动后端可直接设置各维度数值）"""
    longing: Optional[float] = None
    contact_desire: Optional[float] = None
    share_desire: Optional[float] = None
    care_desire: Optional[float] = None
    companionship: Optional[float] = None


# ============================================================
# DesireDecayRequest
# ============================================================
class DesireDecayRequest(BaseModel):
    """欲望衰减请求（根据空闲小时数衰减/增长）"""
    hours_elapsed: float = 1.0


# ============================================================
# DesireInnerEventRequest
# ============================================================
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


# ============================================================
# TTSRequest
# ============================================================
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

# ============================================================
# SelfieRequest
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



# ============================================================
# SelfieResponse
# ============================================================
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



# ============================================================
# SelfieFromMessageRequest
# ============================================================
class SelfieFromMessageRequest(BaseModel):
    """P2 新增：从用户原始消息直接生成自拍"""
    user_id: str
    role_id: str
    message: str  # 用户原始消息
    intimacy: int = 30
    psych_states: Optional[Dict[str, float]] = None



# ============================================================
# SelfieFromMessageResponse
# ============================================================
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


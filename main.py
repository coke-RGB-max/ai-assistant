"""
主后端 v4.0 - 端口 8000
对接人格后端 v11.0：session管理 / mode统一(single/group) / 限流处理 / 记忆直存 / 亲密度同步
v3.1: NapCat QQ 接入（HTTP Webhook）/ QQ绑定与数据迁移 / 管理员实时心理状态编辑
v4.0: 语音消息路由（文本→人格后端，语音→语音后端ASR→人格→TTS）/ 对接人格后端v11.0
v4.0.1: 修复QQ消息发送/主动后端熔断/消息去重/WS认证超时
v4.0.2: QQ语音消息完整链路（AMR下载→ffmpeg转WAV→ASR→LLM→TTS→AMR发送）
"""
import asyncio, json, logging, base64, os, time, hmac, hashlib
from typing import Optional, Dict, List
from contextlib import asynccontextmanager
from collections import deque
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main_server")
PERSONALITY_SERVER_URL = os.getenv("PERSONALITY_SERVER_URL", "http://127.0.0.1:8002")
VECTOR_SERVER_URL = os.getenv("VECTOR_SERVER_URL", "http://127.0.0.1:8001")
PROACTIVE_SERVER_URL = os.getenv("PROACTIVE_SERVER_URL", "http://127.0.0.1:8003")
VOICE_SERVER_URL = os.getenv("VOICE_SERVER_URL", "http://127.0.0.1:8004")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "change_me_internal_secret_2026")  # 与 proactive_server.py 保持一致
PORT = int(os.getenv("MAIN_PORT", "8000"))
# 数据目录：Docker 中挂载到 /data，本地默认脚本所在目录
DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
# ---- NapCat QQ 配置 ----
# NapCat OneBot v11 HTTP API 服务地址（用于主动发消息）。
# 必须配置！NapCat HTTP 上报(post_url)是单向推送，不会读取响应体自动回消息，
# 必须通过 HTTP API 调用 send_private_msg 才能把回复发出去。
# 配置方法: export NAPCAT_HTTP_URL="http://127.0.0.1:3000"
NAPCAT_HTTP_URL = os.getenv("NAPCAT_HTTP_URL", "")  # 例: http://127.0.0.1:3000
# Webhook 签名校验密钥（NapCat 配置中的 secret，留空则不校验）
QQ_WEBHOOK_SECRET = os.getenv("QQ_WEBHOOK_SECRET", "")

# ---- 主动后端熔断机制 ----
# 启动时假设可用，lifespan 健康检查后更新；运行中连续失败 3 次自动熔断，每 60s 放行一次探测
PROACTIVE_AVAILABLE = True
_PROACTIVE_FAIL_COUNT = 0
_PROACTIVE_LAST_PROBE = 0.0
_PROACTIVE_FAIL_THRESHOLD = 3
_PROACTIVE_RECOVER_INTERVAL = 60.0  # 秒

def _proactive_should_skip() -> bool:
    """返回 True 表示当前应跳过主动后端调用（熔断中且未到探测时间）"""
    global _PROACTIVE_LAST_PROBE
    if PROACTIVE_AVAILABLE:
        return False
    now = time.time()
    if now - _PROACTIVE_LAST_PROBE >= _PROACTIVE_RECOVER_INTERVAL:
        _PROACTIVE_LAST_PROBE = now
        return False
    return True

def _proactive_on_success():
    global PROACTIVE_AVAILABLE, _PROACTIVE_FAIL_COUNT
    if not PROACTIVE_AVAILABLE:
        logger.info("[主动后端] 熔断恢复，服务重新可用")
    PROACTIVE_AVAILABLE = True
    _PROACTIVE_FAIL_COUNT = 0

def _proactive_on_fail():
    global PROACTIVE_AVAILABLE, _PROACTIVE_FAIL_COUNT
    _PROACTIVE_FAIL_COUNT += 1
    if _PROACTIVE_FAIL_COUNT >= _PROACTIVE_FAIL_THRESHOLD and PROACTIVE_AVAILABLE:
        PROACTIVE_AVAILABLE = False
        logger.warning(f"[主动后端] 连续失败{_PROACTIVE_FAIL_COUNT}次，熔断60s（后台任务不再阻塞）")

# ---- QQ 消息去重（NapCat 超时重试会导致同一条消息上报多次）----
_recent_msg_ids: Dict[int, float] = {}
_MSG_DEDUP_WINDOW = 60.0  # 秒

# ---- ffmpeg 可用性标记（lifespan 启动时检测）----
FFMPEG_AVAILABLE = False

class UserDB:
    def __init__(self, filepath=None):
        self.filepath = filepath or os.path.join(DATA_DIR, "userdb.json")
        self._ensure_file()
    def _ensure_file(self):
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump({
                    "users": {
                        "admin": {
                            "password": "admin123", "nickname": "管理员",
                            "is_admin": True, "intimacy": {}, "session_id": None
                        }
                    },
                    "qq_bindings": {}  # {qq_number: username}
                }, f, ensure_ascii=False, indent=2)
    def _read(self):
        with open(self.filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    def _write(self, data):
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    def authenticate(self, username, password):
        data = self._read()
        user = data["users"].get(username)
        if user and user["password"] == password:
            return {
                "username": username, "nickname": user.get("nickname", username),
                "is_admin": user.get("is_admin", False),
                "intimacy": user.get("intimacy", {}),
                "session_id": user.get("session_id"),
                "qq_bound": self.get_qq_by_username(username, data) is not None
            }
        return None
    def authenticate_token(self, token):
        try:
            decoded = base64.b64decode(token).decode("utf-8")
            u, p = decoded.split(":", 1)
            return self.authenticate(u, p)
        except Exception:
            return None
    def register(self, username, password, nickname=None):
        data = self._read()
        if username in data["users"]:
            return False
        data["users"][username] = {
            "password": password, "nickname": nickname or username,
            "is_admin": False, "intimacy": {}, "session_id": None
        }
        self._write(data)
        return True
    def update_nickname(self, username, nickname):
        data = self._read()
        if username in data["users"]:
            data["users"][username]["nickname"] = nickname
            self._write(data)
            return True
        return False
    def set_intimacy(self, username, role_id, value):
        data = self._read()
        if username in data["users"]:
            data["users"][username].setdefault("intimacy", {})[role_id] = max(0, min(100, value))
            self._write(data)
            return True
        return False
    def get_intimacy(self, username, role_id):
        return self._read().get("users", {}).get(username, {}).get("intimacy", {}).get(role_id, 30)
    def get_all_intimacy(self, username):
        return self._read().get("users", {}).get(username, {}).get("intimacy", {})
    def set_all_intimacy(self, username, intimacy_map):
        data = self._read()
        if username in data["users"]:
            data["users"][username]["intimacy"] = {
                k: max(0, min(100, int(v))) for k, v in intimacy_map.items()
            }
            self._write(data)
            return True
        return False
    def get_session(self, username):
        return self._read().get("users", {}).get(username, {}).get("session_id")
    def set_session(self, username, session_id):
        data = self._read()
        if username in data["users"]:
            data["users"][username]["session_id"] = session_id
            self._write(data)
            return True
        return False
    def get_all_users(self):
        data = self._read()
        qq_reverse = {v: k for k, v in data.get("qq_bindings", {}).items()}
        return [
            {
                "username": u, "nickname": d.get("nickname", u),
                "is_admin": d.get("is_admin", False),
                "qq": qq_reverse.get(u)
            }
            for u, d in data["users"].items()
        ]
    def reset_password(self, username, new_password):
        data = self._read()
        if username in data["users"]:
            data["users"][username]["password"] = new_password
            self._write(data)
            return True
        return False
    def admin_create_user(self, username, password, nickname=None):
        return self.register(username, password, nickname)
    def admin_set_intimacy(self, username, role_id, value):
        return self.set_intimacy(username, role_id, value)
    # ---- QQ 绑定 ----
    def get_qq_by_username(self, username, data=None):
        if data is None:
            data = self._read()
        for qq, uname in data.get("qq_bindings", {}).items():
            if uname == username:
                return qq
        return None
    def get_username_by_qq(self, qq):
        data = self._read()
        return data.get("qq_bindings", {}).get(str(qq))
    def bind_qq(self, qq, username):
        """绑定 QQ 号到正式账号，返回 (success, old_tmp_username)"""
        data = self._read()
        bindings = data.setdefault("qq_bindings", {})
        qq_str = str(qq)
        if qq_str in bindings:
            return False, None
        old_tmp = f"qq_tmp_{qq_str}"
        bindings[qq_str] = username
        self._write(data)
        return True, old_tmp
    def unbind_qq(self, username):
        data = self._read()
        bindings = data.get("qq_bindings", {})
        for qq, uname in list(bindings.items()):
            if uname == username:
                del bindings[qq]
                self._write(data)
                return qq
        return None
    def ensure_tmp_user(self, tmp_username):
        """确保 QQ 临时用户存在于 userdb"""
        data = self._read()
        if tmp_username not in data["users"]:
            data["users"][tmp_username] = {
                "password": base64.b64encode(os.urandom(18)).decode(),
                "nickname": f"QQ用户",
                "is_admin": False, "intimacy": {}, "session_id": None,
                "is_qq_tmp": True
            }
            self._write(data)
        return True
user_db = UserDB()
# 后台任务引用集合
background_tasks = set()
# QQ 用户的内存聊天历史（重启后丢失；心理状态由 personality_server session 持久化）
qq_chat_history: Dict[str, List[Dict]] = {}
# ============================================================
# 耗时统计工具
# ============================================================
def _fmt_ms(seconds: float) -> str:
    """格式化耗时：<1s 显示 ms，>=1s 显示 s"""
    ms = seconds * 1000
    if ms < 1000:
        return f"{ms:.0f}ms"
    return f"{seconds:.2f}s"
class StepTimer:
    """收集多步骤耗时，最后输出格式化日志"""
    def __init__(self, label: str):
        self.label = label
        self._total_start = time.perf_counter()
        self._steps: List[tuple] = []  # (name, duration_seconds)
        self._last = self._total_start
    def mark(self, name: str):
        """记录上一步到现在的耗时"""
        now = time.perf_counter()
        self._steps.append((name, now - self._last))
        self._last = now
    def elapsed_total(self) -> float:
        return time.perf_counter() - self._total_start
    def log(self, extra: str = ""):
        parts = [f"{n}={_fmt_ms(d)}" for n, d in self._steps]
        total = self.elapsed_total()
        logger.info(f"[耗时][{self.label}] {' | '.join(parts)} | 总计={_fmt_ms(total)}{extra}")
def _log_port_timing(service: str, url: str, duration: float, status: str = "ok"):
    """记录端口调用耗时日志"""
    logger.info(f"[端口][{service}] {url} -> {status} 耗时={_fmt_ms(duration)}")
async def create_personality_session():
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{PERSONALITY_SERVER_URL}/api/session/create")
            _log_port_timing("人格后端", "/api/session/create", time.perf_counter() - t0,
                              f"HTTP{resp.status_code}")
            if resp.status_code == 200:
                return resp.json().get("session_id")
    except Exception as e:
        _log_port_timing("人格后端", "/api/session/create", time.perf_counter() - t0, f"ERROR:{e}")
        logger.error(f"创建session失败: {e}")
    return None
async def call_personality_generate(role_ids, user_message, memory_context, chat_history,
                                     session_id=None, intimacy_map=None, temperature=0.9, max_tokens=500):
    mode = "group" if len(role_ids) > 1 else "single"
    payload = {
        "mode": mode, "role_ids": role_ids, "user_message": user_message,
        "memory_context": memory_context, "chat_history": chat_history,
        "temperature": temperature, "max_tokens": max_tokens,
        "return_debug": True, "enable_memory_analysis": True
    }
    if session_id:
        payload["session_id"] = session_id
    if intimacy_map:
        payload["intimacy_map"] = intimacy_map
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(f"{PERSONALITY_SERVER_URL}/api/generate", json=payload, timeout=120.0)
        dur = time.perf_counter() - t0
        _log_port_timing("人格后端", "/api/generate", dur, f"HTTP{resp.status_code}")
        if resp.status_code == 429:
            return {"success": False, "error": "rate_limited", "reply": "……你说话太快了，让我喘口气。"}
        if resp.status_code == 200:
            return resp.json()
        return {"success": False, "error": f"http_{resp.status_code}", "reply": "抱歉，我暂时无法回应..."}
    except httpx.TimeoutException:
        _log_port_timing("人格后端", "/api/generate", time.perf_counter() - t0, "TIMEOUT")
        return {"success": False, "error": "timeout", "reply": "……等一下，我刚才走神了。"}
    except Exception as e:
        _log_port_timing("人格后端", "/api/generate", time.perf_counter() - t0, f"ERROR:{e}")
        logger.error(f"人格后端调用失败: {e}")
        return {"success": False, "error": str(e), "reply": "抱歉，我暂时无法回应..."}
async def call_vector_search(user_id, query, top_k=5, role_id=""):
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {"user_id": user_id, "query": query, "top_k": top_k}
            if role_id:
                payload["role_id"] = role_id
            resp = await client.post(f"{VECTOR_SERVER_URL}/api/memory/search",
                                     json=payload, timeout=30.0)
            _log_port_timing("记忆后端", "/api/memory/search", time.perf_counter() - t0,
                              f"HTTP{resp.status_code}")
            if resp.status_code == 200 and resp.json().get("success"):
                return resp.json().get("context_text", "")
    except Exception as e:
        _log_port_timing("记忆后端", "/api/memory/search", time.perf_counter() - t0, f"ERROR:{e}")
        logger.error(f"记忆检索失败: {e}")
    return ""
async def call_vector_add_direct(user_id, content, memory_type="episodic", importance=50,
                                  reason="", source="", role_id="", conversation_id=""):
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {
                "user_id": user_id, "content": content, "memory_type": memory_type,
                "importance": importance, "reason": reason, "source": source, "role_id": role_id
            }
            if conversation_id:
                payload["conversation_id"] = conversation_id
            resp = await client.post(f"{VECTOR_SERVER_URL}/api/memory/add_direct",
                                     json=payload, timeout=30.0)
            _log_port_timing("记忆后端", "/api/memory/add_direct", time.perf_counter() - t0,
                              f"HTTP{resp.status_code}")
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        _log_port_timing("记忆后端", "/api/memory/add_direct", time.perf_counter() - t0, f"ERROR:{e}")
        logger.error(f"记忆直存失败: {e}")
    return {"success": False}
async def call_vector_add(user_id, user_message, assistant_reply, role_names,
                           role_id="", conversation_id=""):
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            payload = {
                "user_id": user_id, "user_message": user_message,
                "assistant_reply": assistant_reply, "role_names": role_names
            }
            if role_id:
                payload["role_id"] = role_id
            if conversation_id:
                payload["conversation_id"] = conversation_id
            resp = await client.post(f"{VECTOR_SERVER_URL}/api/memory/add",
                                     json=payload, timeout=60.0)
            _log_port_timing("记忆后端", "/api/memory/add", time.perf_counter() - t0,
                              f"HTTP{resp.status_code}")
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        _log_port_timing("记忆后端", "/api/memory/add", time.perf_counter() - t0, f"ERROR:{e}")
        logger.error(f"记忆添加失败: {e}")
    return {"success": False, "intimacy_change": 0}
async def call_vector_migrate(old_user_id, new_user_id):
    """迁移向量库中的用户数据"""
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(f"{VECTOR_SERVER_URL}/api/memory/migrate_user",
                                     json={"old_user_id": old_user_id, "new_user_id": new_user_id},
                                     timeout=60.0)
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        logger.error(f"向量记忆迁移失败: {e}")
    return {"success": False, "migrated": 0}
# -------------------------- 主动消息后端对接（带熔断） --------------------------
async def call_proactive_report(user_id, role_id, intimacy, attachment=None, psych=None):
    if _proactive_should_skip():
        return
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            payload = {"user_id": user_id, "role_id": role_id, "intimacy": int(intimacy)}
            if attachment is not None:
                try:
                    payload["attachment"] = float(attachment)
                except (TypeError, ValueError):
                    pass
            if psych and isinstance(psych, dict):
                payload["psych"] = psych
            resp = await client.post(f"{PROACTIVE_SERVER_URL}/api/activity/report", json=payload, timeout=10.0)
            dur = time.perf_counter() - t0
            _log_port_timing("主动后端", "/api/activity/report", dur, f"HTTP{resp.status_code}")
            if resp.status_code == 200:
                _proactive_on_success()
            else:
                _proactive_on_fail()
    except Exception as e:
        _proactive_on_fail()
        _log_port_timing("主动后端", "/api/activity/report", time.perf_counter() - t0, f"ERROR:{e}")
        logger.warning(f"proactive活动上报失败: {e}")
async def call_proactive_mark_replied(user_id, role_id=None):
    if _proactive_should_skip():
        return
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            payload = {"user_id": user_id}
            if role_id:
                payload["role_id"] = role_id
            resp = await client.post(f"{PROACTIVE_SERVER_URL}/api/mark_replied", json=payload, timeout=10.0)
            dur = time.perf_counter() - t0
            _log_port_timing("主动后端", "/api/mark_replied", dur, f"HTTP{resp.status_code}")
            if resp.status_code == 200:
                _proactive_on_success()
            else:
                _proactive_on_fail()
    except Exception as e:
        _proactive_on_fail()
        _log_port_timing("主动后端", "/api/mark_replied", time.perf_counter() - t0, f"ERROR:{e}")
        logger.warning(f"proactive标记回复失败: {e}")
async def call_proactive_migrate(old_user_id, new_user_id):
    """迁移主动消息后端的用户数据"""
    if not PROACTIVE_AVAILABLE:
        return False
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{PROACTIVE_SERVER_URL}/api/internal/migrate_user",
                json={"old_user_id": old_user_id, "new_user_id": new_user_id},
                headers={"X-Internal-Token": INTERNAL_TOKEN}, timeout=10.0)
            return resp.status_code == 200
    except Exception as e:
        logger.warning(f"proactive数据迁移失败: {e}")
    return False
class ConnectionManager:
    def __init__(self):
        self.active_connections = {}
        self.pending_messages = {}
    async def connect(self, username, ws):
        self.active_connections[username] = ws
    def disconnect(self, username):
        self.active_connections.pop(username, None)
manager = ConnectionManager()
async def get_role_names(role_ids):
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{PERSONALITY_SERVER_URL}/api/roles")
            if resp.status_code == 200:
                rd = resp.json()
                return [rd[r]["name"] for r in role_ids if r in rd]
    except Exception:
        pass
    return role_ids
async def _background_vector_add(username, user_message, reply, role_ids, session_id=None, role_id=""):
    try:
        role_names = await get_role_names(role_ids)
        rid = role_id or (role_ids[0] if len(role_ids) == 1 else "group:" + "+".join(sorted(role_ids)))
        await call_vector_add(username, user_message, reply, role_names,
                              role_id=rid, conversation_id=session_id or "")
    except Exception as e:
        logger.warning(f"后台记忆存储失败: {e}")
def extract_intimacy_from_debug(result, role_ids):
    intimacy_map = {}
    debug = result.get("debug") or {}
    rs = debug.get("role_states")
    if rs and isinstance(rs, dict):
        for rid in role_ids:
            if "intimacy" in rs.get(rid, {}):
                intimacy_map[rid] = rs[rid]["intimacy"]
    if not intimacy_map and "intimacy" in debug:
        for rid in role_ids:
            intimacy_map[rid] = debug["intimacy"]
    return intimacy_map
async def process_chat_message(identity, user_message, role_ids=None, chat_history=None):
    """
    统一的消息处理管线，供 WebSocket 和 QQ Webhook 共用。
    identity: 用户名（正式账号或 qq_tmp_xxx）
    返回 dict: {reply, role_ids, mode, intimacy, session_id, used_llm_analysis}
    """
    if role_ids is None:
        role_ids = ["jingwen"]
    if chat_history is None:
        chat_history = []
    mode = "group" if len(role_ids) > 1 else "single"
    mem_role_id = role_ids[0] if len(role_ids) == 1 else "group:" + "+".join(sorted(role_ids))
    timer = StepTimer(f"{identity}|{mode}|{'+'.join(role_ids)}")
    # 用户主动发言，标记主动消息已回复（后台异步，不阻塞）
    _t = asyncio.create_task(call_proactive_mark_replied(identity))
    background_tasks.add(_t)
    _t.add_done_callback(background_tasks.discard)
    # 获取/创建 session
    session_id = user_db.get_session(identity)
    if not session_id:
        session_id = await create_personality_session()
        if session_id:
            user_db.set_session(identity, session_id)
            logger.info(f"用户 {identity} 创建session: {session_id}")
    timer.mark("session")
    # 读取亲密度
    intimacy_map = {rid: user_db.get_intimacy(identity, rid) for rid in role_ids}
    # 向量记忆检索
    memory_context = await call_vector_search(identity, user_message, role_id=mem_role_id)
    timer.mark("记忆检索")
    # 人格引擎生成（最耗时：LLM调用）
    result = await call_personality_generate(
        role_ids=role_ids, user_message=user_message,
        memory_context=memory_context, chat_history=chat_history,
        session_id=session_id, intimacy_map=intimacy_map)
    timer.mark("人格生成(LLM)")
    if not isinstance(result, dict):
        result = {"success": False, "reply": "抱歉，服务返回异常..."}
    if result.get("session_id") and result["session_id"] != session_id:
        session_id = result["session_id"]
        user_db.set_session(identity, session_id)
    reply = result.get("reply", "") or ""
    if not result.get("success") and not reply:
        reply = "抱歉，我暂时无法回应..."
    # 更新聊天历史
    chat_history.append({"role": "user", "content": user_message})
    chat_history.append({"role": "assistant", "content": reply})
    if len(chat_history) > 20:
        del chat_history[:-20]
    # 提取并写入亲密度
    updated_intimacy = extract_intimacy_from_debug(result, role_ids)
    for rid, val in updated_intimacy.items():
        try:
            user_db.set_intimacy(identity, rid, int(val))
        except Exception as ie:
            logger.warning(f"亲密度写入失败: {ie}")
    response_intimacy = dict(intimacy_map)
    response_intimacy.update(updated_intimacy)
    timer.mark("后处理")
    # 后台记忆存储（不阻塞回复）
    mem_cand = result.get("memory_candidate")
    if mem_cand and mem_cand.get("remember") and mem_cand.get("content"):
        task = asyncio.create_task(call_vector_add_direct(
            user_id=identity, content=mem_cand["content"],
            memory_type=mem_cand.get("type", "episodic"),
            importance=mem_cand.get("importance", 50),
            reason=mem_cand.get("reason", ""),
            source=user_message[:500],
            role_id=mem_role_id,
            conversation_id=session_id or ""))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
    elif len(user_message) >= 10:
        task = asyncio.create_task(_background_vector_add(
            identity, user_message, reply, role_ids, session_id, mem_role_id))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
    # 上报活动给主动消息后端（后台异步，不阻塞）
    if mode == "single":
        rid = role_ids[0]
        dbg = result.get("debug") or {}
        psych_snap = dbg.get("psychological_state") or {}
        attachment = psych_snap.get("attachment")
        _t2 = asyncio.create_task(call_proactive_report(
            identity, rid, response_intimacy.get(rid, 30), attachment, psych_snap))
        background_tasks.add(_t2)
        _t2.add_done_callback(background_tasks.discard)
    # 输出耗时汇总日志
    llm_used = result.get("used_llm_analysis", False)
    timer.log(f" | 回复长度={len(reply)} | LLM分析={'是' if llm_used else '否'}")
    return {
        "reply": reply,
        "role_ids": role_ids,
        "mode": mode,
        "intimacy": response_intimacy,
        "session_id": session_id,
        "used_llm_analysis": llm_used
    }
# -------------------------- 语音消息处理 --------------------------
async def call_voice_server(audio_base64, role_ids, session_id=None, intimacy_map=None,
                             chat_history=None, audio_format="wav"):
    """
    调用语音后端 /api/voice/chat 完成 ASR→人格→TTS 全流程。
    返回 dict: {success, asr_text, reply, audio_base64, audio_format, session_id, error}
    """
    payload = {
        "audio_base64": audio_base64,
        "audio_format": audio_format,
        "role_ids": role_ids,
        "chat_history": chat_history or [],
    }
    if session_id:
        payload["session_id"] = session_id
    if intimacy_map:
        payload["intimacy_map"] = intimacy_map
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.post(f"{VOICE_SERVER_URL}/api/voice/chat",
                                     json=payload, timeout=180.0)
        dur = time.perf_counter() - t0
        _log_port_timing("语音后端", "/api/voice/chat", dur, f"HTTP{resp.status_code}")
        if resp.status_code == 200:
            return resp.json()
        return {"success": False, "error": f"语音后端返回HTTP{resp.status_code}",
                "reply": "语音服务暂时不可用..."}
    except httpx.TimeoutException:
        _log_port_timing("语音后端", "/api/voice/chat", time.perf_counter() - t0, "TIMEOUT")
        return {"success": False, "error": "timeout", "reply": "语音处理超时，请重试或发送文字"}
    except Exception as e:
        _log_port_timing("语音后端", "/api/voice/chat", time.perf_counter() - t0, f"ERROR:{e}")
        logger.error(f"语音后端调用失败: {e}")
        return {"success": False, "error": str(e), "reply": "语音服务连接失败，请确认voice_server已启动"}
async def process_voice_message(identity, audio_base64, role_ids=None, chat_history=None,
                                 audio_format="wav"):
    """
    语音消息处理管线：语音后端(ASR→人格→TTS) → 后处理(亲密度/记忆/主动上报)
    identity: 用户名
    返回 dict: {reply, asr_text, audio_base64, audio_format, role_ids, mode, intimacy, session_id}
    """
    if role_ids is None:
        role_ids = ["jingwen"]
    if chat_history is None:
        chat_history = []
    mode = "group" if len(role_ids) > 1 else "single"
    mem_role_id = role_ids[0] if len(role_ids) == 1 else "group:" + "+".join(sorted(role_ids))
    timer = StepTimer(f"{identity}|voice|{mode}|{'+'.join(role_ids)}")
    # 用户主动发言，标记主动消息已回复
    _t = asyncio.create_task(call_proactive_mark_replied(identity))
    background_tasks.add(_t)
    _t.add_done_callback(background_tasks.discard)
    # 获取/创建 session
    session_id = user_db.get_session(identity)
    if not session_id:
        session_id = await create_personality_session()
        if session_id:
            user_db.set_session(identity, session_id)
    timer.mark("session")
    # 读取亲密度
    intimacy_map = {rid: user_db.get_intimacy(identity, rid) for rid in role_ids}
    # 调用语音后端（ASR→人格→TTS 全流程）
    result = await call_voice_server(
        audio_base64=audio_base64, role_ids=role_ids,
        session_id=session_id, intimacy_map=intimacy_map,
        chat_history=chat_history, audio_format=audio_format)
    timer.mark("语音后端(ASR+LLM+TTS)")
    if not isinstance(result, dict):
        result = {"success": False, "reply": "语音服务返回异常..."}
    asr_text = result.get("asr_text", "") or ""
    reply = result.get("reply", "") or ""
    audio_out = result.get("audio_base64", "") or ""
    audio_out_fmt = result.get("audio_format", "mp3")
    if result.get("session_id") and result["session_id"] != session_id:
        session_id = result["session_id"]
        user_db.set_session(identity, session_id)
    if not result.get("success") and not reply:
        reply = "抱歉，我没听清你说什么..."
    # 更新聊天历史（用ASR识别出的文本）
    if asr_text:
        chat_history.append({"role": "user", "content": asr_text})
        chat_history.append({"role": "assistant", "content": reply})
        if len(chat_history) > 20:
            del chat_history[:-20]
    # 后台记忆存储（用ASR文本）
    if asr_text and len(asr_text) >= 5:
        task = asyncio.create_task(_background_vector_add(
            identity, asr_text, reply, role_ids, session_id, mem_role_id))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
    # 上报活动给主动消息后端
    if mode == "single" and asr_text:
        rid = role_ids[0]
        _t2 = asyncio.create_task(call_proactive_report(
            identity, rid, intimacy_map.get(rid, 30), None, {}))
        background_tasks.add(_t2)
        _t2.add_done_callback(background_tasks.discard)
    timer.log(f" | ASR文本={asr_text[:30]} | 回复长度={len(reply)} | 音频={'有' if audio_out else '无'}")
    return {
        "reply": reply,
        "asr_text": asr_text,
        "audio_base64": audio_out,
        "audio_format": audio_out_fmt,
        "role_ids": role_ids,
        "mode": mode,
        "intimacy": intimacy_map,
        "session_id": session_id,
    }
# -------------------------- OneBot v11 消息解析 --------------------------
def extract_onebot_text(message):
    """从 OneBot v11 message 字段提取纯文本"""
    if isinstance(message, str):
        return message.strip()
    if isinstance(message, list):
        parts = []
        for seg in message:
            if isinstance(seg, dict) and seg.get("type") == "text":
                parts.append(seg.get("data", {}).get("text", ""))
        return "".join(parts).strip()
    return ""
def onebot_has_voice(message):
    """检测 OneBot v11 消息是否包含语音段(record)"""
    if isinstance(message, list):
        for seg in message:
            if isinstance(seg, dict) and seg.get("type") == "record":
                return True
    return False
def extract_onebot_voice_url(message) -> Optional[str]:
    """从 OneBot v11 message 数组中提取语音段(record)的下载 URL"""
    if isinstance(message, list):
        for seg in message:
            if isinstance(seg, dict) and seg.get("type") == "record":
                url = seg.get("data", {}).get("url", "")
                if url:
                    return url
    return None
# -------------------------- 音频下载与转码（ffmpeg） --------------------------
async def _download_file(url: str) -> Optional[bytes]:
    """下载 URL 内容，返回字节"""
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.content
            logger.error(f"下载文件失败: HTTP {resp.status_code} url={url[:100]}")
    except Exception as e:
        logger.error(f"下载文件异常: {e}")
    return None
async def _ffmpeg_convert(input_bytes: bytes, output_fmt: str,
                           sample_rate: int = 16000, channels: int = 1,
                           bitrate: str = "") -> Optional[bytes]:
    """
    用 ffmpeg 通过管道转码音频。
    input_bytes: 输入音频字节（格式由 ffmpeg 自动探测）
    output_fmt: 输出格式，如 "wav" / "amr" / "mp3"
    sample_rate: 输出采样率
    channels: 输出声道数
    bitrate: 输出比特率（如 "12.2k"），仅对有损格式有效
    返回输出字节，失败返回 None
    """
    if not FFMPEG_AVAILABLE:
        logger.error("ffmpeg 不可用，无法转码音频")
        return None
    cmd = ["ffmpeg", "-y", "-i", "pipe:0",
           "-ar", str(sample_rate), "-ac", str(channels)]
    if bitrate:
        cmd += ["-ab", bitrate]
    cmd += ["-f", output_fmt, "pipe:1"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(input=input_bytes), timeout=30.0)
        if proc.returncode == 0 and stdout:
            return stdout
        err_msg = stderr.decode(errors="replace")[-500:]
        logger.error(f"ffmpeg转码失败(returncode={proc.returncode}): {err_msg}")
    except asyncio.TimeoutError:
        logger.error("ffmpeg转码超时(30s)")
    except Exception as e:
        logger.error(f"ffmpeg转码异常: {e}")
    return None
# -------------------------- NapCat 消息发送 --------------------------
async def send_qq_private_msg(qq_number, text):
    """通过 NapCat HTTP API 发送 QQ 私聊文本消息"""
    if not NAPCAT_HTTP_URL:
        logger.error("[QQ] NAPCAT_HTTP_URL 未配置，无法发送消息！"
                     "请设置环境变量 NAPCAT_HTTP_URL=http://127.0.0.1:3000")
        return False
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{NAPCAT_HTTP_URL.rstrip('/')}/send_private_msg",
                json={"user_id": int(qq_number), "message": text},
                timeout=10.0)
            _log_port_timing("NapCat", "/send_private_msg", time.perf_counter() - t0,
                              f"HTTP{resp.status_code}")
            if resp.status_code == 200:
                return True
            logger.error(f"[QQ] NapCat 返回非200: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        _log_port_timing("NapCat", "/send_private_msg", time.perf_counter() - t0, f"ERROR:{e}")
        logger.error(f"NapCat 发送消息失败: {e}")
        return False
async def send_qq_private_record(qq_number, audio_b64: str, audio_format: str = "mp3") -> bool:
    """
    通过 NapCat HTTP API 发送 QQ 私聊语音消息。
    将 TTS 音频统一转成 MP3 后 base64 发送。
    NapCat 收到非 silk 格式会自动调用 ffmpeg 转 silk（需 NapCat 所在机器安装 ffmpeg，
    下载地址: https://www.gyan.dev/ffmpeg/builds/ ，解压后 bin 目录加入 PATH）。
    """
    if not NAPCAT_HTTP_URL:
        return False
    try:
        raw_bytes = base64.b64decode(audio_b64)
        # 统一转成 MP3（apt 版 ffmpeg 不支持 AMR 编码，但 MP3 编码可用；NapCat 会自动转 silk）
        if audio_format.lower() != "mp3":
            mp3_bytes = await _ffmpeg_convert(
                raw_bytes, "mp3", sample_rate=24000, channels=1, bitrate="128k")
            if mp3_bytes:
                raw_bytes = mp3_bytes
                logger.info(f"[QQ] TTS音频已转MP3: {len(mp3_bytes)}B")
            else:
                logger.warning(f"[QQ] MP3转码失败，使用原始格式({audio_format})发送")
        file_field = "base64://" + base64.b64encode(raw_bytes).decode()
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{NAPCAT_HTTP_URL.rstrip('/')}/send_private_msg",
                json={
                    "user_id": int(qq_number),
                    "message": [{"type": "record", "data": {"file": file_field}}]
                },
                timeout=15.0)
            if resp.status_code == 200:
                return True
            logger.error(f"[QQ] NapCat 语音发送返回: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"NapCat 发送语音失败: {e}")
        return False
@asynccontextmanager
async def lifespan(app):
    global PROACTIVE_AVAILABLE, FFMPEG_AVAILABLE
    logger.info("主后端 v4.0.2 启动 - 端口 8000")
    if not NAPCAT_HTTP_URL:
        logger.warning("=" * 60)
        logger.warning("NAPCAT_HTTP_URL 未配置！QQ 回复将无法发送。")
        logger.warning("NapCat HTTP 上报(post_url)是单向推送，不会读取响应体自动回消息。")
        logger.warning("必须配置 NapCat HTTP API 地址，例如：")
        logger.warning("  export NAPCAT_HTTP_URL=http://127.0.0.1:3000")
        logger.warning("=" * 60)
    # 检测 ffmpeg（语音消息下载转码依赖）
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-version",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await proc.communicate()
        if proc.returncode == 0:
            FFMPEG_AVAILABLE = True
            logger.info("ffmpeg 可用，QQ语音消息转码已启用")
        else:
            logger.warning("ffmpeg 异常，QQ语音消息可能无法转码")
    except FileNotFoundError:
        FFMPEG_AVAILABLE = False
        logger.warning("ffmpeg 未安装！QQ语音消息功能不可用。请执行: sudo apt install ffmpeg")
    await asyncio.sleep(1)
    for name, url in [("人格后端", PERSONALITY_SERVER_URL), ("记忆后端", VECTOR_SERVER_URL),
                      ("主动后端", PROACTIVE_SERVER_URL), ("语音后端", VOICE_SERVER_URL)]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(f"{url}/health")
                ok = r.status_code == 200
                logger.info(f"{name}连接正常" if ok else f"{name}未响应")
                if name == "主动后端":
                    PROACTIVE_AVAILABLE = ok
                    if not ok:
                        logger.warning("主动后端不可用，mark_replied/activity_report 将跳过（不阻塞消息处理）")
        except Exception:
            logger.warning(f"{name}未启动")
            if name == "主动后端":
                PROACTIVE_AVAILABLE = False
                logger.warning("主动后端不可用，mark_replied/activity_report 将跳过（不阻塞消息处理）")
    # 向主动后端注册现有用户（仅在主动后端可用时执行，避免启动时长时间阻塞）
    if PROACTIVE_AVAILABLE:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                data = user_db._read()
                for uname, udata in data.get("users", {}).items():
                    intim = udata.get("intimacy", {}) or {}
                    if not intim:
                        intim = {"jingwen": 30}
                    for rid, val in intim.items():
                        if rid.startswith("group:"):
                            continue
                        try:
                            await client.post(f"{PROACTIVE_SERVER_URL}/api/activity/report",
                                              json={"user_id": uname, "role_id": rid, "intimacy": int(val)},
                                              timeout=5.0)
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(f"注册用户到主动后端失败: {e}")
    yield
    logger.info("主后端关闭")
app = FastAPI(title="FlexiChrono 主后端", version="4.0.2", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                   allow_methods=["*"], allow_headers=["*"])
@app.get("/")
async def root():
    return {"status": "ok", "service": "FlexiChrono 主后端", "version": "4.0.2"}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "main_server", "version": "4.0.2", "port": str(PORT)}

@app.get("/api/roles")
async def get_roles():
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{PERSONALITY_SERVER_URL}/api/roles")
            if r.status_code == 200:
                return r.json()
    except Exception:
        pass
    return {}
# -------------------------- NapCat QQ Webhook --------------------------
@app.post("/api/qq/webhook")
async def qq_webhook(request: Request):
    """
    接收 NapCat OneBot v11 HTTP 上报。
    NapCat HTTP 插件 post_url 配置为: http://你的IP:8000/api/qq/webhook
    仅处理私聊消息。
    """
    webhook_t0 = time.perf_counter()
    if QQ_WEBHOOK_SECRET:
        sig = request.headers.get("X-Signature", "")
        body_bytes = await request.body()
        expected = "sha256=" + hmac.new(
            QQ_WEBHOOK_SECRET.encode(), body_bytes, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return JSONResponse({"error": "invalid signature"}, status_code=403)
        body = json.loads(body_bytes)
    else:
        body = await request.json()
    if body.get("post_type") != "message":
        return {"status": "ignored"}
    if body.get("message_type") != "private":
        return {"status": "ignored_group"}
    # ---- 消息去重：NapCat 上报超时会重试，同一条消息可能到达多次 ----
    msg_id = body.get("message_id")
    if msg_id is not None:
        now = time.time()
        expired = [mid for mid, t in _recent_msg_ids.items() if now - t > _MSG_DEDUP_WINDOW]
        for mid in expired:
            del _recent_msg_ids[mid]
        if msg_id in _recent_msg_ids:
            logger.info(f"[QQ] 重复消息忽略 msg_id={msg_id} qq={body.get('user_id')}")
            return {"status": "duplicate"}
        _recent_msg_ids[msg_id] = now
    qq_number = str(body.get("user_id", ""))
    # 查绑定表（语音和文本分支共用）
    bound_username = user_db.get_username_by_qq(qq_number)
    if bound_username:
        identity = bound_username
        logger.info(f"[QQ] {qq_number} 已绑定账号 {identity}")
    else:
        identity = f"qq_tmp_{qq_number}"
        user_db.ensure_tmp_user(identity)
        logger.info(f"[QQ] {qq_number} 未绑定，使用临时身份 {identity}")
    history = qq_chat_history.setdefault(identity, [])
    # ---- 语音消息：下载AMR → ffmpeg转WAV → 语音后端(ASR→LLM→TTS) → 文本+语音回复 ----
    if onebot_has_voice(body.get("message", "")):
        voice_url = extract_onebot_voice_url(body.get("message", ""))
        if not voice_url:
            logger.warning(f"[QQ] 语音消息无下载URL qq={qq_number}")
            reply_text = "语音消息获取失败，请发文字给我吧～"
            await send_qq_private_msg(qq_number, reply_text)
            return {"reply": reply_text, "sent_via_api": True}
        logger.info(f"[QQ] 收到语音消息 qq={qq_number}, 正在下载AMR...")
        # 1. 下载 AMR 语音文件
        amr_bytes = await _download_file(voice_url)
        if not amr_bytes:
            reply_text = "语音下载失败，请发文字给我吧～"
            await send_qq_private_msg(qq_number, reply_text)
            return {"reply": reply_text, "sent_via_api": True}
        logger.info(f"[QQ] AMR下载完成: {len(amr_bytes)}B, 正在ffmpeg转WAV(16kHz mono)...")
        # 2. AMR → WAV (16kHz mono, ASR 输入格式)
        wav_bytes = await _ffmpeg_convert(amr_bytes, "wav", sample_rate=16000, channels=1)
        if not wav_bytes:
            reply_text = "语音转码失败（请确认服务器已安装ffmpeg），请发文字给我吧～"
            await send_qq_private_msg(qq_number, reply_text)
            return {"reply": reply_text, "sent_via_api": True}
        audio_b64 = base64.b64encode(wav_bytes).decode()
        logger.info(f"[QQ] WAV转码完成: {len(wav_bytes)}B, 送语音后端处理...")
        # 3. 调用语音后端全流程 ASR→人格→TTS
        try:
            result = await process_voice_message(
                identity, audio_b64, ["jingwen"], history, audio_format="wav")
            reply_text = result["reply"]
            tts_audio_b64 = result.get("audio_base64", "")
            tts_fmt = result.get("audio_format", "mp3")
            asr_text = result.get("asr_text", "")
            logger.info(f"[QQ] 语音处理完成: ASR识别='{asr_text[:50]}' 回复长度={len(reply_text)} TTS音频={'有' if tts_audio_b64 else '无'}")
        except Exception as e:
            logger.error(f"[QQ] 语音处理失败: {e}", exc_info=True)
            reply_text = "抱歉，我刚才没听清，能再说一遍吗？"
            tts_audio_b64 = ""
            tts_fmt = "mp3"
        # 4. 发送文本回复
        sent = await send_qq_private_msg(qq_number, reply_text)
        # 5. 发送 TTS 语音回复（转AMR后通过NapCat发送）
        if tts_audio_b64:
            voice_sent = await send_qq_private_record(qq_number, tts_audio_b64, tts_fmt)
            if not voice_sent:
                logger.warning(f"[QQ] TTS语音回复发送失败（文本回复已发送）")
        total_dur = time.perf_counter() - webhook_t0
        logger.info(f"[QQ][总耗时] qq={qq_number} identity={identity} "
                    f"语音全流程={_fmt_ms(total_dur)} 发送={'成功' if sent else '失败/未配置'}")
        return {"reply": reply_text, "sent_via_api": sent}
    # ---- 文本消息 ----
    text = extract_onebot_text(body.get("message", ""))
    if not text:
        return {"status": "empty"}
    logger.info(f"[QQ] 收到消息 qq={qq_number}: {text[:50]}")
    try:
        result = await process_chat_message(identity, text, ["jingwen"], history)
        reply_text = result["reply"]
    except Exception as e:
        logger.error(f"[QQ] 消息处理失败: {e}", exc_info=True)
        reply_text = "抱歉，我刚才走神了，能再说一遍吗？"
    sent = await send_qq_private_msg(qq_number, reply_text)
    if not sent and NAPCAT_HTTP_URL:
        logger.warning(f"[QQ] NapCat API 发送失败，回复放在响应体中")
    total_dur = time.perf_counter() - webhook_t0
    logger.info(f"[QQ][总耗时] qq={qq_number} identity={identity} "
                f"回复长度={len(reply_text)} 发送={'成功' if sent else '失败/未配置'} "
                f"Webhook全流程={_fmt_ms(total_dur)}")
    return {"reply": reply_text, "sent_via_api": sent}
# -------------------------- QQ 绑定与数据迁移 --------------------------
@app.post("/api/user/bind-qq")
async def bind_qq(request: Request):
    """
    正式账号绑定 QQ 号。
    将 qq_tmp_{qq} 下的所有数据（亲密度/session/向量记忆/主动消息数据）迁移到正式账号。
    """
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    user = user_db.authenticate_token(token)
    if not user:
        return JSONResponse({"error": "未登录"}, status_code=401)
    body = await request.json()
    qq_number = str(body.get("qq_number", "")).strip()
    if not qq_number or not qq_number.isdigit() or len(qq_number) < 5:
        return JSONResponse({"success": False, "error": "QQ号格式不正确"}, status_code=400)
    username = user["username"]
    existing = user_db.get_username_by_qq(qq_number)
    if existing and existing != username:
        return JSONResponse({"success": False, "error": "该QQ号已绑定其他账号"}, status_code=400)
    old_qq = user_db.get_qq_by_username(username)
    if old_qq and old_qq != qq_number:
        return JSONResponse({"success": False, "error": "该账号已绑定其他QQ号，请先解绑"}, status_code=400)
    tmp_identity = f"qq_tmp_{qq_number}"
    migrated = {"intimacy": False, "session": False, "memories": 0, "proactive": False}
    # 1. 迁移亲密度
    tmp_intimacy = user_db.get_all_intimacy(tmp_identity)
    if tmp_intimacy:
        formal_intimacy = user_db.get_all_intimacy(username)
        for rid, val in tmp_intimacy.items():
            if rid not in formal_intimacy:
                formal_intimacy[rid] = val
        user_db.set_all_intimacy(username, formal_intimacy)
        migrated["intimacy"] = True
    # 2. 迁移 session
    tmp_session = user_db.get_session(tmp_identity)
    formal_session = user_db.get_session(username)
    if tmp_session and not formal_session:
        user_db.set_session(username, tmp_session)
        migrated["session"] = True
    elif tmp_session and formal_session:
        try:
            async with httpx.AsyncClient(timeout=10.0) as c:
                await c.delete(f"{PERSONALITY_SERVER_URL}/api/session/{tmp_session}")
        except Exception:
            pass
    # 3. 迁移向量记忆
    mem_result = await call_vector_migrate(tmp_identity, username)
    migrated["memories"] = mem_result.get("migrated", 0)
    # 4. 迁移主动消息数据
    migrated["proactive"] = await call_proactive_migrate(tmp_identity, username)
    # 5. 清理临时聊天历史
    qq_chat_history.pop(tmp_identity, None)
    # 6. 写入绑定表
    success, _ = user_db.bind_qq(qq_number, username)
    if not success:
        return JSONResponse({"success": False, "error": "绑定失败"}, status_code=400)
    logger.info(f"[QQ绑定] {username} 绑定 QQ {qq_number}, 迁移: {migrated}")
    return {"success": True, "qq_number": qq_number, "migrated": migrated}
@app.post("/api/user/unbind-qq")
async def unbind_qq(request: Request):
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    user = user_db.authenticate_token(token)
    if not user:
        return JSONResponse({"error": "未登录"}, status_code=401)
    qq = user_db.unbind_qq(user["username"])
    if qq:
        return {"success": True, "qq_number": qq}
    return JSONResponse({"success": False, "error": "未绑定QQ"}, status_code=400)
@app.get("/api/user/qq-binding")
async def get_qq_binding(request: Request):
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    user = user_db.authenticate_token(token)
    if not user:
        return JSONResponse({"error": "未登录"}, status_code=401)
    qq = user_db.get_qq_by_username(user["username"])
    return {"bound": qq is not None, "qq_number": qq}
# -------------------------- 主动消息推送 --------------------------
@app.post("/api/internal/proactive_push")
async def internal_proactive_push(request: Request, x_internal_token: str = Header(None)):
    if x_internal_token != INTERNAL_TOKEN:
        return JSONResponse({"error": "forbidden"}, status_code=403)
    body = await request.json()
    user_id = body.get("user_id")
    role_id = body.get("role_id")
    content = body.get("content", "")
    message_id = body.get("message_id")
    if not user_id or not content:
        return JSONResponse({"error": "bad_request"}, status_code=400)
    msg = {"type": "proactive", "role_id": role_id, "content": content,
           "timestamp": time.time(), "message_id": message_id}
    ws = manager.active_connections.get(user_id)
    if ws:
        try:
            await ws.send_text(json.dumps(msg, ensure_ascii=False))
            return {"delivered": True}
        except Exception as e:
            logger.warning(f"主动推送WS发送失败，转离线: {e}")
    manager.pending_messages.setdefault(user_id, []).append(msg)
    return {"delivered": False}
# -------------------------- 认证 --------------------------
@app.post("/api/login")
async def login(request: Request):
    body = await request.json()
    user = user_db.authenticate(body.get("username", ""), body.get("password", ""))
    if user:
        token = base64.b64encode(f"{user['username']}:{body.get('password','')}".encode()).decode()
        return {
            "success": True, "token": token, "username": user["username"],
            "nickname": user["nickname"], "is_admin": user["is_admin"],
            "intimacy": user.get("intimacy", {}), "qq_bound": user.get("qq_bound", False)
        }
    return JSONResponse({"success": False, "error": "用户名或密码错误"}, status_code=401)
@app.post("/api/admin/login")
async def admin_login(request: Request):
    body = await request.json()
    user = user_db.authenticate(body.get("username", ""), body.get("password", ""))
    if user and user["is_admin"]:
        token = base64.b64encode(f"{user['username']}:{body.get('password','')}".encode()).decode()
        return {"success": True, "token": token}
    return JSONResponse({"success": False, "error": "管理员验证失败"}, status_code=401)
@app.post("/api/reset-password")
async def reset_password(request: Request):
    body = await request.json()
    if user_db.reset_password(body.get("username", ""), body.get("new_password", "")):
        return {"success": True}
    return JSONResponse({"success": False, "error": "用户不存在"}, status_code=404)
# -------------------------- 管理员接口 --------------------------
@app.get("/api/admin/users")
async def admin_get_users(request: Request):
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    user = user_db.authenticate_token(token)
    if not user or not user["is_admin"]:
        return JSONResponse({"error": "无权限"}, status_code=403)
    return {"users": user_db.get_all_users()}
@app.post("/api/admin/users")
async def admin_create_user(request: Request):
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    admin = user_db.authenticate_token(token)
    if not admin or not admin["is_admin"]:
        return JSONResponse({"error": "无权限"}, status_code=403)
    body = await request.json()
    if user_db.admin_create_user(body.get("username", ""), body.get("password", ""), body.get("nickname")):
        return {"success": True}
    return JSONResponse({"success": False, "error": "用户已存在"}, status_code=400)
@app.post("/api/admin/users/{username}/intimacy")
async def admin_set_intimacy(username: str, request: Request):
    """保留兼容：旧版亲密度设置接口"""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    admin = user_db.authenticate_token(token)
    if not admin or not admin["is_admin"]:
        return JSONResponse({"error": "无权限"}, status_code=403)
    body = await request.json()
    if user_db.admin_set_intimacy(username, body.get("role_id", ""), body.get("value", 30)):
        return {"success": True}
    return JSONResponse({"success": False, "error": "用户不存在"}, status_code=404)
@app.get("/api/admin/users/{username}/psych")
async def admin_get_user_psych(username: str, request: Request):
    """获取指定用户与所有角色的实时心理状态"""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    admin = user_db.authenticate_token(token)
    if not admin or not admin["is_admin"]:
        return JSONResponse({"error": "无权限"}, status_code=403)
    roles = {}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{PERSONALITY_SERVER_URL}/api/roles")
            if r.status_code == 200:
                roles = r.json()
    except Exception:
        pass
    intimacy_map = user_db.get_all_intimacy(username)
    session_id = user_db.get_session(username)
    session_states = {}
    if session_id:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(f"{PERSONALITY_SERVER_URL}/api/session/{session_id}/state")
                if r.status_code == 200:
                    session_states = r.json().get("states", {})
        except Exception as e:
            logger.warning(f"获取session状态失败: {e}")
    result = {}
    for rid, rdef in roles.items():
        baseline = rdef.get("psych_baseline", {})
        saved = session_states.get(rid, {})
        result[rid] = {
            "role_name": rdef.get("name", rid),
            "role_emoji": rdef.get("emoji", ""),
            "intimacy": saved.get("intimacy", intimacy_map.get(rid, 30)),
            "trust": saved.get("trust", baseline.get("trust", 50)),
            "security": saved.get("security", baseline.get("security", 50)),
            "attachment": saved.get("attachment", baseline.get("attachment", 20)),
            "jealousy": saved.get("jealousy", 0),
            "fatigue": saved.get("fatigue", 0),
            "mood": saved.get("mood", baseline.get("mood", 50)),
            "trauma_flag": saved.get("trauma_flag", False),
            "has_session_data": rid in session_states,
        }
    return {"username": username, "session_id": session_id, "states": result}
@app.post("/api/admin/users/{username}/psych")
async def admin_set_user_psych(username: str, request: Request):
    """修改指定用户与某角色的实时心理数据"""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    admin = user_db.authenticate_token(token)
    if not admin or not admin["is_admin"]:
        return JSONResponse({"error": "无权限"}, status_code=403)
    body = await request.json()
    role_id = body.get("role_id", "")
    if not role_id:
        return JSONResponse({"success": False, "error": "缺少 role_id"}, status_code=400)
    session_id = user_db.get_session(username)
    if not session_id:
        session_id = await create_personality_session()
        if not session_id:
            return JSONResponse({"success": False, "error": "无法创建session"}, status_code=500)
        user_db.set_session(username, session_id)
    update_fields = {}
    for field in ("intimacy", "trust", "security", "attachment", "jealousy", "fatigue", "mood"):
        if field in body and body[field] is not None:
            try:
                update_fields[field] = float(body[field])
            except (TypeError, ValueError):
                pass
    if not update_fields:
        return JSONResponse({"success": False, "error": "没有要更新的字段"}, status_code=400)
    if "intimacy" in update_fields:
        user_db.set_intimacy(username, role_id, int(update_fields["intimacy"]))
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.put(
                f"{PERSONALITY_SERVER_URL}/api/session/{session_id}/state/{role_id}",
                json=update_fields, timeout=10.0)
            if r.status_code == 200:
                return {"success": True, "state": r.json().get("state", {})}
            return JSONResponse({"success": False, "error": f"人格后端返回 {r.status_code}"},
                                status_code=502)
    except Exception as e:
        logger.error(f"更新心理状态失败: {e}")
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)
@app.put("/api/user/nickname")
async def update_nickname(request: Request):
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    user = user_db.authenticate_token(token)
    if not user:
        return JSONResponse({"error": "未登录"}, status_code=401)
    body = await request.json()
    if user_db.update_nickname(user["username"], body.get("nickname", "")):
        return {"success": True, "nickname": body.get("nickname", "")}
    return JSONResponse({"success": False}, status_code=400)
# -------------------------- WebSocket --------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    authenticated = False
    username = None
    session_id = None
    try:
        await websocket.accept()
        # 认证超时从 10s 放宽到 30s，避免前端页面打开后未立即登录导致频繁断连警告
        auth_msg = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
        auth_data = json.loads(auth_msg)
        if auth_data.get("action") != "auth":
            await websocket.send_text(json.dumps({"type": "error", "message": "请先认证"}))
            return
        user_info = user_db.authenticate_token(auth_data.get("token", ""))
        if not user_info:
            await websocket.send_text(json.dumps({"type": "error", "message": "认证失败"}))
            return
        authenticated = True
        username = user_info["username"]
        await manager.connect(username, websocket)
        qq_bound = user_db.get_qq_by_username(username) is not None
        await websocket.send_text(json.dumps({
            "type": "auth_ok", "username": username,
            "nickname": user_info["nickname"], "qq_bound": qq_bound
        }, ensure_ascii=False))
        logger.info(f"用户 {username} 已连接")
        for pm in manager.pending_messages.pop(username, []):
            try:
                await websocket.send_text(json.dumps(pm, ensure_ascii=False))
            except Exception:
                pass
        try:
            async with httpx.AsyncClient(timeout=5.0) as c:
                r = await c.get(f"{PROACTIVE_SERVER_URL}/api/pending/{username}", timeout=5.0)
                if r.status_code == 200:
                    for pm in r.json().get("messages", []):
                        out = {
                            "type": "proactive", "role_id": pm.get("role_id"),
                            "content": pm.get("content", ""), "timestamp": pm.get("created_at"),
                            "message_id": pm.get("id")
                        }
                        await websocket.send_text(json.dumps(out, ensure_ascii=False))
                        await c.post(f"{PROACTIVE_SERVER_URL}/api/mark_delivered",
                                     json={"message_id": pm.get("id")}, timeout=5.0)
        except Exception as e:
            logger.warning(f"拉取proactive待发消息失败: {e}")
        session_id = user_db.get_session(username)
        if session_id:
            logger.info(f"用户 {username} 恢复session: {session_id}")
        chat_history = []
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            action = msg.get("action", "chat")
            if action == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue
            if action == "reset_session":
                old_sid = user_db.get_session(username)
                if old_sid:
                    try:
                        async with httpx.AsyncClient(timeout=10.0) as c:
                            await c.delete(f"{PERSONALITY_SERVER_URL}/api/session/{old_sid}")
                    except Exception:
                        pass
                user_db.set_session(username, None)
                session_id = None
                chat_history = []
                await websocket.send_text(json.dumps({"type": "session_reset"}))
                continue
            if action == "set_roles":
                await websocket.send_text(json.dumps({
                    "type": "roles_updated", "role_ids": msg.get("role_ids", ["jingwen"])}))
                continue
            if action == "set_nickname":
                nn = msg.get("nickname", "")
                if user_db.update_nickname(username, nn):
                    await websocket.send_text(json.dumps({"type": "nickname_updated", "nickname": nn}))
                continue
            if action == "voice_chat":
                # 语音消息：转发到语音后端处理（ASR→人格→TTS）
                try:
                    audio_b64 = msg.get("audio", "")
                    role_ids = msg.get("role_ids", ["jingwen"])
                    audio_fmt = msg.get("audio_format", "wav")
                    if not audio_b64:
                        await websocket.send_text(json.dumps(
                            {"type": "error", "message": "未收到语音数据"}, ensure_ascii=False))
                        continue
                    if not role_ids:
                        role_ids = ["jingwen"]
                    logger.info(f"[{username}] 语音对话: roles={role_ids}, 音频base64长度={len(audio_b64)}")
                    result = await process_voice_message(
                        username, audio_b64, role_ids, chat_history, audio_fmt)
                    session_id = result["session_id"]
                    await websocket.send_text(json.dumps({
                        "type": "voice_reply",
                        "content": result["reply"],
                        "asr_text": result["asr_text"],
                        "audio": result["audio_base64"],
                        "audio_format": result["audio_format"],
                        "role_ids": result["role_ids"], "mode": result["mode"],
                        "intimacy": result["intimacy"], "session_id": session_id,
                    }, ensure_ascii=False))
                except WebSocketDisconnect:
                    raise
                except Exception as ve:
                    logger.error(f"处理语音消息时出错: {ve}", exc_info=True)
                    try:
                        await websocket.send_text(json.dumps(
                            {"type": "error", "message": "语音处理失败，请重试或发送文字"},
                            ensure_ascii=False))
                    except Exception:
                        pass
                continue
            if action != "chat":
                continue
            try:
                user_message = msg.get("message", "").strip()
                role_ids = msg.get("role_ids", ["jingwen"])
                if not user_message:
                    continue
                if not role_ids:
                    role_ids = ["jingwen"]
                logger.info(f"[{username}] 对话: roles={role_ids}")
                result = await process_chat_message(username, user_message, role_ids, chat_history)
                session_id = result["session_id"]
                await websocket.send_text(json.dumps({
                    "type": "reply", "content": result["reply"],
                    "role_ids": result["role_ids"], "mode": result["mode"],
                    "intimacy": result["intimacy"], "session_id": session_id,
                    "used_llm_analysis": result["used_llm_analysis"]
                }, ensure_ascii=False))
            except WebSocketDisconnect:
                raise
            except Exception as me:
                logger.error(f"处理消息时出错(不断开连接): {me}", exc_info=True)
                try:
                    await websocket.send_text(json.dumps(
                        {"type": "error", "message": "抱歉，我刚才走神了，能再说一遍吗？"},
                        ensure_ascii=False))
                except Exception:
                    pass
    except WebSocketDisconnect:
        logger.info(f"用户 {username} 断开连接")
    except asyncio.TimeoutError:
        logger.warning(f"用户 {username} 认证超时（30s内未发送auth消息）")
    except Exception as e:
        logger.error(f"WebSocket 错误: {e}", exc_info=True)
    finally:
        if username:
            manager.disconnect(username)
# -------------------------- 预留模块 --------------------------
from fastapi import FastAPI as FastAPIApp
mc_app = FastAPIApp()
pet_app = FastAPIApp()
@mc_app.websocket("/ws")
async def mc_ws(ws):
    await ws.close(code=4000, reason="MC 模块尚未实现")
@pet_app.websocket("/ws")
async def pet_ws(ws):
    await ws.close(code=4000, reason="Pet 模块尚未实现")
app.mount("/mc", mc_app)
app.mount("/pet", pet_app)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")

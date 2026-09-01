
"""
主后端 v4.0 - 端口 8000
对接人格后端 v11.0：session管理 / mode统一(single/group) / 限流处理 / 记忆直存 / 亲密度同步
v3.1: NapCat QQ 接入（HTTP Webhook）/ QQ绑定与数据迁移 / 管理员实时心理状态编辑
v4.0: 语音消息路由（文本→人格后端，语音→语音后端ASR→人格→TTS）/ 对接人格后端v11.0
v4.0.1: 修复QQ消息发送/主动后端熔断/消息去重/WS认证超时
v4.0.2: QQ语音消息完整链路（AMR下载→ffmpeg转WAV→ASR→LLM→TTS→AMR发送）
v4.0.3: 修复NapCat新版本QQ偏移不全时语音url残缺问题，增加get_record fallback
"""
import asyncio, json, logging, base64, os, time, hmac, hashlib
from typing import Optional, Dict, List, Any
from contextlib import asynccontextmanager
from collections import deque
import httpx
from common.security import hash_password, verify_password
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
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
# NapCat HTTP API 访问令牌（NapCat HTTP服务器配置中的 access_token）。
# 如果 NapCat 配置了 token，所有 API 请求必须在 Header 中带 Authorization: Bearer <token>，否则返回 403 token verify failed
# 配置方法: export NAPCAT_ACCESS_TOKEN="你的token"
NAPCAT_ACCESS_TOKEN = os.getenv("NAPCAT_ACCESS_TOKEN", "")
# Webhook 签名校验密钥（NapCat 配置中的 secret，留空则不校验）
QQ_WEBHOOK_SECRET = os.getenv("QQ_WEBHOOK_SECRET", "")

# P3：主动联系时自动发图（自拍）
# 启用后，proactive_server 主动联系用户时，会自动生成一张角色自拍图片一起发到QQ
# 配置方法: export PROACTIVE_AUTO_IMAGE=true
PROACTIVE_AUTO_IMAGE = os.getenv("PROACTIVE_AUTO_IMAGE", "false").lower() == "true"
# 主动发图的亲密度阈值（低于此值不发图，避免陌生人主动发自拍）
PROACTIVE_IMAGE_INTIMACY_THRESHOLD = int(os.getenv("PROACTIVE_IMAGE_INTIMACY_THRESHOLD", "50"))
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
# ============================================================
# 对话意图检测器 —— 检测用户是否想结束对话，避免AI强行延续话题
# ============================================================
class ConversationIntentDetector:
    """对话意图检测器 —— 检测用户是否想结束对话"""

    # 高置信度关键词（用户明确想结束）
    HIGH_CONFIDENCE_KEYWORDS = [
        "不聊了", "别聊了", "不聊了", "晚安", "该睡了", "去睡觉", "睡觉",
        "别烦我", "别理我", "烦死了", "闭嘴", "不想聊", "没空",
        "在忙", "忙死了", "走了", "再见", "拜拜", "拜", "下次聊",
        "改天聊", "不说了", "睡了", "困死了", "太困了",
        "太晚了", "不早了", "要睡了", "去睡了", "先睡了",
    ]

    # 中置信度关键词（用户可能想结束）
    MEDIUM_CONFIDENCE_KEYWORDS = [
        "有点困", "想休息", "想睡了", "有点累", "累了", "好困",
        "有点烦", "不想说话", "不想理人", "想一个人", "安静",
        "算了", "随便",
    ]

    def detect(self, text: str) -> Dict[str, Any]:
        """检测用户消息的意图，返回 intent 类型和提示词"""
        if not text:
            return {"intent": "normal", "confidence": 0.0, "keywords": []}

        text_lower = text.lower()
        matched_high = [kw for kw in self.HIGH_CONFIDENCE_KEYWORDS if kw in text_lower]
        matched_mid = [kw for kw in self.MEDIUM_CONFIDENCE_KEYWORDS if kw in text_lower]

        if matched_high:
            return {
                "intent": "goodbye",
                "confidence": 0.9,
                "level": "high",
                "keywords": matched_high,
                "hint": "用户已经明确想结束对话了，请用符合角色性格的方式温暖告别，不要强行延续话题，不要问新问题。简短、温柔、符合角色性格地告别即可。"
            }
        elif matched_mid:
            return {
                "intent": "goodbye",
                "confidence": 0.6,
                "level": "medium",
                "keywords": matched_mid,
                "hint": "用户可能有点想结束对话了，请体贴地回应，轻轻带过，不要强行延续话题或问新问题。可以适当建议对方休息或做自己的事情。"
            }

        return {"intent": "normal", "confidence": 0.0, "keywords": []}


# 用户晚安记录：key = "user_id:role_id", value = timestamp
_user_goodbye_tracker: Dict[str, float] = {}
_GOODBYE_COOLDOWN_SECONDS = 1800  # 30分钟冷却期


async def _report_goodbye(user_id: str, role_id: str):
    """通知主动消息后端用户已说晚安，暂停主动消息"""
    global _user_goodbye_tracker
    key = f"{user_id}:{role_id}"
    _user_goodbye_tracker[key] = time.time()
    logger.info(f"[意图检测] 用户 {user_id} 角色 {role_id} 说了晚安，30分钟内不主动推送")
    # 异步通知主动消息后端
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"{PROACTIVE_SERVER_URL}/api/goodbye/report",
                json={"user_id": user_id, "role_id": role_id},
                timeout=5.0
            )
    except Exception as e:
        logger.debug(f"[意图检测] 报告晚安到主动后端失败: {e}")

class UserDB:
    def __init__(self, filepath=None):
        self.filepath = filepath or os.path.join(DATA_DIR, "userdb.json")
        import threading
        self._lock = threading.Lock()
        self._ensure_file()
    def _ensure_file(self):
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump({
                    "users": {
                        "admin": {
                            "password": hash_password("admin123"), "nickname": "管理员",
                            "is_admin": True, "intimacy": {}, "session_id": None
                        }
                    },
                    "qq_bindings": {}  # {qq_number: username}
                }, f, ensure_ascii=False, indent=2)
    def _read(self):
        with self._lock:
            with open(self.filepath, "r", encoding="utf-8") as f:
                return json.load(f)
    def _write(self, data):
        with self._lock:
            # 先写临时文件再原子替换，避免写入中途崩溃导致文件损坏
            tmp = self.filepath + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.filepath)
    def authenticate(self, username, password):
        data = self._read()
        user = data["users"].get(username)
        if not user:
            return None
        stored = user.get("password", "")
        # 兼容旧明文密码：验证成功后自动升级为 bcrypt 哈希
        if not stored.startswith("$2") and not stored.startswith("pbkdf2$"):
            if stored == password:
                user["password"] = hash_password(password)
                self._write(data)
                logger.info(f"[安全] 用户 {username} 的明文密码已自动升级为哈希")
            else:
                return None
        elif not verify_password(password, stored):
            return None
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
            "password": hash_password(password), "nickname": nickname or username,
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
            data["users"][username]["password"] = hash_password(new_password)
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
                "password": hash_password(base64.b64encode(os.urandom(18)).decode()),
                "nickname": f"QQ用户",
                "is_admin": False, "intimacy": {}, "session_id": None,
                "is_qq_tmp": True
            }
            self._write(data)
        return True
    def set_user_field(self, username, field, value):
        """设置用户的任意字段（如 disabled），用户不存在时返回 False"""
        data = self._read()
        if username in data["users"]:
            data["users"][username][field] = value
            self._write(data)
            return True
        return False
user_db = UserDB()

async def _get_user_intimacy(user_id: str, role_id: str) -> int:
    """P3 修复：获取用户对角色的亲密度（两层平均值）。
    第一层：userdb.json 的 intimacy（长期基础好感度，admin可手动设置）
    第二层：proactive_server 的 last_intimacy（当下实时好感度，随对话波动）
    取两层平均值，更全面地反映关系状态。
    """
    # 1. 读取第一层：userdb.json 的基础亲密度（用 get_intimacy 代替不存在的 get_user）
    intimacy_userdb = 0
    try:
        intimacy_userdb = int(user_db.get_intimacy(user_id, role_id))
    except Exception:
        pass
    
    # 2. 读取第二层：proactive_server 的当下亲密度
    intimacy_proactive = 0
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{PROACTIVE_SERVER_URL}/api/status/{user_id}")
            if resp.status_code == 200:
                data = resp.json()
                # data 是列表，每个元素是一个角色的状态
                if isinstance(data, list):
                    for role_status in data:
                        if role_status.get("role_id") == role_id:
                            proactive_val = role_status.get("intimacy")
                            if proactive_val is not None:
                                intimacy_proactive = int(proactive_val)
                                break
                elif isinstance(data, dict):
                    # 兼容返回字典的情况
                    roles = data.get("roles", data.get("status", []))
                    if isinstance(roles, list):
                        for role_status in roles:
                            if role_status.get("role_id") == role_id:
                                proactive_val = role_status.get("intimacy")
                                if proactive_val is not None:
                                    intimacy_proactive = int(proactive_val)
                                    break
    except Exception as e:
        logger.debug(f"[亲密度] 从proactive_server读取失败: {e}")
    
    # 3. 取两层平均值（只有有效值参与计算）
    valid_values = []
    if intimacy_userdb > 0:
        valid_values.append(intimacy_userdb)
    if intimacy_proactive > 0:
        valid_values.append(intimacy_proactive)
    
    if valid_values:
        intimacy = int(sum(valid_values) / len(valid_values))
    else:
        intimacy = 0  # 两层都没有值，返回0
    
    logger.debug(f"[亲密度] user_id={user_id} role={role_id} "
                 f"第一层={intimacy_userdb} 第二层={intimacy_proactive} "
                 f"平均值={intimacy}")
    
    return max(0, min(100, intimacy))
# 后台任务引用集合
background_tasks = set()
# QQ 用户的内存聊天历史（重启后丢失；心理状态由 personality_server session 持久化）
qq_chat_history: Dict[str, List[Dict]] = {}

# v13.0: 待纳入对话历史的主动消息缓存
# 场景：proactive_server 推送话题延续/主动消息后，用户下一次发消息时需要把这条主动消息
# 加入 chat_history，这样大模型才知道自己刚才说了什么。key=user_id, value={role_id, content, timestamp}
pending_proactive_in_history: Dict[str, Dict] = {}

# ============================================================
# Identity FIFO Queue — 每个用户独立队列，保证消息严格串行处理
# 解决：用户短时间内连发多条消息时回复顺序错乱的问题
# 三层保障：Identity Queue(保证串行) → process_chat_message(整体原子执行) → message_id去重(保证幂等)
# ============================================================
class IdentityQueueManager:
    """
    每个 identity 一个独立的 FIFO 队列 + worker 协程。
    同一 identity 的消息严格排队、逐个处理，不会交叉；
    不同 identity 之间并行，互不影响。

    用法:
        result = await identity_queue.submit(
            identity, message_id,
            lambda: process_chat_message(identity, text, role_ids, history)
        )
    """
    def __init__(self):
        self._queues: Dict[str, asyncio.Queue] = {}
        self._workers: Dict[str, asyncio.Task] = {}
        self._create_lock = asyncio.Lock()

    async def submit(self, identity: str, message_id: Optional[str], coro_factory):
        """
        提交任务到 identity 队列，阻塞等待结果并返回。
        coro_factory: 无参数可调用对象，返回协程（延迟创建，确保在 worker 上下文中执行）
        """
        # 第一层幂等检查：入队前就命中则直接返回缓存结果
        if message_id and message_id in _processed_messages:
            logger.info(f"[IdentityQueue] 幂等命中(入队前) msg_id={message_id} identity={identity}")
            return _processed_messages[message_id]

        # 确保该 identity 的队列和 worker 已创建
        async with self._create_lock:
            if identity not in self._queues:
                self._queues[identity] = asyncio.Queue()
                self._workers[identity] = asyncio.create_task(self._worker(identity))
                logger.info(f"[IdentityQueue] 创建 worker identity={identity}")

        loop = asyncio.get_event_loop()
        future = loop.create_future()
        await self._queues[identity].put((message_id, coro_factory, future))
        return await future

    async def _worker(self, identity: str):
        """worker 协程：循环从队列取任务，串行执行，保证同一 identity 同一时刻只有一个任务在跑。"""
        queue = self._queues[identity]
        while True:
            message_id, coro_factory, future = await queue.get()
            try:
                # 第二层幂等检查：排队期间可能已被其他途径处理
                if message_id and message_id in _processed_messages:
                    if not future.done():
                        future.set_result(_processed_messages[message_id])
                    continue

                # 执行协程工厂得到协程并 await —— 整个处理流程在此原子执行，不释放给同 identity 的其他任务
                coro = coro_factory()
                result = await coro

                # 记录已处理消息（幂等缓存）
                if message_id:
                    _mark_message_processed(message_id, result)

                if not future.done():
                    future.set_result(result)
            except Exception as e:
                logger.error(f"[IdentityQueue] worker 异常 identity={identity} msg_id={message_id}: {e}", exc_info=True)
                if not future.done():
                    future.set_exception(e)
            finally:
                queue.task_done()


# ---- message_id 幂等去重（防止 QQ 重复上报 / 客户端重试 / 网络超时重发导致同一条消息被处理两次）----
_processed_messages: Dict[str, Dict] = {}
_PROCESSED_MSG_CACHE_MAX = 20000  # 最多缓存 20000 条结果，超量清理最早的一半


def _mark_message_processed(message_id: str, result: Dict):
    """记录一条消息已处理完成，缓存其结果供幂等命中使用。"""
    _processed_messages[message_id] = result
    if len(_processed_messages) > _PROCESSED_MSG_CACHE_MAX:
        # 清理最早的一半（dict 保序，近似 FIFO）
        keys = list(_processed_messages.keys())
        for k in keys[:len(keys) // 2]:
            del _processed_messages[k]
        logger.info(f"[IdentityQueue] 幂等缓存清理，剩余 {len(_processed_messages)} 条")


# 全局队列管理器实例
identity_queue = IdentityQueueManager()

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
                                     session_id=None, intimacy_map=None, temperature=0.9, max_tokens=500,
                                     goodbye_hint=None, enable_bystander=False, active_role_id=None):
    mode = "group" if len(role_ids) > 1 else "single"
    payload = {
        "mode": mode, "role_ids": role_ids, "user_message": user_message,
        "memory_context": memory_context, "chat_history": chat_history,
        "temperature": temperature, "max_tokens": max_tokens,
        "return_debug": True, "enable_memory_analysis": True,
        "goodbye_hint": goodbye_hint,
        "enable_bystander": enable_bystander,
    }
    if active_role_id:
        payload["active_role_id"] = active_role_id
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


async def call_vector_insert_session(user_id, conversation_id, role, text):
    """写入会话片段到 Pinecone 会话库（7天自动过期，后台调用不阻塞）"""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {
                "username": user_id,
                "conversation_id": conversation_id,
                "role": role,
                "text": text
            }
            resp = await client.post(
                f"{VECTOR_SERVER_URL}/api/vector/insert_session_vector",
                json=payload, timeout=30.0)
            return resp.status_code == 200 and resp.json().get("ok")
    except Exception as e:
        logger.debug(f"会话片段写入失败: {e}")
        return False


async def call_vector_search_session(user_id, conversation_id, query, top_k=3):
    """检索短期会话历史（最近相关的对话片段），返回格式化文本"""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {
                "username": user_id,
                "conversation_id": conversation_id,
                "query_text": query,
                "top_k": top_k
            }
            resp = await client.post(
                f"{VECTOR_SERVER_URL}/api/vector/search_session_history",
                json=payload, timeout=30.0)
            if resp.status_code == 200 and resp.json().get("ok"):
                items = resp.json().get("data", [])
                parts = []
                for item in items:
                    role = item.get("role", "user")
                    text = item.get("text", "")
                    if text:
                        role_label = "用户" if role == "user" else "AI"
                        parts.append(f"{role_label}: {text}")
                return "\n".join(parts)
    except Exception as e:
        logger.debug(f"会话历史检索失败: {e}")
    return ""
# -------------------------- 主动消息后端对接（带熔断） --------------------------
async def call_proactive_report(user_id, role_id, intimacy, attachment=None, psych=None, session_id=None):
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
            if session_id:
                payload["session_id"] = session_id
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

# -------------------------- v13.0: 话题延续引擎调用 --------------------------
async def call_proactive_user_spoke(user_id, role_id, message: str = ""):
    """用户发消息时调用：重置话题延续状态（取消待发的新话题和收尾）
    v13.1: 传入用户消息文本，供主动后端检测终止对话意图（晚安/不聊了等）
    """
    if _proactive_should_skip():
        return
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.post(
                f"{PROACTIVE_SERVER_URL}/api/conversation/user_spoke",
                json={"user_id": user_id, "role_id": role_id, "message": message}, timeout=8.0)
            dur = time.perf_counter() - t0
            _log_port_timing("主动后端", "/api/conversation/user_spoke", dur, f"HTTP{resp.status_code}")
            if resp.status_code == 200:
                _proactive_on_success()
            else:
                _proactive_on_fail()
    except Exception as e:
        _proactive_on_fail()
        _log_port_timing("主动后端", "/api/conversation/user_spoke", time.perf_counter() - t0, f"ERROR:{e}")
        logger.warning(f"proactive用户发言上报失败: {e}")

async def call_proactive_ai_replied(user_id, role_id, recent_messages):
    """AI回复完成后调用：记录AI回复时间+缓存最近对话，启动话题延续计时"""
    if _proactive_should_skip():
        return
    t0 = time.perf_counter()
    try:
        # 只保留最近6轮，格式转换为 {"role":"user"/"ai","content":"..."}
        msgs = []
        for m in (recent_messages or [])[-6:]:
            role = m.get("role", "")
            content = m.get("content", "")
            if role == "user":
                msgs.append({"role": "user", "content": content})
            elif role == "assistant":
                msgs.append({"role": "ai", "content": content})
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.post(
                f"{PROACTIVE_SERVER_URL}/api/conversation/ai_replied",
                json={"user_id": user_id, "role_id": role_id, "recent_messages": msgs},
                timeout=8.0)
            dur = time.perf_counter() - t0
            _log_port_timing("主动后端", "/api/conversation/ai_replied", dur, f"HTTP{resp.status_code}")
            if resp.status_code == 200:
                _proactive_on_success()
            else:
                _proactive_on_fail()
    except Exception as e:
        _proactive_on_fail()
        _log_port_timing("主动后端", "/api/conversation/ai_replied", time.perf_counter() - t0, f"ERROR:{e}")
        logger.warning(f"proactive AI回复上报失败: {e}")
class ConnectionManager:
    def __init__(self):
        self.active_connections = {}
        self.pending_messages = {}
        self.admin_connections = {}  # v4.0.4: 在线管理员连接，用于实时推送数据更新
    async def connect(self, username, ws, is_admin=False):
        self.active_connections[username] = ws
        if is_admin:
            self.admin_connections[username] = ws
            logger.info(f"[管理员推送] 管理员 {username} 已上线，已加入实时推送列表")
    def disconnect(self, username):
        self.active_connections.pop(username, None)
        if username in self.admin_connections:
            self.admin_connections.pop(username, None)
            logger.info(f"[管理员推送] 管理员 {username} 已下线")
    async def broadcast_admin_update(self, data: dict):
        """v4.0.4: 向所有在线管理员推送数据更新（亲密度变化等）"""
        if not self.admin_connections:
            return
        msg = json.dumps(data, ensure_ascii=False)
        disconnected = []
        for admin_name, ws in self.admin_connections.items():
            try:
                await ws.send_text(msg)
            except Exception:
                disconnected.append(admin_name)
        for name in disconnected:
            self.admin_connections.pop(name, None)
manager = ConnectionManager()

async def _notify_admin_intimacy_update(username: str, intimacy_map: dict):
    """v4.0.4: 亲密度变化时通知在线管理员，触发管理员后台实时刷新"""
    try:
        await manager.broadcast_admin_update({
            "type": "admin_update",
            "event": "intimacy_changed",
            "username": username,
            "intimacy": intimacy_map,
            "timestamp": time.time(),
        })
    except Exception as e:
        logger.debug(f"[管理员推送] 推送亲密度更新失败: {e}")
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
        role_ids = ["nianqi"]
    if chat_history is None:
        chat_history = []
    mode = "group" if len(role_ids) > 1 else "single"
    mem_role_id = role_ids[0] if len(role_ids) == 1 else "group:" + "+".join(sorted(role_ids))
    timer = StepTimer(f"{identity}|{mode}|{'+'.join(role_ids)}")
    # 用户主动发言，标记主动消息已回复（后台异步，不阻塞）
    _t = asyncio.create_task(call_proactive_mark_replied(identity))
    background_tasks.add(_t)
    _t.add_done_callback(background_tasks.discard)
    # v13.0: 单聊模式下，通知话题延续引擎用户发言了（重置状态）
    if mode == "single":
        _ts = asyncio.create_task(call_proactive_user_spoke(identity, role_ids[0], user_message))
        background_tasks.add(_ts)
        _ts.add_done_callback(background_tasks.discard)
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
    # 向量记忆检索：长期记忆（摘要）+ 短期会话历史（原文）
    memory_context = await call_vector_search(identity, user_message, role_id=mem_role_id)
    session_context = await call_vector_search_session(identity, session_id or "", user_message, top_k=3)
    # 合并上下文：会话历史放前面（更贴近当前话题），长期记忆放后面
    if session_context and memory_context:
        memory_context = f"【最近对话片段】\n{session_context}\n\n【长期记忆】\n{memory_context}"
    elif session_context:
        memory_context = f"【最近对话片段】\n{session_context}"
    timer.mark("记忆检索")
    # 人格引擎生成（最耗时：LLM调用）
    # 对话意图检测：判断用户是否想结束对话
    intent = ConversationIntentDetector().detect(user_message)
    goodbye_hint = intent.get("hint") if intent["intent"] == "goodbye" else None
    if intent["intent"] == "goodbye":
        logger.info(f"[意图检测] 用户 {identity} 想结束对话: level={intent.get('level')} keywords={intent.get('keywords')}")
        # 后台通知主动消息后端（记录晚安，暂停主动推送）
        rid_for_goodbye = role_ids[0] if len(role_ids) == 1 else "group"
        _t_gd = asyncio.create_task(_report_goodbye(identity, rid_for_goodbye))
        background_tasks.add(_t_gd)
        _t_gd.add_done_callback(background_tasks.discard)
    result = await call_personality_generate(
        role_ids=role_ids, user_message=user_message,
        memory_context=memory_context, chat_history=chat_history,
        session_id=session_id, intimacy_map=intimacy_map,
        goodbye_hint=goodbye_hint,
        enable_bystander=(mode == "single"),
        active_role_id=role_ids[0] if mode == "single" else None
    )
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
    # v13.0: 如果有主动推送的消息（话题延续等）还没纳入历史，先加进去，保证大模型知道自己刚才说了什么
    pending = pending_proactive_in_history.pop(identity, None)
    if pending and mode == "single":
        chat_history.append({"role": "assistant", "content": pending.get("content", "")})
    chat_history.append({"role": "user", "content": user_message})
    chat_history.append({"role": "assistant", "content": reply})
    if len(chat_history) > 20:
        del chat_history[:-20]
    # 后台写入会话片段到 Pinecone（用户消息 + AI回复各一条，7天自动过期）
    _sess_task1 = asyncio.create_task(call_vector_insert_session(
        identity, session_id or "", "user", user_message))
    background_tasks.add(_sess_task1)
    _sess_task1.add_done_callback(background_tasks.discard)
    _sess_task2 = asyncio.create_task(call_vector_insert_session(
        identity, session_id or "", "assistant", reply))
    background_tasks.add(_sess_task2)
    _sess_task2.add_done_callback(background_tasks.discard)
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
    # v4.0.4: 亲密度变化时通知在线管理员实时刷新
    if updated_intimacy:
        asyncio.create_task(_notify_admin_intimacy_update(identity, response_intimacy))
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
            identity, rid, response_intimacy.get(rid, 30), attachment, psych_snap,
            session_id=session_id))
        background_tasks.add(_t2)
        _t2.add_done_callback(background_tasks.discard)
        # v13.0: 通知话题延续引擎AI回复完成，传入最近对话用于生成延伸话题
        _t3 = asyncio.create_task(call_proactive_ai_replied(
            identity, rid, list(chat_history)))
        background_tasks.add(_t3)
        _t3.add_done_callback(background_tasks.discard)
    # 输出耗时汇总日志
    llm_used = result.get("used_llm_analysis", False)
    timer.log(f" | 回复长度={len(reply)} | LLM分析={'是' if llm_used else '否'}")
    return {
        "reply": reply,
        "role_ids": role_ids,
        "mode": mode,
        "intimacy": response_intimacy,
        "session_id": session_id,
        "used_llm_analysis": llm_used,
        "bystander_replies": result.get("bystander_replies") or []
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
        role_ids = ["nianqi"]
    if chat_history is None:
        chat_history = []
    mode = "group" if len(role_ids) > 1 else "single"
    mem_role_id = role_ids[0] if len(role_ids) == 1 else "group:" + "+".join(sorted(role_ids))
    timer = StepTimer(f"{identity}|voice|{mode}|{'+'.join(role_ids)}")
    # 用户主动发言，标记主动消息已回复
    _t = asyncio.create_task(call_proactive_mark_replied(identity))
    background_tasks.add(_t)
    _t.add_done_callback(background_tasks.discard)
    # v13.0: 单聊模式下，通知话题延续引擎用户发言了（重置状态）
    if mode == "single":
        _ts = asyncio.create_task(call_proactive_user_spoke(identity, role_ids[0]))
        background_tasks.add(_ts)
        _ts.add_done_callback(background_tasks.discard)
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

    # v13.1: ASR完成后用识别文本上报用户发言，供主动后端检测终止对话意图（晚安等）
    if mode == "single" and asr_text:
        _ts_intent = asyncio.create_task(call_proactive_user_spoke(identity, role_ids[0], asr_text))
        background_tasks.add(_ts_intent)
        _ts_intent.add_done_callback(background_tasks.discard)
    reply = result.get("reply", "") or ""
    audio_out = result.get("audio_base64", "") or ""
    audio_out_fmt = result.get("audio_format", "mp3")
    if result.get("session_id") and result["session_id"] != session_id:
        session_id = result["session_id"]
        user_db.set_session(identity, session_id)
    if not result.get("success") and not reply:
        reply = "抱歉，我没听清你说什么..."
    # v4.0.4: 语音消息亲密度变化时通知在线管理员实时刷新
    if result.get("success") and intimacy_map:
        asyncio.create_task(_notify_admin_intimacy_update(identity, intimacy_map))
    # 更新聊天历史（用ASR识别出的文本）
    if asr_text:
        # v13.0: 如果有主动推送的消息还没纳入历史，先加进去
        pending = pending_proactive_in_history.pop(identity, None)
        if pending and mode == "single":
            chat_history.append({"role": "assistant", "content": pending.get("content", "")})
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
            identity, rid, intimacy_map.get(rid, 30), None, {},
            session_id=session_id))
        background_tasks.add(_t2)
        _t2.add_done_callback(background_tasks.discard)
        # v13.0: 通知话题延续引擎AI回复完成
        _t3 = asyncio.create_task(call_proactive_ai_replied(
            identity, rid, list(chat_history)))
        background_tasks.add(_t3)
        _t3.add_done_callback(background_tasks.discard)
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
def extract_onebot_voice_segment(message) -> Optional[dict]:
    """从 OneBot v11 message 数组中提取语音段(record)的完整 data"""
    if isinstance(message, list):
        for seg in message:
            if isinstance(seg, dict) and seg.get("type") == "record":
                return seg.get("data", {})
    return None
def extract_onebot_voice_url(message) -> Optional[str]:
    """从 OneBot v11 message 数组中提取语音段(record)的下载 URL（保留兼容）"""
    seg = extract_onebot_voice_segment(message)
    if seg:
        url = seg.get("url", "")
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
async def _download_voice_via_napcat(record_data: dict) -> Optional[bytes]:
    """
    当 webhook 上报的语音 url 不是完整 http 链接时，
    调用 NapCat /get_record 接口，通过 file_id 获取语音二进制。
    适用于 NapCat 新版本QQ偏移不全、webhook只返回文件名的场景。
    """
    if not NAPCAT_HTTP_URL:
        logger.error("[QQ] NAPCAT_HTTP_URL 未配置，无法通过NapCat获取语音")
        return None
    file_id = record_data.get("file_id") or record_data.get("file")
    if not file_id:
        logger.error(f"[QQ] 语音消息缺少file_id: {record_data}")
        return None
    try:
        headers = {}
        if NAPCAT_ACCESS_TOKEN:
            headers["Authorization"] = f"Bearer {NAPCAT_ACCESS_TOKEN}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            # 指定 out_format=wav：wav 编码器(pcm_s16le)是 ffmpeg 内置的，避免 amr 编码器缺失导致"未找到编码器"
            # 后端下载后自行确认/转换为 16kHz mono 给 ASR
            resp = await client.post(
                f"{NAPCAT_HTTP_URL.rstrip('/')}/get_record",
                json={"file_id": file_id, "out_format": "wav"},
                headers=headers,
                timeout=30.0)
            raw_text = resp.text
            logger.info(f"[QQ] get_record HTTP{resp.status_code} 原始响应: {raw_text[:300]}")
            if resp.status_code != 200:
                logger.error(f"[QQ] get_record HTTP{resp.status_code}: {raw_text[:200]}")
                return None
            try:
                data = resp.json()
            except Exception as je:
                logger.error(f"[QQ] get_record 响应不是合法JSON: {type(je).__name__}: {je} 原始内容={raw_text[:200]}")
                return None
            # 检查 NapCat 业务状态
            if isinstance(data, dict) and data.get("status") == "failed":
                err_msg = data.get("message") or data.get("wording") or "未知错误"
                logger.error(f"[QQ] get_record 业务失败: {err_msg} (NapCat服务器可能缺少对应编码器，建议安装ffmpeg full版)")
                return None
            # 安全获取 data 字段（兼容返回 null / 非dict 的情况）
            resp_data = data.get("data") if isinstance(data, dict) else None
            if not isinstance(resp_data, dict):
                resp_data = data if isinstance(data, dict) else {}
            # NapCat get_record 返回的 data.url 是完整http链接，再下载一次
            # 兼容多种字段名：url / file / file_url
            real_url = (resp_data.get("url") or resp_data.get("file")
                        or resp_data.get("file_url") or "")
            if real_url and real_url.startswith(("http://", "https://")):
                logger.info(f"[QQ] get_record 解析到语音url: {real_url[:80]}")
                raw_bytes = await _download_file(real_url)
                if raw_bytes:
                    # 下载到原始格式后，统一转成 wav(16kHz mono) 给 ASR
                    wav_bytes = await _ffmpeg_convert(
                        raw_bytes, "wav", sample_rate=16000, channels=1)
                    if wav_bytes:
                        return wav_bytes
                    logger.warning("[QQ] 语音转wav失败，尝试直接返回原始字节")
                    return raw_bytes
                return None
            # v4.0.4 修复：NapCat 返回的是本地文件路径（如 C:\Users\...\xxx.wav），
            # 通过 NapCat 自身的 HTTP 文件服务接口 /file?path= 下载
            if real_url and (":\\" in real_url or real_url.startswith("/") or "\\" in real_url):
                import urllib.parse
                encoded_path = urllib.parse.quote(real_url)
                # NapCat HTTP 文件服务接口（需在 NapCat 配置中启用 http.enableFile）
                file_download_url = f"{NAPCAT_HTTP_URL.rstrip('/')}/file?path={encoded_path}"
                logger.info(f"[QQ] get_record 返回本地路径，尝试通过NapCat文件服务下载: {real_url[:80]}")
                try:
                    dl_headers = {}
                    if NAPCAT_ACCESS_TOKEN:
                        dl_headers["Authorization"] = f"Bearer {NAPCAT_ACCESS_TOKEN}"
                    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as dl_client:
                        dl_resp = await dl_client.get(file_download_url, headers=dl_headers)
                    if dl_resp.status_code == 200 and len(dl_resp.content) > 100:
                        logger.info(f"[QQ] 通过NapCat文件服务下载成功: {len(dl_resp.content)}字节")
                        wav_bytes = await _ffmpeg_convert(
                            dl_resp.content, "wav", sample_rate=16000, channels=1)
                        if wav_bytes:
                            return wav_bytes
                        logger.warning("[QQ] 语音转wav失败，尝试直接返回原始字节")
                        return dl_resp.content
                    else:
                        logger.error(f"[QQ] NapCat文件服务下载失败 HTTP{dl_resp.status_code}: {dl_resp.text[:200]}")
                        logger.error("[QQ] 请确认 NapCat 配置中已启用 http.enableFile=true")
                except Exception as dl_e:
                    logger.error(f"[QQ] 通过NapCat文件服务下载异常: {dl_e}")
            # 有些版本直接返回 file 字段是本地路径，不可用
            logger.error(f"[QQ] get_record 返回无有效url: data={resp_data} 完整响应={raw_text[:200]}")
    except Exception as e:
        err_detail = repr(e) if not str(e) else str(e)
        logger.error(f"[QQ] 通过NapCat获取语音失败: type={type(e).__name__} error={err_detail}", exc_info=True)
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
        headers = {}
        if NAPCAT_ACCESS_TOKEN:
            headers["Authorization"] = f"Bearer {NAPCAT_ACCESS_TOKEN}"
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{NAPCAT_HTTP_URL.rstrip('/')}/send_private_msg",
                json={"user_id": int(qq_number), "message": text},
                headers=headers,
                timeout=15.0)
            _log_port_timing("NapCat", "/send_private_msg", time.perf_counter() - t0,
                              f"HTTP{resp.status_code}")
            if resp.status_code == 200:
                return True
            logger.error(f"[QQ] NapCat 返回非200: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        dur = time.perf_counter() - t0
        _log_port_timing("NapCat", "/send_private_msg", dur, f"ERROR:{type(e).__name__}:{e}")
        # 改进错误日志：输出异常类型+详情+repr，避免某些异常 str() 为空导致日志只有冒号
        err_detail = repr(e) if not str(e) else str(e)
        logger.error(f"NapCat 发送消息失败: type={type(e).__name__} qq={qq_number} "
                     f"url={NAPCAT_HTTP_URL.rstrip('/')} error={err_detail}")
        # 常见异常的额外诊断信息
        if isinstance(e, httpx.ConnectError):
            logger.error(f"  → 连接失败：NapCat HTTP服务未启动/地址错误/防火墙拦截，NAPCAT_HTTP_URL={NAPCAT_HTTP_URL}")
        elif isinstance(e, httpx.TimeoutException):
            logger.error(f"  → 超时：NapCat响应慢或卡死，超时10s。注意：超时不代表消息未发送，NapCat可能已收到请求并发送消息，因此按发送成功处理以避免重复发送。")
            # v13.0修复：超时情况下NapCat大概率已经收到请求并发送了消息，
            # 如果返回False会把回复放在响应体中导致NapCat重复发送，因此返回True。
            return True
        elif isinstance(e, httpx.RemoteProtocolError):
            logger.error(f"  → 协议错误：NapCat返回了非法HTTP响应，可能是服务异常")
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
        headers = {}
        if NAPCAT_ACCESS_TOKEN:
            headers["Authorization"] = f"Bearer {NAPCAT_ACCESS_TOKEN}"
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{NAPCAT_HTTP_URL.rstrip('/')}/send_private_msg",
                json={
                    "user_id": int(qq_number),
                    "message": [{"type": "record", "data": {"file": file_field}}]
                },
                headers=headers,
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
    logger.info("主后端 v4.0.3 启动 - 端口 8000")
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
                        intim = {"nianqi": 30}
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
app = FastAPI(title="FlexiChrono 主后端", version="4.0.3", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                   allow_methods=["*"], allow_headers=["*"])
@app.get("/")
async def root():
    return {"status": "ok", "service": "FlexiChrono 主后端", "version": "4.0.3"}
@app.get("/health")
async def health():
    return {"status": "ok", "service": "main_server", "version": "4.0.3", "port": str(PORT)}
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


# -------------------------- P3：第一层亲密度管理（userdb.json） --------------------------
class SetIntimacyRequest(BaseModel):
    username: str
    role_id: str
    value: int  # 0-100

@app.post("/api/admin/set_intimacy")
async def admin_set_intimacy(req: SetIntimacyRequest, request: Request,
                             x_internal_token: str = Header(None)):
    """P3 新增：管理员设置第一层亲密度（userdb.json 的基础好感度）。
    第二层亲密度（proactive_server 当下状态）在 admin 面板的心理状态编辑里设置。
    自拍判断时取两层平均值。
    """
    # 鉴权：内部 token 或 管理员登录态（二选一）
    is_internal = (x_internal_token == INTERNAL_TOKEN)
    is_admin = False
    if not is_internal:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        admin = user_db.authenticate_token(token)
        is_admin = bool(admin and admin.get("is_admin"))
    if not is_internal and not is_admin:
        raise HTTPException(status_code=403, detail="需要内部令牌或管理员权限")
    
    value = max(0, min(100, int(req.value)))
    success = user_db.set_intimacy(req.username, req.role_id, value)
    if success:
        logger.info(f"[亲密度] admin设置第一层亲密度: {req.username}/{req.role_id} = {value}")
        return {"success": True, "username": req.username, "role_id": req.role_id, "intimacy": value}
    else:
        raise HTTPException(status_code=404, detail=f"用户 {req.username} 不存在")


@app.get("/api/admin/get_intimacy/{username}")
async def admin_get_intimacy(username: str, request: Request):
    """P3 新增：获取用户第一层亲密度（所有角色）"""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    admin = user_db.authenticate_token(token)
    if not admin or not admin["is_admin"]:
        raise HTTPException(status_code=403, detail="无权限")
    
    # 用 get_all_intimacy 代替不存在的 get_user
    intimacy = user_db.get_all_intimacy(username)
    if not intimacy:
        # 检查用户是否存在
        all_users = user_db.get_all_users()
        if not any(u.get("username") == username for u in all_users):
            raise HTTPException(status_code=404, detail=f"用户 {username} 不存在")
    
    return {
        "success": True,
        "username": username,
        "intimacy": intimacy,
        "note": "这是第一层基础亲密度；第二层当下亲密度请查 proactive_server /api/status/{user_id}"
    }


class SetProactiveIntimacyRequest(BaseModel):
    user_id: str
    role_id: str
    intimacy: int  # 0-100

@app.post("/api/admin/set_proactive_intimacy")
async def admin_set_proactive_intimacy(req: SetProactiveIntimacyRequest, request: Request):
    """P3 新增：设置第二层亲密度（proactive_server 的当下亲密度）。
    通过调用 proactive_server /api/activity/report 实现。
    """
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    admin = user_db.authenticate_token(token)
    if not admin or not admin["is_admin"]:
        raise HTTPException(status_code=403, detail="无权限")
    
    value = max(0, min(100, int(req.intimacy)))
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.post(
                f"{PROACTIVE_SERVER_URL}/api/activity/report",
                json={
                    "user_id": req.user_id,
                    "role_id": req.role_id,
                    "intimacy": value,
                    "psych": {},
                    "session_id": None,
                },
                timeout=8.0,
            )
            if resp.status_code == 200:
                logger.info(f"[亲密度] admin设置第二层亲密度: {req.user_id}/{req.role_id} = {value}")
                return {"success": True, "user_id": req.user_id, "role_id": req.role_id, "intimacy": value}
            else:
                raise HTTPException(status_code=500, detail=f"proactive_server 返回 HTTP {resp.status_code}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"设置第二层亲密度失败: {e}")


@app.get("/api/admin/get_both_intimacy/{username}")
async def admin_get_both_intimacy(username: str, request: Request):
    """P3 新增：获取用户两层亲密度（第一层+第二层+平均值），供admin面板展示。"""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    admin = user_db.authenticate_token(token)
    if not admin or not admin["is_admin"]:
        raise HTTPException(status_code=403, detail="无权限")
    
    # 第一层（用 get_all_intimacy 代替不存在的 get_user）
    layer1 = user_db.get_all_intimacy(username)
    
    # 第二层（从 proactive_server 读取）
    layer2 = {}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{PROACTIVE_SERVER_URL}/api/status/{username}")
            if resp.status_code == 200:
                data = resp.json()
                # 兼容两种格式：dict（从 roles 字段读取）或 list（直接读取）
                roles = data.get("roles", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for role_status in roles:
                    rid = role_status.get("role_id")
                    ival = role_status.get("intimacy")
                    if rid and ival is not None:
                        layer2[rid] = int(ival)
    except Exception as e:
        logger.debug(f"[亲密度] 从proactive_server读取第二层失败: {e}")
    
    # 计算平均值
    all_roles = set(list(layer1.keys()) + list(layer2.keys()))
    avg = {}
    for role in all_roles:
        v1 = layer1.get(role, 0)
        v2 = layer2.get(role, 0)
        valid = [v for v in [v1, v2] if v > 0]
        avg[role] = int(sum(valid) / len(valid)) if valid else 0
    
    return {
        "success": True,
        "username": username,
        "layer1_basic": layer1,
        "layer2_realtime": layer2,
        "average": avg,
        "note": "第一层=基础亲密度(userdb)，第二层=当下亲密度(proactive)，自拍判断取平均值"
    }


# -------------------------- P1 图像生成/自拍 --------------------------
@app.post("/api/image/selfie")
async def generate_selfie(request: Request):
    """生成角色自拍（转发到人格服务器）"""
    try:
        body = await request.json()
        async with httpx.AsyncClient(timeout=90.0) as client:
            r = await client.post(
                f"{PERSONALITY_SERVER_URL}/api/image/selfie",
                json=body,
                timeout=90.0,
            )
            if r.status_code == 200:
                return r.json()
            return {"allowed": False, "message": "图像生成服务异常", "error": f"HTTP {r.status_code}"}
    except Exception as e:
        logger.error(f"[ImageAPI] 转发自拍请求失败: {e}")
        return {"allowed": False, "message": "图像生成失败", "error": str(e)}


@app.get("/api/image/status")
async def image_status():
    """获取图像生成系统状态"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{PERSONALITY_SERVER_URL}/api/image/status")
            if r.status_code == 200:
                return r.json()
    except Exception:
        pass
    return {"error": "图像生成服务不可用"}


@app.post("/api/image/selfie_from_message")
async def generate_selfie_from_message(request: Request):
    """P2 新增：从用户原始消息直接生成自拍（自动检测模式/场景/服装）"""
    try:
        body = await request.json()
        async with httpx.AsyncClient(timeout=90.0) as client:
            r = await client.post(
                f"{PERSONALITY_SERVER_URL}/api/image/selfie_from_message",
                json=body,
                timeout=90.0,
            )
            if r.status_code == 200:
                return r.json()
            return {"is_selfie_request": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        logger.error(f"[ImageAPI] 转发自拍请求失败: {e}")
        return {"is_selfie_request": False, "error": str(e)}
# -------------------------- P3 群聊消息处理（多角色调度） --------------------------
async def handle_group_message(body: dict) -> dict:
    """
    P3 新增：处理群聊消息，支持多角色调度。
    借鉴 NoneBot2 的事件处理设计，用 group_router 做角色匹配和调度。

    流程：
    1. group_router 解析消息，判断是否应该回复、哪些角色回复
    2. 对每个角色：构建用户身份 → 图片理解（如有）→ 调用人格后端 → 发送到群聊
    3. 返回处理结果
    """
    try:
        from core.napcat import (
            get_group_router, parse_message, process_image_message,
            send_group_text, is_image_message,
        )
        from core.napcat.group_router import GroupMessageContext

        router = get_group_router()

        # 1. 解析群聊消息
        context = router.parse_group_message(body)
        if context is None:
            return {"status": "not_group"}

        group_id = context.group_id
        user_id = context.user_id

        # 2. 判断是否应该回复
        should_reply, role_ids = router.should_reply(context)
        if not should_reply or not role_ids:
            return {"status": "ignored", "reason": "no_trigger"}

        logger.info(
            f"[QQ群聊] 群{group_id} 用户{user_id} 触发角色{role_ids} "
            f"文本='{context.text_content[:50]}'"
        )

        # 3. 构建用户身份（群聊用户数据隔离）
        identity = router.build_user_identity(context)

        # 4. 图片理解（如果消息包含图片）
        image_description = ""
        if is_image_message(context.message):
            logger.info(f"[QQ群聊] 消息包含图片，开始理解...")
            image_desc, _ = await process_image_message(
                context.message,
                additional_prompt="请以角色的视角描述这张图片，注意图片中的细节和情感。"
            )
            if image_desc:
                image_description = image_desc
                logger.info(f"[QQ群聊] 图片理解完成: {image_description[:80]}...")

        # 5. 对每个角色生成回复并发送
        results = []
        for role_id in role_ids:
            try:
                # 构建发送给人格后端的消息
                user_message = context.text_content
                if image_description:
                    user_message = f"{user_message}\n\n[用户发送了图片，图片内容描述：{image_description}]"

                if not user_message.strip():
                    user_message = "（用户只发了图片或表情）"

                # 调用人格后端生成回复
                reply_text = await _call_personality_for_qq(
                    identity=identity,
                    role_id=role_id,
                    message=user_message,
                    mode="group",
                )

                if not reply_text:
                    logger.warning(f"[QQ群聊] 角色{role_id} 生成回复为空")
                    continue

                # 发送到群聊（@用户）
                sent = await send_group_text(
                    group_id=group_id,
                    text=reply_text,
                    at_qq=user_id,
                )

                results.append({
                    "role_id": role_id,
                    "reply": reply_text[:100],
                    "sent": sent,
                })

                logger.info(
                    f"[QQ群聊] 角色{role_id} 回复: '{reply_text[:50]}...' 发送={'成功' if sent else '失败'}"
                )

            except Exception as e:
                logger.error(f"[QQ群聊] 角色{role_id} 处理失败: {e}", exc_info=True)
                results.append({"role_id": role_id, "error": str(e)})

        return {
            "status": "processed",
            "group_id": group_id,
            "user_id": user_id,
            "roles": role_ids,
            "results": results,
        }

    except Exception as e:
        logger.error(f"[QQ群聊] 群聊消息处理异常: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


async def _call_personality_for_qq(
    identity: str,
    role_id: str,
    message: str,
    mode: str = "single",
) -> str:
    """
    调用人格后端生成回复（供QQ私聊/群聊共用）。
    从现有 process_qq_message 中提取的核心逻辑。
    注意：契约必须与人格后端 GenerateRequest 一致（role_ids/user_message），
    否则 Pydantic 校验直接 422。
    """
    try:
        payload = {
            "mode": mode,
            "role_ids": [role_id],
            "user_message": message,
            "chat_history": [],
            "return_debug": False,
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                f"{PERSONALITY_SERVER_URL}/api/generate",
                json=payload,
                timeout=60.0,
            )
        if r.status_code == 200:
            data = r.json()
            return data.get("reply", "") or data.get("response", "")
        else:
            logger.error(f"[Personality] HTTP {r.status_code}: {r.text[:200]}")
            return ""
    except Exception as e:
        logger.error(f"[Personality] 调用失败: {e}", exc_info=True)
        return ""


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
        # P3：群聊消息走多角色调度处理
        group_result = await handle_group_message(body)
        return group_result
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
        voice_seg = extract_onebot_voice_segment(body.get("message", ""))
        if not voice_seg:
            logger.warning(f"[QQ] 语音消息无record段 qq={qq_number}")
            reply_text = "语音消息获取失败，请发文字给我吧～"
            _sent = await send_qq_private_msg(qq_number, reply_text)
            return {"sent_via_api": _sent, **({"reply": reply_text} if not _sent else {})}
        voice_url = voice_seg.get("url", "")
        logger.info(f"[QQ] 收到语音消息 qq={qq_number}, url={voice_url[:80] if voice_url else '(空)'}")
        # 1. 下载 AMR 语音文件
        #    优先用完整http url直接下载；url残缺时走NapCat get_record接口
        if voice_url and voice_url.startswith(("http://", "https://")):
            amr_bytes = await _download_file(voice_url)
        else:
            logger.info(f"[QQ] 语音url非完整链接，改用NapCat get_record接口获取")
            amr_bytes = await _download_voice_via_napcat(voice_seg)
        if not amr_bytes:
            reply_text = "语音下载失败，请发文字给我吧～"
            _sent = await send_qq_private_msg(qq_number, reply_text)
            return {"sent_via_api": _sent, **({"reply": reply_text} if not _sent else {})}
        logger.info(f"[QQ] AMR下载完成: {len(amr_bytes)}B, 正在ffmpeg转WAV(16kHz mono)...")
        # 2. AMR → WAV (16kHz mono, ASR 输入格式)
        wav_bytes = await _ffmpeg_convert(amr_bytes, "wav", sample_rate=16000, channels=1)
        if not wav_bytes:
            reply_text = "语音转码失败（请确认服务器已安装ffmpeg），请发文字给我吧～"
            _sent = await send_qq_private_msg(qq_number, reply_text)
            return {"sent_via_api": _sent, **({"reply": reply_text} if not _sent else {})}
        audio_b64 = base64.b64encode(wav_bytes).decode()
        logger.info(f"[QQ] WAV转码完成: {len(wav_bytes)}B, 送语音后端处理...")
        # 3. 调用语音后端全流程 ASR→人格→TTS
        try:
            qq_voice_msg_id = str(body.get("message_id", "")) or None
            result = await identity_queue.submit(
                identity, qq_voice_msg_id,
                lambda: process_voice_message(
                    identity, audio_b64, ["jingwen"], history, audio_format="wav")
            )
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
        # HTTP API发送成功时不返回reply，避免NapCat重复发送；失败时用响应体兜底
        resp = {"sent_via_api": sent}
        if not sent:
            resp["reply"] = reply_text
        return resp
    # ---- 文本消息 ----
    text = extract_onebot_text(body.get("message", ""))
    if not text:
        return {"status": "empty"}
    logger.info(f"[QQ] 收到消息 qq={qq_number}: {text[:50]}")

    # ============================================================
    # v13.0: 自拍快速响应流程（提前检测，拒绝立即返回，同意先发文字再发图）
    # ============================================================
    try:
        from core.image_generator import is_selfie_request, get_selfie_system
        if is_selfie_request(text):
            logger.info(f"[QQ][自拍] 检测到自拍请求（快速响应模式）: {text[:50]}")
            user_intimacy = await _get_user_intimacy(identity, "nianqi")
            selfie_system = get_selfie_system()

            if not selfie_system.client.available:
                logger.warning("[QQ][自拍] 图像生成API不可用，降级到正常聊天流程")
            else:
                from core.image_generator import detect_selfie_mode, extract_scene, extract_clothing, extract_expression
                mode = detect_selfie_mode(text)
                scene = extract_scene(text) or "indoor"
                clothing = extract_clothing(text) or "casual"
                expression = extract_expression(text) or "gentle smile"

                # 第一步：快速判断同意/拒绝（不生成图片，毫秒级响应）
                is_allowed, selfie_msg = selfie_system.judger.judge("nianqi", user_intimacy)

                if not is_allowed:
                    # 拒绝：立即发送拒绝消息，直接返回（不走LLM聊天流程，用户秒收）
                    logger.info(f"[QQ][自拍] 拒绝（立即返回）: {selfie_msg[:50]}")
                    sent = await send_qq_private_msg(qq_number, selfie_msg)
                    resp = {"sent_via_api": sent}
                    if not sent:
                        resp["reply"] = selfie_msg
                    return resp

                # 同意：立即发送接受消息（用户秒收到文字回应）
                logger.info(f"[QQ][自拍] 同意，先发接受消息: {selfie_msg[:50]}")
                await send_qq_private_msg(qq_number, selfie_msg)

                # 第二步：生成图片（耗时操作，用户已收到文字回应，不会觉得卡）
                selfie_result = await selfie_system.handle_selfie_request(
                    user_id=identity, role_id="nianqi", intimacy=user_intimacy,
                    scene=scene, expression=expression, clothing=clothing, mode=mode,
                )

                if selfie_result.get("allowed") and selfie_result.get("image_url"):
                    # 图片生成成功，发送图片
                    from core.napcat import send_private_image
                    await send_private_image(qq_number, selfie_result["image_url"])
                    logger.info(f"[QQ][自拍] 图片发送成功: {selfie_result['image_url'][:60]}")
                else:
                    # 图片生成失败（第二次judge拒绝或生成失败），发送失败说明
                    fail_msg = selfie_result.get("message", "图片生成失败了，稍后再试好不好？")
                    await send_qq_private_msg(qq_number, fail_msg)
                    logger.info(f"[QQ][自拍] 图片生成失败: {fail_msg[:50]}")

                # 自拍流程完成，直接返回（不返回reply，避免NapCat重复发送）
                return {"sent_via_api": True}
    except Exception as selfie_e:
        logger.warning(f"[QQ][自拍] 快速响应异常，降级到正常聊天流程: {type(selfie_e).__name__}: {selfie_e}")

    # ============================================================
    # 正常聊天流程（非自拍请求，或自拍快速响应失败降级）
    # ============================================================
    try:
        qq_msg_id = str(body.get("message_id", "")) or None
        result = await identity_queue.submit(
            identity, qq_msg_id,
            lambda: process_chat_message(identity, text, ["nianqi"], history)
        )
        reply_text = result["reply"]
        # v13.0: 收集旁观者插话（其他角色旁听后主动插话）
        bystander_replies = result.get("bystander_replies") or []
    except Exception as e:
        logger.error(f"[QQ] 消息处理失败: {e}", exc_info=True)
        reply_text = "抱歉，我刚才走神了，能再说一遍吗？"
        bystander_replies = []

    # 发送消息
    sent = await send_qq_private_msg(qq_number, reply_text)

    # v13.0: 发送旁观者插话（每条以角色名开头，间隔发送）
    for br in bystander_replies:
        try:
            br_name = br.get("role_name") or br.get("role_id", "")
            br_content = br.get("content", "")
            if br_content:
                br_text = f"{br_name}：{br_content}"
                await asyncio.sleep(0.8)  # 模拟角色思考间隔，避免消息同时到达
                await send_qq_private_msg(qq_number, br_text)
                logger.info(f"[QQ] 旁观者插话: {br_name} - {br_content[:40]}")
        except Exception as br_e:
            logger.warning(f"[QQ] 旁观者插话发送失败: {br_e}")

    if not sent and NAPCAT_HTTP_URL:
        logger.warning(f"[QQ] NapCat API 发送失败，回复放在响应体中")
    total_dur = time.perf_counter() - webhook_t0
    logger.info(f"[QQ][总耗时] qq={qq_number} identity={identity} "
                f"回复长度={len(reply_text)} 发送={'成功' if sent else '失败/未配置'} "
                f"Webhook全流程={_fmt_ms(total_dur)}")
    # HTTP API发送成功时不返回reply，避免NapCat重复发送；失败时用响应体兜底
    resp = {"sent_via_api": sent}
    if not sent:
        resp["reply"] = reply_text
    return resp
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
def _extract_qq_number(user_id: str):
    """从 user_id 中提取 QQ 号码。支持 qq_tmp_{qq} 格式和已绑定正式账号的情况。"""
    if user_id.startswith("qq_tmp_"):
        qq = user_id[len("qq_tmp_"):]
        return qq if qq.isdigit() else None
    # 正式账号：查绑定表
    return user_db.get_qq_by_username(user_id)

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
    # v13.0: 缓存主动消息，用户下一次发消息时纳入 chat_history，保证大模型知道自己刚才说了什么
    pending_proactive_in_history[user_id] = {
        "role_id": role_id, "content": content, "timestamp": time.time()
    }

    delivered = False
    ws_sent = False
    qq_sent = False

    # 1. 尝试 WebSocket 推送给网页端在线用户
    ws = manager.active_connections.get(user_id)
    if ws:
        try:
            await ws.send_text(json.dumps(msg, ensure_ascii=False))
            ws_sent = True
            delivered = True
        except Exception as e:
            logger.warning(f"主动推送WS发送失败: {e}")

    # 2. 尝试通过 NapCat 发送 QQ 消息（核心修复：之前完全缺失这一步）
    qq_number = _extract_qq_number(user_id)
    if qq_number:
        try:
            # P3：主动发图（如果启用了且亲密度足够）
            image_url = ""
            if PROACTIVE_AUTO_IMAGE and role_id:
                try:
                    # P3 修复：查询用户亲密度（优先从proactive_server读取admin面板的数值）
                    user_intimacy = await _get_user_intimacy(user_id, role_id)

                    if user_intimacy >= PROACTIVE_IMAGE_INTIMACY_THRESHOLD:
                        logger.info(f"[主动发图] 亲密度{user_intimacy}≥阈值{PROACTIVE_IMAGE_INTIMACY_THRESHOLD}，开始生成自拍...")
                        from core.image_generator import get_selfie_system
                        selfie_system = get_selfie_system()
                        if selfie_system.client.available:
                            img_result = await selfie_system.generate_proactive_image(
                                user_id=user_id,
                                role_id=role_id,
                                intimacy=user_intimacy,
                            )
                            if img_result.get("allowed") and img_result.get("image_url"):
                                image_url = img_result["image_url"]
                                logger.info(f"[主动发图] 图片生成成功: {image_url[:60]}...")
                            else:
                                logger.warning(f"[主动发图] 图片生成失败: {img_result.get('error')}")
                        else:
                            logger.warning("[主动发图] 图像生成API不可用")
                    else:
                        logger.debug(f"[主动发图] 亲密度{user_intimacy}<阈值{PROACTIVE_IMAGE_INTIMACY_THRESHOLD}，跳过发图")
                except Exception as img_e:
                    logger.warning(f"[主动发图] 异常: {type(img_e).__name__}: {img_e}")

            # 发送消息（有图则图文一起发，无图则只发文本）
            if image_url:
                from core.napcat import send_private_image
                qq_sent = await send_private_image(qq_number, image_url, text=content)
            else:
                qq_sent = await send_qq_private_msg(qq_number, content)

            if qq_sent:
                delivered = True
                logger.info(f"[主动消息] QQ发送成功 user={user_id} qq={qq_number} role={role_id} content={content[:30]} image={'有' if image_url else '无'}")
            else:
                logger.warning(f"[主动消息] QQ发送失败 user={user_id} qq={qq_number} role={role_id}")
        except Exception as e:
            logger.error(f"[主动消息] QQ发送异常 user={user_id} qq={qq_number}: {type(e).__name__}: {e}", exc_info=True)
    else:
        logger.debug(f"[主动消息] 用户 {user_id} 未绑定QQ，跳过QQ发送")

    # 3. 如果都没成功，存入 pending 等待网页端上线拉取
    if not delivered:
        manager.pending_messages.setdefault(user_id, []).append(msg)

    return {"delivered": delivered, "ws_sent": ws_sent, "qq_sent": qq_sent}
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
        is_admin = bool(user_info.get("is_admin", False))
        await manager.connect(username, websocket, is_admin=is_admin)
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
                    "type": "roles_updated", "role_ids": msg.get("role_ids", ["nianqi"])}))
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
                    role_ids = msg.get("role_ids", ["nianqi"])
                    audio_fmt = msg.get("audio_format", "wav")
                    if not audio_b64:
                        await websocket.send_text(json.dumps(
                            {"type": "error", "message": "未收到语音数据"}, ensure_ascii=False))
                        continue
                    if not role_ids:
                        role_ids = ["nianqi"]
                    logger.info(f"[{username}] 语音对话: roles={role_ids}, 音频base64长度={len(audio_b64)}")
                    ws_voice_msg_id = msg.get("message_id") or f"ws_{username}_{int(time.time()*1000)}_{id(msg)}"
                    result = await identity_queue.submit(
                        username, ws_voice_msg_id,
                        lambda: process_voice_message(
                            username, audio_b64, role_ids, chat_history, audio_fmt)
                    )
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
                role_ids = msg.get("role_ids", ["nianqi"])
                if not user_message:
                    continue
                if not role_ids:
                    role_ids = ["nianqi"]
                logger.info(f"[{username}] 对话: roles={role_ids}")
                ws_msg_id = msg.get("message_id") or f"ws_{username}_{int(time.time()*1000)}_{id(msg)}"

                # ============================================================
                # v13.0: WebSocket自拍快速响应流程（网页端发自拍请求时）
                # ============================================================
                try:
                    from core.image_generator import is_selfie_request, get_selfie_system, detect_selfie_mode, extract_scene, extract_clothing, extract_expression
                    if is_selfie_request(user_message):
                        logger.info(f"[{username}][自拍] 检测到自拍请求（WebSocket快速响应）: {user_message[:50]}")
                        selfie_role = role_ids[0] if role_ids else "nianqi"
                        user_intimacy = await _get_user_intimacy(username, selfie_role)
                        selfie_system = get_selfie_system()

                        if selfie_system.client.available:
                            mode = detect_selfie_mode(user_message)
                            scene = extract_scene(user_message) or "indoor"
                            clothing = extract_clothing(user_message) or "casual"
                            expression = extract_expression(user_message) or "gentle smile"

                            # 第一步：快速判断同意/拒绝（毫秒级响应）
                            is_allowed, selfie_msg = selfie_system.judger.judge(selfie_role, user_intimacy)

                            if not is_allowed:
                                # 拒绝：立即发送拒绝消息，跳过正常聊天流程
                                logger.info(f"[{username}][自拍] 拒绝（立即返回）: {selfie_msg[:50]}")
                                await websocket.send_text(json.dumps({
                                    "type": "reply", "content": selfie_msg,
                                    "role_ids": [selfie_role], "mode": "single",
                                    "intimacy": {}, "session_id": "",
                                }, ensure_ascii=False))
                                continue

                            # 同意：立即发送接受消息（用户秒收到文字回应）
                            logger.info(f"[{username}][自拍] 同意，先发接受消息: {selfie_msg[:50]}")
                            await websocket.send_text(json.dumps({
                                "type": "reply", "content": selfie_msg,
                                "role_ids": [selfie_role], "mode": "single",
                                "intimacy": {}, "session_id": "",
                            }, ensure_ascii=False))

                            # 第二步：生成图片（耗时操作，用户已收到文字回应）
                            selfie_result = await selfie_system.handle_selfie_request(
                                user_id=username, role_id=selfie_role, intimacy=user_intimacy,
                                scene=scene, expression=expression, clothing=clothing, mode=mode,
                            )

                            if selfie_result.get("allowed") and selfie_result.get("image_url"):
                                # 图片生成成功，发送图片消息
                                await websocket.send_text(json.dumps({
                                    "type": "reply", "content": "",
                                    "role_ids": [selfie_role], "mode": "single",
                                    "intimacy": {}, "session_id": "",
                                    "imageUrl": selfie_result["image_url"],
                                }, ensure_ascii=False))
                                logger.info(f"[{username}][自拍] 图片发送成功: {selfie_result['image_url'][:60]}")
                            else:
                                # 图片生成失败，发送失败说明
                                fail_msg = selfie_result.get("message", "图片生成失败了，稍后再试好不好？")
                                await websocket.send_text(json.dumps({
                                    "type": "reply", "content": fail_msg,
                                    "role_ids": [selfie_role], "mode": "single",
                                    "intimacy": {}, "session_id": "",
                                }, ensure_ascii=False))
                                logger.info(f"[{username}][自拍] 图片生成失败: {fail_msg[:50]}")
                            continue  # 自拍流程完成，跳过正常聊天流程
                except Exception as selfie_e:
                    logger.warning(f"[{username}][自拍] WebSocket快速响应异常，降级到正常聊天流程: {type(selfie_e).__name__}: {selfie_e}")

                # ============================================================
                # 正常聊天流程（非自拍请求，或自拍快速响应失败降级）
                # ============================================================
                result = await identity_queue.submit(
                    username, ws_msg_id,
                    lambda: process_chat_message(username, user_message, role_ids, chat_history)
                )
                session_id = result["session_id"]
                await websocket.send_text(json.dumps({
                    "type": "reply", "content": result["reply"],
                    "role_ids": result["role_ids"], "mode": result["mode"],
                    "intimacy": result["intimacy"], "session_id": session_id,
                    "used_llm_analysis": result["used_llm_analysis"]
                }, ensure_ascii=False))
                # v13.0: 发送旁观者插话（其他角色旁听后主动插话）
                bystander_replies = result.get("bystander_replies") or []
                for br in bystander_replies:
                    try:
                        await websocket.send_text(json.dumps({
                            "type": "bystander_reply",
                            "role_id": br.get("role_id"),
                            "role_name": br.get("role_name"),
                            "content": br.get("content"),
                            "emotion": br.get("emotion"),
                            "emotion_intensity": br.get("emotion_intensity"),
                            "probability": br.get("probability"),
                            "reason": br.get("reason"),
                            "relationship_clue": br.get("relationship_clue"),
                            "session_id": session_id,
                        }, ensure_ascii=False))
                        logger.info(f"[{username}] 旁观者插话: {br.get('role_name')} - {br.get('content','')[:40]}")
                    except Exception as be:
                        logger.warning(f"发送旁观者插话失败: {be}")
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
# -------------------------- MC (Minecraft) 外接模块 --------------------------
# 协议：JSON over WebSocket，连接地址 ws://你的域名/mc/ws
#
# 客户端(Minecraft插件/mod) → 服务端：
#   认证: {"type":"auth","server_id":"生存服1","api_key":"可选"}
#   聊天: {"type":"chat","player_uuid":"xxx","player_name":"玩家名","message":"你好"}
#   事件: {"type":"event","event":"join|quit|death|achievement","player_uuid":"xxx","player_name":"xxx","data":{}}
#   命令: {"type":"command","player_uuid":"xxx","command":"role|status|reset|help","args":["jingwen"]}
#   心跳: {"type":"ping"}
#
# 服务端 → 客户端：
#   认证: {"type":"auth_ok"} / {"type":"auth_fail","reason":"xxx"}
#   回复: {"type":"reply","player_uuid":"xxx","message":"AI回复","role":"jingwen"}
#   错误: {"type":"error","message":"xxx"}
#   心跳: {"type":"pong"}

# ---- MC 配置(环境变量) ----
MC_API_KEY = os.getenv("MC_API_KEY", "")              # 留空则不校验 API Key
MC_DEFAULT_ROLE = os.getenv("MC_DEFAULT_ROLE", "nianqi")
MC_MAX_REPLY_LENGTH = int(os.getenv("MC_MAX_REPLY_LENGTH", "120"))  # Minecraft聊天框长度限制
MC_ENABLE_JOIN_GREET = os.getenv("MC_ENABLE_JOIN_GREET", "true").lower() == "true"

# ---- MC 运行时状态 ----
mc_connections: Dict[str, Dict] = {}          # server_id -> {"ws": ws, "authed": bool}
mc_player_identity: Dict[str, str] = {}       # player_uuid -> identity
mc_chat_history: Dict[str, List[Dict]] = {}   # identity -> chat history
mc_player_role: Dict[str, str] = {}            # player_uuid -> 当前角色(覆盖默认)
MC_KNOWN_ROLES = {"nianqi", "qinghe", "jingwen"}  # 可切换角色列表

# ---- MC / Pet 子应用实例（必须在装饰器使用前定义）----
mc_app = FastAPI(title="FlexiChrono MC 外接模块", version="1.0")
pet_app = FastAPI(title="FlexiChrono Pet 模块", version="0.1")

@mc_app.websocket("/ws")
async def mc_ws(websocket: WebSocket):
    """Minecraft 外接 WebSocket 入口"""
    await websocket.accept()
    server_id = None
    try:
        # ---- 认证阶段(10秒超时) ----
        auth_raw = await asyncio.wait_for(websocket.receive_text(), timeout=10.0)
        auth_msg = json.loads(auth_raw)
        if auth_msg.get("type") != "auth":
            await websocket.send_json({"type": "auth_fail", "reason": "第一条消息必须是 auth"})
            await websocket.close(code=4001)
            return
        server_id = auth_msg.get("server_id", "default")
        if MC_API_KEY and auth_msg.get("api_key") != MC_API_KEY:
            await websocket.send_json({"type": "auth_fail", "reason": "API Key 错误"})
            await websocket.close(code=4002)
            return
        mc_connections[server_id] = {"ws": websocket, "authed": True}
        await websocket.send_json({"type": "auth_ok", "server_id": server_id, "default_role": MC_DEFAULT_ROLE})
        logger.info(f"[MC] 服务器 [{server_id}] 已连接")

        # ---- 消息循环 ----
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            msg_type = msg.get("type", "")

            if msg_type == "chat":
                await _mc_handle_chat(websocket, msg)
            elif msg_type == "event":
                await _mc_handle_event(websocket, msg)
            elif msg_type == "command":
                await _mc_handle_command(websocket, msg)
            elif msg_type == "ping":
                await websocket.send_json({"type": "pong"})
            else:
                await websocket.send_json({"type": "error", "message": f"未知消息类型: {msg_type}"})

    except asyncio.TimeoutError:
        try:
            await websocket.send_json({"type": "auth_fail", "reason": "认证超时(10秒)"})
        except Exception:
            pass
    except WebSocketDisconnect:
        logger.info(f"[MC] 服务器 [{server_id}] 断开连接")
    except Exception as e:
        logger.error(f"[MC] WebSocket 错误: {e}", exc_info=True)
    finally:
        if server_id and server_id in mc_connections:
            del mc_connections[server_id]
        try:
            await websocket.close()
        except Exception:
            pass


def _mc_get_identity(player_uuid: str, player_name: str) -> str:
    """获取或创建玩家对应的 identity(临时身份，可后续绑定正式账号)"""
    if player_uuid in mc_player_identity:
        return mc_player_identity[player_uuid]
    identity = f"mc_tmp_{player_uuid}"
    user_db.ensure_tmp_user(identity)
    mc_player_identity[player_uuid] = identity
    logger.info(f"[MC] 玩家 {player_name}({player_uuid}) 创建临时身份 {identity}")
    return identity


async def _mc_handle_chat(websocket, msg):
    """处理玩家聊天消息 → 调AI人格引擎 → 发回游戏"""
    player_uuid = msg.get("player_uuid", "")
    player_name = msg.get("player_name", player_uuid)
    message = msg.get("message", "").strip()

    if not player_uuid or not message:
        await websocket.send_json({"type": "error", "message": "缺少 player_uuid 或 message"})
        return

    identity = _mc_get_identity(player_uuid, player_name)
    history = mc_chat_history.setdefault(identity, [])
    role_id = mc_player_role.get(player_uuid, MC_DEFAULT_ROLE)

    # 走 Identity Queue 串行处理(保证同一玩家消息不乱序)
    msg_id = f"mc_{player_uuid}_{int(time.time() * 1000)}_{id(msg)}"
    try:
        result = await identity_queue.submit(
            identity, msg_id,
            lambda rid=role_id: process_chat_message(identity, message, [rid], history)
        )
    except Exception as e:
        logger.error(f"[MC] 消息处理失败 player={player_name}: {e}", exc_info=True)
        await websocket.send_json({
            "type": "reply", "player_uuid": player_uuid,
            "message": "抱歉，我刚才走神了，能再说一遍吗？", "role": role_id
        })
        return

    reply = result.get("reply", "") or "……"
    # Minecraft 聊天框长度限制，超长截断
    if len(reply) > MC_MAX_REPLY_LENGTH:
        reply = reply[:MC_MAX_REPLY_LENGTH] + "…"

    await websocket.send_json({
        "type": "reply",
        "player_uuid": player_uuid,
        "player_name": player_name,
        "message": reply,
        "role": role_id,
        "intimacy": result.get("intimacy", {}),
    })


async def _mc_handle_event(websocket, msg):
    """处理游戏事件(玩家加入/离开/死亡/成就等)"""
    event = msg.get("event", "")
    player_uuid = msg.get("player_uuid", "")
    player_name = msg.get("player_name", "")

    if event == "join":
        logger.info(f"[MC] 玩家 {player_name}({player_uuid}) 加入游戏")
        if MC_ENABLE_JOIN_GREET:
            role_id = mc_player_role.get(player_uuid, MC_DEFAULT_ROLE)
            greetings = {
                "nianqi": "你回来啦～我一直在等你呢。",
                "qinghe": "欢迎回来～",
                "jingwen": "哼，你来了啊。",
            }
            await websocket.send_json({
                "type": "reply", "player_uuid": player_uuid,
                "message": greetings.get(role_id, "你好。"), "role": role_id
            })
    elif event == "quit":
        logger.info(f"[MC] 玩家 {player_name}({player_uuid}) 离开游戏")
        mc_player_role.pop(player_uuid, None)  # 清理角色选择，身份和历史保留
    elif event == "death":
        logger.info(f"[MC] 玩家 {player_name} 死亡")
    elif event == "achievement":
        logger.info(f"[MC] 玩家 {player_name} 获得成就: {msg.get('data', {}).get('name', '')}")
    # 更多事件(挖矿、击杀、PVP等)可在此扩展


async def _mc_handle_command(websocket, msg):
    """处理玩家触发的 AI 命令(游戏内输入 /ai role xxx 等)"""
    player_uuid = msg.get("player_uuid", "")
    player_name = msg.get("player_name", "")
    command = msg.get("command", "").lower()
    args = msg.get("args", []) or []

    async def send_reply(text):
        await websocket.send_json({
            "type": "reply", "player_uuid": player_uuid, "message": text,
            "role": mc_player_role.get(player_uuid, MC_DEFAULT_ROLE)
        })

    if command == "role":
        if not args:
            current = mc_player_role.get(player_uuid, MC_DEFAULT_ROLE)
            await send_reply(f"当前角色: {current}。可用: {', '.join(sorted(MC_KNOWN_ROLES))}")
            return
        new_role = args[0].lower()
        if new_role not in MC_KNOWN_ROLES:
            await send_reply(f"未知角色: {new_role}。可用: {', '.join(sorted(MC_KNOWN_ROLES))}")
            return
        mc_player_role[player_uuid] = new_role
        await send_reply(f"角色已切换为 {new_role}")
    elif command == "status":
        identity = _mc_get_identity(player_uuid, player_name)
        role_id = mc_player_role.get(player_uuid, MC_DEFAULT_ROLE)
        await send_reply(f"AI助手运行中 | 身份: {identity} | 角色: {role_id}")
    elif command == "reset":
        identity = mc_player_identity.get(player_uuid)
        if identity:
            mc_chat_history.pop(identity, None)
            user_db.set_session(identity, None)
        await send_reply("对话记忆已重置")
    elif command == "help":
        await send_reply("命令: role [角色] 切换 | status 状态 | reset 重置记忆 | help 帮助")
    else:
        await send_reply(f"未知命令: {command}。输入 /ai help 查看帮助")


@pet_app.websocket("/ws")
async def pet_ws(ws):
    await ws.close(code=4000, reason="Pet 模块尚未实现")

app.mount("/mc", mc_app)
app.mount("/pet", pet_app)


# ============================================================
# P3 模块2：管理后台 API（用户管理、角色配置、数据统计、日志、备份）
# ============================================================

def _require_admin(request: Request):
    """验证管理员权限，兼容两种凭证：
    1. 静态 ADMIN_TOKEN（admin.html 面板使用）
    2. 管理员账号登录后的 base64 token（index.html 网页端使用）
    """
    try:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return None, JSONResponse({"error": "unauthorized"}, status_code=401)
        # 方式1：静态 ADMIN_TOKEN
        admin_token = os.getenv("ADMIN_TOKEN", "flexichrono_admin_2026")
        if hmac.compare_digest(token, admin_token):
            return {"username": "admin", "is_admin": True}, None
        # 方式2：管理员账号的 base64 登录 token
        user = user_db.authenticate_token(token)
        if user and user.get("is_admin"):
            return user, None
        return None, JSONResponse({"error": "forbidden"}, status_code=403)
    except Exception:
        return None, JSONResponse({"error": "unauthorized"}, status_code=401)


# ---------- 用户管理 ----------

@app.get("/api/admin/users")
async def admin_list_users(request: Request, page: int = 1, page_size: int = 50):
    """列出所有用户（分页）。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        users = user_db.get_all_users()
        total = len(users)
        start = (page - 1) * page_size
        end = start + page_size
        page_users = users[start:end]
        # 简化用户信息（不返回密码哈希）；同时包含 index.html（qq/is_admin）
        # 与 admin.html（qq_bound/is_qq_tmp/disabled）两个前端需要的字段
        simplified = []
        for u in page_users:
            simplified.append({
                "username": u.get("username"),
                "nickname": u.get("nickname", ""),
                "is_admin": u.get("is_admin", False),
                "qq": u.get("qq"),
                "created_at": u.get("created_at", ""),
                "is_qq_tmp": u.get("is_qq_tmp", False),
                "qq_bound": u.get("qq") is not None,
                "intimacy": u.get("intimacy", {}),
                "disabled": u.get("disabled", False),
            })
        return {"total": total, "page": page, "page_size": page_size, "users": simplified}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/admin/users/{username}/intimacy")
async def admin_update_intimacy(request: Request, username: str):
    """修改用户亲密度。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        body = await request.json()
        role_id = body.get("role_id")
        value = int(body.get("value", 0))
        if not role_id:
            return JSONResponse({"error": "role_id required"}, status_code=400)
        user_db.set_intimacy(username, role_id, value)
        return {"success": True, "username": username, "role_id": role_id, "intimacy": value}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/admin/users/{username}/password")
async def admin_reset_password(request: Request, username: str):
    """重置用户密码。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        body = await request.json()
        new_password = body.get("password", "")
        if not new_password:
            return JSONResponse({"error": "password required"}, status_code=400)
        success = user_db.reset_password(username, new_password)
        if success:
            return {"success": True, "username": username}
        return JSONResponse({"error": "user not found"}, status_code=404)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/admin/users/{username}/disable")
async def admin_toggle_disable(request: Request, username: str):
    """禁用/启用用户。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        body = await request.json()
        disabled = body.get("disabled", True)
        user_db.set_user_field(username, "disabled", disabled)
        return {"success": True, "username": username, "disabled": disabled}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------- 角色配置 ----------

@app.get("/api/admin/characters")
async def admin_list_characters(request: Request):
    """列出所有角色。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        from core.characters.loader import get_character_loader
        loader = get_character_loader()
        characters = loader.list_characters()
        return {"characters": characters}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/admin/characters/{role_id}")
async def admin_get_character(request: Request, role_id: str):
    """获取角色配置详情。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        from core.characters.loader import get_character_loader
        loader = get_character_loader()
        char = loader.get_character(role_id)
        if not char:
            return JSONResponse({"error": "character not found"}, status_code=404)
        return {"role_id": role_id, "config": char.to_dict()}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------- 数据统计 ----------

@app.get("/api/admin/stats")
async def admin_get_stats(request: Request):
    """获取数据统计（用户数、对话数、亲密度分布、角色活跃度）。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        users = user_db.get_all_users()
        total_users = len(users)
        qq_users = sum(1 for u in users if u.get("is_qq_tmp"))
        bound_users = sum(1 for u in users if u.get("qq_bound"))

        # 亲密度分布
        intimacy_dist = {"0-20": 0, "21-40": 0, "41-60": 0, "61-80": 0, "81-100": 0}
        role_intimacy = {}
        for u in users:
            for role_id, val in u.get("intimacy", {}).items():
                role_intimacy[role_id] = role_intimacy.get(role_id, 0) + 1
                if val <= 20:
                    intimacy_dist["0-20"] += 1
                elif val <= 40:
                    intimacy_dist["21-40"] += 1
                elif val <= 60:
                    intimacy_dist["41-60"] += 1
                elif val <= 80:
                    intimacy_dist["61-80"] += 1
                else:
                    intimacy_dist["81-100"] += 1

        # 服务状态
        services = {
            "main": "running",
            "napcat_available": bool(NAPCAT_HTTP_URL),
            "proactive_auto_image": PROACTIVE_AUTO_IMAGE,
        }

        return {
            "total_users": total_users,
            "qq_tmp_users": qq_users,
            "bound_users": bound_users,
            "intimacy_distribution": intimacy_dist,
            "role_user_count": role_intimacy,
            "services": services,
            "timestamp": time.time(),
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------- 备份管理 ----------

@app.get("/api/admin/backups")
async def admin_list_backups(request: Request):
    """列出所有备份。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        from core.backup import list_backups
        backups = list_backups()
        return {"backups": backups, "total": len(backups)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/admin/backups")
async def admin_create_backup(request: Request):
    """创建备份。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        body = await request.json()
        note = body.get("note", "manual")
        from core.backup import create_backup
        result = create_backup(note=note)
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/admin/backups/{backup_name}/restore")
async def admin_restore_backup(request: Request, backup_name: str):
    """恢复备份。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        body = await request.json()
        overwrite = body.get("overwrite", True)
        from core.backup import restore_backup
        result = restore_backup(backup_name, overwrite=overwrite)
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/admin/backups/{backup_name}")
async def admin_delete_backup(request: Request, backup_name: str):
    """删除备份。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        from core.backup import delete_backup
        result = delete_backup(backup_name)
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/admin/backups/{backup_name}/download")
async def admin_download_backup(request: Request, backup_name: str):
    """下载备份文件。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        from core.backup import get_backup_path
        path = get_backup_path(backup_name)
        if not path:
            return JSONResponse({"error": "backup not found"}, status_code=404)
        from fastapi.responses import FileResponse
        return FileResponse(path, filename=f"{backup_name}.zip", media_type="application/zip")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------- 系统状态 ----------

@app.get("/api/admin/status")
async def admin_system_status(request: Request):
    """获取系统状态（各服务可用性、配置概览）。"""
    _, err = _require_admin(request)
    if err:
        return err
    try:
        return {
            "services": {
                "main": "running",
                "napcat_configured": bool(NAPCAT_HTTP_URL),
                "proactive_auto_image": PROACTIVE_AUTO_IMAGE,
            },
            "config": {
                "data_dir": os.getenv("DATA_DIR", "."),
                "llm_fail_threshold": int(os.getenv("LLM_FAIL_THRESHOLD", "3")),
                "max_user_images_per_hour": int(os.getenv("MAX_USER_IMAGES_PER_HOUR", "5")),
                "max_proactive_images_per_day": int(os.getenv("MAX_PROACTIVE_IMAGES_PER_DAY", "2")),
            },
            "timestamp": time.time(),
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
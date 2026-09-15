"""
语音后端 v2.0 - 全双工语音通话服务
端口：8004

v1.0: 一次性 ASR→LLM→TTS（SiliconFlow SenseVoice + edge-tts）
v2.0: 全双工实时通话
  - ASR: 火山引擎豆包大模型语音识别（WebSocket，一次性 nostream + 实时双向流式）
  - TTS: 火山引擎流式 TTS（WebSocket 双向流式，边生成边播放）
  - 全双工: 用户说话时可打断 AI，AI 说话时抑制回声
  - 对接: 人格后端 /api/generate（保持会话上下文）
  - 记忆: 通话结束后重要内容写入记忆后端

依赖：pip install fastapi uvicorn websockets httpx python-multipart
"""
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import shutil
import struct
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger("voice_server_v2")

# ============================================================
# 配置
# ============================================================
PORT = int(os.getenv("VOICE_PORT", "8004"))
PERSONALITY_SERVER_URL = os.getenv("PERSONALITY_SERVER_URL", "http://127.0.0.1:8002")
VECTOR_SERVER_URL = os.getenv("VECTOR_SERVER_URL", "http://127.0.0.1:8001")
VECTOR_API_TOKEN = os.getenv("VECTOR_API_TOKEN", "change_me_strong_secret_key_123456")

# ---- 阿里云 NLS 实时 ASR 配置（已弃用：ASR 已切换为火山豆包，以下仅保留以便回退）----
ALIYUN_NLS_ACCESS_KEY_ID = os.getenv("ALIYUN_NLS_ACCESS_KEY_ID", "")
ALIYUN_NLS_ACCESS_KEY_SECRET = os.getenv("ALIYUN_NLS_ACCESS_KEY_SECRET", "")
ALIYUN_NLS_APP_KEY = os.getenv("ALIYUN_NLS_APP_KEY", "")
ALIYUN_NLS_REGION = os.getenv("ALIYUN_NLS_REGION", "cn-shanghai")  # cn-shanghai / cn-beijing / cn-shenzhen

# ---- 火山引擎豆包语音 V3 双向流式 TTS 配置 ----
# 统一走 wss://openspeech.bytedance.com/api/v3/tts/bidirection
# 鉴权优先用新版 API Key（控制台>API Key），同时兼容旧版 APP_ID + Access Token
VOLCENGINE_TTS_APP_ID = os.getenv("VOLCENGINE_TTS_APP_ID", "")
VOLCENGINE_TTS_ACCESS_TOKEN = os.getenv("VOLCENGINE_TTS_ACCESS_TOKEN", "")
VOLCENGINE_TTS_API_KEY = os.getenv("VOLCENGINE_TTS_API_KEY", "")
VOLCENGINE_TTS_ENDPOINT = os.getenv(
    "VOLCENGINE_TTS_ENDPOINT", "wss://openspeech.bytedance.com/api/v3/tts/bidirection"
)
# 各代资源 ID（一般不用改，代码按音色代号自动选择）
VOLC_TTS_RES_2 = os.getenv("VOLCENGINE_TTS_RESOURCE_2", "seed-tts-2.0")
VOLC_TTS_RES_1 = os.getenv("VOLCENGINE_TTS_RESOURCE_1", "seed-tts-1.0")
VOLC_TTS_RES_ICL = os.getenv("VOLCENGINE_TTS_RESOURCE_ICL", "seed-icl-2.0")

# ---- 火山引擎豆包「大模型语音识别 ASR」配置（替代阿里云 NLS）----
# 凭证默认复用上面 TTS 的同一把火山 API Key（同一应用在控制台开通“语音识别”即可），
# 也可用 VOLCENGINE_ASR_* 单独覆盖。
VOLCENGINE_ASR_API_KEY = os.getenv("VOLCENGINE_ASR_API_KEY", "") or VOLCENGINE_TTS_API_KEY
VOLCENGINE_ASR_APP_ID = os.getenv("VOLCENGINE_ASR_APP_ID", "") or VOLCENGINE_TTS_APP_ID
VOLCENGINE_ASR_ACCESS_TOKEN = os.getenv("VOLCENGINE_ASR_ACCESS_TOKEN", "") or VOLCENGINE_TTS_ACCESS_TOKEN
# 1.0 小时版 volc.bigasr.sauc.duration（按量后付费）；2.0 为 volc.seedasr.sauc.duration
VOLCENGINE_ASR_RESOURCE = os.getenv("VOLCENGINE_ASR_RESOURCE", "volc.bigasr.sauc.duration")
VOLCENGINE_ASR_STREAM_ENDPOINT = os.getenv(
    "VOLCENGINE_ASR_STREAM_ENDPOINT", "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel"
)
VOLCENGINE_ASR_NOSTREAM_ENDPOINT = os.getenv(
    "VOLCENGINE_ASR_NOSTREAM_ENDPOINT", "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_nostream"
)

# ---- 角色音色映射 ----
# voice: 豆包音色 ID；speed: 语速倍率（1.0 正常，内部换算为 V3 的 speech_rate）
# tone: 仅 2.0 / 复刻2.0 音色支持的语气指令（context_texts），1.0 音色自动忽略
ROLE_VOICES = {
    "nianqi": {
        "voice": "ICL_uranus_zh_female_tianmeijiaoqiao_tob",  # 甜美娇俏（声音复刻2.0）
        "speed": 0.95, "name": "念琦",
        "tone": "用温柔清甜、带着亲近和一点撒娇的语气说，语速柔和",
    },
    "qinghe": {
        "voice": "zh_female_sophie_uranus_bigtts",            # Sophie（语音合成2.0）
        "speed": 1.0, "name": "清禾",
        "tone": "用表面淡淡的、其实嘴硬心软的语气说，克制但藏着在意",
    },
    "jingwen": {
        "voice": "zh_female_yuanqinvyou_moon_bigtts",         # 元气女友（语音合成1.0，moon 代）
        "speed": 1.05, "name": "璟雯",
        "tone": "",  # 1.0 音色不支持 context_texts，留空
    },
}
DEFAULT_VOICE = {
    "voice": os.getenv("VOLCENGINE_TTS_VOICE", "zh_female_sophie_uranus_bigtts"),
    "speed": 1.0, "name": "默认", "tone": "",
}


def volc_resource_for_voice(voice: str) -> str:
    """按音色代号判断所属资源（连接头 X-Api-Resource-Id）。

    - 真正的“声音复刻克隆音色”才走 seed-icl-2.0：S_ 开头，或克隆查询接口返回的
      icl_ ID（注意官方预置的 icl_uranus_ / icl_saturn_ 情感指令音色不是克隆音色）；
    - _moon_/_jupiter_ 老代号 → 语音合成1.0；
    - 其余（_uranus_/_saturn_ 预置、ICL_uranus_/ICL_saturn_ 指令音色、*_bigtts）→ 语音合成2.0。
    用错资源会报 55000000 “resource ID is mismatched with speaker related resource”。
    """
    v = (voice or "").lower()
    is_clone = v.startswith("s_") or (
        v.startswith("icl_")
        and not v.startswith("icl_uranus_")
        and not v.startswith("icl_saturn_")
    )
    if is_clone:
        return VOLC_TTS_RES_ICL
    if "_moon_" in v or "_jupiter_" in v:
        return VOLC_TTS_RES_1
    return VOLC_TTS_RES_2

# 音频配置
ASR_SAMPLE_RATE = 16000
REQUEST_TIMEOUT = 30.0
PERSONALITY_TIMEOUT = 60.0


# ============================================================
# 阿里云 NLS Token 获取（HMAC-SHA1 签名）
# ============================================================
def _aliyun_nls_meta_endpoint(region: str) -> str:
    return f"http://nls-meta.{region}.aliyuncs.com"


def _aliyun_nls_ws_endpoint(region: str) -> str:
    if region.endswith("-internal"):
        return f"wss://nls-gateway-{region}-internal.aliyuncs.com:80/ws/v1"
    return f"wss://nls-gateway-{region}.aliyuncs.com/ws/v1"


def _canonicalize_query(params: Dict[str, str]) -> str:
    return "&".join(
        f"{_percent_encode(k)}={_percent_encode(v)}"
        for k, v in sorted(params.items())
    )


def _percent_encode(s: str) -> str:
    # 阿里云签名的 URL 编码规则
    return (
        s.replace("+", "%20")
        .replace("*", "%2A")
        .replace("%7E", "~")
    )


async def get_aliyun_nls_token() -> Tuple[str, float]:
    """
    获取阿里云 NLS 临时 token。
    返回 (token, expires_at_timestamp)。
    token 有效期 24 小时，建议缓存复用。
    """
    if not ALIYUN_NLS_ACCESS_KEY_ID or not ALIYUN_NLS_ACCESS_KEY_SECRET:
        raise RuntimeError("阿里云 NLS 未配置 ACCESS_KEY_ID / ACCESS_KEY_SECRET")

    params = {
        "AccessKeyId": ALIYUN_NLS_ACCESS_KEY_ID,
        "Action": "CreateToken",
        "Format": "JSON",
        "RegionId": ALIYUN_NLS_REGION,
        "SignatureMethod": "HMAC-SHA1",
        "SignatureNonce": uuid.uuid4().hex,
        "SignatureVersion": "1.0",
        "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "Version": "2019-02-28",
    }
    canonical_query = _canonicalize_query(params)
    string_to_sign = f"POST&%2F&{_percent_encode(canonical_query)}"
    signature = base64.b64encode(
        hmac.new(
            f"{ALIYUN_NLS_ACCESS_KEY_SECRET}&".encode(),
            string_to_sign.encode(),
            hashlib.sha1,
        ).digest()
    ).decode()

    url = f"{_aliyun_nls_meta_endpoint(ALIYUN_NLS_REGION)}/?Signature={_percent_encode(signature)}&{canonical_query}"

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(url)
        data = resp.json()
        token_info = data.get("Token", {})
        token = token_info.get("Id")
        expire_time = token_info.get("ExpireTime", 0)
        if not token:
            raise RuntimeError(f"获取阿里云 NLS token 失败: {data}")
        logger.info(f"[阿里云NLS] 获取token成功，有效期至 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expire_time))}")
        return token, float(expire_time)


# Token 缓存
_token_cache: Dict[str, Any] = {"token": None, "expires_at": 0}
_token_lock = asyncio.Lock()


async def get_cached_aliyun_token() -> str:
    """获取缓存的阿里云 NLS token，过期前自动刷新。"""
    async with _token_lock:
        now = time.time()
        if _token_cache["token"] and _token_cache["expires_at"] - now > 300:
            return _token_cache["token"]
        token, expires_at = await get_aliyun_nls_token()
        _token_cache["token"] = token
        _token_cache["expires_at"] = expires_at
        return token


# ============================================================
# 火山引擎豆包「大模型语音识别 ASR」二进制协议（替代阿里云 NLS）
# 双向流式:  /api/v3/sauc/bigmodel        （实时通话，边说边出字）
# 流式输入:  /api/v3/sauc/bigmodel_nostream（一次性整段，发完负包出最终结果）
# 帧 = 4B 定长头(大端) [+4B sequence(按flags)] + 4B payload size(大端) + payload
# 不启用 gzip，payload 直接为 JSON 或裸 PCM，最稳。
# ============================================================
_ASR_MSG_FULL_CLIENT = 0b0001    # 端上：请求参数（JSON）
_ASR_MSG_AUDIO_CLIENT = 0b0010   # 端上：音频数据
_ASR_MSG_FULL_SERVER = 0b1001    # 服务端：识别结果
_ASR_MSG_ERROR = 0b1111          # 服务端：错误
_ASR_FLAG_LAST = 0b0010          # 最后一包（负包，不带 sequence）
_ASR_SER_JSON = 0b0001
_ASR_SER_RAW = 0b0000
_ASR_COMP_NONE = 0b0000


def _asr_configured() -> bool:
    """新版 X-Api-Key 或旧版 APP_ID+Access Token 任一可用即视为已配置。"""
    return bool(VOLCENGINE_ASR_API_KEY or (VOLCENGINE_ASR_APP_ID and VOLCENGINE_ASR_ACCESS_TOKEN))


def _asr_frame(msg_type: int, payload: bytes, flags: int = 0, serialization: int = _ASR_SER_RAW) -> bytes:
    """组装一个火山 ASR 二进制帧（不压缩、大端、不带 sequence）。"""
    header = bytes([
        0x11,  # protocol version=1，header size=1（实际 1*4=4 字节）
        ((msg_type & 0x0F) << 4) | (flags & 0x0F),
        ((serialization & 0x0F) << 4) | _ASR_COMP_NONE,
        0x00,
    ])
    return header + struct.pack(">I", len(payload)) + payload


def _asr_full_request(result_type: str = "full") -> bytes:
    """第一包 full client request：声明音频参数与识别选项。"""
    payload_obj = {
        "user": {"uid": "flexichrono"},
        "audio": {
            "format": "pcm", "codec": "raw",
            "rate": ASR_SAMPLE_RATE, "bits": 16, "channel": 1,
            "model_name": "bigmodel",
        },
        "request": {
            "model_name": "bigmodel",
            "enable_itn": True,      # 数字/日期等书面化
            "enable_punc": True,     # 自动标点
            "enable_ddc": False,
            "show_utterances": True, # 输出分句，便于实时上屏与定版触发
            "result_type": result_type,
            "end_window_size": 800,  # 静音 800ms 判停分句
        },
    }
    body = json.dumps(payload_obj, ensure_ascii=False).encode("utf-8")
    return _asr_frame(_ASR_MSG_FULL_CLIENT, body, serialization=_ASR_SER_JSON)


def _asr_audio_frame(pcm: bytes, last: bool = False) -> bytes:
    """音频包（裸 16k/16bit/mono PCM）；last=True 即最后一包（负包）。"""
    flags = _ASR_FLAG_LAST if last else 0b0000
    return _asr_frame(_ASR_MSG_AUDIO_CLIENT, pcm, flags=flags, serialization=_ASR_SER_RAW)


def _asr_handshake_headers() -> Dict[str, str]:
    """WebSocket 握手鉴权头：新版 X-Api-Key 优先，同时兼容旧版 APP_ID/Access Token。"""
    headers = {
        "X-Api-Resource-Id": VOLCENGINE_ASR_RESOURCE,
        "X-Api-Request-Id": str(uuid.uuid4()),
        "X-Api-Sequence": "-1",
    }
    if VOLCENGINE_ASR_API_KEY:
        headers["X-Api-Key"] = VOLCENGINE_ASR_API_KEY
    if VOLCENGINE_ASR_APP_ID:
        headers["X-Api-App-Key"] = VOLCENGINE_ASR_APP_ID
    if VOLCENGINE_ASR_ACCESS_TOKEN:
        headers["X-Api-Access-Key"] = VOLCENGINE_ASR_ACCESS_TOKEN
    return headers


def _asr_parse_frame(data: bytes) -> Tuple[int, int, Optional[Dict[str, Any]]]:
    """解析服务端帧 → (msg_type, flags, payload_json 或 None)。按 flags 处理可选 sequence。"""
    if not data or len(data) < 8:
        raise ValueError(f"火山ASR帧过短: {len(data) if data else 0} 字节")
    b1, b2 = data[1], data[2]
    msg_type = (b1 >> 4) & 0x0F
    flags = b1 & 0x0F
    serialization = (b2 >> 4) & 0x0F
    compression = b2 & 0x0F
    idx = 4
    if flags in (0b0001, 0b0011):  # 这两种 flags 下，header 后带 4 字节 sequence
        idx += 4
    (size,) = struct.unpack(">I", data[idx:idx + 4])
    idx += 4
    payload = data[idx:idx + size]
    if compression == 0b0001:
        import gzip
        payload = gzip.decompress(payload)
    # 只有 JSON 序列化的帧（服务端结果/错误）才解析；裸音频等返回 None
    if not payload or serialization != _ASR_SER_JSON:
        return msg_type, flags, None
    return msg_type, flags, json.loads(payload.decode("utf-8"))


def _asr_one_text(node: Any) -> str:
    if not isinstance(node, dict):
        return ""
    if node.get("text"):
        return node["text"]
    return "".join((u.get("text", "") or "") for u in (node.get("utterances") or []))


def _asr_extract_text(obj: Dict[str, Any]) -> str:
    """从一帧结果取整段文本，兼容 result 为 dict 或 list。"""
    result = obj.get("result")
    if isinstance(result, dict):
        return _asr_one_text(result)
    if isinstance(result, list):
        return "".join(_asr_one_text(r) for r in result)
    return ""


def _asr_split_utterances(obj: Dict[str, Any]) -> Tuple[str, List[str]]:
    """供实时通话：返回 (进行中未定版文本, 本次定版 definite 分句文本列表)。"""
    result = obj.get("result")
    if not isinstance(result, dict):
        return "", []
    utts = result.get("utterances") or []
    ongoing = "".join((u.get("text", "") or "") for u in utts if not u.get("definite"))
    finals = [t.strip() for u in utts for t in [(u.get("text", "") or "").strip()] if u.get("definite") and t]
    if not utts and result.get("text"):
        ongoing = result["text"]
    return ongoing, finals


# ============================================================
# 火山引擎豆包语音 V3 双向流式 TTS WebSocket 二进制协议
# 端点 /api/v3/tts/bidirection：4 字节定长头 + 可选事件/会话 + payload
# ============================================================
# 消息类型（byte1 高 4 位）
V3_MSG_FULL_CLIENT = 0b0001    # 完整客户端请求
V3_MSG_AUDIO_CLIENT = 0b0010   # 仅音频客户端请求
V3_MSG_FULL_SERVER = 0b1001    # 完整服务端响应（JSON 控制帧）
V3_MSG_AUDIO_SERVER = 0b1011   # 服务端音频帧
V3_MSG_ERROR = 0b1111          # 错误帧
# 标志位（byte1 低 4 位）
V3_FLAG_NO_SEQ = 0b0000
V3_FLAG_WITH_EVENT = 0b0100
# 序列化 / 压缩（byte2）
V3_SER_JSON = 0b0001
V3_COMP_NONE = 0b0000


# 事件类型
class V3Event:
    START_CONNECTION = 1
    FINISH_CONNECTION = 2
    CONNECTION_STARTED = 50
    CONNECTION_FAILED = 51
    CONNECTION_FINISHED = 52
    START_SESSION = 100
    CANCEL_SESSION = 101
    FINISH_SESSION = 102
    SESSION_STARTED = 150
    SESSION_CANCELED = 151
    SESSION_FINISHED = 152
    SESSION_FAILED = 153
    TASK_REQUEST = 200
    TTS_SENTENCE_START = 350
    TTS_SENTENCE_END = 351
    TTS_RESPONSE = 352
    TTS_ENDED = 359


# 不携带 session_id 的连接级事件
_V3_CONNECTION_EVENTS = {
    V3Event.START_CONNECTION, V3Event.FINISH_CONNECTION,
    V3Event.CONNECTION_STARTED, V3Event.CONNECTION_FAILED, V3Event.CONNECTION_FINISHED,
}


def _v3_build(msg_type: int, event: Optional[int] = None,
              session_id: str = "", payload: bytes = b"") -> bytes:
    """构建一个 V3 二进制帧（带事件的完整请求）。"""
    frame = bytearray()
    # byte0: 高 4 位 version=1，低 4 位 header_size=1（即 4 字节）
    frame.append((1 << 4) | 1)
    # byte1: 消息类型(高4位) + 标志(低4位)，有事件用 WithEvent
    flag = V3_FLAG_WITH_EVENT if event is not None else V3_FLAG_NO_SEQ
    frame.append((msg_type << 4) | flag)
    # byte2: JSON 序列化 + 不压缩；byte3 保留
    frame.append((V3_SER_JSON << 4) | V3_COMP_NONE)
    frame.append(0x00)

    if event is not None:
        frame.extend(struct.pack(">i", event))
        if event not in _V3_CONNECTION_EVENTS:
            sid = (session_id or "").encode("utf-8")
            frame.extend(struct.pack(">I", len(sid)))
            if sid:
                frame.extend(sid)

    frame.extend(struct.pack(">I", len(payload)))
    if payload:
        frame.extend(payload)
    return bytes(frame)


def _v3_parse(data: bytes) -> Dict:
    """解析服务端 V3 帧：{type,event,session_id,payload,error_code}。"""
    out = {"type": 0, "event": 0, "session_id": "", "payload": b"", "error_code": 0}
    if len(data) < 4:
        return out
    header_size = (data[0] & 0x0F) * 4
    out["type"] = data[1] >> 4
    flag = data[1] & 0x0F
    pos = header_size

    if flag == V3_FLAG_WITH_EVENT and len(data) >= pos + 4:
        out["event"] = struct.unpack(">i", data[pos:pos + 4])[0]
        pos += 4
        if out["event"] not in _V3_CONNECTION_EVENTS and len(data) >= pos + 4:
            sid_len = struct.unpack(">I", data[pos:pos + 4])[0]
            pos += 4
            if sid_len and len(data) >= pos + sid_len:
                out["session_id"] = data[pos:pos + sid_len].decode("utf-8", "replace")
                pos += sid_len

    if out["type"] == V3_MSG_ERROR and len(data) >= pos + 4:
        out["error_code"] = struct.unpack(">I", data[pos:pos + 4])[0]
        pos += 4

    if len(data) >= pos + 4:
        plen = struct.unpack(">I", data[pos:pos + 4])[0]
        pos += 4
        if plen:
            out["payload"] = data[pos:pos + plen]
    return out


def _speed_ratio_to_speech_rate(ratio: float) -> int:
    """语速倍率（1.0 正常）换算为 V3 speech_rate 整数，范围 [-50,100]。"""
    rate = round((float(ratio) - 1.0) * 100)
    return max(-50, min(100, rate))


# ============================================================
# 通话会话状态机
# ============================================================
class CallState:
    IDLE = "idle"           # 空闲
    LISTENING = "listening"  # 聆听用户说话
    THINKING = "thinking"    # AI 思考中（调人格后端）
    SPEAKING = "speaking"    # AI 说话中（TTS 播放中）


class CallSession:
    """
    单个通话会话的完整状态管理。
    负责协调：客户端 WS ↔ 火山豆包 ASR ↔ 人格后端 ↔ 火山引擎 TTS
    """

    def __init__(
        self,
        websocket: WebSocket,
        role_id: str,
        session_id: Optional[str],
        user_id: str,
    ):
        self.ws = websocket
        self.role_id = role_id
        self.session_id = session_id
        self.user_id = user_id
        self.state = CallState.IDLE

        # 通话文字记录
        self.transcript: List[Dict[str, str]] = []  # [{role, content, timestamp}]

        # ASR 相关
        self.asr_ws = None  # 火山 ASR WebSocket
        self.asr_task_id = None
        self.asr_ready = False
        self.asr_partial_text = ""  # 中间识别结果

        # TTS 相关
        self.tts_ws = None  # 火山引擎 WebSocket
        self.tts_ready = False
        self.tts_session_id = None
        self.tts_audio_queue: asyncio.Queue = asyncio.Queue()
        self.tts_playing = False

        # 控制
        self._interrupt_event = asyncio.Event()
        self._closed = False
        self._tasks: List[asyncio.Task] = []

        # 角色配置
        voice_cfg = ROLE_VOICES.get(role_id, DEFAULT_VOICE)
        self.voice = voice_cfg["voice"]
        self.speed = voice_cfg["speed"]
        self.role_name = voice_cfg["name"]
        # 一通电话共用一个 TTS section_id，让 2.0/复刻2.0 音色多句之间语气连贯
        self.tts_section_id = str(uuid.uuid4())

    # ---- 状态转换 ----
    async def set_state(self, new_state: str):
        """切换状态并通知前端。"""
        old = self.state
        self.state = new_state
        logger.info(f"[通话] 状态转换: {old} → {new_state}")
        await self._send_json({"type": "state", "state": new_state})

    # ---- 客户端消息收发 ----
    async def _send_json(self, data: Dict):
        """向客户端发送 JSON 消息。"""
        try:
            await self.ws.send_json(data)
        except Exception as e:
            logger.debug(f"[通话] 发送JSON失败: {e}")

    async def _send_audio(self, audio_bytes: bytes):
        """向客户端发送二进制音频帧。"""
        try:
            await self.ws.send_bytes(audio_bytes)
        except Exception as e:
            logger.debug(f"[通话] 发送音频失败: {e}")

    # ---- ASR: 火山引擎豆包大模型语音识别（双向流式）----
    async def start_asr(self):
        """连接火山 ASR 双向流式接口并启动实时识别。"""
        try:
            if not _asr_configured():
                raise RuntimeError("火山 ASR 未配置 API Key / APP 凭证")
            import websockets
            headers = _asr_handshake_headers()
            # 实时通话用双向流式，single 增量返回（每次只给新分句）
            self.asr_ws = await websockets.connect(
                VOLCENGINE_ASR_STREAM_ENDPOINT, additional_headers=headers,
                ping_interval=None, max_size=16 * 1024 * 1024,
            )
            # 第一包：请求参数；发完即可持续推音频
            await self.asr_ws.send(_asr_full_request(result_type="single"))
            self.asr_ready = True
            task = asyncio.create_task(self._asr_receive_loop())
            self._tasks.append(task)
            logger.info("[火山ASR] 双向流式识别已启动")
            await self.set_state(CallState.LISTENING)
            return True
        except Exception as e:
            logger.error(f"[火山ASR] 启动失败: {e}", exc_info=True)
            await self._send_json({"type": "error", "message": f"ASR启动失败: {e}"})
            return False

    async def _asr_receive_loop(self):
        """持续接收火山 ASR 识别结果（进行中上屏 + definite 定版触发回复）。"""
        try:
            async for message in self.asr_ws:
                if self._closed:
                    break
                if not isinstance(message, (bytes, bytearray)):
                    continue
                try:
                    msg_type, _flags, obj = _asr_parse_frame(bytes(message))
                except Exception as pe:
                    logger.debug(f"[火山ASR] 帧解析失败: {pe}")
                    continue

                if msg_type == _ASR_MSG_ERROR:
                    logger.error(f"[火山ASR] 错误帧: {obj}")
                    continue
                if msg_type != _ASR_MSG_FULL_SERVER or not obj:
                    continue

                ongoing, finals = _asr_split_utterances(obj)

                # 进行中文本：实时上屏；若 AI 正在说话则检测到用户开口 → 打断(barge-in)
                if ongoing:
                    if self.state == CallState.SPEAKING:
                        await self.interrupt_tts()
                    if self.state != CallState.LISTENING:
                        await self.set_state(CallState.LISTENING)
                    self.asr_partial_text = ongoing
                    await self._send_json({"type": "asr_partial", "text": ongoing})

                # definite 定版分句：作为最终识别结果并触发 AI 回复
                for text in finals:
                    logger.info(f"[火山ASR] 定版识别: {text}")
                    self.transcript.append({
                        "role": "user",
                        "content": text,
                        "timestamp": time.strftime("%H:%M:%S"),
                    })
                    await self._send_json({"type": "asr_final", "text": text})
                    self.asr_partial_text = ""
                    asyncio.create_task(self._generate_reply(text))

        except Exception as e:
            if not self._closed:
                logger.error(f"[火山ASR] 接收循环异常: {e}", exc_info=True)

    async def send_audio_to_asr(self, audio_bytes: bytes):
        """将前端采集的 16k PCM 帧用火山二进制音频帧包裹后转发。"""
        if self.asr_ws and self.asr_ready and not self._closed:
            try:
                await self.asr_ws.send(_asr_audio_frame(audio_bytes, last=False))
            except Exception as e:
                logger.debug(f"[火山ASR] 发送音频失败: {e}")

    # ---- 人格后端调用 ----
    async def _generate_reply(self, user_text: str):
        """调用人格后端生成 AI 回复，然后触发 TTS。"""
        if self._closed:
            return

        await self.set_state(CallState.THINKING)

        try:
            payload = {
                "mode": "single",
                "role_ids": [self.role_id],
                "user_message": user_text,
                "memory_context": "",
                "chat_history": [],
                "temperature": 0.9,
                "max_tokens": 500,
                "return_debug": False,
                "enable_memory_analysis": True,
            }
            if self.session_id:
                payload["session_id"] = self.session_id

            async with httpx.AsyncClient(timeout=PERSONALITY_TIMEOUT) as client:
                resp = await client.post(
                    f"{PERSONALITY_SERVER_URL}/api/generate",
                    json=payload,
                )

            if resp.status_code == 429:
                reply = "……你说话太快了，让我喘口气。"
            elif resp.status_code == 200:
                data = resp.json()
                reply = data.get("reply", "") or ""
                # 更新 session_id
                if data.get("session_id"):
                    self.session_id = data["session_id"]
            else:
                reply = "抱歉，我暂时无法回应..."

            if not reply:
                reply = "……"

            # 记录 AI 回复
            self.transcript.append({
                "role": "ai",
                "content": reply,
                "timestamp": time.strftime("%H:%M:%S"),
            })
            await self._send_json({"type": "ai_reply", "text": reply})

            # 触发 TTS 播放
            await self.speak(reply)

        except asyncio.TimeoutError:
            logger.warning("[人格后端] 请求超时")
            await self._send_json({"type": "error", "message": "AI响应超时"})
            await self.set_state(CallState.LISTENING)
        except Exception as e:
            logger.error(f"[人格后端] 调用失败: {e}", exc_info=True)
            await self._send_json({"type": "error", "message": f"AI响应失败: {e}"})
            await self.set_state(CallState.LISTENING)

    # ---- TTS: 火山引擎流式 ----
    async def speak(self, text: str):
        """
        使用豆包语音 V3 双向流式 TTS 合成并播放。
        建立连接→创建会话→发文本→边收音频边转发前端，支持随时打断。
        输出固定 16bit/单声道/24kHz PCM，前端播放端按此采样率解码。
        """
        if self._closed:
            return

        await self.set_state(CallState.SPEAKING)
        self._interrupt_event.clear()

        # 按音色自动选择资源（合成2.0 / 合成1.0 / 复刻2.0）
        resource_id = volc_resource_for_voice(self.voice)
        supports_instruction = resource_id in (VOLC_TTS_RES_2, VOLC_TTS_RES_ICL)
        speech_rate = _speed_ratio_to_speech_rate(self.speed)
        tts_session = str(uuid.uuid4())
        # 固定 PCM / 24k
        audio_params = {"format": "pcm", "sample_rate": 24000, "speech_rate": speech_rate}

        def _session_payload(event: int, with_text: bool = False) -> bytes:
            req_params: Dict = {"speaker": self.voice, "audio_params": dict(audio_params)}
            # 仅 2.0 / 复刻2.0 支持 context_texts 语气指令
            if supports_instruction:
                tone = ROLE_VOICES.get(self.role_id, {}).get("tone", "")
                if tone:
                    req_params["context_texts"] = [tone]
            if with_text:
                req_params["text"] = text
            body: Dict = {
                "user": {"uid": "airi_call"},
                "namespace": "BidirectionalTTS",
                "event": event,
                "req_params": req_params,
            }
            # section_id 同样仅 2.0 / 复刻2.0 支持，整通电话保持一致
            if supports_instruction:
                body["section_id"] = self.tts_section_id
            return json.dumps(body, ensure_ascii=False).encode("utf-8")

        try:
            import websockets

            # 鉴权头：优先新版 X-Api-Key，同时带旧版 APP_ID / Access Token 兜底
            headers = {
                "X-Api-Resource-Id": resource_id,
                "X-Api-Connect-Id": str(uuid.uuid4()),
            }
            if VOLCENGINE_TTS_API_KEY:
                headers["X-Api-Key"] = VOLCENGINE_TTS_API_KEY
            if VOLCENGINE_TTS_APP_ID:
                headers["X-Api-App-Id"] = VOLCENGINE_TTS_APP_ID
            if VOLCENGINE_TTS_ACCESS_TOKEN:
                headers["X-Api-Access-Key"] = VOLCENGINE_TTS_ACCESS_TOKEN

            self.tts_ws = await websockets.connect(
                VOLCENGINE_TTS_ENDPOINT,
                additional_headers=headers,
                ping_interval=20,
                ping_timeout=20,
                max_size=16 * 1024 * 1024,
            )
            logger.info(
                f"[豆包TTS] 连接 resource={resource_id} voice={self.voice} "
                f"speech_rate={speech_rate} text长度={len(text)}"
            )

            async def _send_event(event: int, with_text: bool = False, raw_payload: bytes = b""):
                if raw_payload:
                    payload = raw_payload
                else:
                    payload = _session_payload(event, with_text)
                await self.tts_ws.send(
                    _v3_build(V3_MSG_FULL_CLIENT, event=event,
                              session_id=tts_session, payload=payload)
                )

            async def _wait_event(target_events: set):
                """持续读到目标事件为止；遇到失败事件直接抛错。"""
                while True:
                    raw = await self.tts_ws.recv()
                    m = _v3_parse(raw)
                    if m["type"] == V3_MSG_ERROR or m["event"] in (
                        V3Event.CONNECTION_FAILED, V3Event.SESSION_FAILED
                    ):
                        detail = m["payload"].decode("utf-8", "replace")
                        raise RuntimeError(
                            f"豆包TTS失败 event={m['event']} code={m['error_code']} {detail}"
                        )
                    if m["event"] in target_events:
                        return m

            # 1) 建立连接 → ConnectionStarted
            await _send_event(V3Event.START_CONNECTION, raw_payload=b"{}")
            await _wait_event({V3Event.CONNECTION_STARTED})

            # 2) 创建会话 → SessionStarted
            await _send_event(V3Event.START_SESSION)
            await _wait_event({V3Event.SESSION_STARTED})

            # 3) 发送文本 TaskRequest，随后立即 FinishSession 告知没有后续文本
            await _send_event(V3Event.TASK_REQUEST, with_text=True)
            await _send_event(V3Event.FINISH_SESSION, raw_payload=b"{}")

            # 4) 接收音频流，直到 SessionFinished / TTSEnded / 取消
            session_finished = False
            async for message in self.tts_ws:
                if self._closed or self._interrupt_event.is_set():
                    break
                m = _v3_parse(message)
                if m["type"] == V3_MSG_AUDIO_SERVER and m["payload"]:
                    # 音频帧 → 直接转发前端播放
                    await self._send_audio(m["payload"])
                elif m["type"] == V3_MSG_FULL_SERVER:
                    ev = m["event"]
                    if ev in (V3Event.SESSION_FINISHED, V3Event.TTS_ENDED, V3Event.SESSION_CANCELED):
                        session_finished = (ev == V3Event.SESSION_FINISHED)
                        logger.info(f"[豆包TTS] 合成结束 event={ev}")
                        break
                    # 句子开始/结束、进度帧等忽略
                elif m["type"] == V3_MSG_ERROR:
                    detail = m["payload"].decode("utf-8", "replace")
                    raise RuntimeError(f"豆包TTS错误 code={m['error_code']} {detail}")

            # 播放完成（或被打断）
            if not self._interrupt_event.is_set() and session_finished:
                logger.info("[豆包TTS] 播放完成，回到聆听状态")
            else:
                logger.info("[豆包TTS] 播放被打断或提前结束")

        except Exception as e:
            logger.error(f"[豆包TTS] 合成失败: {e}", exc_info=True)
            await self._send_json({"type": "error", "message": f"TTS失败: {e}"})
        finally:
            # 通知服务端结束连接并关闭 TTS 连接
            if self.tts_ws:
                try:
                    await self.tts_ws.send(
                        _v3_build(V3_MSG_FULL_CLIENT, event=V3Event.FINISH_CONNECTION, payload=b"{}")
                    )
                except Exception:
                    pass
                try:
                    await self.tts_ws.close()
                except Exception:
                    pass
                self.tts_ws = None
                self.tts_ready = False
            # 回到聆听状态（如果没被关闭）
            if not self._closed and self.state == CallState.SPEAKING:
                await self.set_state(CallState.LISTENING)

    async def interrupt_tts(self):
        """打断当前 TTS 播放（用户说话时调用）。"""
        if self.state == CallState.SPEAKING:
            logger.info("[通话] 用户打断 AI 说话")
            self._interrupt_event.set()
            # 通知前端停止播放
            await self._send_json({"type": "tts_interrupt"})
            # 关闭 TTS 连接
            if self.tts_ws:
                try:
                    await self.tts_ws.close()
                except Exception:
                    pass
                self.tts_ws = None

    # ---- 通话结束 ----
    async def hangup(self):
        """挂断通话，清理资源，返回通话记录。"""
        if self._closed:
            return
        self._closed = True
        logger.info(f"[通话] 挂断，共 {len(self.transcript)} 条记录")

        # 取消所有任务
        for task in self._tasks:
            task.cancel()

        # 关闭 ASR（发最后一包负包后关闭连接）
        if self.asr_ws:
            try:
                await self.asr_ws.send(_asr_audio_frame(b"", last=True))
            except Exception:
                pass
            try:
                await self.asr_ws.close()
            except Exception:
                pass
            self.asr_ws = None

        # 关闭 TTS
        if self.tts_ws:
            try:
                await self.tts_ws.close()
            except Exception:
                pass
            self.tts_ws = None

        # 异步写入记忆后端（不阻塞挂断）
        asyncio.create_task(self._save_to_memory())

        # 发送通话记录给前端
        await self._send_json({
            "type": "call_ended",
            "transcript": self.transcript,
            "session_id": self.session_id,
        })

    async def _save_to_memory(self):
        """将通话中的重要内容写入记忆后端。"""
        if not self.transcript:
            return
        try:
            # 拼接通话文本
            conversation_text = "\n".join(
                f"{'用户' if item['role'] == 'user' else self.role_name}: {item['content']}"
                for item in self.transcript
            )
            # 提取用户说的话作为记忆内容
            user_messages = [item["content"] for item in self.transcript if item["role"] == "user"]
            if not user_messages:
                return

            # 调用记忆后端添加记忆
            async with httpx.AsyncClient(timeout=30.0) as client:
                # 逐条添加用户说的重要内容（简单策略：超过10个字的都存）
                for msg in user_messages:
                    if len(msg) >= 10:
                        await client.post(
                            f"{VECTOR_SERVER_URL}/api/memory/add_direct",
                            json={
                                "user_id": self.user_id,
                                "content": f"[语音通话] {msg}",
                                "memory_type": "episodic",
                                "importance": 60,
                                "reason": "语音通话内容",
                                "source": "voice_call",
                                "role_id": self.role_id,
                            },
                            headers={"X-Vector-Token": VECTOR_API_TOKEN},
                        )
            logger.info(f"[记忆] 通话内容已写入记忆后端，共 {len([m for m in user_messages if len(m)>=10])} 条")
        except Exception as e:
            logger.warning(f"[记忆] 写入记忆后端失败: {e}")


# ============================================================
# 一次性语音消息（网页“按住说话”）：解码 → ASR → 人格 → TTS(wav)
# 与实时通话 /ws/call 相互独立，不共享状态
# ============================================================
def _pcm_to_wav(pcm: bytes, sample_rate: int = 24000, channels: int = 1, bits: int = 16) -> bytes:
    """给裸 PCM（小端 16bit）加标准 WAV 头，浏览器 <audio> 可直接播放。"""
    import io
    byte_rate = sample_rate * channels * bits // 8
    block_align = channels * bits // 8
    buf = io.BytesIO()
    buf.write(b"RIFF")
    buf.write(struct.pack("<I", 36 + len(pcm)))
    buf.write(b"WAVE")
    buf.write(b"fmt ")
    buf.write(struct.pack("<I", 16))
    buf.write(struct.pack("<HHIIHH", 1, channels, sample_rate, byte_rate, block_align, bits))
    buf.write(b"data")
    buf.write(struct.pack("<I", len(pcm)))
    buf.write(pcm)
    return buf.getvalue()


async def _decode_to_pcm16(audio_b64: str, audio_format: str) -> bytes:
    """浏览器录音(webm/ogg/wav/m4a) → 16kHz/单声道/16bit 小端 PCM，供火山 ASR。"""
    raw = base64.b64decode(audio_b64)
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("服务器未安装 ffmpeg，无法解码语音")
    fmt = (audio_format or "webm").lower().replace("mpeg", "mp3")
    if fmt not in ("webm", "ogg", "wav", "mp3", "m4a", "mp4", "opus"):
        fmt = "webm"
    tmpdir = tempfile.mkdtemp(prefix="voice_once_")
    try:
        in_path = os.path.join(tmpdir, f"input.{fmt}")
        out_path = os.path.join(tmpdir, "out.pcm")
        with open(in_path, "wb") as f:
            f.write(raw)
        proc = await asyncio.create_subprocess_exec(
            ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", in_path,
            "-ar", str(ASR_SAMPLE_RATE), "-ac", "1", "-f", "s16le", "-acodec", "pcm_s16le",
            out_path,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg转码失败: {stderr[-300:].decode('utf-8', 'replace')}")
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


async def transcribe_once(pcm_bytes: bytes) -> str:
    """一次性整段识别（火山 bigmodel_nostream）：发完全部 PCM 与负包后，取最终文本。"""
    import websockets
    if not _asr_configured():
        raise RuntimeError("火山 ASR 未配置 API Key / APP 凭证")
    if not pcm_bytes:
        return ""

    headers = _asr_handshake_headers()
    final_text = ""

    async with websockets.connect(
        VOLCENGINE_ASR_NOSTREAM_ENDPOINT, additional_headers=headers,
        ping_interval=None, max_size=16 * 1024 * 1024,
    ) as ws:
        # 第一包：请求参数（整段识别用 full，每次返回全量结果）
        await ws.send(_asr_full_request(result_type="full"))

        # 按 100ms（16k/16bit/mono = 3200 字节）分片，最后一片打负包标记
        step = 3200
        total = len(pcm_bytes)
        pos = 0
        while pos < total:
            chunk = pcm_bytes[pos:pos + step]
            pos += step
            await ws.send(_asr_audio_frame(chunk, last=(pos >= total)))
            await asyncio.sleep(0.01)

        async def _collect():
            nonlocal final_text
            async for m in ws:
                if not isinstance(m, (bytes, bytearray)):
                    continue
                mt, flags, obj = _asr_parse_frame(bytes(m))
                if mt == _ASR_MSG_ERROR:
                    raise RuntimeError(f"火山ASR错误帧: {obj}")
                if mt == _ASR_MSG_FULL_SERVER and obj:
                    t = _asr_extract_text(obj)
                    if t:
                        final_text = t  # full 模式：后到的全量覆盖
                    if flags == 0b0011:  # 最后一包结果，定稿
                        return

        try:
            await asyncio.wait_for(_collect(), timeout=25.0)
        except asyncio.TimeoutError:
            logger.warning("[火山ASR] 一次性识别等待结果超时，返回已收到的文本")

    return final_text.strip()


async def _personality_once(role_id: str, user_text: str, session_id: Optional[str] = None,
                            chat_history: Optional[List[Dict]] = None,
                            intimacy_map: Optional[Dict] = None) -> Tuple[str, Optional[str]]:
    """调人格后端 /api/generate 做一次性生成，返回 (reply, session_id)。"""
    payload = {
        "mode": "single",
        "role_ids": [role_id],
        "user_message": user_text,
        "memory_context": "",
        "chat_history": chat_history or [],
        "temperature": 0.9,
        "max_tokens": 500,
        "return_debug": False,
        "enable_memory_analysis": True,
    }
    if session_id:
        payload["session_id"] = session_id
    if intimacy_map:
        payload["intimacy_map"] = intimacy_map
    async with httpx.AsyncClient(timeout=PERSONALITY_TIMEOUT) as client:
        resp = await client.post(f"{PERSONALITY_SERVER_URL}/api/generate", json=payload)
    if resp.status_code == 429:
        return "……你说得太快啦，让我喘口气。", session_id
    if resp.status_code != 200:
        raise RuntimeError(f"人格后端HTTP{resp.status_code}")
    d = resp.json()
    return (d.get("reply", "") or ""), (d.get("session_id") or session_id)


async def tts_once(text: str, role_id: str = "nianqi") -> bytes:
    """豆包语音 V3 双向流式一次性合成整段，收集为 24k/16bit/mono PCM。"""
    import websockets
    cfg = ROLE_VOICES.get(role_id, DEFAULT_VOICE)
    voice, speed = cfg["voice"], cfg["speed"]
    resource_id = volc_resource_for_voice(voice)
    supports_instruction = resource_id in (VOLC_TTS_RES_2, VOLC_TTS_RES_ICL)
    speech_rate = _speed_ratio_to_speech_rate(speed)
    tts_session = str(uuid.uuid4())
    section_id = str(uuid.uuid4())
    audio_params = {"format": "pcm", "sample_rate": 24000, "speech_rate": speech_rate}

    def _payload(event: int, with_text: bool = False) -> bytes:
        req_params: Dict[str, Any] = {"speaker": voice, "audio_params": dict(audio_params)}
        if supports_instruction:
            tone = cfg.get("tone", "")
            if tone:
                req_params["context_texts"] = [tone]
        if with_text:
            req_params["text"] = text
        body: Dict[str, Any] = {
            "user": {"uid": "airi_once"},
            "namespace": "BidirectionalTTS",
            "event": event,
            "req_params": req_params,
        }
        if supports_instruction:
            body["section_id"] = section_id
        return json.dumps(body, ensure_ascii=False).encode("utf-8")

    headers = {
        "X-Api-Resource-Id": resource_id,
        "X-Api-Connect-Id": str(uuid.uuid4()),
    }
    if VOLCENGINE_TTS_API_KEY:
        headers["X-Api-Key"] = VOLCENGINE_TTS_API_KEY
    if VOLCENGINE_TTS_APP_ID:
        headers["X-Api-App-Id"] = VOLCENGINE_TTS_APP_ID
    if VOLCENGINE_TTS_ACCESS_TOKEN:
        headers["X-Api-Access-Key"] = VOLCENGINE_TTS_ACCESS_TOKEN

    ws = await websockets.connect(
        VOLCENGINE_TTS_ENDPOINT, additional_headers=headers,
        ping_interval=20, ping_timeout=20, max_size=16 * 1024 * 1024,
    )
    pcm = bytearray()
    try:
        async def _send(event: int, with_text: bool = False, raw: bytes = b""):
            p = raw if raw else _payload(event, with_text)
            await ws.send(_v3_build(V3_MSG_FULL_CLIENT, event=event,
                                    session_id=tts_session, payload=p))

        async def _wait(targets: set):
            while True:
                m = _v3_parse(await ws.recv())
                if m["type"] == V3_MSG_ERROR or m["event"] in (
                    V3Event.CONNECTION_FAILED, V3Event.SESSION_FAILED
                ):
                    raise RuntimeError(
                        f"豆包TTS失败 event={m['event']} code={m['error_code']} "
                        f"{m['payload'].decode('utf-8', 'replace')}"
                    )
                if m["event"] in targets:
                    return m

        await _send(V3Event.START_CONNECTION, raw=b"{}")
        await _wait({V3Event.CONNECTION_STARTED})
        await _send(V3Event.START_SESSION)
        await _wait({V3Event.SESSION_STARTED})
        await _send(V3Event.TASK_REQUEST, with_text=True)
        await _send(V3Event.FINISH_SESSION, raw=b"{}")
        async for message in ws:
            m = _v3_parse(message)
            if m["type"] == V3_MSG_AUDIO_SERVER and m["payload"]:
                pcm.extend(m["payload"])
            elif m["type"] == V3_MSG_FULL_SERVER:
                if m["event"] in (V3Event.SESSION_FINISHED, V3Event.TTS_ENDED,
                                  V3Event.SESSION_CANCELED):
                    break
            elif m["type"] == V3_MSG_ERROR:
                raise RuntimeError(
                    f"豆包TTS错误 code={m['error_code']} {m['payload'].decode('utf-8', 'replace')}"
                )
        return bytes(pcm)
    finally:
        try:
            await ws.send(_v3_build(V3_MSG_FULL_CLIENT, event=V3Event.FINISH_CONNECTION,
                                    payload=b"{}"))
        except Exception:
            pass
        try:
            await ws.close()
        except Exception:
            pass


# ============================================================
# FastAPI 应用
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🎤 语音后端 v2.0 启动 - 端口 {PORT}")
    logger.info(f"  人格后端: {PERSONALITY_SERVER_URL}")
    logger.info(f"  记忆后端: {VECTOR_SERVER_URL}")
    logger.info(f"  ASR: 火山豆包大模型识别 ({VOLCENGINE_ASR_RESOURCE}) {'已配置' if _asr_configured() else '⚠️ 未配置火山凭证(默认复用TTS Key)!'}")
    _tts_ok = bool(VOLCENGINE_TTS_API_KEY or (VOLCENGINE_TTS_APP_ID and VOLCENGINE_TTS_ACCESS_TOKEN))
    logger.info(f"  TTS: 豆包语音V3双向流式 {'已配置' if _tts_ok else '⚠️ 未配置 API Key / APP凭证!'}")
    for _rid, _cfg in ROLE_VOICES.items():
        logger.info(
            f"    - {_cfg['name']}({_rid}): {_cfg['voice']} → {volc_resource_for_voice(_cfg['voice'])}"
        )
    yield
    logger.info("语音后端关闭")


app = FastAPI(title="Voice Server v2", version="2.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# 通话 WebSocket 端点
# ============================================================
@app.websocket("/ws/call")
async def websocket_call(websocket: WebSocket):
    """
    全双工语音通话 WebSocket 端点。

    客户端消息协议：
    - JSON: {"action": "start", "role_id": "nianqi", "session_id": "...", "user_id": "..."}
    - JSON: {"action": "interrupt"}  (打断 TTS)
    - JSON: {"action": "hangup"}     (挂断)
    - Binary: 16kHz PCM 音频帧 (用户说话时持续发送)

    服务端消息协议：
    - JSON: {"type": "state", "state": "listening|thinking|speaking"}
    - JSON: {"type": "asr_partial", "text": "..."}  (中间识别结果)
    - JSON: {"type": "asr_final", "text": "..."}    (最终识别结果)
    - JSON: {"type": "ai_reply", "text": "..."}      (AI 回复文本)
    - JSON: {"type": "tts_interrupt"}                 (TTS 被打断)
    - JSON: {"type": "call_ended", "transcript": [...]} (通话结束)
    - JSON: {"type": "error", "message": "..."}
    - Binary: PCM 音频帧 (AI 说话时持续发送)
    """
    await websocket.accept()
    session: Optional[CallSession] = None

    try:
        # 等待 start 消息
        first_msg = await websocket.receive_json()
        if first_msg.get("action") != "start":
            await websocket.send_json({"type": "error", "message": "第一条消息必须是 start"})
            await websocket.close()
            return

        role_id = first_msg.get("role_id", "nianqi")
        session_id = first_msg.get("session_id")
        user_id = first_msg.get("user_id", "guest")

        logger.info(f"[通话] 新通话连接: role={role_id}, user={user_id}, session={session_id}")

        # 创建通话会话
        session = CallSession(websocket, role_id, session_id, user_id)

        # 启动 ASR
        asr_ok = await session.start_asr()
        if not asr_ok:
            await websocket.send_json({"type": "error", "message": "ASR启动失败，请检查火山语音识别是否已开通及凭证配置"})
            await websocket.close()
            return

        # 主循环：接收客户端消息
        while True:
            message = await websocket.receive()

            if message["type"] == "websocket.disconnect":
                break

            if message["type"] == "websocket.receive":
                data = message.get("bytes") or message.get("text")

                if isinstance(data, bytes):
                    # 二进制音频帧 → 始终转发给 ASR。
                    # 关键：AI 说话(SPEAKING)时也必须持续喂音频，ASR 才可能检测到用户开口
                    # (SentenceBegin) 从而打断 TTS，实现 barge-in；若仅在 LISTENING/IDLE 转发，
                    # 说话期间 ASR 收不到声音，“开口打断”永远不会触发。回声由前端 echoCancellation 处理。
                    await session.send_audio_to_asr(data)

                elif isinstance(data, str):
                    try:
                        msg = json.loads(data)
                    except json.JSONDecodeError:
                        continue

                    action = msg.get("action", "")

                    if action == "interrupt":
                        await session.interrupt_tts()

                    elif action == "hangup":
                        await session.hangup()
                        break

                    elif action == "ping":
                        await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        logger.info("[通话] 客户端断开连接")
    except Exception as e:
        logger.error(f"[通话] WebSocket 异常: {e}", exc_info=True)
    finally:
        if session:
            await session.hangup()
        try:
            await websocket.close()
        except Exception:
            pass


# ============================================================
# 健康检查
# ============================================================
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "voice_server_v2",
        "version": "2.0.0",
        "port": str(PORT),
        "asr": {
            "provider": "volcengine_doubao_bigmodel",
            "resource": VOLCENGINE_ASR_RESOURCE,
            "stream_endpoint": VOLCENGINE_ASR_STREAM_ENDPOINT,
            "nostream_endpoint": VOLCENGINE_ASR_NOSTREAM_ENDPOINT,
            "configured": _asr_configured(),
        },
        "tts": {
            "provider": "volcengine_doubao_v3_bidirection",
            "voices": {
                k: {"voice": v["voice"], "resource": volc_resource_for_voice(v["voice"])}
                for k, v in ROLE_VOICES.items()
            },
            "configured": bool(
                VOLCENGINE_TTS_API_KEY or (VOLCENGINE_TTS_APP_ID and VOLCENGINE_TTS_ACCESS_TOKEN)
            ),
        },
        "personality_url": PERSONALITY_SERVER_URL,
        "memory_url": VECTOR_SERVER_URL,
    }


# ============================================================
# 一次性语音消息接口（网页“按住说话”：ASR→人格→TTS，返回文字+wav）
# ============================================================
@app.post("/api/voice/chat")
async def voice_chat_once(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"success": False, "error": "无效的JSON"}, status_code=400)

    audio_b64 = body.get("audio_base64", "")
    role_ids = body.get("role_ids") or ["nianqi"]
    role_id = role_ids[0] if role_ids else "nianqi"
    session_id = body.get("session_id")
    chat_history = body.get("chat_history") or []
    intimacy_map = body.get("intimacy_map")
    audio_format = (body.get("audio_format") or "webm").lower()

    if not audio_b64:
        return JSONResponse({"success": False, "error": "缺少audio_base64"}, status_code=400)

    def _empty(reply_text, err="", code=200):
        return JSONResponse({
            "success": False, "error": err, "asr_text": "", "reply": reply_text,
            "audio_base64": "", "audio_format": "wav", "session_id": session_id,
        }, status_code=code)

    # 1) 录音 → 16k PCM
    try:
        pcm16 = await _decode_to_pcm16(audio_b64, audio_format)
    except Exception as e:
        logger.error(f"[一次性语音] 解码失败: {e}")
        return _empty("录音解析失败，请重试", str(e))
    # 短于 0.2 秒视为没说话
    if len(pcm16) < ASR_SAMPLE_RATE * 2 * 0.2:
        return _empty("我没听清，能再说一遍吗？", "audio_too_short")

    # 2) 一次性 ASR
    try:
        asr_text = await transcribe_once(pcm16)
    except Exception as e:
        logger.error(f"[一次性语音] ASR失败: {e}", exc_info=True)
        return _empty("我没听清，能再说一遍吗？", str(e))
    logger.info(f"[一次性语音] role={role_id} ASR文本={asr_text[:40]}")
    if not asr_text:
        return _empty("我没听清，能再说一遍吗？", "empty_asr")

    # 3) 人格生成
    try:
        reply, new_session = await _personality_once(
            role_id, asr_text, session_id, chat_history, intimacy_map)
    except Exception as e:
        logger.error(f"[一次性语音] 人格生成失败: {e}", exc_info=True)
        return JSONResponse({
            "success": False, "error": str(e), "asr_text": asr_text,
            "reply": "我一时没反应过来，等一下再试试好吗？",
            "audio_base64": "", "audio_format": "wav", "session_id": session_id,
        })
    if not reply:
        reply = "……"

    # 4) TTS 整段合成 → wav（合成失败也保留文字回复）
    audio_out = ""
    try:
        pcm24 = await tts_once(reply, role_id)
        if pcm24:
            audio_out = base64.b64encode(_pcm_to_wav(pcm24, 24000)).decode()
    except Exception as e:
        logger.error(f"[一次性语音] TTS失败(仅返回文字): {e}", exc_info=True)

    logger.info(f"[一次性语音] 完成 回复长度={len(reply)} 音频={'有' if audio_out else '无'}")
    return JSONResponse({
        "success": True,
        "asr_text": asr_text,
        "reply": reply,
        "audio_base64": audio_out,
        "audio_format": "wav",
        "session_id": new_session or session_id,
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")

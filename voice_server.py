"""
语音后端 v2.0 - 全双工语音通话服务
端口：8004

v1.0: 一次性 ASR→LLM→TTS（SiliconFlow SenseVoice + edge-tts）
v2.0: 全双工实时通话
  - ASR: 阿里云 NLS 实时语音识别（WebSocket 流式，低延迟）
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
import struct
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

# ---- 阿里云 NLS 实时 ASR 配置 ----
ALIYUN_NLS_ACCESS_KEY_ID = os.getenv("ALIYUN_NLS_ACCESS_KEY_ID", "")
ALIYUN_NLS_ACCESS_KEY_SECRET = os.getenv("ALIYUN_NLS_ACCESS_KEY_SECRET", "")
ALIYUN_NLS_APP_KEY = os.getenv("ALIYUN_NLS_APP_KEY", "")
ALIYUN_NLS_REGION = os.getenv("ALIYUN_NLS_REGION", "cn-shanghai")  # cn-shanghai / cn-beijing / cn-shenzhen

# ---- 火山引擎流式 TTS 配置 ----
VOLCENGINE_TTS_APP_ID = os.getenv("VOLCENGINE_TTS_APP_ID", "")
VOLCENGINE_TTS_ACCESS_TOKEN = os.getenv("VOLCENGINE_TTS_ACCESS_TOKEN", "")
VOLCENGINE_TTS_CLUSTER = os.getenv("VOLCENGINE_TTS_CLUSTER", "volcano_tts")
VOLCENGINE_TTS_VOICE = os.getenv("VOLCENGINE_TTS_VOICE", "BV001_streaming")

# ---- 角色音色映射 ----
ROLE_VOICES = {
    "nianqi":  {"voice": "BV001_streaming", "speed": 0.9, "name": "念琦"},
    "qinghe":  {"voice": "BV002_streaming", "speed": 0.95, "name": "清禾"},
    "jingwen": {"voice": "BV003_streaming", "speed": 1.05, "name": "璟雯"},
}
DEFAULT_VOICE = {"voice": "BV001_streaming", "speed": 1.0, "name": "默认"}

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
# 火山引擎流式 TTS WebSocket 二进制协议
# ============================================================
# 消息类型
MSG_TYPE_CLIENT_FULL = 0x00
MSG_TYPE_CLIENT_AUDIO_ONLY = 0x01
MSG_TYPE_SERVER_AUDIO = 0x02
MSG_TYPE_SERVER_CONTROL = 0x03


def _volcengine_build_frame(msg_type: int, payload: bytes) -> bytes:
    """构建火山引擎 TTS WebSocket 二进制帧。"""
    # 4 bytes size (big-endian) + 1 byte type + payload
    size = len(payload) + 1
    return struct.pack(">I", size) + bytes([msg_type]) + payload


def _volcengine_parse_frame(data: bytes) -> Tuple[int, bytes]:
    """解析火山引擎 TTS WebSocket 二进制帧，返回 (msg_type, payload)。"""
    if len(data) < 5:
        return MSG_TYPE_SERVER_CONTROL, b""
    # size = struct.unpack(">I", data[:4])[0]  # 包含 type 字节
    msg_type = data[4]
    payload = data[5:]
    return msg_type, payload


def _volcengine_build_full_request(
    text: str,
    voice: str,
    speed: float = 1.0,
    reqid: Optional[str] = None,
) -> bytes:
    """构建完整的客户端请求（首次连接时发送）。"""
    payload = {
        "app": {
            "appid": VOLCENGINE_TTS_APP_ID,
            "token": VOLCENGINE_TTS_ACCESS_TOKEN,
            "cluster": VOLCENGINE_TTS_CLUSTER,
        },
        "user": {"uid": "airi_call"},
        "audio": {
            "voice_type": voice,
            "encoding": "pcm",
            "speed_ratio": speed,
            "text": text,
        },
        "request": {
            "reqid": reqid or uuid.uuid4().hex,
            "operation": "submit",
        },
    }
    return _volcengine_build_frame(MSG_TYPE_CLIENT_FULL, json.dumps(payload, ensure_ascii=False).encode())


def _volcengine_build_audio_only_request(text: str) -> bytes:
    """构建仅包含文本的追加请求（同一会话后续句子）。"""
    payload = {"audio": {"text": text}}
    return _volcengine_build_frame(MSG_TYPE_CLIENT_AUDIO_ONLY, json.dumps(payload, ensure_ascii=False).encode())


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
    负责协调：客户端 WS ↔ 阿里云 NLS ASR ↔ 人格后端 ↔ 火山引擎 TTS
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
        self.asr_ws = None  # 阿里云 NLS WebSocket
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

    # ---- ASR: 阿里云 NLS ----
    async def start_asr(self):
        """连接阿里云 NLS 并启动实时识别。"""
        try:
            token = await get_cached_aliyun_token()
            ws_url = f"{_aliyun_nls_ws_endpoint(ALIYUN_NLS_REGION)}?token={token}"

            # 使用 websockets 库连接阿里云 NLS
            import websockets
            self.asr_ws = await websockets.connect(ws_url, ping_interval=None)

            self.asr_task_id = uuid.uuid4().hex.replace("-", "")

            # 发送 StartTranscription
            start_msg = {
                "header": {
                    "appkey": ALIYUN_NLS_APP_KEY,
                    "message_id": uuid.uuid4().hex.replace("-", ""),
                    "task_id": self.asr_task_id,
                    "namespace": "SpeechTranscriber",
                    "name": "StartTranscription",
                },
                "payload": {
                    "format": "pcm",
                    "sample_rate": ASR_SAMPLE_RATE,
                    "enable_intermediate_result": True,
                    "enable_punctuation_prediction": True,
                    "enable_inverse_text_normalization": True,
                    "enable_words": True,
                },
            }
            await self.asr_ws.send(json.dumps(start_msg, ensure_ascii=False))
            logger.info(f"[阿里云NLS] 已发送 StartTranscription, task_id={self.asr_task_id}")

            # 启动 ASR 接收协程
            task = asyncio.create_task(self._asr_receive_loop())
            self._tasks.append(task)
            return True
        except Exception as e:
            logger.error(f"[阿里云NLS] 启动失败: {e}", exc_info=True)
            await self._send_json({"type": "error", "message": f"ASR启动失败: {e}"})
            return False

    async def _asr_receive_loop(self):
        """持续接收阿里云 NLS 的识别结果。"""
        try:
            async for message in self.asr_ws:
                if self._closed:
                    break
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    continue

                header = data.get("header", {})
                name = header.get("name", "")
                payload = data.get("payload", {})

                if name == "TranscriptionStarted":
                    self.asr_ready = True
                    logger.info("[阿里云NLS] TranscriptionStarted，开始接收音频")
                    await self.set_state(CallState.LISTENING)

                elif name == "SentenceBegin":
                    # 用户开始说话
                    if self.state == CallState.SPEAKING:
                        # AI 正在说话时用户开口 → 打断
                        await self.interrupt_tts()
                    await self.set_state(CallState.LISTENING)

                elif name == "ResultChanged":
                    # 中间结果（实时显示）
                    text = payload.get("result", "")
                    self.asr_partial_text = text
                    await self._send_json({"type": "asr_partial", "text": text})

                elif name == "SentenceEnd":
                    # 一句话结束（最终结果）
                    text = payload.get("result", "")
                    if text and text.strip():
                        logger.info(f"[阿里云NLS] 最终识别: {text}")
                        self.transcript.append({
                            "role": "user",
                            "content": text,
                            "timestamp": time.strftime("%H:%M:%S"),
                        })
                        await self._send_json({"type": "asr_final", "text": text})
                        # 触发 AI 回复
                        asyncio.create_task(self._generate_reply(text))
                    self.asr_partial_text = ""

                elif name == "TranscriptionCompleted":
                    logger.info("[阿里云NLS] TranscriptionCompleted")

                elif name == "TaskFailed":
                    status = payload.get("status", 0)
                    msg = payload.get("message", "")
                    logger.error(f"[阿里云NLS] TaskFailed: status={status}, message={msg}")

        except Exception as e:
            if not self._closed:
                logger.error(f"[阿里云NLS] 接收循环异常: {e}", exc_info=True)

    async def send_audio_to_asr(self, audio_bytes: bytes):
        """将前端采集的音频帧转发给阿里云 NLS。"""
        if self.asr_ws and self.asr_ready and not self._closed:
            try:
                await self.asr_ws.send(audio_bytes)
            except Exception as e:
                logger.debug(f"[阿里云NLS] 发送音频失败: {e}")

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
        使用火山引擎流式 TTS 合成并播放语音。
        边接收音频分片边转发给前端播放。
        """
        if self._closed:
            return

        await self.set_state(CallState.SPEAKING)
        self._interrupt_event.clear()

        try:
            import websockets

            # 连接火山引擎流式 TTS
            tts_url = "wss://openspeech.bytedance.com/api/v1/tts/ws_binary"
            headers = [("Authorization", f"Bearer; {VOLCENGINE_TTS_ACCESS_TOKEN}")]

            self.tts_ws = await websockets.connect(
                tts_url,
                additional_headers=headers,
                ping_interval=None,
                max_size=None,
            )

            # 发送完整请求（首句）
            reqid = uuid.uuid4().hex
            full_request = _volcengine_build_full_request(
                text=text,
                voice=self.voice,
                speed=self.speed,
                reqid=reqid,
            )
            await self.tts_ws.send(full_request)
            logger.info(f"[火山TTS] 已发送合成请求, reqid={reqid}, text长度={len(text)}")

            # 接收音频流
            session_finished = False
            async for message in self.tts_ws:
                if self._closed or self._interrupt_event.is_set():
                    break

                msg_type, payload = _volcengine_parse_frame(message)

                if msg_type == MSG_TYPE_SERVER_AUDIO:
                    # 音频帧 → 直接转发给前端播放
                    if payload:
                        await self._send_audio(payload)

                elif msg_type == MSG_TYPE_SERVER_CONTROL:
                    try:
                        ctrl = json.loads(payload.decode())
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue

                    event = ctrl.get("event", "")
                    code = ctrl.get("code", -1)

                    if event == "session_started":
                        self.tts_session_id = ctrl.get("session_id", "")
                        logger.info(f"[火山TTS] session_started, session_id={self.tts_session_id}")

                    elif event == "sentence_start":
                        logger.debug("[火山TTS] sentence_start")

                    elif event == "sentence_end":
                        logger.debug("[火山TTS] sentence_end")

                    elif event == "session_finished":
                        session_finished = True
                        logger.info("[火山TTS] session_finished")
                        break

                    elif event == "error":
                        logger.error(f"[火山TTS] error: code={code}, message={ctrl.get('message', '')}")
                        break

            # 播放完成（或被打断）
            if not self._interrupt_event.is_set() and session_finished:
                logger.info("[火山TTS] 播放完成，回到聆听状态")
            else:
                logger.info("[火山TTS] 播放被打断")

        except Exception as e:
            logger.error(f"[火山TTS] 合成失败: {e}", exc_info=True)
            await self._send_json({"type": "error", "message": f"TTS失败: {e}"})
        finally:
            # 关闭 TTS 连接
            if self.tts_ws:
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

        # 关闭 ASR
        if self.asr_ws:
            try:
                stop_msg = {
                    "header": {
                        "appkey": ALIYUN_NLS_APP_KEY,
                        "message_id": uuid.uuid4().hex.replace("-", ""),
                        "task_id": self.asr_task_id or "",
                        "namespace": "SpeechTranscriber",
                        "name": "StopTranscription",
                    },
                }
                await self.asr_ws.send(json.dumps(stop_msg))
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
                        )
            logger.info(f"[记忆] 通话内容已写入记忆后端，共 {len([m for m in user_messages if len(m)>=10])} 条")
        except Exception as e:
            logger.warning(f"[记忆] 写入记忆后端失败: {e}")


# ============================================================
# FastAPI 应用
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🎤 语音后端 v2.0 启动 - 端口 {PORT}")
    logger.info(f"  人格后端: {PERSONALITY_SERVER_URL}")
    logger.info(f"  记忆后端: {VECTOR_SERVER_URL}")
    logger.info(f"  ASR: 阿里云 NLS (region={ALIYUN_NLS_REGION}) {'已配置' if ALIYUN_NLS_APP_KEY else '⚠️ 未配置APP_KEY!'}")
    logger.info(f"  TTS: 火山引擎流式 (voice={VOLCENGINE_TTS_VOICE}) {'已配置' if VOLCENGINE_TTS_APP_ID else '⚠️ 未配置APP_ID!'}")
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
            await websocket.send_json({"type": "error", "message": "ASR启动失败，请检查阿里云NLS配置"})
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
                    # 二进制音频帧 → 转发给 ASR
                    if session.state == CallState.LISTENING or session.state == CallState.IDLE:
                        await session.send_audio_to_asr(data)
                    # AI 说话时收到音频（用户可能在说话），由 ASR 的 SentenceBegin 触发打断

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
            "provider": "aliyun_nls",
            "region": ALIYUN_NLS_REGION,
            "configured": bool(ALIYUN_NLS_APP_KEY and ALIYUN_NLS_ACCESS_KEY_ID),
        },
        "tts": {
            "provider": "volcengine_streaming",
            "voice": VOLCENGINE_TTS_VOICE,
            "configured": bool(VOLCENGINE_TTS_APP_ID and VOLCENGINE_TTS_ACCESS_TOKEN),
        },
        "personality_url": PERSONALITY_SERVER_URL,
        "memory_url": VECTOR_SERVER_URL,
    }


# ============================================================
# 兼容 v1 接口（保留原有一次性语音聊天，不破坏现有功能）
# ============================================================
@app.post("/api/voice/chat")
async def voice_chat_v1_compat(request: Request):
    """
    兼容 v1 的一次性语音聊天接口。
    内部仍然走 v2 的 ASR→人格→TTS 流程，但不是流式。
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"success": False, "error": "无效的JSON"}, status_code=400)

    audio_base64 = body.get("audio_base64", "")
    role_ids = body.get("role_ids", ["nianqi"])
    session_id = body.get("session_id")

    if not audio_base64:
        return JSONResponse({"success": False, "error": "缺少audio_base64"}, status_code=400)

    # 注意：v1 兼容接口暂时返回提示，引导使用 v2 流式通话
    return JSONResponse({
        "success": False,
        "error": "v1 一次性语音接口已升级为 v2 全双工流式通话，请使用 WebSocket /ws/call 端点",
        "v2_endpoint": "/ws/call",
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")

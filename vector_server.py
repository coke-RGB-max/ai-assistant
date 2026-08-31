"""
语音后端 v2.0 - 端口 8004
P4 序号3：TTS 多引擎抽象层重构
  - BaseTTSProvider 抽象基类
  - EdgeTTSProvider（免费微软TTS，兜底）
  - OpenAITTSProvider（OpenAI兼容接口）
  - FishAudioProvider（Fish-Audio 云API，语音克隆）
  - GPTSoVITSProvider（本地GPT-SoVITS，预留）
  - 情感TTS：根据角色情绪调整 rate/pitch
  - TTS_ENGINE 环境变量切换，引擎挂了自动降级 edge-tts

流程：接收语音 → ASR语音转文本 → 调人格后端生成回复 → TTS文本转语音 → 返回语音
支持：
  - ASR: OpenAI Whisper API 兼容接口（可接本地Whisper/Groq/OpenAI/SiliconFlow SenseVoice）
  - TTS: 多引擎可切换，支持语音克隆
  - 不同角色映射不同音色/说话人
  - 情感TTS（开心/难过/害羞/生气/平静）
依赖：pip install edge-tts httpx fastapi uvicorn python-multipart
"""
import asyncio
import base64
import io
import json
import logging
import os
import tempfile
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, List
import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# P3 修复：减少httpx的HTTP请求日志，避免Railway日志速率限制
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger("voice_server")

# ============================================================
# 配置
# ============================================================
PORT = int(os.getenv("VOICE_PORT", "8004"))
PERSONALITY_SERVER_URL = os.getenv("PERSONALITY_SERVER_URL", "http://127.0.0.1:8002")

# ---- ASR 配置 ----
ASR_API_KEY = os.getenv("ASR_API_KEY", "")
ASR_BASE_URL = os.getenv("ASR_BASE_URL", "https://api.siliconflow.cn/v1")
ASR_MODEL = os.getenv("ASR_MODEL", "FunAudioLLM/SenseVoiceSmall")
ASR_LANGUAGE = os.getenv("ASR_LANGUAGE", "zh")

# ---- TTS 通用配置 ----
# TTS 引擎: "edge-tts"（免费兜底）| "openai"（OpenAI兼容）| "fish-audio"（语音克隆云API）| "gpt-sovits"（本地GPT-SoVITS）
TTS_ENGINE = os.getenv("TTS_ENGINE", "edge-tts")
TTS_FORMAT = os.getenv("TTS_FORMAT", "mp3")  # wav/mp3
TTS_TIMEOUT = float(os.getenv("TTS_TIMEOUT", "60"))

# ---- OpenAI TTS 配置 ----
TTS_API_KEY = os.getenv("TTS_API_KEY", "")
TTS_BASE_URL = os.getenv("TTS_BASE_URL", "https://api.openai.com/v1")
TTS_MODEL = os.getenv("TTS_MODEL", "tts-1")

# ---- Fish-Audio 语音克隆配置 ----
FISH_AUDIO_API_KEY = os.getenv("FISH_AUDIO_API_KEY", "")
FISH_AUDIO_BASE_URL = os.getenv("FISH_AUDIO_BASE_URL", "https://api.fish.audio/v1")
FISH_AUDIO_FORMAT = os.getenv("FISH_AUDIO_FORMAT", "mp3")
FISH_AUDIO_TEMPERATURE = float(os.getenv("FISH_AUDIO_TEMPERATURE", "0.7"))
FISH_AUDIO_TOP_P = float(os.getenv("FISH_AUDIO_TOP_P", "0.7"))

# ---- GPT-SoVITS 本地配置 ----
GPT_SOVITS_BASE_URL = os.getenv("GPT_SOVITS_BASE_URL", "http://127.0.0.1:9880")
GPT_SOVITS_TEXT_LANG = os.getenv("GPT_SOVITS_TEXT_LANG", "zh")
GPT_SOVITS_PROMPT_LANG = os.getenv("GPT_SOVITS_PROMPT_LANG", "zh")

# ---- 角色音色映射（每个引擎对应不同的音色/说话人ID）----
ROLE_VOICES = {
    "nianqi": {
        "edge": "zh-CN-XiaohanNeural",      # 温柔细腻女声，安全型依恋
        "openai": "nova",
        "fish_audio_reference_id": os.getenv("NIANQI_FISH_REFERENCE_ID", ""),  # Fish-Audio 说话人ID
        "gpt_sovits_ref_audio": os.getenv("NIANQI_GPT_SOVITS_REF_AUDIO", ""),  # GPT-SoVITS 参考音频路径
        "gpt_sovits_prompt_text": os.getenv("NIANQI_GPT_SOVITS_PROMPT", ""),   # 参考音频文本
        "rate": "-3%",
        "pitch": "+1Hz",
    },
    "qinghe": {
        "edge": "zh-CN-XiaoxiaoNeural",     # 温柔知性女声
        "openai": "alloy",
        "fish_audio_reference_id": os.getenv("QINGHE_FISH_REFERENCE_ID", ""),
        "gpt_sovits_ref_audio": os.getenv("QINGHE_GPT_SOVITS_REF_AUDIO", ""),
        "gpt_sovits_prompt_text": os.getenv("QINGHE_GPT_SOVITS_PROMPT", ""),
        "rate": "-5%",
        "pitch": "-1Hz",
    },
    "jingwen": {
        "edge": "zh-CN-XiaoyiNeural",      # 年轻女声，带点傲娇感
        "openai": "nova",
        "fish_audio_reference_id": os.getenv("JINGWEN_FISH_REFERENCE_ID", ""),
        "gpt_sovits_ref_audio": os.getenv("JINGWEN_GPT_SOVITS_REF_AUDIO", ""),
        "gpt_sovits_prompt_text": os.getenv("JINGWEN_GPT_SOVITS_PROMPT", ""),
        "rate": "+5%",
        "pitch": "+2Hz",
    },
}
DEFAULT_VOICE = {
    "edge": "zh-CN-XiaoxiaoNeural",
    "openai": "alloy",
    "fish_audio_reference_id": "",
    "gpt_sovits_ref_audio": "",
    "gpt_sovits_prompt_text": "",
    "rate": "+0%",
    "pitch": "+0Hz",
}

# ---- 情感TTS参数映射（在角色基础rate/pitch上叠加）----
EMOTION_TTS_PARAMS = {
    "happy":   {"rate_delta": "+10%", "pitch_delta": "+2Hz"},
    "sad":     {"rate_delta": "-10%", "pitch_delta": "-2Hz"},
    "shy":     {"rate_delta": "-5%",  "pitch_delta": "+1Hz"},
    "angry":   {"rate_delta": "+15%", "pitch_delta": "+3Hz"},
    "calm":    {"rate_delta": "+0%",  "pitch_delta": "+0Hz"},
    "excited": {"rate_delta": "+12%", "pitch_delta": "+2Hz"},
    "lonely":  {"rate_delta": "-8%",  "pitch_delta": "-1Hz"},
}

# 音频配置
MAX_AUDIO_SIZE = 25 * 1024 * 1024  # 25MB
REQUEST_TIMEOUT = 120.0


# ============================================================
# TTS 多引擎抽象层（P4 序号3）
# ============================================================
class BaseTTSProvider(ABC):
    """TTS 引擎抽象基类，所有 TTS 引擎继承此类。"""
    name: str = "base"

    def __init__(self):
        self.available = self._check_available()

    @abstractmethod
    def _check_available(self) -> bool:
        """检查该引擎是否可用（API Key是否配置、依赖是否安装等）。"""
        pass

    @abstractmethod
    async def synthesize(self, text: str, voice_cfg: Dict, emotion: Optional[str] = None) -> bytes:
        """
        合成语音。
        Args:
            text: 要合成的文本
            voice_cfg: 角色音色配置
            emotion: 情绪（happy/sad/shy/angry/calm等），None则用角色默认
        Returns:
            音频字节
        """
        pass

    def _apply_emotion(self, voice_cfg: Dict, emotion: Optional[str]) -> Dict:
        """根据情绪调整 rate/pitch，返回调整后的配置副本。"""
        cfg = dict(voice_cfg)
        if emotion and emotion in EMOTION_TTS_PARAMS:
            params = EMOTION_TTS_PARAMS[emotion]
            # 叠加 rate_delta（简单的百分比加减，实际edge-tts用字符串）
            base_rate = cfg.get("rate", "+0%")
            base_pitch = cfg.get("pitch", "+0Hz")
            # 简单处理：如果有情绪偏移，直接用情绪值覆盖（更可控）
            if params["rate_delta"] != "+0%":
                cfg["rate"] = params["rate_delta"]
            if params["pitch_delta"] != "+0Hz":
                cfg["pitch"] = params["pitch_delta"]
            logger.debug(f"[TTS][{self.name}] 情绪={emotion} rate={cfg['rate']} pitch={cfg['pitch']}")
        return cfg


class EdgeTTSProvider(BaseTTSProvider):
    """edge-tts（免费微软TTS）—— 兜底引擎，永远可用（只要装了edge-tts）。"""
    name = "edge-tts"

    def _check_available(self) -> bool:
        try:
            import edge_tts  # noqa: F401
            return True
        except ImportError:
            logger.warning("[TTS][edge-tts] 未安装 edge-tts，该引擎不可用")
            return False

    async def synthesize(self, text: str, voice_cfg: Dict, emotion: Optional[str] = None) -> bytes:
        if not self.available:
            raise HTTPException(status_code=500, detail="edge-tts 未安装，请执行: pip install edge-tts")
        import edge_tts
        cfg = self._apply_emotion(voice_cfg, emotion)
        voice = cfg.get("edge", DEFAULT_VOICE["edge"])
        rate = cfg.get("rate", "+0%")
        pitch = cfg.get("pitch", "+0Hz")

        tmp_path = os.path.join(tempfile.gettempdir(), f"tts_edge_{uuid.uuid4().hex}.mp3")
        try:
            communicate = edge_tts.Communicate(text, voice, rate=rate, pitch=pitch)
            await communicate.save(tmp_path)
            with open(tmp_path, "rb") as f:
                return f.read()
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass


class OpenAITTSProvider(BaseTTSProvider):
    """OpenAI TTS 兼容接口。"""
    name = "openai"

    def _check_available(self) -> bool:
        return bool(TTS_API_KEY and TTS_BASE_URL and TTS_MODEL)

    async def synthesize(self, text: str, voice_cfg: Dict, emotion: Optional[str] = None) -> bytes:
        if not self.available:
            raise HTTPException(status_code=500, detail="OpenAI TTS 未配置 API Key")
        cfg = self._apply_emotion(voice_cfg, emotion)
        voice = cfg.get("openai", DEFAULT_VOICE["openai"])

        async with httpx.AsyncClient(timeout=TTS_TIMEOUT) as client:
            resp = await client.post(
                f"{TTS_BASE_URL}/audio/speech",
                headers={"Authorization": f"Bearer {TTS_API_KEY}", "Content-Type": "application/json"},
                json={"model": TTS_MODEL, "input": text, "voice": voice, "response_format": TTS_FORMAT}
            )
        if resp.status_code != 200:
            logger.error(f"[TTS][openai] 失败 HTTP{resp.status_code}: {resp.text[:300]}")
            raise HTTPException(status_code=502, detail=f"OpenAI TTS返回错误: HTTP{resp.status_code}")
        return resp.content


class FishAudioProvider(BaseTTSProvider):
    """Fish-Audio 云 API —— 语音克隆，高质量音色。
    API文档: https://docs.fish.audio/api-reference
    """
    name = "fish-audio"

    def _check_available(self) -> bool:
        return bool(FISH_AUDIO_API_KEY and FISH_AUDIO_BASE_URL)

    async def synthesize(self, text: str, voice_cfg: Dict, emotion: Optional[str] = None) -> bytes:
        if not self.available:
            raise HTTPException(status_code=500, detail="Fish-Audio 未配置 API Key (FISH_AUDIO_API_KEY)")
        reference_id = voice_cfg.get("fish_audio_reference_id", "")
        if not reference_id:
            raise HTTPException(
                status_code=500,
                detail=f"角色未配置 Fish-Audio 说话人ID (fish_audio_reference_id)，请设置环境变量"
            )

        payload = {
            "text": text,
            "reference_id": reference_id,
            "format": FISH_AUDIO_FORMAT,
            "temperature": FISH_AUDIO_TEMPERATURE,
            "top_p": FISH_AUDIO_TOP_P,
        }
        # Fish-Audio 支持 emotion 参数（如果模型支持）
        if emotion and emotion in EMOTION_TTS_PARAMS:
            payload["emotion"] = emotion

        async with httpx.AsyncClient(timeout=TTS_TIMEOUT) as client:
            resp = await client.post(
                f"{FISH_AUDIO_BASE_URL}/tts",
                headers={
                    "Authorization": f"Bearer {FISH_AUDIO_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        if resp.status_code != 200:
            logger.error(f"[TTS][fish-audio] 失败 HTTP{resp.status_code}: {resp.text[:300]}")
            raise HTTPException(status_code=502, detail=f"Fish-Audio返回错误: HTTP{resp.status_code}")
        return resp.content


class GPTSoVITSProvider(BaseTTSProvider):
    """本地 GPT-SoVITS —— 自建语音克隆服务，需要单独部署 GPU 服务器。
    预留接口，默认指向本地 9880 端口。
    """
    name = "gpt-sovits"

    def _check_available(self) -> bool:
        # 不主动探测（可能服务没启动），只要配置了基础URL就认为可用
        return bool(GPT_SOVITS_BASE_URL)

    async def synthesize(self, text: str, voice_cfg: Dict, emotion: Optional[str] = None) -> bytes:
        if not self.available:
            raise HTTPException(status_code=500, detail="GPT-SoVITS 未配置基础URL")
        ref_audio = voice_cfg.get("gpt_sovits_ref_audio", "")
        prompt_text = voice_cfg.get("gpt_sovits_prompt_text", "")
        if not ref_audio or not prompt_text:
            raise HTTPException(
                status_code=500,
                detail="角色未配置 GPT-SoVITS 参考音频(gpt_sovits_ref_audio)和提示文本(gpt_sovits_prompt_text)"
            )

        params = {
            "text": text,
            "text_lang": GPT_SOVITS_TEXT_LANG,
            "ref_audio_path": ref_audio,
            "prompt_text": prompt_text,
            "prompt_lang": GPT_SOVITS_PROMPT_LANG,
        }
        # GPT-SoVITS 部分版本支持情绪参数
        if emotion:
            params["emotion"] = emotion

        async with httpx.AsyncClient(timeout=TTS_TIMEOUT) as client:
            resp = await client.get(GPT_SOVITS_BASE_URL, params=params)
        if resp.status_code != 200:
            logger.error(f"[TTS][gpt-sovits] 失败 HTTP{resp.status_code}: {resp.text[:300]}")
            raise HTTPException(status_code=502, detail=f"GPT-SoVITS返回错误: HTTP{resp.status_code}")
        return resp.content


# ============================================================
# TTS 引擎管理器（自动降级）
# ============================================================
class TTSManager:
    """TTS 引擎管理器：按优先级选择引擎，失败自动降级到 edge-tts。"""
    def __init__(self):
        self.providers: Dict[str, BaseTTSProvider] = {
            "edge-tts": EdgeTTSProvider(),
            "openai": OpenAITTSProvider(),
            "fish-audio": FishAudioProvider(),
            "gpt-sovits": GPTSoVITSProvider(),
        }
        self._fallback_chain = self._build_fallback_chain()
        self._log_available()

    def _build_fallback_chain(self) -> List[str]:
        """构建降级链：首选引擎 → 其他可用引擎 → edge-tts（兜底）。"""
        chain = []
        # 首选引擎放第一个
        if TTS_ENGINE in self.providers and self.providers[TTS_ENGINE].available:
            chain.append(TTS_ENGINE)
        # 然后其他可用引擎（除了edge-tts）
        for name, provider in self.providers.items():
            if name not in chain and name != "edge-tts" and provider.available:
                chain.append(name)
        # edge-tts 永远放最后兜底
        if "edge-tts" not in chain and self.providers["edge-tts"].available:
            chain.append("edge-tts")
        return chain

    def _log_available(self):
        available = [f"{name}({'可用' if p.available else '未配置'})" for name, p in self.providers.items()]
        logger.info(f"[TTSManager] 引擎状态: {', '.join(available)}")
        logger.info(f"[TTSManager] 首选引擎: {TTS_ENGINE} | 降级链: {' → '.join(self._fallback_chain)}")

    async def synthesize(self, text: str, role_id: str = "nianqi", emotion: Optional[str] = None) -> bytes:
        """
        合成语音，自动降级。
        Args:
            text: 文本
            role_id: 角色ID
            emotion: 情绪
        Returns:
            音频字节
        """
        voice_cfg = ROLE_VOICES.get(role_id, DEFAULT_VOICE)
        last_error = None

        for engine_name in self._fallback_chain:
            provider = self.providers[engine_name]
            if not provider.available:
                continue
            try:
                t0 = time.perf_counter()
                audio = await provider.synthesize(text, voice_cfg, emotion)
                dur = time.perf_counter() - t0
                logger.info(
                    f"[TTS] 合成成功 引擎={engine_name} 角色={role_id} "
                    f"情绪={emotion or '默认'} 耗时={dur:.2f}s 大小={len(audio)}字节"
                )
                return audio
            except HTTPException as e:
                last_error = e
                logger.warning(f"[TTS] {engine_name} 失败: {e.detail}，尝试降级")
            except Exception as e:
                last_error = e
                logger.warning(f"[TTS] {engine_name} 异常: {type(e).__name__}: {e}，尝试降级")

        # 全部失败
        error_msg = str(last_error.detail) if isinstance(last_error, HTTPException) else str(last_error)
        logger.error(f"[TTS] 所有引擎都失败: {error_msg}")
        raise HTTPException(status_code=500, detail=f"语音合成失败，所有引擎都不可用: {error_msg}")

    def get_status(self) -> Dict[str, Any]:
        """获取TTS引擎状态（用于健康检查/管理员查看）。"""
        return {
            "current_engine": TTS_ENGINE,
            "fallback_chain": self._fallback_chain,
            "providers": {
                name: {"available": p.available} for name, p in self.providers.items()
            },
        }


# 全局 TTS 管理器单例
_tts_manager: Optional[TTSManager] = None


def get_tts_manager() -> TTSManager:
    global _tts_manager
    if _tts_manager is None:
        _tts_manager = TTSManager()
    return _tts_manager


# ============================================================
# ASR 语音转文本
# ============================================================
async def speech_to_text(audio_bytes: bytes, filename: str = "audio.wav") -> str:
    """调用 OpenAI Whisper 兼容接口进行语音识别。"""
    if not ASR_API_KEY:
        raise HTTPException(status_code=500, detail="未配置 ASR_API_KEY，无法进行语音识别")
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            files = {"file": (filename, audio_bytes, "audio/wav")}
            data = {"model": ASR_MODEL, "language": ASR_LANGUAGE}
            headers = {"Authorization": f"Bearer {ASR_API_KEY}"}
            resp = await client.post(
                f"{ASR_BASE_URL}/audio/transcriptions",
                files=files, data=data, headers=headers)
        dur = time.perf_counter() - t0
        if resp.status_code == 200:
            result = resp.json()
            text = result.get("text", "").strip()
            logger.info(f"[ASR] 识别成功 耗时={dur:.2f}s 文本长度={len(text)} 内容={text[:50]}")
            return text
        else:
            logger.error(f"[ASR] 识别失败 HTTP{resp.status_code}: {resp.text[:300]}")
            raise HTTPException(status_code=502, detail=f"ASR服务返回错误: HTTP{resp.status_code}")
    except httpx.TimeoutException:
        logger.error("[ASR] 识别超时")
        raise HTTPException(status_code=504, detail="语音识别超时")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[ASR] 识别异常: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"语音识别失败: {str(e)}")


# ============================================================
# 调用人格后端
# ============================================================
async def call_personality_generate(role_ids, user_message, session_id=None,
                                     intimacy_map=None, chat_history=None,
                                     temperature=0.9, max_tokens=500) -> Dict[str, Any]:
    """调人格后端 /api/generate 获取文本回复。"""
    mode = "group" if len(role_ids) > 1 else "single"
    payload = {
        "mode": mode,
        "role_ids": role_ids,
        "user_message": user_message,
        "memory_context": "",
        "chat_history": chat_history or [],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "return_debug": False,
        "enable_memory_analysis": True,
    }
    if session_id:
        payload["session_id"] = session_id
    if intimacy_map:
        payload["intimacy_map"] = intimacy_map
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.post(
                f"{PERSONALITY_SERVER_URL}/api/generate", json=payload, timeout=REQUEST_TIMEOUT)
        dur = time.perf_counter() - t0
        logger.info(f"[人格后端] /api/generate HTTP{resp.status_code} 耗时={dur:.2f}s")
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            return {"success": False, "reply": "……你说话太快了，让我喘口气。"}
        else:
            logger.error(f"[人格后端] 返回错误 HTTP{resp.status_code}: {resp.text[:300]}")
            return {"success": False, "reply": "抱歉，我暂时无法回应..."}
    except httpx.TimeoutException:
        logger.error("[人格后端] 请求超时")
        return {"success": False, "reply": "……等一下，我刚才走神了。"}
    except Exception as e:
        logger.error(f"[人格后端] 调用异常: {e}", exc_info=True)
        return {"success": False, "reply": "抱歉，我暂时无法回应..."}


# ============================================================
# FastAPI
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🎤 语音后端 v2.0 启动 - 端口 {PORT}")
    logger.info(f"  人格后端: {PERSONALITY_SERVER_URL}")
    logger.info(f"  ASR: {ASR_BASE_URL} 模型={ASR_MODEL} {'已配置' if ASR_API_KEY else '未配置API Key!'}")
    # 初始化 TTS 管理器
    tts_mgr = get_tts_manager()
    logger.info(f"  TTS: 首选={TTS_ENGINE} | 降级链={' → '.join(tts_mgr._fallback_chain)}")
    # 检查人格后端健康
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{PERSONALITY_SERVER_URL}/health")
            if r.status_code == 200:
                logger.info("  人格后端连接正常")
    except Exception:
        logger.warning("  人格后端未启动，请先启动 personality_server.py")
    yield
    logger.info("语音后端关闭")


app = FastAPI(title="Voice Server", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                   allow_methods=["*"], allow_headers=["*"])


# ============================================================
# 请求模型
# ============================================================
class VoiceChatRequest(BaseModel):
    """语音聊天请求（base64编码音频）"""
    audio_base64: str
    audio_format: str = "wav"
    role_ids: list = ["nianqi"]
    session_id: Optional[str] = None
    intimacy_map: Optional[Dict[str, int]] = None
    chat_history: list = []
    temperature: float = 0.9
    max_tokens: int = 500
    emotion: Optional[str] = None  # P4: 情绪参数，传递给TTS


class VoiceChatResponse(BaseModel):
    success: bool
    asr_text: str = ""
    reply: str = ""
    audio_base64: str = ""
    audio_format: str = "mp3"
    session_id: Optional[str] = None
    tts_engine: str = ""  # P4: 实际使用的TTS引擎
    error: str = ""


class TTSRequest(BaseModel):
    """独立TTS请求"""
    text: str
    role_id: str = "nianqi"
    emotion: Optional[str] = None


# ============================================================
# 接口
# ============================================================
@app.get("/health")
async def health():
    tts_mgr = get_tts_manager()
    return {
        "status": "ok", "service": "voice_server", "version": "2.0.0", "port": str(PORT),
        "asr_configured": bool(ASR_API_KEY),
        "tts": tts_mgr.get_status(),
        "personality_url": PERSONALITY_SERVER_URL,
    }


@app.post("/api/voice/chat", response_model=VoiceChatResponse)
async def voice_chat(request: VoiceChatRequest):
    """
    语音聊天完整流程：
    1. base64音频 → ASR语音转文本
    2. 文本 → 人格后端生成回复
    3. 回复文本 → TTS文本转语音（多引擎自动降级）
    4. 返回文本 + base64音频
    """
    try:
        # 1. 解码音频
        try:
            audio_bytes = base64.b64decode(request.audio_base64)
        except Exception:
            return VoiceChatResponse(success=False, error="音频base64解码失败")
        if len(audio_bytes) > MAX_AUDIO_SIZE:
            return VoiceChatResponse(success=False, error=f"音频过大（{len(audio_bytes)}字节），上限25MB")
        if len(audio_bytes) < 100:
            return VoiceChatResponse(success=False, error="音频数据过短，可能未录到声音")
        logger.info(f"[语音聊天] 收到音频 {len(audio_bytes)}字节 角色={request.role_ids}")

        # 2. ASR 语音转文本
        asr_text = await speech_to_text(audio_bytes, f"audio.{request.audio_format}")
        if not asr_text:
            return VoiceChatResponse(success=False, error="未识别到语音内容，请重试")

        # 3. 调人格后端生成回复
        result = await call_personality_generate(
            role_ids=request.role_ids,
            user_message=asr_text,
            session_id=request.session_id,
            intimacy_map=request.intimacy_map,
            chat_history=request.chat_history,
            temperature=request.temperature,
            max_tokens=request.max_tokens)
        reply_text = result.get("reply", "") or ""
        if not reply_text:
            return VoiceChatResponse(success=False, asr_text=asr_text, error="人格后端未返回回复")

        # 4. TTS 文本转语音（失败不影响文本回复，仅语音缺失）
        tts_role = request.role_ids[0] if request.role_ids else "nianqi"
        audio_b64 = ""
        tts_engine_used = ""
        try:
            tts_mgr = get_tts_manager()
            audio_out = await tts_mgr.synthesize(reply_text, tts_role, emotion=request.emotion)
            audio_b64 = base64.b64encode(audio_out).decode("utf-8")
            tts_engine_used = TTS_ENGINE  # 实际使用的引擎在synthesize里有日志
        except HTTPException as te:
            logger.warning(f"[语音聊天] TTS失败，仅返回文本回复: {te.detail}")
        except Exception as te:
            logger.warning(f"[语音聊天] TTS异常，仅返回文本回复: {te}")

        return VoiceChatResponse(
            success=True,
            asr_text=asr_text,
            reply=reply_text,
            audio_base64=audio_b64,
            audio_format=TTS_FORMAT,
            session_id=result.get("session_id"),
            tts_engine=tts_engine_used,
        )
    except HTTPException as e:
        return VoiceChatResponse(success=False, error=str(e.detail))
    except Exception as e:
        logger.error(f"[语音聊天] 处理失败: {e}", exc_info=True)
        return VoiceChatResponse(success=False, error=str(e))


@app.post("/api/voice/asr")
async def voice_asr(file: UploadFile = File(...)):
    """独立ASR接口：接收音频文件上传，返回识别文本。"""
    audio_bytes = await file.read()
    if len(audio_bytes) > MAX_AUDIO_SIZE:
        raise HTTPException(status_code=413, detail="音频文件过大")
    text = await speech_to_text(audio_bytes, file.filename or "audio.wav")
    return {"success": True, "text": text}


@app.post("/api/voice/tts")
async def voice_tts(request: TTSRequest):
    """
    独立TTS接口：接收文本+角色+情绪，返回音频文件。
    P4 序号3：支持多引擎和情感TTS。
    """
    if not request.text:
        raise HTTPException(status_code=400, detail="缺少text参数")
    tts_mgr = get_tts_manager()
    audio_bytes = await tts_mgr.synthesize(request.text, request.role_id, emotion=request.emotion)
    return StreamingResponse(
        io.BytesIO(audio_bytes),
        media_type=f"audio/{TTS_FORMAT}",
        headers={"Content-Disposition": f"attachment; filename=tts.{TTS_FORMAT}"})


@app.get("/api/voice/tts/status")
async def tts_status():
    """获取TTS引擎状态（管理员查看）。"""
    return get_tts_manager().get_status()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")

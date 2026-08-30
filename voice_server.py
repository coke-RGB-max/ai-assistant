"""
语音后端 v1.0 - 端口 8004
流程：接收语音 → ASR语音转文本 → 调人格后端生成回复 → TTS文本转语音 → 返回语音
支持：
  - ASR: OpenAI Whisper API 兼容接口（可接本地Whisper/Groq/OpenAI）
  - TTS: edge-tts（免费微软TTS）或 OpenAI TTS API 兼容接口
  - 不同角色映射不同音色
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
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# P3 修复：减少httpx的HTTP请求日志，避免Railway日志速率限制
# httpx默认会输出每个HTTP请求的INFO日志，这是日志量最大的来源
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
# SiliconFlow SenseVoice
ASR_API_KEY = os.getenv("ASR_API_KEY", "")
ASR_BASE_URL = os.getenv("ASR_BASE_URL", "https://api.siliconflow.cn/v1")
ASR_MODEL = os.getenv("ASR_MODEL", "FunAudioLLM/SenseVoiceSmall")
ASR_LANGUAGE = os.getenv("ASR_LANGUAGE", "zh")

# ---- TTS 配置 ----
# TTS 引擎: "edge-tts"（免费）或 "openai"（OpenAI兼容接口）
TTS_ENGINE = os.getenv("TTS_ENGINE", "edge-tts")
TTS_API_KEY = os.getenv("TTS_API_KEY", "")
TTS_BASE_URL = os.getenv("TTS_BASE_URL", "https://api.openai.com/v1")
TTS_MODEL = os.getenv("TTS_MODEL", "tts-1")
TTS_FORMAT = os.getenv("TTS_FORMAT", "wav")  # wav/mp3

# ---- 角色音色映射 ----
# edge-tts 中文音色
ROLE_VOICES = {
    "nianqi": {
        "edge": "zh-CN-XiaohanNeural",      # 温柔细腻女声，安全型依恋
        "openai": "nova",
        "rate": "-3%",
        "pitch": "+1Hz",
    },
    "qinghe": {
        "edge": "zh-CN-XiaoxiaoNeural",     # 温柔知性女声
        "openai": "alloy",
        "rate": "-5%",
        "pitch": "-1Hz",
    },
    "jingwen": {
        "edge": "zh-CN-XiaoyiNeural",      # 年轻女声，带点傲娇感
        "openai": "nova",
        "rate": "+5%",
        "pitch": "+2Hz",
    },
}
DEFAULT_VOICE = {"edge": "zh-CN-XiaoxiaoNeural", "openai": "alloy", "rate": "+0%", "pitch": "+0Hz"}

# 音频配置
MAX_AUDIO_SIZE = 25 * 1024 * 1024  # 25MB
REQUEST_TIMEOUT = 120.0


# ============================================================
# ASR 语音转文本
# ============================================================
async def speech_to_text(audio_bytes: bytes, filename: str = "audio.wav") -> str:
    """
    调用 OpenAI Whisper 兼容接口进行语音识别。
    返回识别出的文本。
    """
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
# TTS 文本转语音
# ============================================================
async def text_to_speech(text: str, role_id: str = "nianqi") -> bytes:
    """
    将文本转换为语音。
    根据 TTS_ENGINE 选择 edge-tts 或 OpenAI TTS。
    返回音频字节。
    """
    voice_cfg = ROLE_VOICES.get(role_id, DEFAULT_VOICE)
    t0 = time.perf_counter()
    try:
        if TTS_ENGINE == "edge-tts":
            audio_bytes = await _tts_edge(text, voice_cfg)
        elif TTS_ENGINE == "openai":
            audio_bytes = await _tts_openai(text, voice_cfg)
        else:
            raise HTTPException(status_code=500, detail=f"未知TTS引擎: {TTS_ENGINE}")
        dur = time.perf_counter() - t0
        logger.info(f"[TTS] 合成成功 引擎={TTS_ENGINE} 角色={role_id} 耗时={dur:.2f}s 音频大小={len(audio_bytes)}字节")
        return audio_bytes
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[TTS] 合成异常: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"语音合成失败: {str(e)}")


async def _tts_edge(text: str, voice_cfg: Dict) -> bytes:
    """使用 edge-tts（免费微软TTS）合成语音。"""
    try:
        import edge_tts
    except ImportError:
        raise HTTPException(status_code=500, detail="未安装 edge-tts，请执行: pip install edge-tts")
    voice = voice_cfg.get("edge", DEFAULT_VOICE["edge"])
    rate = voice_cfg.get("rate", "+0%")
    pitch = voice_cfg.get("pitch", "+0Hz")
    # edge-tts 输出 mp3，写入临时文件后读取
    tmp_path = os.path.join(tempfile.gettempdir(), f"tts_{uuid.uuid4().hex}.mp3")
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


async def _tts_openai(text: str, voice_cfg: Dict) -> bytes:
    """使用 OpenAI TTS 兼容接口合成语音。"""
    if not TTS_API_KEY:
        raise HTTPException(status_code=500, detail="未配置 TTS_API_KEY")
    voice = voice_cfg.get("openai", DEFAULT_VOICE["openai"])
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        resp = await client.post(
            f"{TTS_BASE_URL}/audio/speech",
            headers={"Authorization": f"Bearer {TTS_API_KEY}", "Content-Type": "application/json"},
            json={"model": TTS_MODEL, "input": text, "voice": voice, "response_format": TTS_FORMAT})
    if resp.status_code != 200:
        logger.error(f"[TTS] OpenAI TTS失败 HTTP{resp.status_code}: {resp.text[:300]}")
        raise HTTPException(status_code=502, detail=f"TTS服务返回错误: HTTP{resp.status_code}")
    return resp.content


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
    logger.info(f"🎤 语音后端 v1.0 启动 - 端口 {PORT}")
    logger.info(f"  人格后端: {PERSONALITY_SERVER_URL}")
    logger.info(f"  ASR: {ASR_BASE_URL} 模型={ASR_MODEL} {'已配置' if ASR_API_KEY else '未配置API Key!'}")
    logger.info(f"  TTS: 引擎={TTS_ENGINE} {'已配置' if TTS_ENGINE == 'edge-tts' or TTS_API_KEY else '未配置API Key!'}")
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


app = FastAPI(title="Voice Server", version="1.0.0", lifespan=lifespan)
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


class VoiceChatResponse(BaseModel):
    success: bool
    asr_text: str = ""
    reply: str = ""
    audio_base64: str = ""
    audio_format: str = "mp3"
    session_id: Optional[str] = None
    error: str = ""


# ============================================================
# 接口
# ============================================================
@app.get("/health")
async def health():
    return {
        "status": "ok", "service": "voice_server", "version": "1.0.0", "port": str(PORT),
        "asr_configured": bool(ASR_API_KEY),
        "tts_engine": TTS_ENGINE,
        "personality_url": PERSONALITY_SERVER_URL,
    }


@app.post("/api/voice/chat", response_model=VoiceChatResponse)
async def voice_chat(request: VoiceChatRequest):
    """
    语音聊天完整流程：
    1. base64音频 → ASR语音转文本
    2. 文本 → 人格后端生成回复
    3. 回复文本 → TTS文本转语音
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
        out_format = "mp3" if TTS_ENGINE == "edge-tts" else TTS_FORMAT
        try:
            audio_out = await text_to_speech(reply_text, tts_role)
            audio_b64 = base64.b64encode(audio_out).decode("utf-8")
        except HTTPException as te:
            logger.warning(f"[语音聊天] TTS失败，仅返回文本回复: {te.detail}")
        except Exception as te:
            logger.warning(f"[语音聊天] TTS异常，仅返回文本回复: {te}")
        return VoiceChatResponse(
            success=True,
            asr_text=asr_text,
            reply=reply_text,
            audio_base64=audio_b64,
            audio_format=out_format,
            session_id=result.get("session_id"),
        )
    except HTTPException as e:
        return VoiceChatResponse(success=False, error=str(e.detail))
    except Exception as e:
        logger.error(f"[语音聊天] 处理失败: {e}", exc_info=True)
        return VoiceChatResponse(success=False, error=str(e))


@app.post("/api/voice/asr")
async def voice_asr(file: UploadFile = File(...)):
    """
    独立ASR接口：接收音频文件上传，返回识别文本。
    供主后端或其他服务直接调用。
    """
    audio_bytes = await file.read()
    if len(audio_bytes) > MAX_AUDIO_SIZE:
        raise HTTPException(status_code=413, detail="音频文件过大")
    text = await speech_to_text(audio_bytes, file.filename or "audio.wav")
    return {"success": True, "text": text}


@app.post("/api/voice/tts")
async def voice_tts(request: Request):
    """
    独立TTS接口：接收文本+角色，返回音频文件。
    """
    body = await request.json()
    text = body.get("text", "").strip()
    role_id = body.get("role_id", "nianqi")
    if not text:
        raise HTTPException(status_code=400, detail="缺少text参数")
    audio_bytes = await text_to_speech(text, role_id)
    out_format = "mp3" if TTS_ENGINE == "edge-tts" else TTS_FORMAT
    return StreamingResponse(
        io.BytesIO(audio_bytes),
        media_type=f"audio/{out_format}",
        headers={"Content-Disposition": f"attachment; filename=tts.{out_format}"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")

"""
NapCat 统一消息发送模块。
封装 OneBot v11 HTTP API，支持私聊/群聊的文本、图片、语音、混合消息发送。

借鉴 NoneBot2 的 Bot.call_api 设计，简化为适配 NapCat 的版本。
"""

import os
import time
import logging
import httpx
from typing import Any, Dict, List, Optional

logger = logging.getLogger("napcat.sender")


# ============================================================
# 配置
# ============================================================

NAPCAT_HTTP_URL = os.getenv("NAPCAT_HTTP_URL", "")
NAPCAT_ACCESS_TOKEN = os.getenv("NAPCAT_ACCESS_TOKEN", "")
SEND_TIMEOUT = 30  # 发送超时30秒


# ============================================================
# 底层API调用
# ============================================================

async def call_napcat_api(api: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    调用 NapCat HTTP API。

    Args:
        api: API名称（如 send_private_msg, send_group_msg）
        data: 请求数据

    Returns:
        API响应JSON，失败返回None
    """
    if not NAPCAT_HTTP_URL:
        logger.error("[NapCatSender] NAPCAT_HTTP_URL 未配置，无法发送消息")
        return None

    url = f"{NAPCAT_HTTP_URL.rstrip('/')}/{api}"
    headers = {"Content-Type": "application/json"}
    if NAPCAT_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {NAPCAT_ACCESS_TOKEN}"

    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=SEND_TIMEOUT) as client:
            resp = await client.post(url, json=data, headers=headers)

        dur = time.perf_counter() - t0
        if resp.status_code == 200:
            result = resp.json()
            # NapCat OneBot v11 返回格式: {"status": "ok", "retcode": 0, "data": {...}}
            if result.get("retcode") == 0 or result.get("status") == "ok":
                logger.info(f"[NapCatSender] {api} 成功 ({dur*1000:.0f}ms)")
                return result
            else:
                logger.error(f"[NapCatSender] {api} 业务失败: retcode={result.get('retcode')} msg={result.get('msg', '')}")
                return None
        else:
            logger.error(f"[NapCatSender] {api} HTTP{resp.status_code}: {resp.text[:200]}")
            return None

    except httpx.TimeoutException:
        logger.error(f"[NapCatSender] {api} 超时 ({SEND_TIMEOUT}s)")
        return None
    except Exception as e:
        logger.error(f"[NapCatSender] {api} 异常: {type(e).__name__}: {e}", exc_info=True)
        return None


# ============================================================
# 消息构建
# ============================================================

def build_text(text: str) -> Dict[str, Any]:
    """构建文本消息段。"""
    return {"type": "text", "data": {"text": text}}


def build_image(image_url: str) -> Dict[str, Any]:
    """
    构建图片消息段。
    NapCat 支持 URL、本地路径、base64。
    """
    return {"type": "image", "data": {"file": image_url}}


def build_voice(audio_url_or_b64: str) -> Dict[str, Any]:
    """构建语音消息段。"""
    return {"type": "record", "data": {"file": audio_url_or_b64}}


def build_at(qq: str, name: str = "") -> Dict[str, Any]:
    """构建@消息段。"""
    data = {"qq": str(qq)}
    if name:
        data["name"] = name
    return {"type": "at", "data": data}


def build_face(face_id: str) -> Dict[str, Any]:
    """构建QQ表情消息段。"""
    return {"type": "face", "data": {"id": str(face_id)}}


def build_reply(message_id: str) -> Dict[str, Any]:
    """构建回复消息段。"""
    return {"type": "reply", "data": {"id": str(message_id)}}


# ============================================================
# 私聊消息发送
# ============================================================

async def send_private_text(user_id: str, text: str) -> bool:
    """发送私聊文本消息。"""
    result = await call_napcat_api("send_private_msg", {
        "user_id": int(user_id),
        "message": [build_text(text)],
    })
    return result is not None


async def send_private_image(user_id: str, image_url: str, text: str = "") -> bool:
    """发送私聊图片消息（可选附带文本）。"""
    message = []
    if text:
        message.append(build_text(text + "\n"))
    message.append(build_image(image_url))

    result = await call_napcat_api("send_private_msg", {
        "user_id": int(user_id),
        "message": message,
    })
    return result is not None


async def send_private_voice(user_id: str, audio_url_or_b64: str) -> bool:
    """发送私聊语音消息。"""
    result = await call_napcat_api("send_private_msg", {
        "user_id": int(user_id),
        "message": [build_voice(audio_url_or_b64)],
    })
    return result is not None


async def send_private_mixed(
    user_id: str,
    text: str = "",
    image_url: str = "",
    audio_url: str = "",
) -> bool:
    """
    发送私聊混合消息（文本+图片+语音）。
    按 文本 → 图片 → 语音 的顺序发送。
    """
    message = []
    if text:
        message.append(build_text(text))
    if image_url:
        if text:
            message.append(build_text("\n"))
        message.append(build_image(image_url))
    if audio_url:
        message.append(build_voice(audio_url))

    if not message:
        return False

    result = await call_napcat_api("send_private_msg", {
        "user_id": int(user_id),
        "message": message,
    })
    return result is not None


# ============================================================
# 群聊消息发送
# ============================================================

async def send_group_text(group_id: str, text: str, at_qq: str = "") -> bool:
    """发送群聊文本消息（可选@某人）。"""
    message = []
    if at_qq:
        message.append(build_at(at_qq))
        message.append(build_text(" "))
    message.append(build_text(text))

    result = await call_napcat_api("send_group_msg", {
        "group_id": int(group_id),
        "message": message,
    })
    return result is not None


async def send_group_image(
    group_id: str,
    image_url: str,
    text: str = "",
    at_qq: str = "",
) -> bool:
    """发送群聊图片消息（可选附带文本和@）。"""
    message = []
    if at_qq:
        message.append(build_at(at_qq))
        message.append(build_text(" "))
    if text:
        message.append(build_text(text + "\n"))
    message.append(build_image(image_url))

    result = await call_napcat_api("send_group_msg", {
        "group_id": int(group_id),
        "message": message,
    })
    return result is not None


async def send_group_voice(group_id: str, audio_url_or_b64: str) -> bool:
    """发送群聊语音消息。"""
    result = await call_napcat_api("send_group_msg", {
        "group_id": int(group_id),
        "message": [build_voice(audio_url_or_b64)],
    })
    return result is not None


async def send_group_mixed(
    group_id: str,
    text: str = "",
    image_url: str = "",
    audio_url: str = "",
    at_qq: str = "",
) -> bool:
    """发送群聊混合消息。"""
    message = []
    if at_qq:
        message.append(build_at(at_qq))
        message.append(build_text(" "))
    if text:
        message.append(build_text(text))
    if image_url:
        if text:
            message.append(build_text("\n"))
        message.append(build_image(image_url))
    if audio_url:
        message.append(build_voice(audio_url))

    if not message:
        return False

    result = await call_napcat_api("send_group_msg", {
        "group_id": int(group_id),
        "message": message,
    })
    return result is not None


# ============================================================
# 主动发图（P2/P3 联动）
# ============================================================

async def send_proactive_image_to_qq(
    qq_number: str,
    image_url: str,
    text: str = "",
    is_group: bool = False,
    group_id: str = "",
) -> bool:
    """
    发送主动发图到QQ（私聊或群聊）。
    与 proactive_server 和 image_generator 联动。

    Args:
        qq_number: 目标QQ号（私聊）或要@的QQ号（群聊）
        image_url: 图片URL
        text: 附带文本（角色的开场白）
        is_group: 是否群聊
        group_id: 群号（is_group=True时必填）

    Returns:
        是否发送成功
    """
    if is_group:
        if not group_id:
            logger.error("[ProactiveImage] 群聊发送需要group_id")
            return False
        return await send_group_image(group_id, image_url, text, at_qq=qq_number)
    else:
        return await send_private_image(qq_number, image_url, text)


# ============================================================
# 工具函数
# ============================================================

def is_available() -> bool:
    """检查NapCat HTTP API是否可用（配置了URL）。"""
    return bool(NAPCAT_HTTP_URL)


def get_status() -> Dict[str, Any]:
    """获取发送模块状态。"""
    return {
        "available": is_available(),
        "napcat_url": NAPCAT_HTTP_URL[:30] + "..." if NAPCAT_HTTP_URL else "",
        "has_token": bool(NAPCAT_ACCESS_TOKEN),
        "timeout": SEND_TIMEOUT,
    }

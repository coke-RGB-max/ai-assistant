"""
NapCat 图片消息理解模块。
用户发图片给角色，角色用豆包多模态模型"看到"图片内容并回复。

功能：
1. 从NapCat消息提取图片URL
2. 下载图片并转base64
3. 调用豆包多模态模型理解图片内容
4. 将图片描述传给人格系统
"""

import os
import base64
import logging
import httpx
from typing import Any, Dict, List, Optional, Tuple

from .message import Message

logger = logging.getLogger("napcat.image")


# ============================================================
# 配置
# ============================================================

# 豆包多模态模型（火山方舟）
# 目前可用的图片理解模型：Doubao-Seed-Evolving（支持图片+视频+文本理解）
# 注意：Seed-1.6/1.8、doubao-vision-pro 等旧版已下线
DOUBAO_VISION_MODEL = os.getenv("DOUBAO_VISION_MODEL", "doubao-seed-evolving")
DOUBAO_VISION_API_KEY = os.getenv("DOUBAO_VISION_API_KEY", os.getenv("DOUBAO_API_KEY", ""))
DOUBAO_VISION_BASE_URL = os.getenv(
    "DOUBAO_VISION_BASE_URL",
    "https://ark.cn-beijing.volces.com/api/v3"
)

# 图片处理配置
MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 最大10MB
IMAGE_DOWNLOAD_TIMEOUT = 30  # 下载超时30秒
VISION_API_TIMEOUT = 60  # 多模态API超时60秒


# ============================================================
# 图片下载
# ============================================================

async def download_image(image_url: str) -> Optional[bytes]:
    """
    下载图片。

    Args:
        image_url: 图片URL（NapCat返回的图片URL，可能是http链接或本地路径）

    Returns:
        图片字节数据，失败返回None
    """
    if not image_url:
        return None

    # 处理base64格式的图片
    if image_url.startswith("base64://"):
        try:
            return base64.b64decode(image_url[len("base64://"):])
        except Exception as e:
            logger.error(f"[Image] base64解码失败: {e}")
            return None

    # 处理本地文件路径
    if image_url.startswith("file://") or os.path.exists(image_url):
        path = image_url[len("file://"):] if image_url.startswith("file://") else image_url
        try:
            with open(path, "rb") as f:
                data = f.read()
            if len(data) > MAX_IMAGE_SIZE:
                logger.warning(f"[Image] 图片过大: {len(data)}B > {MAX_IMAGE_SIZE}B")
                return None
            return data
        except Exception as e:
            logger.error(f"[Image] 读取本地文件失败: {e}")
            return None

    # HTTP下载
    if not image_url.startswith("http"):
        logger.warning(f"[Image] 不支持的图片URL格式: {image_url[:50]}")
        return None

    try:
        async with httpx.AsyncClient(timeout=IMAGE_DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
            resp = await client.get(image_url)
            if resp.status_code != 200:
                logger.error(f"[Image] 下载失败 HTTP{resp.status_code}: {image_url[:80]}")
                return None
            data = resp.content
            if len(data) > MAX_IMAGE_SIZE:
                logger.warning(f"[Image] 图片过大: {len(data)}B > {MAX_IMAGE_SIZE}B")
                return None
            logger.info(f"[Image] 下载成功: {len(data)}B")
            return data
    except Exception as e:
        logger.error(f"[Image] 下载异常: {type(e).__name__}: {e}")
        return None


def image_to_base64(image_bytes: bytes) -> str:
    """将图片字节转为base64字符串。"""
    return base64.b64encode(image_bytes).decode("utf-8")


def detect_image_format(image_bytes: bytes) -> str:
    """检测图片格式（通过文件头）。"""
    if image_bytes[:8] == b'\x89PNG\r\n\x1a\n':
        return "png"
    if image_bytes[:3] == b'\xff\xd8\xff':
        return "jpeg"
    if image_bytes[:6] in (b'GIF87a', b'GIF89a'):
        return "gif"
    if image_bytes[:4] == b'RIFF' and image_bytes[8:12] == b'WEBP':
        return "webp"
    return "jpeg"  # 默认jpeg


# ============================================================
# 豆包多模态图片理解
# ============================================================

async def understand_image(
    image_bytes: bytes,
    prompt: str = "请详细描述这张图片的内容，包括画面中的人物、物体、场景、氛围等。",
) -> Optional[str]:
    """
    调用豆包多模态模型理解图片内容。

    Args:
        image_bytes: 图片字节数据
        prompt: 理解提示词

    Returns:
        图片描述文本，失败返回None
    """
    if not DOUBAO_VISION_API_KEY:
        logger.warning("[ImageVision] 未配置 DOUBAO_VISION_API_KEY，图片理解不可用")
        return None

    try:
        img_format = detect_image_format(image_bytes)
        img_b64 = image_to_base64(image_bytes)
        data_url = f"data:image/{img_format};base64,{img_b64}"

        headers = {
            "Authorization": f"Bearer {DOUBAO_VISION_API_KEY}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": DOUBAO_VISION_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
            "max_tokens": 1024,
            "temperature": 0.3,
        }

        url = f"{DOUBAO_VISION_BASE_URL.rstrip('/')}/chat/completions"

        async with httpx.AsyncClient(timeout=VISION_API_TIMEOUT) as client:
            resp = await client.post(url, json=payload, headers=headers)

        if resp.status_code != 200:
            logger.error(f"[ImageVision] API失败 HTTP{resp.status_code}: {resp.text[:200]}")
            return None

        data = resp.json()
        description = data.get("choices", [{}])[0].get("message", {}).get("content", "")

        if description:
            logger.info(f"[ImageVision] 图片理解成功: {description[:80]}...")
            return description.strip()
        else:
            logger.warning(f"[ImageVision] API返回空内容: {data}")
            return None

    except Exception as e:
        logger.error(f"[ImageVision] 图片理解异常: {type(e).__name__}: {e}", exc_info=True)
        return None


# ============================================================
# 图片消息处理（整合下载+理解）
# ============================================================

async def process_image_message(
    message: Message,
    additional_prompt: str = "",
) -> Tuple[Optional[str], List[str]]:
    """
    处理图片消息：下载所有图片并理解内容。

    Args:
        message: 解析后的Message对象
        additional_prompt: 额外的提示词（如角色性格相关的描述要求）

    Returns:
        (combined_description, image_urls)
        - combined_description: 所有图片的合并描述，失败返回None
        - image_urls: 成功处理的图片URL列表
    """
    image_urls = message.get_image_urls()
    if not image_urls:
        return None, []

    logger.info(f"[Image] 消息包含 {len(image_urls)} 张图片")

    descriptions = []
    processed_urls = []

    base_prompt = "请详细描述这张图片的内容，包括画面中的人物、物体、场景、氛围等。"
    if additional_prompt:
        base_prompt += f"\n\n额外要求：{additional_prompt}"

    for i, img_url in enumerate(image_urls):
        logger.info(f"[Image] 处理第 {i+1}/{len(image_urls)} 张图片: {img_url[:60]}...")

        # 下载图片
        img_bytes = await download_image(img_url)
        if img_bytes is None:
            logger.warning(f"[Image] 第 {i+1} 张图片下载失败，跳过")
            continue

        # 理解图片
        desc = await understand_image(img_bytes, base_prompt)
        if desc:
            descriptions.append(f"[图片{i+1}]: {desc}")
            processed_urls.append(img_url)
        else:
            logger.warning(f"[Image] 第 {i+1} 张图片理解失败")

    if not descriptions:
        return None, processed_urls

    combined = "\n\n".join(descriptions)
    logger.info(f"[Image] 成功理解 {len(descriptions)}/{len(image_urls)} 张图片")
    return combined, processed_urls


def is_image_message(message: Message) -> bool:
    """判断消息是否包含图片。"""
    return message.has_image()

"""
NapCat OneBot v11 集成模块（P3 新增）。
借鉴 NoneBot2 的 MessageSegment/Event/Bot 设计，简化为适配 NapCat 的版本。

模块结构：
- message: 消息段解析（文字/图片/语音/@/表情）
- group_router: 群聊多角色调度
- image_handler: 图片消息接收+多模态理解
- sender: 统一消息发送（文本+图片+语音，私聊+群聊）
"""

from .message import (
    MessageSegment,
    Message,
    parse_message,
    build_text_message,
    build_image_message,
    build_voice_message,
)
from .group_router import (
    GroupRoleConfig,
    GroupConfig,
    GroupConfigManager,
    GroupMessageContext,
    GroupMessageRouter,
    get_group_config_manager,
    get_group_router,
)
from .image_handler import (
    download_image,
    understand_image,
    process_image_message,
    is_image_message,
)
from .sender import (
    send_private_text,
    send_private_image,
    send_private_voice,
    send_private_mixed,
    send_group_text,
    send_group_image,
    send_group_voice,
    send_group_mixed,
    send_proactive_image_to_qq,
    is_available,
    get_status,
)

__all__ = [
    # message
    "MessageSegment", "Message", "parse_message",
    "build_text_message", "build_image_message", "build_voice_message",
    # group_router
    "GroupRoleConfig", "GroupConfig", "GroupConfigManager",
    "GroupMessageContext", "GroupMessageRouter",
    "get_group_config_manager", "get_group_router",
    # image_handler
    "download_image", "understand_image", "process_image_message", "is_image_message",
    # sender
    "send_private_text", "send_private_image", "send_private_voice", "send_private_mixed",
    "send_group_text", "send_group_image", "send_group_voice", "send_group_mixed",
    "send_proactive_image_to_qq", "is_available", "get_status",
]

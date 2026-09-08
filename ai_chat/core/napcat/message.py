"""
NapCat OneBot v11 消息段解析模块。
借鉴 NoneBot2 的 MessageSegment 设计，简化为适配 NapCat 的版本。

支持的消息段类型：
- text: 纯文本
- image: 图片
- record: 语音
- at: @提及
- face: QQ表情
- reply: 回复消息
- node: 合并转发
- poke: 戳一戳
- gift: 礼物/红包
- json: JSON卡片（如分享链接）
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class MessageSegment:
    """OneBot v11 消息段。"""
    type: str
    data: Dict[str, Any] = field(default_factory=dict)

    def is_text(self) -> bool:
        return self.type == "text"

    def is_image(self) -> bool:
        return self.type == "image"

    def is_voice(self) -> bool:
        return self.type == "record"

    def is_at(self) -> bool:
        return self.type == "at"

    def is_face(self) -> bool:
        return self.type == "face"

    def is_reply(self) -> bool:
        return self.type == "reply"

    def is_gift(self) -> bool:
        return self.type in ("gift", "redbag")

    def get_text(self) -> str:
        """获取文本内容（仅text类型）。"""
        return self.data.get("text", "") if self.is_text() else ""

    def get_image_url(self) -> Optional[str]:
        """获取图片URL。"""
        if self.is_image():
            return self.data.get("url") or self.data.get("file")
        return None

    def get_at_qq(self) -> Optional[str]:
        """获取@的QQ号。"""
        if self.is_at():
            qq = self.data.get("qq")
            return str(qq) if qq else None
        return None

    def get_face_id(self) -> Optional[str]:
        """获取表情ID。"""
        if self.is_face():
            return str(self.data.get("id", ""))
        return None


class Message(List[MessageSegment]):
    """OneBot v11 消息序列（消息段列表）。"""

    def __init__(self, message: Any = None):
        super().__init__()
        if message is None:
            return
        if isinstance(message, str):
            # 纯文本直接作为一个text段
            self.append(MessageSegment(type="text", data={"text": message}))
        elif isinstance(message, MessageSegment):
            self.append(message)
        elif isinstance(message, list):
            # OneBot v11 message 数组格式: [{"type": "text", "data": {"text": "..."}}, ...]
            for seg in message:
                if isinstance(seg, dict):
                    self.append(MessageSegment(
                        type=seg.get("type", "unknown"),
                        data=seg.get("data", {}),
                    ))
                elif isinstance(seg, MessageSegment):
                    self.append(seg)

    def extract_plain_text(self) -> str:
        """提取所有纯文本段的内容。"""
        return "".join(seg.get_text() for seg in self if seg.is_text())

    def has_image(self) -> bool:
        """是否包含图片。"""
        return any(seg.is_image() for seg in self)

    def has_voice(self) -> bool:
        """是否包含语音。"""
        return any(seg.is_voice() for seg in self)

    def has_at(self) -> bool:
        """是否包含@。"""
        return any(seg.is_at() for seg in self)

    def has_at_me(self, bot_qq: str) -> bool:
        """是否@了机器人。"""
        for seg in self:
            if seg.is_at():
                qq = seg.get_at_qq()
                if qq == str(bot_qq) or qq == "all":
                    return True
        return False

    def get_at_qq_list(self) -> List[str]:
        """获取所有@的QQ号列表。"""
        return [seg.get_at_qq() for seg in self if seg.is_at() and seg.get_at_qq()]

    def get_image_urls(self) -> List[str]:
        """获取所有图片URL。"""
        return [seg.get_image_url() for seg in self if seg.is_image() and seg.get_image_url()]

    def get_voice_segments(self) -> List[MessageSegment]:
        """获取所有语音段。"""
        return [seg for seg in self if seg.is_voice()]

    def remove_at_segments(self) -> "Message":
        """移除所有@段，返回新的Message（用于提取用户说的话）。"""
        return Message([seg for seg in self if not seg.is_at()])

    def __str__(self) -> str:
        return self.extract_plain_text()


def parse_message(message: Any) -> Message:
    """
    解析 OneBot v11 message 字段。
    message 可以是：
    - 字符串（纯文本）
    - 数组（消息段列表）
    """
    return Message(message)


def build_text_message(text: str) -> List[Dict[str, Any]]:
    """构建纯文本消息（用于发送）。"""
    return [{"type": "text", "data": {"text": text}}]


def build_image_message(image_url: str, text: str = "") -> List[Dict[str, Any]]:
    """
    构建图片消息（用于发送）。
    NapCat 支持通过 URL 发送图片，也支持 base64。
    """
    msg = []
    if text:
        msg.append({"type": "text", "data": {"text": text}})
    msg.append({"type": "image", "data": {"file": image_url}})
    return msg


def build_voice_message(audio_url_or_b64: str) -> List[Dict[str, Any]]:
    """构建语音消息（用于发送）。"""
    return [{"type": "record", "data": {"file": audio_url_or_b64}}]

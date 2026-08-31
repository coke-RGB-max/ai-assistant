"""
插件基类 - 所有插件继承此类
P4 序号5：插件/技能系统

插件开发指南：
1. 继承 BasePlugin
2. 设置 name、description、version、commands
3. 重写 can_handle() 和 handle() 方法
4. 把插件文件放到 plugins/ 目录，启动时自动加载

示例：
    from plugins.base import BasePlugin

    class WeatherPlugin(BasePlugin):
        name = "weather"
        description = "天气查询插件"
        version = "1.0.0"
        commands = ["天气", "weather", "气温"]

        async def handle(self, message: str, context: dict) -> str:
            return "今天天气晴朗，25度。"
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BasePlugin(ABC):
    """插件基类，所有插件必须继承此类。"""

    # 插件元信息（子类必须设置）
    name: str = "base"              # 插件唯一标识
    description: str = ""           # 插件描述
    version: str = "1.0.0"          # 版本号
    author: str = ""                 # 作者
    commands: List[str] = []         # 触发命令/关键词列表
    enabled: bool = True             # 是否启用

    def __init__(self):
        """插件初始化。可以在这里加载配置、模型等。"""
        pass

    def can_handle(self, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        判断插件是否能处理这条消息。
        默认实现：检查消息中是否包含 commands 中的关键词。
        子类可以重写此方法实现更复杂的判断逻辑。

        Args:
            message: 用户消息文本
            context: 上下文信息（session_id, role_id, user_id, psych_state 等）
        Returns:
            True 表示能处理，False 表示不处理
        """
        if not self.commands:
            return False
        message_lower = message.lower()
        return any(cmd.lower() in message_lower for cmd in self.commands)

    @abstractmethod
    async def handle(self, message: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        处理消息，返回插件生成的回复文本。
        子类必须实现此方法。

        Args:
            message: 用户消息文本
            context: 上下文信息
        Returns:
            插件生成的回复文本。如果返回空字符串，则不拦截，继续走正常 LLM 生成。
        """
        pass

    async def on_before_generate(self, prompt: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        生成前钩子：在 LLM 生成之前调用，可以修改 prompt。
        子类可选重写。

        Args:
            prompt: 当前的 prompt
            context: 上下文信息
        Returns:
            修改后的 prompt。如果不需要修改，直接返回原 prompt。
        """
        return prompt

    async def on_after_generate(self, response: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        生成后钩子：在 LLM 生成之后调用，可以修改回复。
        子类可选重写。

        Args:
            response: LLM 生成的回复
            context: 上下文信息
        Returns:
            修改后的回复。如果不需要修改，直接返回原回复。
        """
        return response

    def get_info(self) -> Dict[str, Any]:
        """获取插件信息（用于管理后台展示）。"""
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "author": self.author,
            "commands": self.commands,
            "enabled": self.enabled,
        }

    def __repr__(self):
        return f"<Plugin {self.name} v{self.version}>"

"""
FlexiChrono 插件系统
P4 序号5：插件/技能系统

快速开始：
    from plugins import init_plugins, get_plugin_manager

    # 初始化并加载所有插件
    init_plugins()

    # 获取插件管理器
    manager = get_plugin_manager()

    # 处理消息（插件拦截）
    reply = await manager.process_message(message, context)
    if reply:
        # 插件拦截了消息，直接返回插件的回复
        return reply
    # 否则继续正常 LLM 生成

插件开发：
    1. 在 plugins/ 目录创建 .py 文件
    2. 继承 plugins.base.BasePlugin
    3. 实现 handle() 方法
    4. 启动时自动加载

示例插件：
    - weather.py: 天气查询
    - joke.py: 讲笑话
    - time_plugin.py: 报时
"""

from .base import BasePlugin
from .api import PluginAPI
from .manager import PluginManager, get_plugin_manager, init_plugins

__all__ = [
    "BasePlugin",
    "PluginAPI",
    "PluginManager",
    "get_plugin_manager",
    "init_plugins",
]

__version__ = "1.0.0"

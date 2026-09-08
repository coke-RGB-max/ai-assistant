"""
插件管理器 - 自动扫描、加载、管理插件
P4 序号5：插件/技能系统

功能：
- 启动时自动扫描 plugins/ 目录，加载所有插件
- 支持插件启用/禁用
- 消息处理流水线：插件拦截 → LLM生成 → 插件后处理
- 提供插件状态查询 API

插件加载规则：
1. plugins/ 目录下每个 .py 文件都是一个插件（除了 base.py, api.py, manager.py, __init__.py）
2. 每个文件中必须有且仅有一个继承 BasePlugin 的类
3. 插件类会被自动实例化并注册
4. 插件文件以 _ 开头的会被忽略（如 _draft.py）

环境变量：
- PLUGINS_ENABLED: 是否启用插件系统（默认 true）
- PLUGINS_DIR: 插件目录（默认 ./plugins）
- DISABLED_PLUGINS: 禁用的插件名，逗号分隔（如 "weather,joke"）
"""
import importlib.util
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import BasePlugin
from .api import PluginAPI

logger = logging.getLogger("plugin_manager")


class PluginManager:
    """插件管理器 - 单例模式。"""

    _instance: Optional["PluginManager"] = None

    def __init__(self, plugins_dir: Optional[str] = None):
        self.plugins: Dict[str, BasePlugin] = {}
        self.plugins_dir = Path(plugins_dir or os.getenv("PLUGINS_DIR", "plugins"))
        self.enabled = os.getenv("PLUGINS_ENABLED", "true").lower() == "true"
        self.disabled_plugins = set(
            p.strip() for p in os.getenv("DISABLED_PLUGINS", "").split(",") if p.strip()
        )
        self._loaded = False

    @classmethod
    def get_instance(cls) -> "PluginManager":
        """获取单例。"""
        if cls._instance is None:
            cls._instance = PluginManager()
        return cls._instance

    def load_plugins(self) -> int:
        """
        扫描插件目录，加载所有插件。
        Returns:
            成功加载的插件数量
        """
        if not self.enabled:
            logger.info("[PluginManager] 插件系统已禁用（PLUGINS_ENABLED=false）")
            return 0

        if not self.plugins_dir.exists():
            logger.warning(f"[PluginManager] 插件目录不存在: {self.plugins_dir}")
            return 0

        loaded_count = 0
        # 排除的文件名（系统文件，不是插件）
        excluded = {"__init__.py", "base.py", "api.py", "manager.py"}

        for plugin_file in sorted(self.plugins_dir.glob("*.py")):
            # 跳过系统文件和以 _ 开头的草稿文件
            if plugin_file.name in excluded or plugin_file.name.startswith("_"):
                continue

            try:
                plugin = self._load_single_plugin(plugin_file)
                if plugin:
                    # 检查是否在禁用列表
                    if plugin.name in self.disabled_plugins:
                        plugin.enabled = False
                        logger.info(f"[PluginManager] 插件已禁用: {plugin.name}")
                    self.plugins[plugin.name] = plugin
                    loaded_count += 1
                    logger.info(f"[PluginManager] 加载插件成功: {plugin.name} v{plugin.version} - {plugin.description}")
            except Exception as e:
                logger.error(f"[PluginManager] 加载插件失败 {plugin_file.name}: {e}", exc_info=True)

        self._loaded = True
        logger.info(f"[PluginManager] 插件加载完成，共 {loaded_count} 个插件")
        return loaded_count

    def _load_single_plugin(self, plugin_file: Path) -> Optional[BasePlugin]:
        """加载单个插件文件。"""
        module_name = f"plugins_{plugin_file.stem}"

        # 动态导入模块
        spec = importlib.util.spec_from_file_location(module_name, plugin_file)
        if spec is None or spec.loader is None:
            raise ImportError(f"无法创建模块规范: {plugin_file}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # 查找继承 BasePlugin 的类
        plugin_class = None
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, BasePlugin)
                and attr is not BasePlugin
            ):
                plugin_class = attr
                break

        if plugin_class is None:
            raise ValueError(f"插件文件 {plugin_file.name} 中没有找到继承 BasePlugin 的类")

        # 实例化插件
        plugin = plugin_class()

        # 注入 PluginAPI
        plugin.api = PluginAPI(plugin_name=plugin.name)

        return plugin

    def get_plugin(self, name: str) -> Optional[BasePlugin]:
        """获取指定插件。"""
        return self.plugins.get(name)

    def list_plugins(self, include_disabled: bool = True) -> List[Dict[str, Any]]:
        """列出所有插件信息。"""
        result = []
        for plugin in self.plugins.values():
            if not include_disabled and not plugin.enabled:
                continue
            result.append(plugin.get_info())
        return result

    def enable_plugin(self, name: str) -> bool:
        """启用插件。"""
        plugin = self.plugins.get(name)
        if plugin:
            plugin.enabled = True
            logger.info(f"[PluginManager] 启用插件: {name}")
            return True
        return False

    def disable_plugin(self, name: str) -> bool:
        """禁用插件。"""
        plugin = self.plugins.get(name)
        if plugin:
            plugin.enabled = False
            logger.info(f"[PluginManager] 禁用插件: {name}")
            return True
        return False

    async def process_message(self, message: str, context: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        消息处理流水线：让所有启用的插件尝试处理消息。
        如果某个插件返回非空字符串，则拦截消息，直接返回插件的回复。
        如果所有插件都返回空，则返回 None，表示继续走正常 LLM 生成。

        Args:
            message: 用户消息
            context: 上下文（session_id, role_id, user_id 等）
        Returns:
            插件生成的回复，或 None（继续 LLM 生成）
        """
        if not self.enabled or not self._loaded:
            return None

        context = context or {}
        for plugin in self.plugins.values():
            if not plugin.enabled:
                continue
            try:
                if plugin.can_handle(message, context):
                    logger.info(f"[PluginManager] 插件 {plugin.name} 拦截消息: {message[:30]}")
                    reply = await plugin.handle(message, context)
                    if reply:
                        return reply
            except Exception as e:
                logger.error(f"[PluginManager] 插件 {plugin.name} 处理消息失败: {e}", exc_info=True)
                # 插件失败不影响其他插件和正常流程
                continue
        return None

    async def process_before_generate(self, prompt: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        生成前钩子：所有启用的插件可以修改 prompt。
        Args:
            prompt: 原始 prompt
            context: 上下文
        Returns:
            修改后的 prompt
        """
        if not self.enabled or not self._loaded:
            return prompt

        current_prompt = prompt
        for plugin in self.plugins.values():
            if not plugin.enabled:
                continue
            try:
                current_prompt = await plugin.on_before_generate(current_prompt, context)
            except Exception as e:
                logger.error(f"[PluginManager] 插件 {plugin.name} before_generate 失败: {e}", exc_info=True)
        return current_prompt

    async def process_after_generate(self, response: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        生成后钩子：所有启用的插件可以修改回复。
        Args:
            response: LLM 生成的回复
            context: 上下文
        Returns:
            修改后的回复
        """
        if not self.enabled or not self._loaded:
            return response

        current_response = response
        for plugin in self.plugins.values():
            if not plugin.enabled:
                continue
            try:
                current_response = await plugin.on_after_generate(current_response, context)
            except Exception as e:
                logger.error(f"[PluginManager] 插件 {plugin.name} after_generate 失败: {e}", exc_info=True)
        return current_response

    def get_status(self) -> Dict[str, Any]:
        """获取插件系统状态（用于健康检查/管理后台）。"""
        return {
            "enabled": self.enabled,
            "plugins_dir": str(self.plugins_dir),
            "total": len(self.plugins),
            "enabled_count": sum(1 for p in self.plugins.values() if p.enabled),
            "disabled_count": sum(1 for p in self.plugins.values() if not p.enabled),
            "plugins": self.list_plugins(),
        }


# 全局单例快捷函数
def get_plugin_manager() -> PluginManager:
    """获取插件管理器单例。"""
    return PluginManager.get_instance()


def init_plugins(plugins_dir: Optional[str] = None) -> int:
    """初始化插件系统并加载所有插件。"""
    manager = PluginManager.get_instance()
    if plugins_dir:
        manager.plugins_dir = Path(plugins_dir)
    return manager.load_plugins()

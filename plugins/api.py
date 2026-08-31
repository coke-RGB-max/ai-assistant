"""
插件 API - 提供给插件调用的核心能力接口
P4 序号5：插件/技能系统

插件可以通过 PluginAPI 调用系统核心能力，不需要直接访问内部模块。
所有方法都是异步的，插件在 handle() 中可以 await 调用。

可用能力：
- get_personality_state(): 获取角色心理状态
- get_memory(): 查询记忆
- add_memory(): 添加记忆
- generate_image(): 生成角色自拍
- get_intimacy(): 获取亲密度
- set_intimacy(): 设置亲密度
- send_proactive(): 触发主动消息
- get_roles(): 获取角色列表
- http_get/post(): 通用 HTTP 请求（插件可以调用外部 API）
"""
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("plugin_api")


class PluginAPI:
    """
    插件 API - 插件调用系统核心能力的统一接口。
    每个插件实例都会持有一个 PluginAPI 对象，通过 self.api 访问。
    """

    def __init__(self, plugin_name: str = "unknown"):
        self.plugin_name = plugin_name
        self.personality_url = os.getenv("PERSONALITY_SERVER_URL", "http://127.0.0.1:8002")
        self.vector_url = os.getenv("VECTOR_SERVER_URL", "http://127.0.0.1:8001")
        self.proactive_url = os.getenv("PROACTIVE_SERVER_URL", "http://127.0.0.1:8003")
        self.main_url = os.getenv("MAIN_SERVER_URL", "http://127.0.0.1:8000")
        self.internal_token = os.getenv("INTERNAL_TOKEN", "")
        self.timeout = 10.0

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.internal_token:
            headers["Authorization"] = f"Bearer {self.internal_token}"
        return headers

    # ============================================================
    # 人格状态
    # ============================================================
    async def get_personality_state(self, session_id: str, role_id: str) -> Optional[Dict[str, Any]]:
        """
        获取角色的心理状态。
        Returns:
            心理状态字典（mood, energy, intimacy, emotion 等），失败返回 None
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(
                    f"{self.personality_url}/api/session/{session_id}/state",
                    headers=self._headers(),
                )
                if resp.status_code == 200:
                    data = resp.json()
                    # 找到指定角色的状态
                    if isinstance(data, dict) and "states" in data:
                        return data["states"].get(role_id)
                    return data
        except Exception as e:
            logger.warning(f"[PluginAPI][{self.plugin_name}] 获取心理状态失败: {e}")
        return None

    # ============================================================
    # 记忆
    # ============================================================
    async def get_memory(self, session_id: str, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        查询记忆。
        Args:
            session_id: 会话ID
            query: 查询文本
            top_k: 返回条数
        Returns:
            记忆列表，每条包含 content, timestamp, importance 等
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    f"{self.vector_url}/api/memory/search",
                    json={"session_id": session_id, "query": query, "top_k": top_k},
                    headers=self._headers(),
                )
                if resp.status_code == 200:
                    return resp.json().get("memories", [])
        except Exception as e:
            logger.warning(f"[PluginAPI][{self.plugin_name}] 查询记忆失败: {e}")
        return []

    async def add_memory(self, session_id: str, content: str, importance: float = 0.5,
                         memory_type: str = "plugin") -> bool:
        """
        添加记忆。
        Args:
            session_id: 会话ID
            content: 记忆内容
            importance: 重要度 0-1
            memory_type: 记忆类型
        Returns:
            是否成功
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    f"{self.vector_url}/api/memory/add",
                    json={
                        "session_id": session_id,
                        "content": content,
                        "importance": importance,
                        "type": memory_type,
                    },
                    headers=self._headers(),
                )
                return resp.status_code == 200
        except Exception as e:
            logger.warning(f"[PluginAPI][{self.plugin_name}] 添加记忆失败: {e}")
        return False

    # ============================================================
    # 亲密度
    # ============================================================
    async def get_intimacy(self, user_id: str, role_id: str) -> Optional[int]:
        """获取用户对角色的亲密度（0-100）。"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(
                    f"{self.proactive_url}/api/status/{user_id}",
                    headers=self._headers(),
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list):
                        for item in data:
                            if item.get("role_id") == role_id:
                                return int(item.get("intimacy", 0))
        except Exception as e:
            logger.warning(f"[PluginAPI][{self.plugin_name}] 获取亲密度失败: {e}")
        return None

    # ============================================================
    # 图像生成
    # ============================================================
    async def generate_image(self, role_id: str, prompt: str = "",
                              mode: str = "direct") -> Optional[Dict[str, Any]]:
        """
        生成角色自拍图片。
        Args:
            role_id: 角色ID
            prompt: 额外提示词（场景/服装/表情）
            mode: 模式 direct/mirror
        Returns:
            图片信息字典（url, base64, path 等），失败返回 None
        """
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    f"{self.personality_url}/api/image/selfie",
                    json={"role_id": role_id, "prompt": prompt, "mode": mode},
                    headers=self._headers(),
                )
                if resp.status_code == 200:
                    return resp.json()
        except Exception as e:
            logger.warning(f"[PluginAPI][{self.plugin_name}] 生成图片失败: {e}")
        return None

    # ============================================================
    # 角色信息
    # ============================================================
    async def get_roles(self) -> List[Dict[str, Any]]:
        """获取所有角色列表。"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(f"{self.personality_url}/api/roles")
                if resp.status_code == 200:
                    return resp.json().get("roles", [])
        except Exception as e:
            logger.warning(f"[PluginAPI][{self.plugin_name}] 获取角色列表失败: {e}")
        return []

    # ============================================================
    # 通用 HTTP（插件可以调用外部 API）
    # ============================================================
    async def http_get(self, url: str, params: Optional[Dict] = None,
                       headers: Optional[Dict] = None, timeout: float = 10.0) -> Optional[Dict]:
        """
        通用 HTTP GET 请求（插件调用外部 API 用）。
        Returns:
            响应 JSON 字典，失败返回 None
        """
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(url, params=params, headers=headers or {})
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(f"[PluginAPI][{self.plugin_name}] HTTP GET {url} 返回 {resp.status_code}")
        except Exception as e:
            logger.warning(f"[PluginAPI][{self.plugin_name}] HTTP GET 失败: {e}")
        return None

    async def http_post(self, url: str, json_data: Optional[Dict] = None,
                        headers: Optional[Dict] = None, timeout: float = 10.0) -> Optional[Dict]:
        """通用 HTTP POST 请求。"""
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(url, json=json_data, headers=headers or {})
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(f"[PluginAPI][{self.plugin_name}] HTTP POST {url} 返回 {resp.status_code}")
        except Exception as e:
            logger.warning(f"[PluginAPI][{self.plugin_name}] HTTP POST 失败: {e}")
        return None

    # ============================================================
    # 日志
    # ============================================================
    def log(self, message: str, level: str = "info"):
        """插件日志（统一前缀，方便排查）。"""
        msg = f"[Plugin:{self.plugin_name}] {message}"
        if level == "warning":
            logger.warning(msg)
        elif level == "error":
            logger.error(msg)
        else:
            logger.info(msg)

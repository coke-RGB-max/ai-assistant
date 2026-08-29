"""
多模型抽象层 + 智能降级路由
P1 模块1：统一 LLM 调用接口，支持4个模型3级降级，连续失败3次自动切换。

模型优先级：
  1级（主力）：豆包（火山方舟）
  2级（同级轮流）：Kimi（官方） + DeepSeek（火山方舟）
  3级（兜底）：千问（火山方舟）
  备选（未启用）：DeepSeek 官方 API

降级规则：
  - 连续失败 FAIL_THRESHOLD（默认3）次，自动降级到下一级
  - 降级后每 RECOVER_INTERVAL（默认60秒）放行一次探测，成功则恢复
  - 2级的 Kimi 和 DeepSeek 同级，用 round-robin 轮流使用
  - 所有模型调用统一接口，上层代码无感知

用法：
    from core.llm_router import get_llm_router

    router = get_llm_router()
    result = await router.chat(
        messages=[{"role": "user", "content": "你好"}],
        temperature=0.9,
        max_tokens=500,
    )
"""
import asyncio
import time
import logging
import random
from typing import Optional, List, Dict, Any, AsyncGenerator

import httpx

logger = logging.getLogger("llm_router")

# ============================================================
# 配置（从环境变量读取）
# ============================================================
import os

# 降级参数
FAIL_THRESHOLD = int(os.getenv("LLM_FAIL_THRESHOLD", "3"))       # 连续失败几次降级
RECOVER_INTERVAL = float(os.getenv("LLM_RECOVER_INTERVAL", "60"))  # 降级后多少秒探测一次
REQUEST_TIMEOUT = float(os.getenv("LLM_REQUEST_TIMEOUT", "60"))     # 单次请求超时（秒）
MAX_RETRIES_PER_MODEL = int(os.getenv("LLM_MAX_RETRIES", "2"))     # 单个模型内部重试次数

# ---- 1级：豆包（火山方舟）----
DOUBAO_API_KEY = os.getenv("DOUBAO_API_KEY", "")
DOUBAO_BASE_URL = os.getenv("DOUBAO_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3")
DOUBAO_MODEL = os.getenv("DOUBAO_MODEL", "")

# ---- 2级：Kimi（官方）----
KIMI_API_KEY = os.getenv("KIMI_API_KEY", "")
KIMI_BASE_URL = os.getenv("KIMI_BASE_URL", "https://api.moonshot.cn/v1")
KIMI_MODEL = os.getenv("KIMI_MODEL", "moonshot-v1-8k")

# ---- 2级：DeepSeek（火山方舟接入）----
DEEPSEEK_VOLC_API_KEY = os.getenv("DEEPSEEK_VOLC_API_KEY", DOUBAO_API_KEY)  # 默认和豆包共用
DEEPSEEK_VOLC_BASE_URL = os.getenv("DEEPSEEK_VOLC_BASE_URL", DOUBAO_BASE_URL)
DEEPSEEK_VOLC_MODEL = os.getenv("DEEPSEEK_VOLC_MODEL", "")

# ---- 3级：千问（火山方舟接入）----
QWEN_VOLC_API_KEY = os.getenv("QWEN_VOLC_API_KEY", DOUBAO_API_KEY)  # 默认和豆包共用
QWEN_VOLC_BASE_URL = os.getenv("QWEN_VOLC_BASE_URL", DOUBAO_BASE_URL)
QWEN_VOLC_MODEL = os.getenv("QWEN_VOLC_MODEL", "")

# ---- 备选：DeepSeek 官方（未启用，留作备用）----
DEEPSEEK_OFFICIAL_API_KEY = os.getenv("DEEPSEEK_OFFICIAL_API_KEY", "")
DEEPSEEK_OFFICIAL_BASE_URL = os.getenv("DEEPSEEK_OFFICIAL_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_OFFICIAL_MODEL = os.getenv("DEEPSEEK_OFFICIAL_MODEL", "deepseek-chat")


# ============================================================
# 统一 LLM 客户端基类
# ============================================================
class BaseLLMClient:
    """LLM 客户端基类，所有模型客户端继承此类。"""

    name: str = "base"
    level: int = 0  # 1=主力, 2=同级, 3=兜底

    def __init__(self, api_key: str, base_url: str, model: str):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.available = bool(api_key and model)
        if not self.available:
            logger.warning(f"[{self.name}] 未配置 API Key 或 Model，将不可用")

    async def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.9,
        max_tokens: int = 500,
        json_mode: bool = False,
        timeout: float = REQUEST_TIMEOUT,
    ) -> Optional[str]:
        """
        统一聊天接口。

        Args:
            messages: 对话消息列表
            temperature: 温度
            max_tokens: 最大输出token
            json_mode: 是否强制JSON输出
            timeout: 超时时间

        Returns:
            模型回复文本，失败返回 None
        """
        if not self.available:
            logger.warning(f"[{self.name}] 不可用，跳过")
            return None

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if json_mode:
            payload["response_format"] = {"type": "json_object"}

        t0 = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
            dur = time.perf_counter() - t0

            if resp.status_code == 200:
                content = resp.json()["choices"][0]["message"]["content"]
                logger.info(
                    f"[LLM][{self.name}] 成功 耗时={dur:.2f}s "
                    f"输出长度={len(content) if content else 0}"
                )
                return content
            elif resp.status_code == 400:
                logger.error(f"[LLM][{self.name}] 400(不重试): {resp.text[:200]}")
                return None
            elif resp.status_code == 401:
                logger.error(f"[LLM][{self.name}] 401(API Key无效): {resp.text[:200]}")
                return None
            else:
                logger.warning(f"[LLM][{self.name}] HTTP{resp.status_code}: {resp.text[:200]}")
                return None

        except httpx.TimeoutException:
            logger.warning(f"[LLM][{self.name}] 超时 ({timeout}s)")
            return None
        except httpx.ConnectError as e:
            logger.warning(f"[LLM][{self.name}] 连接失败: {e}")
            return None
        except Exception as e:
            logger.warning(f"[LLM][{self.name}] 异常: {type(e).__name__}: {e}")
            return None

    async def chat_stream(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.9,
        max_tokens: int = 500,
        timeout: float = REQUEST_TIMEOUT,
    ) -> AsyncGenerator[str, None]:
        """流式聊天接口（子类可覆盖，默认不支持流式则返回空）"""
        logger.warning(f"[{self.name}] 流式调用未实现，降级为非流式")
        result = await self.chat(messages, temperature, max_tokens, timeout=timeout)
        if result:
            yield result


# ============================================================
# 各模型客户端
# ============================================================
class DoubaoClient(BaseLLMClient):
    """1级：豆包（火山方舟）"""
    name = "doubao"
    level = 1


class KimiClient(BaseLLMClient):
    """2级：Kimi（官方）"""
    name = "kimi"
    level = 2


class DeepSeekVolcClient(BaseLLMClient):
    """2级：DeepSeek（火山方舟接入）"""
    name = "deepseek_volc"
    level = 2


class QwenVolcClient(BaseLLMClient):
    """3级：千问（火山方舟接入）"""
    name = "qwen_volc"
    level = 3


class DeepSeekOfficialClient(BaseLLMClient):
    """备选：DeepSeek 官方 API（未启用，留作备用）"""
    name = "deepseek_official"
    level = 99  # 不参与自动降级，手动启用


# ============================================================
# 智能降级路由器
# ============================================================
class LLMRouter:
    """
    多模型智能降级路由器。

    优先级：豆包(1级) → Kimi/DeepSeek(2级轮流) → 千问(3级)
    连续失败3次自动降级，降级后每60秒探测一次，成功则恢复。
    """

    def __init__(self):
        # 先初始化状态属性（必须在 _init_clients 之前，因为 _init_clients 会用到）
        self.clients: Dict[str, BaseLLMClient] = {}
        self._current_level = 1  # 当前使用的级别
        self._fail_counts: Dict[str, int] = {}  # 各模型连续失败次数
        self._level_circuit: Dict[int, bool] = {}  # 各级别是否熔断
        self._last_probe_time: Dict[int, float] = {}  # 各级别最后探测时间
        self._level2_index = 0  # 2级 round-robin 索引

        # 初始化各客户端
        self._init_clients()

        # 统计
        self._call_count = 0
        self._fail_count = 0
        self._level_switch_count = 0

        self._log_status()

    def _init_clients(self):
        """初始化所有模型客户端。"""
        # 1级：豆包
        if DOUBAO_API_KEY and DOUBAO_MODEL:
            self.clients["doubao"] = DoubaoClient(DOUBAO_API_KEY, DOUBAO_BASE_URL, DOUBAO_MODEL)

        # 2级：Kimi
        if KIMI_API_KEY and KIMI_MODEL:
            self.clients["kimi"] = KimiClient(KIMI_API_KEY, KIMI_BASE_URL, KIMI_MODEL)

        # 2级：DeepSeek（火山方舟）
        if DEEPSEEK_VOLC_API_KEY and DEEPSEEK_VOLC_MODEL:
            self.clients["deepseek_volc"] = DeepSeekVolcClient(
                DEEPSEEK_VOLC_API_KEY, DEEPSEEK_VOLC_BASE_URL, DEEPSEEK_VOLC_MODEL
            )

        # 3级：千问（火山方舟）
        if QWEN_VOLC_API_KEY and QWEN_VOLC_MODEL:
            self.clients["qwen_volc"] = QwenVolcClient(
                QWEN_VOLC_API_KEY, QWEN_VOLC_BASE_URL, QWEN_VOLC_MODEL
            )

        # 备选：DeepSeek 官方（不自动启用）
        if DEEPSEEK_OFFICIAL_API_KEY:
            self.clients["deepseek_official"] = DeepSeekOfficialClient(
                DEEPSEEK_OFFICIAL_API_KEY, DEEPSEEK_OFFICIAL_BASE_URL, DEEPSEEK_OFFICIAL_MODEL
            )

        # 初始化失败计数和熔断状态
        for name in self.clients:
            self._fail_counts[name] = 0
        for level in (1, 2, 3):
            self._level_circuit[level] = False
            self._last_probe_time[level] = 0.0

        available = [f"{c.name}(level{c.level})" for c in self.clients.values() if c.available]
        logger.info(f"[LLMRouter] 初始化完成，可用模型: {available}")

    def _log_status(self):
        """输出当前路由状态。"""
        levels = []
        for level in (1, 2, 3):
            clients = [c.name for c in self.clients.values() if c.level == level and c.available]
            status = "熔断" if self._level_circuit.get(level) else "正常"
            levels.append(f"L{level}({status}): {clients}")
        logger.info(f"[LLMRouter] 当前状态: 当前级别=L{self._current_level} | {' | '.join(levels)}")

    def _get_level_clients(self, level: int) -> List[BaseLLMClient]:
        """获取指定级别的可用客户端列表。"""
        return [
            c for c in self.clients.values()
            if c.level == level and c.available
        ]

    def _should_skip_level(self, level: int) -> bool:
        """判断该级别是否应跳过（熔断中且未到探测时间）。"""
        if not self._level_circuit.get(level, False):
            return False
        now = time.time()
        if now - self._last_probe_time.get(level, 0) >= RECOVER_INTERVAL:
            self._last_probe_time[level] = now
            return False  # 到探测时间，放行一次
        return True

    def _select_client(self) -> Optional[BaseLLMClient]:
        """
        根据当前级别和降级状态选择客户端。

        Returns:
            选中的客户端，None表示所有级别都不可用
        """
        # 从当前级别开始，逐级向下找可用的
        for level in range(self._current_level, 4):
            if self._should_skip_level(level):
                continue
            clients = self._get_level_clients(level)
            if not clients:
                continue

            if level == 2 and len(clients) > 1:
                # 2级 round-robin 轮流
                client = clients[self._level2_index % len(clients)]
                self._level2_index += 1
            else:
                client = clients[0]

            # 如果选中的级别比当前级别高（说明是探测恢复），更新当前级别
            if level < self._current_level:
                logger.info(f"[LLMRouter] 探测恢复到 L{level}，使用 {client.name}")
                self._current_level = level
                self._level_switch_count += 1

            return client

        return None

    def _on_success(self, client: BaseLLMClient):
        """调用成功处理。"""
        self._fail_counts[client.name] = 0
        self._call_count += 1

        # 如果该级别之前熔断了，现在恢复
        if self._level_circuit.get(client.level, False):
            self._level_circuit[client.level] = False
            logger.info(f"[LLMRouter] L{client.level} 熔断恢复")
            self._log_status()

    def _on_fail(self, client: BaseLLMClient):
        """调用失败处理。"""
        self._fail_counts[client.name] = self._fail_counts.get(client.name, 0) + 1
        self._fail_count += 1

        # 连续失败达到阈值，熔断该级别
        if self._fail_counts[client.name] >= FAIL_THRESHOLD:
            if not self._level_circuit.get(client.level, False):
                self._level_circuit[client.level] = True
                self._last_probe_time[client.level] = time.time()
                logger.warning(
                    f"[LLMRouter] {client.name} 连续失败{self._fail_counts[client.name]}次，"
                    f"L{client.level} 熔断{RECOVER_INTERVAL:.0f}s"
                )

                # 降级到下一级
                if client.level == self._current_level and self._current_level < 3:
                    self._current_level += 1
                    self._level_switch_count += 1
                    logger.info(f"[LLMRouter] 降级到 L{self._current_level}")

                self._log_status()

    async def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.9,
        max_tokens: int = 500,
        json_mode: bool = False,
    ) -> Optional[str]:
        """
        智能降级聊天接口。自动选择可用模型，失败自动降级。

        Args:
            messages: 对话消息
            temperature: 温度
            max_tokens: 最大输出
            json_mode: JSON模式

        Returns:
            模型回复，全部失败返回 None
        """
        # 最多尝试3个级别
        for _ in range(3):
            client = self._select_client()
            if client is None:
                logger.error("[LLMRouter] 所有级别都不可用")
                return None

            # 单个模型内部重试
            for attempt in range(MAX_RETRIES_PER_MODEL + 1):
                result = await client.chat(
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    json_mode=json_mode,
                )
                if result is not None:
                    self._on_success(client)
                    return result

            # 该模型重试全部失败
            self._on_fail(client)

        logger.error("[LLMRouter] 所有模型调用失败")
        return None

    def get_status(self) -> Dict[str, Any]:
        """获取路由器状态（用于调试/管理员查看）。"""
        return {
            "current_level": self._current_level,
            "level_circuit": dict(self._level_circuit),
            "fail_counts": dict(self._fail_counts),
            "available_models": [
                {"name": c.name, "level": c.level, "available": c.available}
                for c in self.clients.values()
            ],
            "stats": {
                "total_calls": self._call_count,
                "total_fails": self._fail_count,
                "level_switches": self._level_switch_count,
            },
        }


# ============================================================
# 全局单例
# ============================================================
_router_instance: Optional[LLMRouter] = None
_router_lock = asyncio.Lock()


async def get_llm_router() -> LLMRouter:
    """获取全局路由器单例（异步初始化）。"""
    global _router_instance
    if _router_instance is None:
        async with _router_lock:
            if _router_instance is None:
                _router_instance = LLMRouter()
    return _router_instance


def get_llm_router_sync() -> LLMRouter:
    """同步获取路由器（用于模块加载时初始化，不等待异步锁）。"""
    global _router_instance
    if _router_instance is None:
        _router_instance = LLMRouter()
    return _router_instance

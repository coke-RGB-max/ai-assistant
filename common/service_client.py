"""
统一服务间 HTTP 客户端
带熔断、重试、健康检查，替代各服务中重复的 httpx 调用代码。
借鉴 AIRI 的可插拔组件架构和主后端手动实现的熔断机制。

特性：
- 自动重试（指数退避）
- 熔断器（连续失败N次后熔断，定期放行探测）
- 超时控制
- 统一日志（耗时、状态码）
- 异步/同步双模式

用法：
    from common.service_client import ServiceClient

    client = ServiceClient(
        name="人格后端",
        base_url="http://127.0.0.1:8002",
        timeout=30.0,
        max_retries=3,
        fail_threshold=3,
        recover_interval=60.0,
    )

    # 异步调用
    resp = await client.post("/api/generate", json={...})
    if resp and resp.status_code == 200:
        data = resp.json()
"""
import time
import logging
import asyncio
from typing import Optional, Dict, Any, Tuple

import httpx

logger = logging.getLogger("service_client")


class CircuitBreaker:
    """熔断器：连续失败达到阈值后熔断，定期放行探测请求。"""

    def __init__(
        self,
        fail_threshold: int = 3,
        recover_interval: float = 60.0,
        name: str = "unknown",
    ):
        self.fail_threshold = fail_threshold
        self.recover_interval = recover_interval
        self.name = name

        self._available = True
        self._fail_count = 0
        self._last_probe_time = 0.0

    @property
    def available(self) -> bool:
        return self._available

    def should_skip(self) -> bool:
        """返回 True 表示当前应跳过调用（熔断中且未到探测时间）。"""
        if self._available:
            return False
        now = time.time()
        if now - self._last_probe_time >= self.recover_interval:
            self._last_probe_time = now
            return False
        return True

    def on_success(self):
        if not self._available:
            logger.info(f"[熔断][{self.name}] 服务恢复可用")
        self._available = True
        self._fail_count = 0

    def on_fail(self):
        self._fail_count += 1
        if self._fail_count >= self.fail_threshold and self._available:
            self._available = False
            logger.warning(
                f"[熔断][{self.name}] 连续失败{self._fail_count}次，"
                f"熔断{self.recover_interval:.0f}s"
            )


class ServiceClient:
    """统一服务间 HTTP 客户端，带熔断、重试、超时。"""

    def __init__(
        self,
        name: str,
        base_url: str,
        timeout: float = 30.0,
        max_retries: int = 2,
        fail_threshold: int = 3,
        recover_interval: float = 60.0,
        retry_on_status: Tuple[int, ...] = (429, 500, 502, 503, 504),
        default_headers: Optional[Dict[str, str]] = None,
    ):
        """
        Args:
            name: 服务名称（用于日志）
            base_url: 服务基础 URL
            timeout: 请求超时（秒）
            max_retries: 最大重试次数（不含首次）
            fail_threshold: 熔断阈值（连续失败次数）
            recover_interval: 熔断恢复探测间隔（秒）
            retry_on_status: 需要重试的 HTTP 状态码
            default_headers: 默认请求头
        """
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_on_status = retry_on_status
        self.default_headers = default_headers or {}

        self.circuit_breaker = CircuitBreaker(
            fail_threshold=fail_threshold,
            recover_interval=recover_interval,
            name=name,
        )

    def _full_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    def _merge_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        merged = dict(self.default_headers)
        if headers:
            merged.update(headers)
        return merged

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> Optional[httpx.Response]:
        """带重试的异步请求。"""
        if self.circuit_breaker.should_skip():
            logger.debug(f"[{self.name}] 熔断中，跳过请求 {method} {path}")
            return None

        url = self._full_url(path)
        headers = self._merge_headers(kwargs.pop("headers", None))
        last_error = None

        for attempt in range(self.max_retries + 1):
            t0 = time.perf_counter()
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    resp = await client.request(
                        method, url, headers=headers, **kwargs
                    )

                duration = time.perf_counter() - t0
                status = resp.status_code

                if status == 200:
                    self.circuit_breaker.on_success()
                    logger.info(
                        f"[{self.name}] {method} {path} -> HTTP{status} "
                        f"耗时={duration:.2f}s"
                    )
                    return resp

                if status in self.retry_on_status and attempt < self.max_retries:
                    wait = min(30, 2 ** attempt) + 0.5
                    logger.warning(
                        f"[{self.name}] {method} {path} -> HTTP{status}，"
                        f"{wait:.1f}s后重试({attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(wait)
                    continue

                # 不重试的错误状态
                self.circuit_breaker.on_fail()
                logger.warning(
                    f"[{self.name}] {method} {path} -> HTTP{status} (不重试) "
                    f"耗时={duration:.2f}s"
                )
                return resp

            except httpx.TimeoutException:
                duration = time.perf_counter() - t0
                last_error = "TIMEOUT"
                self.circuit_breaker.on_fail()
                if attempt < self.max_retries:
                    wait = min(30, 2 ** attempt) + 0.5
                    logger.warning(
                        f"[{self.name}] {method} {path} 超时，{wait:.1f}s后重试"
                        f"({attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(wait)
                    continue
                logger.error(f"[{self.name}] {method} {path} 最终超时")
                return None

            except httpx.ConnectError as e:
                last_error = f"CONNECT_ERROR: {e}"
                self.circuit_breaker.on_fail()
                if attempt < self.max_retries:
                    wait = min(20, 2 ** attempt) + 0.5
                    logger.warning(
                        f"[{self.name}] {method} {path} 连接失败，{wait:.1f}s后重试"
                    )
                    await asyncio.sleep(wait)
                    continue
                logger.error(f"[{self.name}] {method} {path} 连接最终失败: {e}")
                return None

            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
                self.circuit_breaker.on_fail()
                logger.error(f"[{self.name}] {method} {path} 异常: {last_error}")
                return None

        return None

    async def get(self, path: str, **kwargs) -> Optional[httpx.Response]:
        return await self._request_with_retry("GET", path, **kwargs)

    async def post(self, path: str, **kwargs) -> Optional[httpx.Response]:
        return await self._request_with_retry("POST", path, **kwargs)

    async def put(self, path: str, **kwargs) -> Optional[httpx.Response]:
        return await self._request_with_retry("PUT", path, **kwargs)

    async def delete(self, path: str, **kwargs) -> Optional[httpx.Response]:
        return await self._request_with_retry("DELETE", path, **kwargs)

    async def health_check(self, path: str = "/health") -> bool:
        """健康检查，返回服务是否可用。"""
        try:
            resp = await self.get(path)
            return resp is not None and resp.status_code == 200
        except Exception:
            return False

    @property
    def is_available(self) -> bool:
        """服务当前是否可用（未熔断）。"""
        return self.circuit_breaker.available

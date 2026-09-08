"""
耗时统计工具
从各服务重复的 StepTimer 类统一提取，消除5个文件中的重复实现。

用法：
    from common.step_timer import StepTimer, fmt_ms
    timer = StepTimer("对话生成")
    # ... 做事情 ...
    timer.mark("LLM调用")
    # ... 做事情 ...
    timer.mark("后处理")
    timer.log()  # 输出: [耗时][对话生成] LLM调用=1.2s | 后处理=0.3s | 总计=1.5s
"""
import time
import logging
from typing import List, Tuple

logger = logging.getLogger("step_timer")


def fmt_ms(seconds: float) -> str:
    """格式化耗时：<1s 显示 ms，>=1s 显示 s"""
    ms = seconds * 1000
    if ms < 1000:
        return f"{ms:.0f}ms"
    return f"{seconds:.2f}s"


class StepTimer:
    """收集多步骤耗时，最后输出格式化日志。"""

    def __init__(self, label: str):
        self.label = label
        self._total_start = time.perf_counter()
        self._steps: List[Tuple[str, float]] = []  # (name, duration_seconds)
        self._last = self._total_start

    def mark(self, name: str):
        """记录上一步到现在的耗时。"""
        now = time.perf_counter()
        self._steps.append((name, now - self._last))
        self._last = now

    def elapsed_total(self) -> float:
        return time.perf_counter() - self._total_start

    def log(self, extra: str = "", level: int = logging.INFO):
        """输出格式化日志。

        Args:
            extra: 额外附加信息
            level: 日志级别，默认 INFO
        """
        parts = [f"{n}={fmt_ms(d)}" for n, d in self._steps]
        total = self.elapsed_total()
        logger.log(
            level,
            f"[耗时][{self.label}] {' | '.join(parts)} | 总计={fmt_ms(total)}{extra}"
        )

    def to_dict(self) -> dict:
        """导出为字典，便于 API 返回或结构化日志。"""
        return {
            "label": self.label,
            "steps": [{"name": n, "duration": round(d, 4)} for n, d in self._steps],
            "total": round(self.elapsed_total(), 4),
        }

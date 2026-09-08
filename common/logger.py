"""
统一日志配置
各服务原本各自调用 logging.basicConfig，格式不统一。
现在统一通过 setup_logger 配置，确保所有服务日志格式一致。

用法：
    from common.logger import setup_logger
    logger = setup_logger("main_server", level="INFO")
"""
import logging
import sys
from typing import Optional


# 统一日志格式
DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(
    name: str,
    level: str = "INFO",
    log_format: Optional[str] = None,
    date_format: Optional[str] = None,
) -> logging.Logger:
    """
    配置并返回一个 Logger 实例。

    Args:
        name: logger 名称（通常是服务名，如 "main_server" / "personality_server"）
        level: 日志级别，默认 INFO
        log_format: 自定义日志格式，None 则使用默认格式
        date_format: 自定义日期格式，None 则使用默认格式

    Returns:
        配置好的 Logger 实例
    """
    logger = logging.getLogger(name)

    # 避免重复添加 handler（多次调用时）
    if logger.handlers:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # 控制台输出 handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))

    formatter = logging.Formatter(
        log_format or DEFAULT_FORMAT,
        datefmt=date_format or DEFAULT_DATE_FORMAT,
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # 防止日志向上传播导致重复输出
    logger.propagate = False

    return logger


def get_logger(name: str) -> logging.Logger:
    """获取已配置的 logger（如果未配置则用默认配置）。"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logger(name)
    return logger

"""
安全工具模块
- 密码哈希（bcrypt）：替代原 main.py 中的明文密码存储
- Token 生成与验证
- API Key 掩码（日志中不泄露完整 Key）

用法：
    from common.security import hash_password, verify_password, mask_api_key

    hashed = hash_password("my_password")
    ok = verify_password("my_password", hashed)
    masked = mask_api_key("sk-abc123def456")  # "sk-a...456"
"""
import os
import hashlib
import hmac
import base64
import secrets
import logging
from typing import Optional

logger = logging.getLogger("security")

# 尝试导入 bcrypt，不可用时降级到 hashlib（不推荐生产使用）
try:
    import bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    BCRYPT_AVAILABLE = False
    bcrypt = None
    logger.warning(
        "bcrypt 未安装，密码哈希将降级为 hashlib（安全性较低）。"
        "请执行: pip install bcrypt"
    )


# ============================================================
# 密码哈希
# ============================================================

def hash_password(password: str, rounds: int = 12) -> str:
    """
    哈希密码。

    Args:
        password: 明文密码
        rounds: bcrypt 工作因子（默认12，越高越安全但越慢）

    Returns:
        哈希后的密码字符串（包含 salt 和算法标识）
    """
    if not password:
        raise ValueError("密码不能为空")

    if BCRYPT_AVAILABLE:
        # bcrypt 只支持前72字节，超长密码先哈希
        password_bytes = password.encode("utf-8")
        if len(password_bytes) > 72:
            password_bytes = hashlib.sha256(password_bytes).digest()
        salt = bcrypt.gensalt(rounds=rounds)
        hashed = bcrypt.hashpw(password_bytes, salt)
        return hashed.decode("utf-8")
    else:
        # 降级方案：PBKDF2（不推荐生产使用）
        salt = secrets.token_hex(16)
        derived = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt.encode("utf-8"), 100000
        )
        return f"pbkdf2${salt}${base64.b64encode(derived).decode()}"


def verify_password(password: str, hashed: str) -> bool:
    """
    验证密码是否匹配。

    Args:
        password: 明文密码
        hashed: 哈希后的密码

    Returns:
        True 表示匹配
    """
    if not password or not hashed:
        return False

    try:
        if hashed.startswith("pbkdf2$"):
            # 降级方案验证
            _, salt, derived_b64 = hashed.split("$", 2)
            derived = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"), salt.encode("utf-8"), 100000
            )
            return hmac.compare_digest(
                derived, base64.b64decode(derived_b64)
            )
        elif BCRYPT_AVAILABLE:
            # bcrypt 验证
            password_bytes = password.encode("utf-8")
            if len(password_bytes) > 72:
                password_bytes = hashlib.sha256(password_bytes).digest()
            return bcrypt.checkpw(password_bytes, hashed.encode("utf-8"))
        else:
            logger.error("bcrypt 不可用，无法验证 bcrypt 格式的密码")
            return False
    except Exception as e:
        logger.warning(f"密码验证异常: {e}")
        return False


def needs_rehash(hashed: str) -> bool:
    """检查密码哈希是否需要重新哈希（如算法升级或工作因子变更）。"""
    if not hashed:
        return True
    if hashed.startswith("pbkdf2$"):
        return True  # 降级方案，建议升级到 bcrypt
    if BCRYPT_AVAILABLE and hashed.startswith("$2b$"):
        # 检查工作因子
        try:
            rounds = int(hashed.split("$")[2])
            return rounds < 12
        except (IndexError, ValueError):
            return True
    return False


# ============================================================
# Token 工具
# ============================================================

def generate_token(length: int = 32) -> str:
    """生成安全的随机 token。"""
    return secrets.token_hex(length)


def generate_api_key(prefix: str = "sk") -> str:
    """生成 API Key 格式的字符串。"""
    return f"{prefix}-{secrets.token_hex(24)}"


def constant_time_compare(a: str, b: str) -> bool:
    """常量时间字符串比较（防止时序攻击）。"""
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


# ============================================================
# API Key 掩码（日志安全）
# ============================================================

def mask_api_key(api_key: str, keep_prefix: int = 4, keep_suffix: int = 4) -> str:
    """
    掩码 API Key，用于日志输出。

    Args:
        api_key: 原始 API Key
        keep_prefix: 保留前几位
        keep_suffix: 保留后几位

    Returns:
        掩码后的字符串，如 "sk-a...456"
    """
    if not api_key:
        return ""
    if len(api_key) <= keep_prefix + keep_suffix:
        return "***"
    return f"{api_key[:keep_prefix]}...{api_key[-keep_suffix:]}"


def mask_secret(secret: str) -> str:
    """通用密钥掩码，只显示前3位。"""
    if not secret:
        return ""
    if len(secret) <= 3:
        return "***"
    return f"{secret[:3]}***"

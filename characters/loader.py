"""
角色配置加载器
从 characters/ 目录下的 YAML 文件加载角色定义，替代原 personality_server.py 中的硬编码 ROLES_DEFINITION。

设计原则（借鉴 Clawra SOUL.md 理念）：
- 人格即配置文件，改人设不需要改代码、不需要重新部署
- 启动时自动加载 characters/ 目录下所有 *.yaml
- 加载失败时输出明确错误，不静默降级
- 支持运行时热重载（管理员触发）
"""
import os
import logging
import threading
from typing import Dict, Optional, List, Any

import yaml

logger = logging.getLogger("character_loader")

# 角色配置目录（相对于项目根目录）
CHARACTERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))

# 全局缓存
_roles_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = threading.Lock()
_loaded = False


def _load_single_role(filepath: str) -> Optional[Dict[str, Any]]:
    """加载单个角色 YAML 文件，失败返回 None 并记录错误。"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            logger.error(f"角色文件格式错误（不是字典）: {filepath}")
            return None
        if "id" not in data:
            logger.error(f"角色文件缺少 id 字段: {filepath}")
            return None
        return data
    except yaml.YAMLError as e:
        logger.error(f"角色文件 YAML 解析失败 {filepath}: {e}")
        return None
    except Exception as e:
        logger.error(f"角色文件读取失败 {filepath}: {e}")
        return None


def load_all_roles(force_reload: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    加载 characters/ 目录下所有角色配置。
    
    Args:
        force_reload: 强制重新加载（忽略缓存），用于热重载
    
    Returns:
        {role_id: role_data} 字典
    """
    global _roles_cache, _loaded
    
    with _cache_lock:
        if _loaded and not force_reload:
            return dict(_roles_cache)
        
        roles = {}
        if not os.path.isdir(CHARACTERS_DIR):
            logger.error(f"角色配置目录不存在: {CHARACTERS_DIR}")
            return roles
        
        # 先加载 index.yaml（如果存在），获取角色顺序和默认角色
        index_path = os.path.join(CHARACTERS_DIR, "index.yaml")
        role_order = []
        default_role = None
        if os.path.exists(index_path):
            try:
                with open(index_path, "r", encoding="utf-8") as f:
                    idx = yaml.safe_load(f)
                if isinstance(idx, dict):
                    role_order = [r["id"] for r in idx.get("roles", []) if "id" in r]
                    default_role = idx.get("default_role")
            except Exception as e:
                logger.warning(f"index.yaml 解析失败，将按文件名顺序加载: {e}")
        
        # 扫描所有 *.yaml 文件（排除 index.yaml）
        yaml_files = []
        for filename in sorted(os.listdir(CHARACTERS_DIR)):
            if filename.endswith(".yaml") and filename != "index.yaml":
                yaml_files.append(os.path.join(CHARACTERS_DIR, filename))
        
        # 加载每个角色
        for filepath in yaml_files:
            role = _load_single_role(filepath)
            if role:
                rid = role["id"]
                roles[rid] = role
                logger.info(f"已加载角色: {role.get('name', rid)} ({rid}) - {os.path.basename(filepath)}")
        
        # 按 index.yaml 中的顺序排序（如果有）
        if role_order:
            ordered = {}
            for rid in role_order:
                if rid in roles:
                    ordered[rid] = roles[rid]
            # 把不在 index 中的角色追加到后面
            for rid, r in roles.items():
                if rid not in ordered:
                    ordered[rid] = r
            roles = ordered
        
        _roles_cache = roles
        _loaded = True
        
        logger.info(f"角色加载完成，共 {len(roles)} 个角色: {list(roles.keys())}")
        if default_role:
            logger.info(f"默认角色: {default_role}")
        
        return dict(roles)


def get_role(role_id: str) -> Optional[Dict[str, Any]]:
    """获取单个角色配置，不存在返回 None。"""
    if not _loaded:
        load_all_roles()
    return _roles_cache.get(role_id)


def get_all_roles() -> Dict[str, Dict[str, Any]]:
    """获取所有角色配置的副本。"""
    if not _loaded:
        load_all_roles()
    return dict(_roles_cache)


def get_role_ids() -> List[str]:
    """获取所有角色 ID 列表。"""
    if not _loaded:
        load_all_roles()
    return list(_roles_cache.keys())


def reload_roles() -> Dict[str, Dict[str, Any]]:
    """热重载角色配置（管理员调用）。"""
    logger.info("触发角色配置热重载...")
    return load_all_roles(force_reload=True)


# 模块导入时自动加载（但不阻塞，失败只记录日志）
try:
    load_all_roles()
except Exception as e:
    logger.warning(f"角色配置初始加载失败，将在首次访问时重试: {e}")

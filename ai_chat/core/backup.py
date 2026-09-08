"""
数据备份与恢复模块（P3 模块3）。

功能：
1. 自动备份：SQLite数据库 + userdb.json + 角色配置 + 群聊配置
2. 导出导入：一键导出所有数据为zip包，一键导入恢复
3. 备份管理：列出备份、下载备份、恢复备份、删除备份
"""

import os
import json
import time
import zipfile
import shutil
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("backup")


# ============================================================
# 配置
# ============================================================

def get_backup_dir() -> str:
    """获取备份目录。"""
    data_dir = os.getenv("DATA_DIR", ".")
    backup_dir = os.path.join(data_dir, "backups")
    os.makedirs(backup_dir, exist_ok=True)
    return backup_dir


def get_data_dir() -> str:
    """获取数据目录。"""
    return os.getenv("DATA_DIR", ".")


# 需要备份的文件/目录列表
def get_backup_targets() -> List[Tuple[str, str]]:
    """
    获取需要备份的目标列表。
    返回 [(源路径, 压缩包内路径), ...]
    """
    data_dir = get_data_dir()
    targets = []

    # 1. userdb.json（用户数据库，包含亲密度、QQ绑定等）
    userdb_path = os.path.join(data_dir, "userdb.json")
    if os.path.exists(userdb_path):
        targets.append((userdb_path, "userdb.json"))

    # 2. SQLite 数据库（proactive_server的主动消息数据）
    for db_file in ["proactive.db", "flexichrono.db", "data.db"]:
        db_path = os.path.join(data_dir, db_file)
        if os.path.exists(db_path):
            targets.append((db_path, db_file))
            # 同时备份 -wal 和 -shm 文件（SQLite WAL模式）
            for suffix in ["-wal", "-shm"]:
                wal_path = db_path + suffix
                if os.path.exists(wal_path):
                    targets.append((wal_path, db_file + suffix))

    # 3. 角色配置目录（characters/）
    chars_dir = os.path.join(data_dir, "characters")
    if os.path.isdir(chars_dir):
        for root, dirs, files in os.walk(chars_dir):
            for f in files:
                full_path = os.path.join(root, f)
                rel_path = os.path.relpath(full_path, data_dir)
                targets.append((full_path, rel_path))

    # 4. 群聊配置
    group_config_path = os.path.join(data_dir, "group_config.json")
    if os.path.exists(group_config_path):
        targets.append((group_config_path, "group_config.json"))

    # 5. 向量数据目录（如果有本地向量存储）
    vector_dir = os.path.join(data_dir, "vector_data")
    if os.path.isdir(vector_dir):
        for root, dirs, files in os.walk(vector_dir):
            for f in files:
                full_path = os.path.join(root, f)
                rel_path = os.path.relpath(full_path, data_dir)
                targets.append((full_path, rel_path))

    # 6. 聊天历史（如果有持久化）
    history_path = os.path.join(data_dir, "chat_history.json")
    if os.path.exists(history_path):
        targets.append((history_path, "chat_history.json"))

    return targets


# ============================================================
# 备份创建
# ============================================================

def create_backup(backup_name: Optional[str] = None, note: str = "") -> Dict[str, Any]:
    """
    创建备份。

    Args:
        backup_name: 备份名称（默认用时间戳）
        note: 备份备注

    Returns:
        备份信息字典
    """
    backup_dir = get_backup_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = backup_name or f"backup_{timestamp}"
    zip_path = os.path.join(backup_dir, f"{name}.zip")

    logger.info(f"[Backup] 开始创建备份: {name}")

    targets = get_backup_targets()
    file_count = 0
    total_size = 0

    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # 写入备份元数据
            metadata = {
                "backup_name": name,
                "created_at": timestamp,
                "created_at_iso": datetime.now().isoformat(),
                "note": note,
                "version": "1.0",
                "file_count": len(targets),
            }
            zf.writestr("backup_metadata.json", json.dumps(metadata, ensure_ascii=False, indent=2))

            # 写入数据文件
            for src_path, arc_name in targets:
                if os.path.exists(src_path):
                    zf.write(src_path, arc_name)
                    file_count += 1
                    total_size += os.path.getsize(src_path)
                    logger.debug(f"[Backup] 已添加: {arc_name} ({os.path.getsize(src_path)}B)")

        # 更新元数据中的实际文件数
        metadata["actual_file_count"] = file_count
        metadata["total_size"] = total_size

        # 保存备份信息到索引
        index_path = os.path.join(backup_dir, "backup_index.json")
        index = _load_backup_index()
        index[name] = {
            "filename": f"{name}.zip",
            "created_at": timestamp,
            "created_at_iso": datetime.now().isoformat(),
            "note": note,
            "file_count": file_count,
            "size": total_size,
            "size_human": _human_size(total_size),
        }
        _save_backup_index(index)

        logger.info(f"[Backup] 备份创建成功: {name}, {file_count}个文件, {_human_size(total_size)}")

        return {
            "success": True,
            "backup_name": name,
            "filename": f"{name}.zip",
            "path": zip_path,
            "file_count": file_count,
            "size": total_size,
            "size_human": _human_size(total_size),
            "created_at": timestamp,
        }

    except Exception as e:
        logger.error(f"[Backup] 备份创建失败: {e}", exc_info=True)
        # 清理失败的zip
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return {"success": False, "error": str(e)}


# ============================================================
# 备份恢复
# ============================================================

def restore_backup(backup_name: str, overwrite: bool = True) -> Dict[str, Any]:
    """
    从备份恢复数据。

    Args:
        backup_name: 备份名称（不含.zip）
        overwrite: 是否覆盖现有文件

    Returns:
        恢复结果
    """
    backup_dir = get_backup_dir()
    zip_path = os.path.join(backup_dir, f"{backup_name}.zip")
    data_dir = get_data_dir()

    if not os.path.exists(zip_path):
        return {"success": False, "error": f"备份不存在: {backup_name}"}

    logger.info(f"[Backup] 开始恢复备份: {backup_name}")

    try:
        restored_files = []
        skipped_files = []

        with zipfile.ZipFile(zip_path, 'r') as zf:
            # 读取元数据
            try:
                metadata = json.loads(zf.read("backup_metadata.json").decode("utf-8"))
                logger.info(f"[Backup] 备份元数据: {metadata.get('created_at_iso', 'unknown')}")
            except Exception:
                metadata = {}

            # 恢复文件
            for info in zf.infolist():
                if info.filename == "backup_metadata.json":
                    continue

                target_path = os.path.join(data_dir, info.filename)

                # 安全检查：防止路径穿越
                if not os.path.abspath(target_path).startswith(os.path.abspath(data_dir)):
                    logger.warning(f"[Backup] 跳过不安全路径: {info.filename}")
                    skipped_files.append(info.filename)
                    continue

                # 检查是否覆盖
                if os.path.exists(target_path) and not overwrite:
                    skipped_files.append(info.filename)
                    continue

                # 创建目录
                os.makedirs(os.path.dirname(target_path), exist_ok=True)

                # 写入文件
                with zf.open(info) as src, open(target_path, 'wb') as dst:
                    shutil.copyfileobj(src, dst)

                restored_files.append(info.filename)
                logger.debug(f"[Backup] 已恢复: {info.filename}")

        logger.info(f"[Backup] 恢复完成: {len(restored_files)}个文件恢复, {len(skipped_files)}个跳过")

        return {
            "success": True,
            "backup_name": backup_name,
            "restored_count": len(restored_files),
            "skipped_count": len(skipped_files),
            "restored_files": restored_files[:50],  # 最多返回50个文件名
            "metadata": metadata,
        }

    except Exception as e:
        logger.error(f"[Backup] 恢复失败: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# 备份管理
# ============================================================

def list_backups() -> List[Dict[str, Any]]:
    """列出所有备份。"""
    index = _load_backup_index()
    backups = []
    for name, info in index.items():
        backups.append({
            "name": name,
            "filename": info.get("filename", f"{name}.zip"),
            "created_at": info.get("created_at", ""),
            "created_at_iso": info.get("created_at_iso", ""),
            "note": info.get("note", ""),
            "file_count": info.get("file_count", 0),
            "size": info.get("size", 0),
            "size_human": info.get("size_human", "0B"),
        })
    # 按创建时间倒序
    backups.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return backups


def delete_backup(backup_name: str) -> Dict[str, Any]:
    """删除备份。"""
    backup_dir = get_backup_dir()
    zip_path = os.path.join(backup_dir, f"{backup_name}.zip")

    if not os.path.exists(zip_path):
        return {"success": False, "error": f"备份不存在: {backup_name}"}

    try:
        os.remove(zip_path)
        # 从索引中移除
        index = _load_backup_index()
        if backup_name in index:
            del index[backup_name]
            _save_backup_index(index)

        logger.info(f"[Backup] 备份已删除: {backup_name}")
        return {"success": True, "backup_name": backup_name}
    except Exception as e:
        logger.error(f"[Backup] 删除失败: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


def get_backup_path(backup_name: str) -> Optional[str]:
    """获取备份文件路径（用于下载）。"""
    backup_dir = get_backup_dir()
    zip_path = os.path.join(backup_dir, f"{backup_name}.zip")
    if os.path.exists(zip_path):
        return zip_path
    return None


# ============================================================
# 自动备份（定时任务）
# ============================================================

def cleanup_old_backups(max_count: int = 30, max_age_days: int = 30) -> Dict[str, Any]:
    """
    清理旧备份。

    Args:
        max_count: 最多保留多少个备份
        max_age_days: 备份最大保留天数

    Returns:
        清理结果
    """
    backups = list_backups()
    deleted = []

    # 按时间排序（最新的在前）
    backups.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    # 超过数量限制的删除
    if len(backups) > max_count:
        for b in backups[max_count:]:
            result = delete_backup(b["name"])
            if result.get("success"):
                deleted.append(b["name"])

    # 超过天数的删除
    cutoff = datetime.now() - timedelta(days=max_age_days)
    for b in backups:
        try:
            created = datetime.strptime(b.get("created_at", ""), "%Y%m%d_%H%M%S")
            if created < cutoff and b["name"] not in deleted:
                result = delete_backup(b["name"])
                if result.get("success"):
                    deleted.append(b["name"])
        except Exception:
            pass

    logger.info(f"[Backup] 清理完成，删除了{len(deleted)}个旧备份")
    return {"deleted_count": len(deleted), "deleted": deleted}


async def auto_backup_task():
    """
    自动备份任务（供定时调度调用）。
    每天自动备份一次，并清理旧备份。
    """
    try:
        logger.info("[Backup] 自动备份任务开始")
        result = create_backup(note="auto_daily")
        if result.get("success"):
            logger.info(f"[Backup] 自动备份成功: {result['backup_name']}")
            # 清理旧备份（保留30个或30天）
            cleanup_old_backups(max_count=30, max_age_days=30)
        else:
            logger.error(f"[Backup] 自动备份失败: {result.get('error')}")
        return result
    except Exception as e:
        logger.error(f"[Backup] 自动备份任务异常: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# 内部工具函数
# ============================================================

def _load_backup_index() -> Dict[str, Any]:
    """加载备份索引。"""
    index_path = os.path.join(get_backup_dir(), "backup_index.json")
    if os.path.exists(index_path):
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_backup_index(index: Dict[str, Any]):
    """保存备份索引。"""
    index_path = os.path.join(get_backup_dir(), "backup_index.json")
    try:
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"[Backup] 保存索引失败: {e}")


def _human_size(size_bytes: int) -> str:
    """字节数转人类可读格式。"""
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f}KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f}MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f}GB"


# ============================================================
# 单例
# ============================================================

_backup_manager = None


def get_backup_manager():
    """获取备份管理器（单例，实际就是模块函数的封装）。"""
    global _backup_manager
    if _backup_manager is None:
        _backup_manager = type('BackupManager', (), {
            'create': staticmethod(create_backup),
            'restore': staticmethod(restore_backup),
            'list': staticmethod(list_backups),
            'delete': staticmethod(delete_backup),
            'get_path': staticmethod(get_backup_path),
            'cleanup': staticmethod(cleanup_old_backups),
            'auto_backup': staticmethod(auto_backup_task),
        })
    return _backup_manager

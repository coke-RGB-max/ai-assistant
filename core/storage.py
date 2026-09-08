"""
SQLite 连接底座（P4 拆分共享层 + 多进程并发加固）
------------------------------------------------------------------
- 统一 row_factory 与连接参数
- WAL 模式：5 个服务进程共写同一库时，允许读写并发、避免
  “database is locked”；WAL 是数据库级持久设置，重复执行无害
- busy_timeout：锁竞争时等待最多 5s 而不是立即抛错
"""
import sqlite3

from core.config import DB_PATH


def _get_db(db_path: str = None):
    """返回一个 SQLite 连接（调用方负责 close）。"""
    conn = sqlite3.connect(db_path or DB_PATH, check_same_thread=False, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.DatabaseError:
        # 极端情况下（只读库/内存库）pragma 失败不影响主链路
        pass
    return conn

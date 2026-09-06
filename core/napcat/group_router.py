"""
NapCat OneBot v11 群聊多角色调度路由（P3 模块）。

职责：
1. 解析 NapCat 上报的 OneBot v11 群消息 → GroupMessageContext
2. 按群配置判断「是否回复 / 由哪些角色回复」：
   - at        : @机器人 触发（群内全部启用角色）
   - reply     : 回复机器人发出的消息触发（群内全部启用角色）
   - keyword   : 消息文本命中角色别名时，仅触发对应角色
   - probability: 普通群聊按概率插话（默认关闭）
3. 构建群内隔离的用户身份标识，供人格后端做会话隔离

配置文件为 JSON，路径由环境变量 GROUP_CONFIG_PATH 指定，
默认 ${DATA_DIR}/group_config.json（容器内 DATA_DIR=/data）。结构示例：
{
  "enabled": true,
  "default_roles": ["nianqi"],
  "default_trigger": ["at", "reply"],
  "default_probability": 0.0,
  "groups": {
    "123456789": {
      "enabled": true,
      "roles": ["nianqi", "qinghe"],
      "trigger": ["at", "reply", "keyword"],
      "probability": 0.1,
      "role_aliases": {"nianqi": ["念琦", "小念"], "qinghe": ["清禾"]}
    }
  }
}

对外导出（core/napcat/__init__.py 与 main.py 均依赖这些符号，不可改名）：
- GroupRoleConfig / GroupConfig / GroupConfigManager
- GroupMessageContext / GroupMessageRouter
- get_group_config_manager() / get_group_router()
"""

import os
import json
import random
import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .message import Message, parse_message

logger = logging.getLogger("napcat.group_router")

# ============================================================
# 常量与默认配置
# ============================================================

# 全部合法触发方式
VALID_TRIGGERS = ("at", "reply", "keyword", "probability")

# 角色默认别名（可被群配置 role_aliases 覆盖/扩展）
DEFAULT_ROLE_ALIASES: Dict[str, List[str]] = {
    "nianqi":  ["念琦", "小念"],
    "qinghe":  ["清禾", "小禾"],
    "jingwen": ["璟雯", "小璟"],
}

# 默认回复角色（可用环境变量 QQ_GROUP_DEFAULT_ROLES="nianqi,qinghe" 覆盖）
_env_roles = os.getenv("QQ_GROUP_DEFAULT_ROLES", "nianqi")
DEFAULT_ROLES: List[str] = [r.strip() for r in _env_roles.split(",") if r.strip()] or ["nianqi"]

# 默认触发方式（可用环境变量 QQ_GROUP_DEFAULT_TRIGGER 覆盖）
_env_trig = os.getenv("QQ_GROUP_DEFAULT_TRIGGER", "at,reply")
DEFAULT_TRIGGER: List[str] = [t.strip() for t in _env_trig.split(",") if t.strip() in VALID_TRIGGERS] \
    or ["at", "reply"]

# 机器人自身 QQ（NapCat 上报一般自带 self_id；此处为兜底）
BOT_QQ = str(os.getenv("NAPCAT_BOT_QQ", os.getenv("BOT_QQ", "")) or "")


def _default_config_path() -> str:
    """群配置 JSON 路径：GROUP_CONFIG_PATH > DATA_DIR/group_config.json > 项目根。"""
    env_path = os.getenv("GROUP_CONFIG_PATH", "")
    if env_path:
        return env_path
    data_dir = os.getenv("DATA_DIR", "")
    if data_dir:
        return os.path.join(data_dir, "group_config.json")
    # core/napcat/ 向上两级即项目根目录
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(project_root, "group_config.json")


# ============================================================
# 配置数据类
# ============================================================

@dataclass
class GroupRoleConfig:
    """单个角色在某个群内的配置。"""
    role_id: str
    aliases: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {"role_id": self.role_id, "aliases": list(self.aliases)}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GroupRoleConfig":
        return cls(
            role_id=str(data.get("role_id", "")),
            aliases=[str(a) for a in data.get("aliases", []) if a],
        )


@dataclass
class GroupConfig:
    """单个群的调度配置。"""
    group_id: str
    enabled: bool = True
    roles: List[str] = field(default_factory=lambda: list(DEFAULT_ROLES))
    trigger: List[str] = field(default_factory=lambda: list(DEFAULT_TRIGGER))
    probability: float = 0.0
    # role_id -> 该角色在本群的别名列表
    role_aliases: Dict[str, List[str]] = field(default_factory=dict)

    def role_enabled(self, role_id: str) -> bool:
        return self.enabled and role_id in self.roles

    def aliases_of(self, role_id: str) -> List[str]:
        """本群配置别名优先，缺省回落到内置别名。"""
        if role_id in self.role_aliases:
            return [a for a in self.role_aliases[role_id] if a]
        return DEFAULT_ROLE_ALIASES.get(role_id, [])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "roles": list(self.roles),
            "trigger": list(self.trigger),
            "probability": self.probability,
            "role_aliases": {k: list(v) for k, v in self.role_aliases.items()},
        }

    @classmethod
    def from_dict(cls, group_id: str, data: Dict[str, Any]) -> "GroupConfig":
        trigger = [t for t in data.get("trigger", DEFAULT_TRIGGER) if t in VALID_TRIGGERS]
        if not trigger:
            trigger = list(DEFAULT_TRIGGER)
        try:
            prob = float(data.get("probability", 0.0))
        except (TypeError, ValueError):
            prob = 0.0
        prob = min(1.0, max(0.0, prob))
        roles = [str(r) for r in data.get("roles", DEFAULT_ROLES) if r] or list(DEFAULT_ROLES)
        aliases = {
            str(k): [str(a) for a in v if a]
            for k, v in data.get("role_aliases", {}).items()
            if isinstance(v, list)
        }
        return cls(
            group_id=str(group_id),
            enabled=bool(data.get("enabled", True)),
            roles=roles,
            trigger=trigger,
            probability=prob,
            role_aliases=aliases,
        )


# ============================================================
# 群配置管理器（JSON 持久化 + 内存缓存 + 线程锁）
# ============================================================

class GroupConfigManager:
    """加载/保存/查询群配置。文件缺失或损坏时安全回落到默认配置，不阻断消息处理。"""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or _default_config_path()
        self._lock = threading.RLock()
        self._raw: Dict[str, Any] = {}
        self._cache: Dict[str, GroupConfig] = {}
        self.load()

    # ---------- 持久化 ----------

    def load(self) -> Dict[str, Any]:
        """从磁盘加载配置；文件不存在时初始化默认结构。"""
        with self._lock:
            if os.path.exists(self.config_path):
                try:
                    with open(self.config_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if not isinstance(data, dict):
                        raise ValueError("配置根节点必须是 JSON 对象")
                    self._raw = data
                except Exception as e:
                    logger.error(f"[GroupConfig] 配置文件解析失败，回落到默认配置: "
                                 f"{self.config_path} | {type(e).__name__}: {e}")
                    self._raw = {}
            else:
                self._raw = {}
            self._cache.clear()
            return dict(self._raw)

    def save(self) -> bool:
        """原子写入（先写临时文件再 os.replace），避免写一半被读取。"""
        with self._lock:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.config_path)), exist_ok=True)
                tmp_path = self.config_path + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(self._raw, f, ensure_ascii=False, indent=2)
                os.replace(tmp_path, self.config_path)
                return True
            except Exception as e:
                logger.error(f"[GroupConfig] 配置保存失败: {type(e).__name__}: {e}")
                return False

    def reload(self) -> Dict[str, Any]:
        return self.load()

    # ---------- 全局配置 ----------

    @property
    def global_enabled(self) -> bool:
        return bool(self._raw.get("enabled", True))

    def get_default_roles(self) -> List[str]:
        roles = self._raw.get("default_roles")
        if isinstance(roles, list) and roles:
            return [str(r) for r in roles if r]
        return list(DEFAULT_ROLES)

    def get_default_trigger(self) -> List[str]:
        trig = self._raw.get("default_trigger")
        if isinstance(trig, list) and trig:
            valid = [t for t in trig if t in VALID_TRIGGERS]
            if valid:
                return valid
        return list(DEFAULT_TRIGGER)

    def get_default_probability(self) -> float:
        try:
            return min(1.0, max(0.0, float(self._raw.get("default_probability", 0.0))))
        except (TypeError, ValueError):
            return 0.0

    # ---------- 群配置 ----------

    def get_group_config(self, group_id: str) -> GroupConfig:
        """获取群配置；未配置的群合并全局默认值。"""
        group_id = str(group_id)
        with self._lock:
            if group_id in self._cache:
                return self._cache[group_id]
            groups = self._raw.get("groups", {})
            data = groups.get(group_id) if isinstance(groups, dict) else None
            if isinstance(data, dict):
                cfg = GroupConfig.from_dict(group_id, data)
                # 群级未显式配置的字段补全局默认
                if "roles" not in data:
                    cfg.roles = self.get_default_roles()
                if "trigger" not in data:
                    cfg.trigger = self.get_default_trigger()
                if "probability" not in data:
                    cfg.probability = self.get_default_probability()
            else:
                cfg = GroupConfig(
                    group_id=group_id,
                    enabled=True,
                    roles=self.get_default_roles(),
                    trigger=self.get_default_trigger(),
                    probability=self.get_default_probability(),
                )
            self._cache[group_id] = cfg
            return cfg

    def upsert_group_config(self, group_id: str, config: Dict[str, Any]) -> bool:
        """新增/更新某个群的配置并落盘。"""
        group_id = str(group_id)
        with self._lock:
            groups = self._raw.setdefault("groups", {})
            groups[group_id] = config
            self._cache.pop(group_id, None)
            return self.save()

    def remove_group_config(self, group_id: str) -> bool:
        group_id = str(group_id)
        with self._lock:
            groups = self._raw.get("groups", {})
            if group_id in groups:
                del groups[group_id]
                self._cache.pop(group_id, None)
                return self.save()
            return True

    def list_groups(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._raw.get("groups", {}))


# ============================================================
# 群消息上下文
# ============================================================

@dataclass
class GroupMessageContext:
    """解析后的群聊消息上下文（main.py handle_group_message 依赖以下字段，不可缺）。"""
    group_id: str
    user_id: str
    text_content: str          # 去掉 @ 段后的纯文本
    message: Message           # 解析后的消息段序列（image_handler 依赖 .has_image()/.get_image_urls()）
    raw_message: str = ""      # OneBot 原始 CQ 码文本
    message_id: Any = None
    self_id: str = ""          # 机器人自身 QQ
    sender: Dict[str, Any] = field(default_factory=dict)
    at_me: bool = False        # 是否 @ 了机器人
    reply_to_me: bool = False  # 是否回复了机器人的消息
    sub_type: str = "normal"
    raw_body: Dict[str, Any] = field(default_factory=dict)


# ============================================================
# 群聊路由器
# ============================================================

class GroupMessageRouter:
    """群消息解析与多角色触发调度。"""

    def __init__(self, config_manager: Optional[GroupConfigManager] = None):
        self.config = config_manager or get_group_config_manager()

    # ---------- 解析 ----------

    def parse_group_message(self, body: Dict[str, Any]) -> Optional[GroupMessageContext]:
        """
        把 OneBot v11 上报 body 解析成 GroupMessageContext。
        非群消息 / 机器人自身消息 / 缺关键字段时返回 None（main.py 据此返回 not_group）。
        """
        if not isinstance(body, dict):
            return None
        if body.get("post_type") != "message":
            return None
        if body.get("message_type") != "group":
            return None

        group_id = body.get("group_id")
        user_id = body.get("user_id")
        if group_id is None or user_id is None:
            logger.warning(f"[GroupRouter] 群消息缺少 group_id/user_id: keys={list(body.keys())}")
            return None
        group_id, user_id = str(group_id), str(user_id)

        # 机器人自身 QQ：优先上报里的 self_id，回落环境变量
        self_id = str(body.get("self_id", "") or BOT_QQ)

        # 防止回环：不处理机器人自己发出的消息
        if self_id and user_id == self_id:
            return None

        message = parse_message(body.get("message", body.get("raw_message", "")))
        text_content = message.remove_at_segments().extract_plain_text().strip()
        raw_message = str(body.get("raw_message", "") or "")

        at_me = bool(self_id) and message.has_at_me(self_id)
        reply_to_me = self._is_reply_to_me(message, self_id)

        return GroupMessageContext(
            group_id=group_id,
            user_id=user_id,
            text_content=text_content,
            message=message,
            raw_message=raw_message,
            message_id=body.get("message_id"),
            self_id=self_id,
            sender=body.get("sender", {}) if isinstance(body.get("sender"), dict) else {},
            at_me=at_me,
            reply_to_me=reply_to_me,
            sub_type=str(body.get("sub_type", "normal")),
            raw_body=body,
        )

    @staticmethod
    def _is_reply_to_me(message: Message, self_id: str) -> bool:
        """判断 reply 消息段回复的是否为机器人（NapCat reply 段 data.qq 为被回复者 QQ）。"""
        if not self_id:
            return False
        for seg in message:
            if seg.type == "reply":
                target = seg.data.get("qq") or seg.data.get("user_id")
                if target is not None and str(target) == str(self_id):
                    return True
        return False

    # ---------- 触发判定 ----------

    def should_reply(self, context: GroupMessageContext) -> Tuple[bool, List[str]]:
        """
        判断是否回复及回复角色列表。
        返回: (是否回复, 角色id列表)；不回复时返回 (False, [])。
        """
        # 全局开关
        if not self.config.global_enabled:
            return False, []

        cfg = self.config.get_group_config(context.group_id)
        if not cfg.enabled:
            return False, []
        if not cfg.roles:
            return False, []

        triggers = set(cfg.trigger)
        text = context.text_content or ""

        # 1) @机器人 → 群内全部启用角色
        if "at" in triggers and context.at_me:
            logger.info(f"[GroupRouter] 群{context.group_id} 用户{context.user_id} @机器人，"
                        f"触发角色 {cfg.roles}")
            return True, list(cfg.roles)

        # 2) 回复机器人消息 → 群内全部启用角色
        if "reply" in triggers and context.reply_to_me:
            logger.info(f"[GroupRouter] 群{context.group_id} 用户{context.user_id} 回复机器人，"
                        f"触发角色 {cfg.roles}")
            return True, list(cfg.roles)

        # 3) 角色别名关键词 → 仅触发被点名的角色
        if "keyword" in triggers and text:
            hit_roles = self._match_roles_by_text(text, cfg)
            if hit_roles:
                logger.info(f"[GroupRouter] 群{context.group_id} 用户{context.user_id} "
                            f"文本命中角色别名 {hit_roles}")
                return True, hit_roles

        # 4) 概率插话 → 群内全部启用角色（纯表情/空文本不插话）
        if "probability" in triggers and cfg.probability > 0 and text:
            if random.random() < cfg.probability:
                logger.info(f"[GroupRouter] 群{context.group_id} 概率插话命中(p={cfg.probability})，"
                            f"角色 {cfg.roles}")
                return True, list(cfg.roles)

        return False, []

    @staticmethod
    def _match_roles_by_text(text: str, cfg: GroupConfig) -> List[str]:
        """文本命中哪些启用角色的别名（支持多角色同时被点名）。"""
        hit = []
        for role_id in cfg.roles:
            for alias in cfg.aliases_of(role_id):
                if alias and alias in text:
                    hit.append(role_id)
                    break
        return hit

    # ---------- 身份构建 ----------

    @staticmethod
    def build_user_identity(context: GroupMessageContext) -> str:
        """
        构建群内隔离的用户身份标识。
        与 main.py 私聊 qq_tmp_{qq} 风格保持一致，群维度隔离避免不同群/私聊串会话。
        """
        return f"qq_group_{context.group_id}_u_{context.user_id}"


# ============================================================
# 单例工厂（main.py / __init__.py 通过这两个函数获取实例）
# ============================================================

_config_manager_singleton: Optional[GroupConfigManager] = None
_router_singleton: Optional[GroupMessageRouter] = None
_singleton_lock = threading.Lock()


def get_group_config_manager(config_path: Optional[str] = None) -> GroupConfigManager:
    """获取全局唯一的群配置管理器（首次调用时创建）。"""
    global _config_manager_singleton
    with _singleton_lock:
        if _config_manager_singleton is None or config_path is not None:
            _config_manager_singleton = GroupConfigManager(config_path)
        return _config_manager_singleton


def get_group_router(config_manager: Optional[GroupConfigManager] = None) -> GroupMessageRouter:
    """获取全局唯一的群聊路由器。"""
    global _router_singleton
    with _singleton_lock:
        if _router_singleton is None or config_manager is not None:
            _router_singleton = GroupMessageRouter(
                config_manager or get_group_config_manager())
        return _router_singleton

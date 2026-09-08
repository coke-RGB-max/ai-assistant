"""群聊路由器测试：解析过滤、四种触发方式、防自发回环、身份隔离、配置持久化。"""
import pytest

from core.napcat.group_router import (
    GroupConfigManager, GroupMessageRouter,
)

BOT = "10000"


def make_manager(tmp_path):
    return GroupConfigManager(config_path=str(tmp_path / "group_config.json"))


def make_router(tmp_path):
    return GroupMessageRouter(make_manager(tmp_path))


def body(user="20001", segments=None, self_id=BOT, **extra):
    return {
        "post_type": "message", "message_type": "group",
        "group_id": "555", "user_id": user, "self_id": self_id,
        "message": segments if segments is not None else [
            {"type": "text", "data": {"text": "你好"}}],
        "raw_message": "", "message_id": 1,
        **extra,
    }


class TestParse:
    def test_not_group_returns_none(self, tmp_path):
        r = make_router(tmp_path)
        assert r.parse_group_message({"post_type": "notice"}) is None
        assert r.parse_group_message({"post_type": "message",
                                      "message_type": "private"}) is None

    def test_missing_ids_returns_none(self, tmp_path):
        r = make_router(tmp_path)
        assert r.parse_group_message({"post_type": "message",
                                      "message_type": "group"}) is None

    def test_self_loop_blocked(self, tmp_path):
        # 机器人自己发的消息（user_id == self_id）必须丢弃，防止回环
        r = make_router(tmp_path)
        assert r.parse_group_message(body(user=BOT)) is None

    def test_at_me_flag(self, tmp_path):
        r = make_router(tmp_path)
        ctx = r.parse_group_message(body(segments=[
            {"type": "at", "data": {"qq": BOT}},
            {"type": "text", "data": {"text": " 在吗"}}]))
        assert ctx is not None and ctx.at_me is True
        assert ctx.text_content == "在吗"


class TestTrigger:
    def _cfg(self, mgr, **kw):
        cfg = {"enabled": True, "roles": ["nianqi"], "trigger": ["at", "reply"],
               "probability": 0.0, "role_aliases": {"nianqi": ["小念"]}}
        cfg.update(kw)
        mgr.upsert_group_config("555", cfg)

    def test_at_triggers_all_roles(self, tmp_path):
        r = make_router(tmp_path)
        self._cfg(r.config)
        ctx = r.parse_group_message(body(segments=[
            {"type": "at", "data": {"qq": BOT}},
            {"type": "text", "data": {"text": "说话"}}]))
        ok, roles = r.should_reply(ctx)
        assert ok and roles == ["nianqi"]

    def test_reply_trigger(self, tmp_path):
        r = make_router(tmp_path)
        self._cfg(r.config)
        ctx = r.parse_group_message(body(segments=[
            {"type": "reply", "data": {"qq": BOT}},
            {"type": "text", "data": {"text": "嗯嗯"}}]))
        ok, roles = r.should_reply(ctx)
        assert ok and ctx.reply_to_me and roles == ["nianqi"]

    def test_keyword_alias_only_hits_named_role(self, tmp_path):
        r = make_router(tmp_path)
        self._cfg(r.config, trigger=["keyword"], roles=["nianqi", "qinghe"])
        ctx = r.parse_group_message(body(segments=[
            {"type": "text", "data": {"text": "小念你在干嘛"}}]))
        ok, roles = r.should_reply(ctx)
        assert ok and roles == ["nianqi"]

    def test_plain_text_no_trigger(self, tmp_path):
        r = make_router(tmp_path)
        self._cfg(r.config, trigger=["at", "reply"])
        ctx = r.parse_group_message(body())
        ok, roles = r.should_reply(ctx)
        assert not ok and roles == []

    def test_probability_one_always(self, tmp_path):
        r = make_router(tmp_path)
        self._cfg(r.config, trigger=["probability"], probability=1.0)
        ctx = r.parse_group_message(body())
        ok, _ = r.should_reply(ctx)
        assert ok

    def test_probability_zero_never(self, tmp_path):
        r = make_router(tmp_path)
        self._cfg(r.config, trigger=["probability"], probability=0.0)
        ctx = r.parse_group_message(body())
        ok, _ = r.should_reply(ctx)
        assert not ok

    def test_disabled_group_silent(self, tmp_path):
        r = make_router(tmp_path)
        self._cfg(r.config, enabled=False)
        ctx = r.parse_group_message(body(segments=[
            {"type": "at", "data": {"qq": BOT}}]))
        assert r.should_reply(ctx) == (False, [])


def test_identity_isolation(tmp_path):
    r = make_router(tmp_path)
    ctx = r.parse_group_message(body())
    ident = GroupMessageRouter.build_user_identity(ctx)
    assert ident == "qq_group_555_u_20001"


def test_config_persistence_roundtrip(tmp_path):
    path = str(tmp_path / "cfg.json")
    m1 = GroupConfigManager(config_path=path)
    m1.upsert_group_config("555", {"enabled": True, "roles": ["nianqi"],
                                   "trigger": ["at"], "probability": 0.0})
    # 重新加载（模拟进程重启）
    m2 = GroupConfigManager(config_path=path)
    cfg = m2.get_group_config("555")
    assert cfg.roles == ["nianqi"] and cfg.trigger == ["at"]

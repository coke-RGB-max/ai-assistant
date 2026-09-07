"""v15 主动话题“续聊意愿闸门”测试：深夜/末句告别/模型否决都不得发起话题（回归“该睡觉了仍被找话题”）。"""
import asyncio

import pytest

import proactive_server as pro
from proactive_server import (
    assess_continue_willingness, _last_user_text, clear_resume_block,
    _resume_block, _intent_cache, ConversationIntentDetector,
)


def hist(*pairs):
    """pairs: (role, content) 交替，生成 recent_messages JSON。"""
    import json
    msgs = [{"role": r, "content": c} for r, c in pairs]
    return json.dumps(msgs, ensure_ascii=False)


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    _intent_cache.clear()
    _resume_block.clear()
    # 默认白天、不接外接 LLM，走纯规则路径
    monkeypatch.setattr(pro, "PROACTIVE_LLM_API_KEY", "")
    monkeypatch.setattr(pro, "is_silent_hour_now", lambda: False)
    yield
    _intent_cache.clear()
    _resume_block.clear()


def run(coro):
    return asyncio.run(coro)


def test_last_user_text_picks_final_user_msg():
    j = hist(("user", "在吗"), ("ai", "在呀"), ("user", "今天好累"))
    assert _last_user_text(j) == "今天好累"


def test_normal_chat_allows_resume():
    j = hist(("user", "今天吃到一家超好吃的店"), ("ai", "什么店呀"),
             ("user", "下次带你去"))
    can, reason = run(assess_continue_willingness("u1", "nianqi", j, 100.0))
    assert can is True and reason == "rule_allow"


def test_sleep_intent_blocks_regardless_of_ttl():
    # ★本次事故：用户最后一句要睡觉，哪怕超过30分钟缓存也不许发起话题
    j = hist(("ai", "傍晚啦"), ("user", "时间不早了，该睡觉了"))
    can, reason = run(assess_continue_willingness("u1", "nianqi", j, 999999.0))
    assert can is False and reason == "last_user_end_intent"


@pytest.mark.parametrize("text", [
    "晚安", "我先去洗澡了", "先这样吧", "明天聊", "不聊了", "撑不住了",
])
def test_various_goodbye_phrases_block(text):
    j = hist(("ai", "好"), ("user", text))
    can, _ = run(assess_continue_willingness("u", "nianqi", j, 1.0))
    assert can is False


def test_silent_hour_blocks(monkeypatch):
    monkeypatch.setattr(pro, "is_silent_hour_now", lambda: True)
    j = hist(("user", "哈哈"), ("ai", "嗯嗯"))
    can, reason = run(assess_continue_willingness("u", "nianqi", j, 1.0))
    assert can is False and reason == "silent_hour"


def test_intent_cache_short_circuits():
    _intent_cache["u1"] = __import__("time").time()
    j = hist(("user", "随便一句"))
    can, reason = run(assess_continue_willingness("u1", "nianqi", j, 1.0))
    assert can is False and reason == "intent_cache_active"


def test_llm_block_is_memoized_per_state(monkeypatch):
    monkeypatch.setattr(pro, "PROACTIVE_LLM_API_KEY", "sk-test")
    calls = {"n": 0}

    async def fake_llm(_j):
        calls["n"] += 1
        return False, "对方已道晚安"

    monkeypatch.setattr(pro, "_llm_assess_continue", fake_llm)
    j = hist(("ai", "在吗"), ("user", "嗯"))  # 规则不命中，交给模型
    can1, r1 = run(assess_continue_willingness("u", "nianqi", j, 50.0))
    can2, r2 = run(assess_continue_willingness("u", "nianqi", j, 50.0))  # 同状态
    assert can1 is False and can2 is False
    assert r1 == "llm_blocked:对方已道晚安"
    assert calls["n"] == 1, "同一对话状态不得重复调用模型"


def test_clear_block_after_user_speaks_again(monkeypatch):
    monkeypatch.setattr(pro, "PROACTIVE_LLM_API_KEY", "sk-test")

    async def fake_llm(_j):
        return False, "要睡了"

    monkeypatch.setattr(pro, "_llm_assess_continue", fake_llm)
    j_old = hist(("user", "嗯"))
    run(assess_continue_willingness("u", "nianqi", j_old, 50.0))
    assert _resume_block  # 已被否决
    # 用户重新开口 → 清除否决；新对话状态模型这次放行
    clear_resume_block("u", "nianqi")

    async def allow(_j):
        return True, "对方在主动分享"

    monkeypatch.setattr(pro, "_llm_assess_continue", allow)
    j_new = hist(("user", "我跟你说个事"))
    can, reason = run(assess_continue_willingness("u", "nianqi", j_new, 99.0))
    assert can is True and reason == "llm_allow"


def test_keyword_detector_covers_sleep():
    assert ConversationIntentDetector.detect("时间不早了，该睡觉了") is not None
    assert ConversationIntentDetector.detect("今天天气不错") is None

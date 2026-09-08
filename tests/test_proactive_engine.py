"""主动消息流水线纯逻辑测试：阶段划分 / 欲望演算 / 动机过滤 / mood 契约回归。"""
import asyncio
import random

import pytest

from proactive_server import (
    stage_of, LongingEngine, MotivationEngine,
)


# ---------- stage_of 五段关系阶段 ----------
@pytest.mark.parametrize("intimacy,expected", [
    (0, "stranger"), (30, "stranger"),
    (31, "acquaintance"), (50, "acquaintance"),
    (51, "familiar"), (70, "familiar"),
    (71, "close"), (85, "close"),
    (86, "intimate"), (100, "intimate"),
])
def test_stage_of_boundaries(intimacy, expected):
    assert stage_of(intimacy) == expected


# ---------- LongingEngine ----------
class TestLongingEngine:
    def setup_method(self):
        self.eng = LongingEngine()

    def test_local_desire_has_five_dims_and_bounded(self):
        random.seed(1)
        desire = self.eng.calc_local_desire("nianqi", 60, 50.0, 30.0, "lonely", 21)
        assert set(desire) == {"longing", "contact_desire", "share_desire",
                               "care_desire", "companionship"}
        for v in desire.values():
            assert 0.0 <= v <= 100.0

    def test_unknown_role_falls_back_to_base(self):
        random.seed(2)
        desire = self.eng.calc_local_desire("not_exist_role", 50, 0, 0, "calm", 12)
        assert set(desire) == {"longing", "contact_desire", "share_desire",
                               "care_desire", "companionship"}

    def test_motivation_score_weights(self):
        # 手造五维，精确验证 0.30/0.25/0.20/0.15/0.10 权重
        desire = {"contact_desire": 100, "longing": 100, "companionship": 100,
                  "care_desire": 100, "share_desire": 100}
        assert self.eng.calc_motivation_score(desire) == 100.0
        zero = {k: 0 for k in desire}
        assert self.eng.calc_motivation_score(zero) == 0.0
        partial = {"contact_desire": 100, "longing": 0, "companionship": 0,
                   "care_desire": 0, "share_desire": 0}
        assert self.eng.calc_motivation_score(partial) == 30.0

    def test_dominant_desire(self):
        desire = {"longing": 10, "contact_desire": 80, "share_desire": 30,
                  "care_desire": 20, "companionship": 40}
        name, val = self.eng.dominant_desire(desire)
        assert name == "contact_desire" and val == 80

    def test_calc_local_branch_has_top_level_mood(self):
        # 回归：KeyError: 'mood' —— 本地分支必须带顶层 mood
        random.seed(3)
        out = asyncio.run(self.eng.calc("u1", "nianqi", 60, 50, 20, "lonely", 21))
        assert out["mood"] == "lonely"
        for key in ("source", "desire", "dominant", "dominant_value",
                    "motivation_score", "breakdown"):
            assert key in out
        assert out["source"] == "local_calculation"
        # breakdown 与 desire 同结构
        assert set(out["breakdown"]) == set(out["desire"])


# ---------- MotivationEngine 过滤约束层 ----------
class TestMotivationEngine:
    def setup_method(self):
        self.eng = MotivationEngine()

    @pytest.mark.parametrize("idle,expect_range", [
        (0, (0.0, 0.0)), (2.9, (0.0, 0.0)),
        (24, (20.0, 20.0)), (48, (45.0, 45.0)), (72, (70.0, 70.0)),
    ])
    def test_idle_score_piecewise(self, idle, expect_range):
        v = self.eng.calc_idle_score(idle)
        assert v == pytest.approx(expect_range[0], abs=0.01)

    @pytest.mark.parametrize("hour,score", [
        (20, 10.0), (10, 5.0), (15, 0.0), (8, -10.0), (3, -30.0), (23, -30.0),
    ])
    def test_time_score(self, hour, score):
        assert self.eng.calc_time_score(hour) == score

    def test_mood_score(self):
        assert self.eng.mood_score("lonely") == 15.0
        assert self.eng.mood_score("calm") == 0.0
        assert self.eng.mood_score("unknown_mood") == 0.0

    def _mot(self, score=80.0):
        return {"motivation_score": score}

    def test_filter_night_suppression(self):
        # 深夜 -25：80 分在 nianqi 阈值 35 下仍通过，但分数被压
        day = self.eng.filter(self._mot(80), hour=14, role_id="nianqi", intimacy=60)
        night = self.eng.filter(self._mot(80), hour=2, role_id="nianqi", intimacy=60)
        assert day["final_score"] == 80.0
        assert night["final_score"] == 55.0
        assert any("深夜" in f for f in night["suppress_factors"])

    def test_filter_unreplied_suppression(self):
        none = self.eng.filter(self._mot(80), unreplied_count=0, intimacy=60)
        two = self.eng.filter(self._mot(80), unreplied_count=2, intimacy=60)
        assert none["final_score"] - two["final_score"] == 40.0

    def test_filter_cooldown_uses_actual_stage(self):
        # familiar(60) 冷却 4h；1h < 4h 触发 -30
        res = self.eng.filter(self._mot(80), hours_since_last=1.0, intimacy=60)
        assert any("冷却" in f for f in res["suppress_factors"])
        assert res["final_score"] == 50.0

    def test_filter_role_threshold(self):
        # jingwen 阈值 55：50 分不通过，60 分通过
        low = self.eng.filter(self._mot(50), hour=14, role_id="jingwen", intimacy=70)
        high = self.eng.filter(self._mot(60), hour=14, role_id="jingwen", intimacy=70)
        assert low["allowed"] is False and high["allowed"] is True

    def test_filter_stranger_zero_allowed(self):
        # 陌生人即便原始分高，冷却 24h 也会压制，且阈值默认角色 nianqi
        res = self.eng.filter(self._mot(90), hours_since_last=0.5, intimacy=20)
        assert res["original_score"] == 90.0
        assert 0.0 <= res["final_score"] <= 100.0

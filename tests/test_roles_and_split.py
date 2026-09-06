"""共享底座与 P4 拆分归位测试（防止内联回退/双份实现复活）。"""
import core.roles as roles
from core.roles import (
    _resolve_stage_key, get_cached_persona, reload_role_definitions,
    ROLES_DEFINITION,
)
import pytest


@pytest.mark.parametrize("intimacy,key", [
    (0, "0-30"), (30, "0-30"), (31, "31-50"), (50, "31-50"),
    (51, "51-70"), (70, "51-70"), (71, "71-85"), (85, "71-85"),
    (86, "86-100"), (100, "86-100"),
])
def test_stage_key_boundaries(intimacy, key):
    assert _resolve_stage_key(intimacy) == key


def test_three_roles_loaded():
    assert set(ROLES_DEFINITION) == {"nianqi", "qinghe", "jingwen"}


def test_persona_cache_hit_and_content():
    first = get_cached_persona("nianqi", 20)
    second = get_cached_persona("nianqi", 20)
    assert first == second and len(first) > 0
    # 不同阶段生成不同 prompt
    other = get_cached_persona("nianqi", 95)
    assert other != first


def test_reload_keeps_same_object_identity():
    # 热重载必须原地 clear+update，跨模块持有的引用才能同步
    before = ROLES_DEFINITION
    result = reload_role_definitions()
    assert result is ROLES_DEFINITION and before is ROLES_DEFINITION
    assert set(ROLES_DEFINITION) == {"nianqi", "qinghe", "jingwen"}


def test_domain_packages_self_contained():
    # 领域包必须能独立导入（不依赖 personality_server 全局命名空间）
    import emotion, psych, memory, knowledge, group, quality, topic, scene
    assert emotion.EmotionEngine.__module__ == "emotion"
    assert psych.PsychologicalState.__module__ == "psych"
    assert memory.MemorySystem.__module__ == "memory"
    assert group.GroupBrain.__module__ == "group"


def test_personality_classes_come_from_packages():
    # 回归：不得回退到 personality_server 内联副本
    import personality_server as ps
    expect = {
        "EmotionEngine": "emotion", "PsychologicalState": "psych",
        "MemorySystem": "memory", "KnowledgeRouter": "knowledge",
        "GroupBrain": "group", "QualityChecker": "quality",
        "TopicInitiator": "topic", "SceneModeEngine": "scene",
    }
    for cls_name, pkg in expect.items():
        cls = getattr(ps, cls_name)
        assert cls.__module__.split(".")[0] == pkg, f"{cls_name} 未归位到 {pkg}"
    # 共享底座必须是同一个对象
    assert ps.ROLES_DEFINITION is roles.ROLES_DEFINITION

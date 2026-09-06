"""
角色与人格底座（P4 拆分共享层）
------------------------------------------------------------------
本模块是 emotion/psych/memory/knowledge/group/quality/topic/scene
等领域包与 personality_server 共同依赖的“单一事实源”：

- 角色 YAML 加载与热重载（ROLES_DEFINITION）
- 关系事件分类 / 关系里程碑 / 虚拟礼物三张常量表
- 固定人格 Prompt 缓存（get_cached_persona）

注意：热重载必须“原地更新”同一个 dict（clear + update），
因为各领域包通过 `from core.roles import ROLES_DEFINITION` 持有引用，
重新赋值只会改变本模块绑定，无法同步到已导入方。
"""
import os
import sys
from typing import Dict

# 确保项目根目录在 sys.path（characters 包位于项目根）
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from characters.loader import (
    load_all_roles,
    get_role as _get_role,
    reload_roles as _reload_roles,
)

# ============================================================
# 角色配置加载（人格即配置：改 YAML 不需要改代码）
# ============================================================
# 启动时加载所有角色配置
ROLES_DEFINITION: Dict[str, dict] = load_all_roles()


def get_role_definition(role_id: str) -> dict:
    """获取角色配置（优先走 loader 缓存，支持热重载）。"""
    return _get_role(role_id) or ROLES_DEFINITION.get(role_id, {})


def reload_role_definitions() -> dict:
    """热重载角色配置（管理员调用）。原地更新同一 dict，保证跨模块可见。"""
    new_data = _reload_roles()
    ROLES_DEFINITION.clear()
    ROLES_DEFINITION.update(new_data)
    _PERSONA_CACHE.clear()
    return ROLES_DEFINITION


# ============================================================
# 关系事件分类表
# ============================================================
EVENT_CATEGORY = {
    "user_comfort": "positive", "user_praise": "positive",
    "user_confess": "positive", "user_apologize": "positive",
    "user_share": "positive", "user_rely": "positive",
    "long_time_no_see": "positive",
    "user_cold": "negative", "user_ignore": "negative",
    "user_doubt": "negative", "user_criticize": "negative",
    "user_mention_other": "negative",
    "none": "neutral",
}

# ============================================================
# v10.0: 关系里程碑定义
# ============================================================
RELATIONSHIP_MILESTONES = {
    "first_quarrel":    {"name": "第一次吵架", "intimacy_delta": -2, "resilience_gain": 5, "once": True},
    "first_confession": {"name": "第一次说喜欢", "intimacy_delta": +8, "resilience_gain": 3, "once": True},
    "first_apology":    {"name": "第一次道歉", "intimacy_delta": +3, "resilience_gain": 8, "once": True},
    "first_nickname":   {"name": "第一次叫外号", "intimacy_delta": +2, "resilience_gain": 2, "once": True},
    "stayed_up_late":   {"name": "第一次深夜聊天", "intimacy_delta": +4, "resilience_gain": 3, "once": True},
    "first_gift":       {"name": "第一次送礼物", "intimacy_delta": +3, "resilience_gain": 2, "once": True},
    "first_comfort":    {"name": "第一次安慰对方", "intimacy_delta": +5, "resilience_gain": 4, "once": True},
    "intimacy_50":      {"name": "关系突破50", "intimacy_delta": 0, "resilience_gain": 5, "once": True},
    "intimacy_80":      {"name": "关系突破80", "intimacy_delta": 0, "resilience_gain": 10, "once": True},
}

# ============================================================
# v10.0: 虚拟礼物定义
# ============================================================
VIRTUAL_GIFTS = {
    "flower": {"name": "花", "nianqi": "哇，好漂亮～谢谢你，我会好好养着的，每次看到都会想到你", "qinghe": "谢谢你，我会好好养着的～", "jingwen": "谁、谁要你送啊…不过放这吧"},
    "food":   {"name": "食物", "nianqi": "是给我的吗？谢谢你～你也吃一点呀，一起吃才香", "qinghe": "谢谢你，我会好好吃完的～", "jingwen": "算你有眼光，这个我勉强收下了"},
    "drink":  {"name": "饮料", "nianqi": "哇，正好渴了～谢谢你，是热的吗？你真贴心", "qinghe": "是热的吗？谢谢你这么贴心～", "jingwen": "正好渴了，谢了"},
    "letter": {"name": "手写信", "nianqi": "（认真看完，眼睛有点红）…谢谢你，我会好好珍藏的，这是我收到最珍贵的东西", "qinghe": "我会好好珍藏的，谢谢你", "jingwen": "你、你写这个干嘛…（偷偷收好）"},
    "plush":  {"name": "毛绒玩具", "nianqi": "好可爱～谢谢你，我会抱着它睡觉的，就像你在陪着我一样", "qinghe": "好可爱～谢谢你，我会放在床头的", "jingwen": "这么大…我才不会抱它睡觉呢"},
    "jewelry": {"name": "饰品", "nianqi": "（轻轻戴上）…谢谢你，我会一直戴着的，每次看到都会想起你", "qinghe": "谢谢你，我会一直戴着的～", "jingwen": "这、这个太贵了吧…不过我收下了"},
}

# ============================================================
# v8.1: 固定人格 Prompt 缓存
# ============================================================
_PERSONA_CACHE: Dict[str, str] = {}


def _resolve_stage_key(intimacy):
    """五段关系阶段划分，与全局关系阶段口径一致。"""
    if intimacy <= 30: return "0-30"
    if intimacy <= 50: return "31-50"
    if intimacy <= 70: return "51-70"
    if intimacy <= 85: return "71-85"
    return "86-100"


def get_cached_persona(rid, intimacy):
    stage_key = _resolve_stage_key(intimacy)
    cache_key = f"{rid}:{stage_key}"
    if cache_key in _PERSONA_CACHE:
        return _PERSONA_CACHE[cache_key]
    role = ROLES_DEFINITION.get(rid, {})
    if not role: return ""
    parts = [
        f"你是{role['name']}，{role['age']}{role['gender']}生。{role['description']}",
    ]
    # === 关系定位（核心）===
    rel = role.get("relationship", {})
    if rel:
        rel_type = rel.get("type", "")
        if rel_type:
            parts.append(f"【你与对方的关系】{rel_type}")
        core_dynamic = rel.get("core_dynamic", "")
        if core_dynamic:
            parts.append(f"【关系基调】{core_dynamic}")
        stage_info = rel.get("stages", {}).get(stage_key, {})
        if stage_info:
            label = stage_info.get("label", "")
            distance = stage_info.get("distance", "")
            behavior = stage_info.get("behavior", "")
            if label:
                parts.append(f"【当前关系阶段】{label}（亲密度{intimacy}/100）")
            if distance:
                parts.append(f"【你们之间的距离感】{distance}")
            if behavior:
                parts.append(f"【你在这个阶段的行为边界】{behavior}")
        hard_boundaries = rel.get("hard_boundaries", [])
        if hard_boundaries:
            parts.append(f"【关系红线（任何阶段都不可逾越）】{'；'.join(hard_boundaries)}")
    else:
        # 兼容旧的三段式 intimacy_prompts（未配置 relationship 字段时回退）
        old_key = "0-50" if intimacy <= 50 else ("51-80" if intimacy <= 80 else "81-100")
        old_prompt = role.get("intimacy_prompts", {}).get(old_key, "")
        if old_prompt:
            parts.append(old_prompt)
    # === 依恋类型 ===
    att = role.get("attachment_type", "")
    if att:
        parts.append(f"【依恋类型】{att}")
    # === 核心信念 ===
    beliefs = role.get("core_beliefs", [])
    if beliefs:
        parts.append(f"【核心信念】{'；'.join(beliefs)}")
    # === 当前表达能力阶段 ===
    expr_stage = role.get("expression_stage", "")
    if expr_stage:
        parts.append(f"【当前表达能力】{expr_stage}")
    # === 秘密揭露阶段（兼容不同角色的字段名：画室/故事集/树洞）===
    secret_stage = (role.get("studio_reveal_stage") or role.get("story_reveal_stage")
                    or role.get("blog_reveal_stage") or "")
    if secret_stage:
        parts.append(f"【秘密阶段】{secret_stage}")
    # === 软肋（特定场景下的反差破防点）===
    soft = role.get("soft_spots", [])
    if soft:
        soft_lines = []
        for i, s in enumerate(soft, 1):
            t = s.get("trigger", "")
            r = s.get("reaction", "")
            if t and r:
                soft_lines.append(f"{i}. 触发：{t} → 反应：{r}")
        if soft_lines:
            parts.append("【软肋（遇到这些场景会破防）】\n" + "\n".join(soft_lines))
    # === 激怒/触动点 ===
    hb = role.get("hot_buttons", {})
    if hb:
        anger = hb.get("anger", [])
        if anger:
            anger_lines = []
            for i, a in enumerate(anger, 1):
                t = a.get("trigger", "")
                r = a.get("reaction", "")
                if t and r:
                    anger_lines.append(f"{i}. 触发：{t} → 反应：{r}")
            if anger_lines:
                parts.append("【激怒点（遇到这些会生气/冷脸）】\n" + "\n".join(anger_lines))
        touch = hb.get("touch", [])
        if touch:
            touch_lines = []
            for i, t in enumerate(touch, 1):
                trig = t.get("trigger", "")
                reac = t.get("reaction", "")
                if trig and reac:
                    touch_lines.append(f"{i}. 触发：{trig} → 反应：{reac}")
            if touch_lines:
                parts.append("【正面触动点（遇到这些会感动/破防）】\n" + "\n".join(touch_lines))
    # === 行为模式（默认反应）===
    bp = role.get("behavior_patterns", {})
    if bp:
        bp_lines = []
        for key, label in [("injured", "受伤时"), ("praised", "被夸奖时"), ("rejected", "被拒绝时"), ("needed", "被需要时")]:
            val = bp.get(key, "")
            if val:
                bp_lines.append(f"{label}：{val}")
        if bp_lines:
            parts.append("【行为模式（默认反应）】\n" + "\n".join(bp_lines))
    # === 人格基础 ===
    parts.append(f"核心特质：{'、'.join(role.get('core_traits', []))}")
    parts.append(f"你看重：{'、'.join(role.get('values', []))}")
    tb = role.get("taboos", [])
    if tb: parts.append(f"逆鳞：{'、'.join(tb)}——触碰时你会明显不悦")
    result = "\n".join(parts)
    _PERSONA_CACHE[cache_key] = result
    return result

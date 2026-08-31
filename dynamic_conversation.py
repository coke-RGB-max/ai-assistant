"""
dynamic_conversation.py — 动态对话引擎
========================================
为对话增加「动态感」：主动性、情绪记忆、节奏变化、角色专属剧情推进。

设计原则：
- 所有角色特定内容通过 YAML 的 dynamic 字段配置，代码不写死任何角色
- 后面加新人物只需在 YAML 里加 dynamic 配置，无需改本文件
- 生成的动态上下文以文本形式注入 system prompt，不修改回复后处理逻辑

四个子模块：
1. InitiativeDecider    — 主动性决策（主动追问/转移话题/反客为主）
2. MemoryReferencer     — 情绪记忆引用（自然提到用户之前说过的事）
3. RhythmController     — 节奏控制（长段落/短句子/沉默停顿）
4. PlotEngine           — 剧情推进（角色专属剧情线，好感度阈值触发）
"""

import random
import re
from typing import Optional


# ============================================================
# 1. 主动性决策器
# ============================================================
class InitiativeDecider:
    """决定角色是否主动以及主动什么。

    主动性程度随好感度增长，但不同角色的主动方式不同（由 YAML 配置）。
    """

    # 通用主动类型（角色可以在 YAML 中启用/禁用某些类型）
    INITIATIVE_TYPES = {
        "follow_up": "主动追问：问他一个关于他自己的问题，比如他最近在忙什么、今天过得怎么样、那件事后来怎么样了",
        "change_topic": "转移话题：不说当前这个话题了，主动提起一件你今天遇到的事、一个你看到的东西、或者一个你突然想到的问题",
        "reverse": "反客为主：突然抛出一个你自己的问题或感受，让他来回应你、安慰你、或者了解你",
    }

    def decide(self, dynamic_config: dict, user_message: str,
               conversation_history: list, affection: int) -> Optional[str]:
        """生成主动性指令文本，不需要主动时返回 None。"""
        initiative_cfg = dynamic_config.get("initiative", {})
        if not initiative_cfg.get("enabled", True):
            return None

        style = initiative_cfg.get("style", "")
        if not style:
            return None

        # 计算主动性程度（0-1）
        level = self._calc_level(affection, initiative_cfg)
        if level < 0.15:
            return None

        # 判断是否应该主动
        if not self._should_initiate(user_message, conversation_history, level):
            return None

        # 选择主动类型
        enabled_types = initiative_cfg.get("types", ["follow_up", "change_topic", "reverse"])
        if level < 0.35:
            # 低主动性：只敢小心翼翼追问
            chosen_type = "follow_up" if "follow_up" in enabled_types else None
        elif level < 0.65:
            # 中主动性：可以追问或转移话题
            pool = [t for t in ["follow_up", "change_topic"] if t in enabled_types]
            chosen_type = random.choice(pool) if pool else None
        else:
            # 高主动性：三种都可以
            pool = [t for t in enabled_types if t in self.INITIATIVE_TYPES]
            chosen_type = random.choice(pool) if pool else None

        if not chosen_type:
            return None

        type_desc = self.INITIATIVE_TYPES[chosen_type]
        return (
            f"【主动性指令】\n"
            f"{style}\n\n"
            f"本次对话你可以{type_desc}。注意要自然，不要太刻意，"
            f"要符合你当前和他的关系程度。"
        )

    def _calc_level(self, affection: int, cfg: dict) -> float:
        """根据好感度计算主动性程度。"""
        # 基础曲线：好感度越高越主动
        base = min(affection / 100.0, 1.0)
        # 角色可以调整主动性系数（比如害羞的角色系数低，外向的角色系数高）
        multiplier = cfg.get("multiplier", 1.0)
        return min(base * multiplier, 1.0)

    def _should_initiate(self, user_message: str, history: list, level: float) -> bool:
        """判断本次是否应该主动。"""
        msg = user_message.strip()
        # 用户消息越短/越敷衍，越可能主动
        if len(msg) < 4:
            base_prob = 0.55
        elif len(msg) < 12:
            base_prob = 0.35
        elif len(msg) < 25:
            base_prob = 0.20
        else:
            base_prob = 0.10

        # 结合主动性程度
        prob = base_prob * (0.5 + level)
        return random.random() < min(prob, 0.65)


# ============================================================
# 2. 情绪记忆引用器
# ============================================================
class MemoryReferencer:
    """从记忆中提取用户说过的事，自然地引用到对话中。

    不同角色引用记忆的方式不同（由 YAML 配置）。
    """

    def reference(self, dynamic_config: dict, user_message: str,
                  memories: list, affection: int) -> Optional[str]:
        """生成记忆引用指令文本，没有合适记忆时返回 None。"""
        mem_cfg = dynamic_config.get("memory", {})
        if not mem_cfg.get("enabled", True):
            return None

        style = mem_cfg.get("style", "")
        if not style or not memories:
            return None

        # 记忆引用概率
        prob = mem_cfg.get("probability", 0.35)
        if random.random() > prob:
            return None

        # 从记忆中挑选 1-2 条相关的
        relevant = self._pick_relevant(memories, user_message, mem_cfg)
        if not relevant:
            return None

        mem_texts = []
        for m in relevant[:2]:
            content = m.get("content", "") if isinstance(m, dict) else str(m)
            if content:
                mem_texts.append(f"- {content}")

        if not mem_texts:
            return None

        return (
            f"【情绪记忆】\n"
            f"你记得他之前说过/经历过这些事：\n"
            f"{chr(10).join(mem_texts)}\n\n"
            f"{style}\n"
            f"你可以在回复中自然地提到其中一件事，让他知道你记得。"
            f"不要太刻意，不要说'你上次说过'，要像随口提到一样。"
        )

    def _pick_relevant(self, memories: list, user_message: str, cfg: dict) -> list:
        """从记忆中挑选相关的。简单实现：随机挑选，优先选包含用户消息关键词的。"""
        if not memories:
            return []

        # 尝试关键词匹配
        msg_words = set(re.findall(r'[\u4e00-\u9fa5a-zA-Z]{2,}', user_message))
        scored = []
        for m in memories:
            content = m.get("content", "") if isinstance(m, dict) else str(m)
            score = 0
            for w in msg_words:
                if w in content:
                    score += 1
            # 记忆本身的权重
            if isinstance(m, dict):
                score += m.get("weight", 50) / 100.0
            scored.append((score, m))

        scored.sort(key=lambda x: x[0], reverse=True)
        # 优先返回高分的，如果最高分是0则随机返回
        if scored[0][0] > 0:
            return [m for _, m in scored[:2]]
        else:
            return random.sample(memories, min(1, len(memories)))


# ============================================================
# 3. 节奏控制器
# ============================================================
class RhythmController:
    """根据当前情绪状态控制回复的节奏（长段落/短句子/沉默停顿）。

    不同角色的节奏风格不同（由 YAML 配置）。
    """

    # 通用情绪→节奏映射（角色可以在 YAML 中覆盖）
    EMOTION_RHYTHM = {
        "shy": {
            "instruction": "你现在很害羞/不好意思，用短句子回复，每句不超过10个字，带省略号，不敢直接表达，可能会有停顿",
            "example": "……嗯。" "我知道了。" "别、别这样。",
        },
        "angry": {
            "instruction": "你现在有点生气/不爽，用短促的句子，带感叹号，语速快，带刺，可能会有连续的短句",
            "example": "你笨蛋啊！" "谁要你管！" "滚。",
        },
        "serious": {
            "instruction": "你现在很认真/在想事情，用长段落，慢慢说，有条理，表达你的真实想法，可能会有停顿和转折",
            "example": "其实我一直想跟你说一件事……",
        },
        "happy": {
            "instruction": "你现在很开心/兴奋，用中等长度的句子，语气轻快，带语气词，可能会有重复和感叹",
            "example": "真的吗？太好了！" "哇——！",
        },
        "sad": {
            "instruction": "你现在有点难过/低落，用短句子，带省略号，语速慢，可能会有沉默和停顿，不要长篇大论",
            "example": "……嗯。" "我没事。" "别管我。",
        },
        "jealous": {
            "instruction": "你现在有点吃醋/不爽，用带刺的短句，话里有话，可能会有反问和冷嘲热讽",
            "example": "哦，她啊。" "挺好的。" "关我什么事。",
        },
    }

    def control(self, dynamic_config: dict, user_message: str,
                emotion: str, affection: int) -> Optional[str]:
        """生成节奏控制指令文本，正常情绪时返回 None。"""
        rhythm_cfg = dynamic_config.get("rhythm", {})
        if not rhythm_cfg.get("enabled", True):
            return None

        style = rhythm_cfg.get("style", "")
        if not style:
            return None

        # 检测当前情绪对应的节奏
        emotion_lower = (emotion or "").lower()
        rhythm_info = None

        # 先看角色自定义的情绪节奏映射
        custom_map = rhythm_cfg.get("emotion_map", {})
        if emotion_lower in custom_map:
            rhythm_info = custom_map[emotion_lower]
        # 再用通用映射
        elif emotion_lower in self.EMOTION_RHYTHM:
            rhythm_info = self.EMOTION_RHYTHM[emotion_lower]

        if not rhythm_info:
            return None

        instruction = rhythm_info.get("instruction", "") if isinstance(rhythm_info, dict) else str(rhythm_info)
        if not instruction:
            return None

        return (
            f"【节奏控制】\n"
            f"{style}\n\n"
            f"{instruction}\n"
            f"注意：节奏变化是为了让对话更真实，不是每次都要极端，"
            f"根据情绪强度适度调整。"
        )


# ============================================================
# 4. 剧情引擎
# ============================================================
class PlotEngine:
    """角色专属剧情线推进，基于好感度阈值触发剧情节点。

    剧情事件在 YAML 的 dynamic.plot.events 中配置，每个事件有：
    - affection: 触发好感度阈值
    - content: 剧情内容描述
    - id: 可选，事件唯一标识（不填则用 affection 作为标识）

    剧情状态在内存中跟踪，每个用户+角色独立。
    """

    def __init__(self):
        # 剧情状态缓存：{user_id: {role_id: {event_id: True}}}
        self._state = {}

    def advance(self, dynamic_config: dict, role_id: str, user_id: str,
                affection: int, user_message: str) -> Optional[str]:
        """检查是否触发新的剧情事件，返回剧情指令文本。"""
        plot_cfg = dynamic_config.get("plot", {})
        if not plot_cfg.get("enabled", True):
            return None

        events = plot_cfg.get("events", [])
        if not events:
            return None

        # 获取用户的剧情状态
        user_state = self._state.setdefault(user_id, {}).setdefault(role_id, {})

        # 检查是否有新触发的剧情事件
        triggered = None
        for event in events:
            threshold = event.get("affection", 0)
            event_id = event.get("id", f"affection_{threshold}")

            # 好感度达到阈值且未触发过
            if affection >= threshold and event_id not in user_state:
                # 检查是否是合适的触发时机（用户消息不是太敷衍）
                if len(user_message.strip()) >= 2:
                    triggered = event
                    user_state[event_id] = True
                    break

        if not triggered:
            return None

        content = triggered.get("content", "")
        threshold = triggered.get("affection", 0)

        return (
            f"【剧情事件】\n"
            f"当前好感度达到 {threshold}，触发了新的剧情节点：\n"
            f"{content}\n\n"
            f"在本次回复中自然地融入这个剧情节点，不要太刻意，"
            f"不要直接说'好感度达到了XX'，要通过角色的言行来体现。"
        )

    def get_state(self, user_id: str, role_id: str) -> dict:
        """获取用户的剧情状态（用于调试/持久化）。"""
        return self._state.get(user_id, {}).get(role_id, {})

    def reset(self, user_id: str = None, role_id: str = None):
        """重置剧情状态（用于调试）。"""
        if user_id and role_id:
            self._state.get(user_id, {}).pop(role_id, None)
        elif user_id:
            self._state.pop(user_id, None)
        else:
            self._state.clear()


# ============================================================
# 主引擎：动态对话引擎
# ============================================================
class DynamicConversationEngine:
    """动态对话引擎——整合主动性、情绪记忆、节奏控制、剧情推进。

    用法：
        engine = DynamicConversationEngine()
        context = engine.generate(
            role_id="jingwen",
            role_config=role_yaml_dict,
            user_message="你好",
            conversation_history=[...],
            affection=65,
            emotion="happy",
            memories=[...],
            user_id="user_123",
        )
        # 将 context 注入到 system prompt 中
    """

    def __init__(self):
        self.initiative = InitiativeDecider()
        self.memory = MemoryReferencer()
        self.rhythm = RhythmController()
        self.plot = PlotEngine()

    def generate(self, role_id: str, role_config: dict,
                 user_message: str, conversation_history: list,
                 affection: int, emotion: str = "neutral",
                 memories: list = None, user_id: str = "default") -> str:
        """生成动态对话上下文，注入到 system prompt 中。

        Args:
            role_id: 角色ID
            role_config: 角色YAML配置（完整字典，包含 dynamic 字段）
            user_message: 用户当前消息
            conversation_history: 对话历史
            affection: 当前好感度（0-100）
            emotion: 当前情绪（happy/sad/angry/shy/serious/jealous/neutral 等）
            memories: 记忆列表（从 MemorySystem 处理后的结果）
            user_id: 用户ID（用于剧情状态跟踪）

        Returns:
            动态上下文字符串，没有动态内容时返回空字符串
        """
        dynamic_config = role_config.get("dynamic", {})
        if not dynamic_config:
            return ""

        parts = []

        # 1. 主动性
        initiative_text = self.initiative.decide(
            dynamic_config, user_message, conversation_history, affection)
        if initiative_text:
            parts.append(initiative_text)

        # 2. 情绪记忆引用
        memory_text = self.memory.reference(
            dynamic_config, user_message, memories or [], affection)
        if memory_text:
            parts.append(memory_text)

        # 3. 节奏控制
        rhythm_text = self.rhythm.control(
            dynamic_config, user_message, emotion, affection)
        if rhythm_text:
            parts.append(rhythm_text)

        # 4. 剧情推进
        plot_text = self.plot.advance(
            dynamic_config, role_id, user_id, affection, user_message)
        if plot_text:
            parts.append(plot_text)

        return "\n\n".join(parts) if parts else ""

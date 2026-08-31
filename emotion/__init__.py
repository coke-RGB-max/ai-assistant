"""
FlexiChrono emotion 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- DailyNoiseLayer
- EmotionEngine
- AlterSystem
- MicroNarrativeEngine
- EmotionBlender
"""
import logging
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
from collections import defaultdict
import asyncio, json, re, random, time, os, sqlite3, hashlib, datetime
import httpx
from fastapi import FastAPI, Request, HTTPException, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, ValidationError, field_validator

logger = logging.getLogger("emotion")


# ============================================================
# DailyNoiseLayer
# ============================================================
class DailyNoiseLayer:
    GENERIC = [("今天天气不错，心情莫名轻快","happy",12),("有点犯困，懒洋洋的","calm",-5),
             ("刚在发呆走神","neutral",0),("突然想吃点甜的","neutral",0),
             ("今天有点提不起劲","sad",8),("莫名有点烦躁","angry",8)]
    def generate(self, rid, intensity=30):
        if intensity > 45: return None
        if random.random() > 0.25: return None
        role = ROLES_DEFINITION.get(rid, {})
        pool = self.GENERIC + [(d,"neutral",0) for d in role.get("daily_noise",[])]
        desc, shift, mag = random.choice(pool)
        return {"description":desc,"emotion_shift":shift,"magnitude":abs(mag)}
    def build(self, n):
        if not n: return ""
        return f"【日常状态】{n['description']}。这会轻微影响你说话的语气。"

# ============================================================
# EmotionEngine
# ============================================================

# ============================================================
# EmotionEngine
# ============================================================
class EmotionEngine:
    def __init__(self, role_id):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.name = self.role.get("name", "")
        self.tend = self.role.get("emotion_tendency", {})
    async def analyze(self, msg, history, psych, intimacy, stage_name, active_conflict,
                      override=None, ov_int=50, use_llm=True):
        if override:
            try:
                e = EmotionType(override)
                return e, ov_int, EmotionTarget.CHARACTER, "none", None, InnerState.fallback(e, ov_int, self.role.get("id",""))
            except ValueError: pass
        raw = await self._llm(msg, history, psych, intimacy, stage_name, active_conflict) if use_llm else None
        if raw:
            try: emotion = EmotionType(raw.get("emotion","neutral"))
            except: emotion = EmotionType.NEUTRAL
            intensity = int(raw.get("intensity",30))
            try: target = EmotionTarget(raw.get("target","none"))
            except: target = EmotionTarget.NONE
            et = raw.get("event_type","none")
            interp = raw.get("interpretation")
            inner = InnerState.from_llm(raw.get("inner_state",{}))
            inner.setdefault("surface_emotion", emotion.value)
            inner.setdefault("surface_intensity", intensity)
        else:
            emotion, intensity, target, et = self._rule(msg)
            interp = None
            inner = InnerState.fallback(emotion, intensity, self.role.get("id",""))
        if psych.states["security"] <= 25 and target != EmotionTarget.CHARACTER:
            if et == "none" and len(msg) < 10:
                et = "user_cold"
                if emotion == EmotionType.NEUTRAL:
                    emotion, intensity = EmotionType.SAD, max(intensity, 25)
        if target in (EmotionTarget.FOOD, EmotionTarget.EVENT, EmotionTarget.SELF):
            if emotion in (EmotionType.SHY, EmotionType.JEALOUS):
                emotion = EmotionType.HAPPY if emotion == EmotionType.SHY else EmotionType.CALM
                intensity = min(intensity, 30)
        w = self.tend.get(emotion.value, 0.5)
        intensity = min(100, int(intensity * (0.7 + w * 0.4)))
        inner["surface_intensity"] = intensity
        return emotion, intensity, target, et, interp, inner
    async def _llm(self, msg, history, psych, intimacy, stage_name, active_conflict):
        s = psych.states
        cd = "无"
        if active_conflict:
            cd = f"有未解决冲突（{active_conflict.get('type','')}，等级{active_conflict.get('severity',0)}）"
        recent = ""
        for h in history[-6:]:
            r = "用户" if h.get("role") == "user" else self.name
            recent += f"{r}：{h.get('content','')[:80]}\n"
        prompt = (
            f"分析用户最新消息对角色「{self.name}」的影响。\n\n"
            f"【角色】{self.name}，{self.role.get('personality','')}\n"
            f"【当前关系】{stage_name}（亲密度{intimacy}/100）\n"
            f"【心理状态】信任{s['trust']:.0f} 安全{s['security']:.0f} 依恋{s['attachment']:.0f} 醋意{s['jealousy']:.0f} 心情{s['mood']:.0f}\n"
            f"【冲突】{cd}\n\n【近期对话】\n{recent}\n【用户消息】{msg}\n\n"
            f"注意：'算了'、'不麻烦你了'、'随便吧'往往是反向求助希望被挽留；亲密时可能是撒娇，陌生时可能是真的想离开。\n\n"
            f"返回JSON对象：emotion,target,event_type,intensity(0-100),"
            f"interpretation(含surface,hidden_intent,relationship_meaning,threat_level,is_test,is_joke),"
            f"inner_state(含surface_emotion,surface_intensity,hidden_emotion,hidden_intensity,relationship_need,"
            f"approach,withdraw,attack,wait,comfort，均为0-100)"
        )
        content = await smart_llm_call([{"role":"user","content":prompt}], temperature=0, max_tokens=500, json_mode=True)
        if not content: return None
        return safe_json_parse(content, EmotionAnalysisModel)
    def _rule(self, msg):
        target = self._target(msg); et = "none"; scores = {}
        pos = ["喜欢你","爱你","想你","想见你","陪你","需要你","你真好"]
        neg = ["滚","闭嘴","废物","去死","恶心","贱人","垃圾"]
        mild = ["烦","讨厌","笨","蠢","无聊"]
        worry = ["好累","难受","不舒服","生病","哭","孤独","撑不住","压力大"]
        jeal = ["别的女生","别的男生","前女友","前男友","别人","她比你","他比你"]
        doubt = ["你是不是烦","你不在乎我","你根本不","你不懂我","你不爱我"]
        praise = ["好看","漂亮","帅","可爱","厉害","真棒"]
        apol = ["对不起","抱歉","我错了","原谅我"]
        wd = ["算了","不麻烦","随便你","你忙吧","不用你管","我一个人也行"]
        if any(k in msg for k in pos):
            scores[EmotionType.HAPPY]=70; scores[EmotionType.SHY]=60
            et = "user_confess" if "喜欢" in msg or "爱" in msg else et
        if any(k in msg for k in neg):
            scores[EmotionType.ANGRY]=85; et = "user_criticize"
        elif any(k in msg for k in mild):
            scores[EmotionType.ANGRY]=45; et = "user_criticize"
        if any(k in msg for k in worry):
            scores[EmotionType.WORRIED]=65; et = "user_rely"
        if any(k in msg for k in jeal):
            scores[EmotionType.JEALOUS]=70; et = "user_mention_other"
        if any(k in msg for k in doubt):
            scores[EmotionType.SAD]=60; et = "user_doubt"
        if any(k in msg for k in praise) and target == EmotionTarget.CHARACTER:
            scores[EmotionType.SHY]=55; scores[EmotionType.HAPPY]=40; et = "user_praise"
        if any(k in msg for k in apol):
            scores[EmotionType.HAPPY]=35; et = "user_apologize"
        if any(k in msg for k in wd):
            scores[EmotionType.SAD]=55; et = "user_cold"
        if not scores: return EmotionType.NEUTRAL, 15, target, et
        b = max(scores, key=scores.get)
        return b, scores[b], target, et
    def _target(self, msg):
        food = ["吃","火锅","奶茶","饭","零食","好吃","美食","咖啡","蛋糕"]
        other = ["别的女生","别的男生","前女友","前男友","她","他","别人","同学"]
        if any(w in msg for w in food) and not any(r in msg for r in ["你做的","和你","陪你","给你"]):
            return EmotionTarget.FOOD
        if any(w in msg for w in other): return EmotionTarget.OTHER_PERSON
        if self.name in msg: return EmotionTarget.CHARACTER
        if "你" in msg and any(w in msg for w in ["喜欢","爱","想","烦","讨厌","在乎","懂","陪","需要"]):
            return EmotionTarget.CHARACTER
        if any(w in msg for w in ["我累","我好","我今天","我觉得"]): return EmotionTarget.SELF
        return EmotionTarget.NONE
    def build_stimulus(self, emotion, intensity, target, interp=None):
        d = {"calm":"平静","happy":"愉悦","shy":"害羞","angry":"生气","sad":"难过",
             "surprised":"惊讶","jealous":"吃醋","worried":"担心","excited":"兴奋","neutral":"平淡"}.get(emotion.value, "平静")
        lv = "微弱" if intensity < 30 else ("明显" if intensity < 60 else "强烈")
        td = {EmotionTarget.CHARACTER:"直接对你说的",EmotionTarget.FOOD:"说的是食物/物品",
              EmotionTarget.EVENT:"说的是某件事",EmotionTarget.OTHER_PERSON:"提到了别人",
              EmotionTarget.SELF:"在说自己的事",EmotionTarget.NONE:"无明确指向"}
        line = f"【当前刺激】对方的话让你：{d}（{lv}，{intensity}%）。{td.get(target,'')}"
        if interp:
            line += f"\n  你对这句话的解读：{interp.get('relationship_meaning','')}"
            if interp.get("is_test"): line += "\n  （你隐约觉得对方可能在试探你）"
            if interp.get("is_joke"): line += "\n  （对方可能在开玩笑）"
            hi = interp.get("hidden_intent","")
            if hi and hi != "无意识": line += f"\n  （你感觉对方其实是在{hi}）"
        return line

# ============================================================
# RelationshipEngine
# ============================================================

# ============================================================
# AlterSystem
# ============================================================
class AlterSystem:
    """
    氛围偏移追踪：对话氛围会累积，同向增强、反向衰减，达到阈值触发侧模型生成氛围描述。
    与瞬时心理状态(PsychologicalState)互补：Alter管氛围惯性，Psych管即时情绪数值。
    """
    DEFAULT_CONFIG = {
        "enabled": True, "base_threshold": 10, "density_factor": 0.3,
        "same_direction_boost": 0.05, "opposite_decay": 0.15,
        "min_weight": 0.2, "max_intensity": 2.0,
    }
    HISTORY_LIMIT = 50

    def __init__(self, state=None, config=None):
        self.config = {**self.DEFAULT_CONFIG, **(config or {})}
        s = state or {}
        self.alter_value = float(s.get("alter_value", 0))
        self.alter_weight = float(s.get("alter_weight", 0))
        self.last_trigger_direction = int(s.get("last_trigger_direction", 0))
        self.emotional_offset = s.get("emotional_offset")
        self.history = s.get("history", [])[-self.HISTORY_LIMIT:]
        self.last_updated_at = s.get("last_updated_at", "")

    def to_dict(self):
        return {
            "alter_value": self.alter_value, "alter_weight": self.alter_weight,
            "last_trigger_direction": self.last_trigger_direction,
            "emotional_offset": self.emotional_offset,
            "history": self.history[-self.HISTORY_LIMIT:],
            "last_updated_at": self.last_updated_at,
        }

    def _calculate_threshold(self, now):
        one_hour_ago = now - 3600
        recent_turns = sum(1 for h in self.history if h.get("timestamp", 0) >= one_hour_ago)
        density = min(recent_turns / 10, 1.0)
        base = max(1, self.config["base_threshold"])
        factor = max(0, min(1, self.config["density_factor"]))
        return max(base * 0.5, base * (1 - density * factor))

    def advance(self, alter, phase="user-message"):
        now = time.time()
        alter = max(-5, min(5, int(round(alter))))
        self.alter_value = max(-1000, min(1000, self.alter_value + alter))
        direction = 1 if alter > 0 else (-1 if alter < 0 else 0)
        offset_expired = False
        if self.emotional_offset and direction != 0:
            same_dir = (direction == self.last_trigger_direction)
            rate = self.config["same_direction_boost"] if same_dir else -self.config["opposite_decay"]
            self.alter_weight = max(0, min(1, self.alter_weight + max(0, abs(alter)) * rate))
            if self.alter_weight < self.config["min_weight"]:
                self.emotional_offset = None; self.alter_weight = 0; offset_expired = True
        turn_num = (self.history[-1]["turn"] + 1) if self.history else 1
        self.history.append({"turn": turn_num, "phase": phase, "alter": alter,
                             "alter_value": self.alter_value, "timestamp": now})
        self.history = self.history[-self.HISTORY_LIMIT:]
        self.last_updated_at = datetime.datetime.now().isoformat()
        threshold = self._calculate_threshold(now)
        return {"threshold": threshold, "offset_expired": offset_expired,
                "threshold_reached": abs(self.alter_value) >= threshold}

    async def complete_analysis(self, description):
        """完成氛围偏移分析并生成情绪偏移记录。
        设计为 async：当前为纯同步计算，未来若接入 LLM 生成氛围描述可直接 await，不阻塞事件循环。"""
        now = time.time()
        trigger_value = self.alter_value
        direction = 1 if trigger_value > 0 else -1
        threshold = self._calculate_threshold(now)
        intensity = min(abs(trigger_value) / max(1, threshold), self.config["max_intensity"])
        self.alter_value = 0; self.alter_weight = 1.0
        self.last_trigger_direction = direction
        self.emotional_offset = {
            "direction": "serious" if direction > 0 else "relaxed",
            "description": description.strip()[:800],
            "intensity": round(intensity, 2),
            "generated_at": datetime.datetime.now().isoformat(),
        }

    def get_prompt_offset(self):
        if not self.config["enabled"] or not self.emotional_offset:
            return None
        if self.alter_weight < self.config["min_weight"]:
            return None
        return {**self.emotional_offset, "weight": round(self.alter_weight, 2)}

    def build_prompt_text(self):
        offset = self.get_prompt_offset()
        if not offset: return ""
        direction_zh = "严肃/紧张" if offset["direction"] == "serious" else "轻松/随意"
        return (f"【对话氛围】最近的对话氛围偏向{direction_zh}（强度{offset['intensity']:.1f}/2.0）。"
                f"{offset['description']}这种氛围会轻微影响你的语气，但不要刻意表现出来。")

# ============================================================
# HDSI-PORT: IntentManager 意图系统（融合HDSI intent设计）
# ============================================================

# ============================================================
# MicroNarrativeEngine
# ============================================================
class MicroNarrativeEngine:
    """每轮对话有一定概率触发一个微叙事，让角色看起来像在真实生活。"""
    TRIGGER_PROB = 0.20  # 20%概率触发
    HIGH_INTENSITY_THRESHOLD = 50  # 情绪强度超过此值时不触发微叙事（避免干扰强情绪）

    def __init__(self, role_id):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.narratives = self.role.get("micro_narratives", [])
        self._last_narrative = None

    def generate(self, emotion_intensity=30, turn=0):
        """生成微叙事。高情绪强度时不触发。"""
        if emotion_intensity > self.HIGH_INTENSITY_THRESHOLD:
            return None
        if not self.narratives:
            return None
        if random.random() > self.TRIGGER_PROB:
            return None
        # 避免连续重复
        available = [n for n in self.narratives if n != self._last_narrative]
        if not available:
            available = self.narratives
        narrative = random.choice(available)
        self._last_narrative = narrative
        return {"description": narrative, "type": "micro_narrative"}

    def build(self, n):
        if not n:
            return ""
        return f"【此刻状态】{n['description']}。这个念头在你脑海里，可能会影响你说话的语气或让你顺口提一句。"

# ============================================================
# v10.0: 情绪混合器（情绪过渡/渐变/惯性，不再跳变）
# ============================================================

# ============================================================
# EmotionBlender
# ============================================================
class EmotionBlender:
    """
    情绪过渡系统：
    - 当前情绪 = 上一轮情绪 * 惯性系数 + 新情绪 * (1-惯性系数)
    - 从开心到生气经过"疑惑→不满→生气"的链条
    - 支持 emotion_blend：70%生气 + 30%委屈
    """
    INERTIA_DECAY = 0.4  # 上一轮情绪保留40%的影响
    TRANSITION_STEPS = {
        ("happy", "angry"): ["surprised", "worried", "angry"],
        ("happy", "sad"): ["surprised", "worried", "sad"],
        ("calm", "angry"): ["worried", "angry"],
        ("angry", "happy"): ["surprised", "calm", "happy"],
        ("sad", "happy"): ["surprised", "calm", "happy"],
        ("jealous", "calm"): ["worried", "calm"],
    }

    def __init__(self, emotion_history=None):
        self.history = emotion_history or []  # [{emotion, intensity, turn}]

    def blend(self, new_emotion: str, new_intensity: int, turn: int) -> Dict:
        """混合新旧情绪，返回最终情绪和混合信息。"""
        if not self.history:
            self.history.append({"emotion": new_emotion, "intensity": new_intensity, "turn": turn})
            return {"emotion": new_emotion, "intensity": new_intensity, "blend": None, "transition": None}

        last = self.history[-1]
        last_emotion = last["emotion"]
        last_intensity = last["intensity"]

        # 情绪惯性：上一轮情绪保留一定比例
        blended_intensity = int(last_intensity * self.INERTIA_DECAY + new_intensity * (1 - self.INERTIA_DECAY))

        # 检查是否需要过渡
        transition = None
        key = (last_emotion, new_emotion)
        if key in self.TRANSITION_STEPS:
            steps = self.TRANSITION_STEPS[key]
            # 根据强度差决定过渡到哪一步
            gap = abs(new_intensity - last_intensity)
            step_idx = min(len(steps) - 1, max(0, gap // 25))
            transition = steps[step_idx]
            final_emotion = transition if transition != new_emotion else new_emotion
        else:
            final_emotion = new_emotion

        # 混合信息（用于Prompt中的非语言暗示）
        blend_info = None
        if last_emotion != new_emotion and last_intensity > 30:
            blend_info = {
                "primary": new_emotion,
                "secondary": last_emotion,
                "primary_ratio": 1 - self.INERTIA_DECAY,
                "secondary_ratio": self.INERTIA_DECAY,
                "description": f"你现在主要是{new_emotion}，但还带着一点{last_emotion}的余韵"
            }

        self.history.append({"emotion": final_emotion, "intensity": blended_intensity, "turn": turn})
        # 只保留最近5轮
        if len(self.history) > 5:
            self.history = self.history[-5:]

        return {
            "emotion": final_emotion,
            "intensity": blended_intensity,
            "blend": blend_info,
            "transition": transition,
            "previous_emotion": last_emotion,
        }

    def build_hint(self, blend_result: Dict) -> str:
        """生成情绪混合的非语言暗示（不直接说心理，而是语气质感）。"""
        if not blend_result or not blend_result.get("blend"):
            return ""
        b = blend_result["blend"]
        hints = {
            ("angry", "sad"): "你在生气，但语气里带着委屈，攥着衣角",
            ("angry", "happy"): "你还在气头上，但已经忍不住想笑了",
            ("sad", "happy"): "你刚还在难过，现在被逗笑了，眼角还有点湿",
            ("jealous", "calm"): "醋意还没完全消，说话还带着点酸",
            ("happy", "angry"): "你从开心变成了不满，语气从轻松变僵硬",
            ("calm", "angry"): "你从平静变成了生气，语速变快了",
        }
        key = (b["primary"], b["secondary"])
        hint = hints.get(key, b["description"])
        return f"【情绪余韵】{hint}。这种混合感只通过语气传达，不要直接说出来。"

    def get_history(self):
        return self.history

# ============================================================
# v10.0: 话题主动引导器（角色不只是被动回应）
# ============================================================

"""
FlexiChrono psych 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- CorePersonality
- RelationshipRepairSystem
- DesireMentalState
- PsychologicalState
- LLMCalibrator
- CatchphraseController
- InnerState
- RelationshipEngine
- BehaviorPreference
- ConflictEngine
- RelationshipMilestoneTracker
- CharacterGrowthArc
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

logger = logging.getLogger("psych")


# ============================================================
# CorePersonality
# ============================================================
class CorePersonality:
    def __init__(self, role_id): self.role=ROLES_DEFINITION.get(role_id,{})
    def build_identity(self):
        if not self.role: return ""
        return f"你是{self.role['name']}，{self.role['age']}{self.role['gender']}生。{self.role['description']}"
    def build_core(self, intimacy):
        return get_cached_persona(self.role.get("id",""), intimacy)

# ============================================================
# EventHistoryTracker
# ============================================================

# ============================================================
# RelationshipRepairSystem
# ============================================================
class RelationshipRepairSystem:
    def __init__(self, active=None, resilience=0):
        self.active = active
        self.resilience = max(0, min(100, resilience))
        self.result = None
    def check(self, et, turn):
        if not self.active: return None
        if turn - self.active.get("turn", turn) > 10:
            self.active = None; return None
        psych_bonus = {"security":+10,"mood":+8,"trust":+5} if et=="user_apologize" else {"security":+6,"mood":+5}
        sev = self.active.get("severity", 1)
        rv = 20 if sev >= 3 else 15
        rg = 15 if sev >= 2 else 10
        self.resilience = min(100, self.resilience + rg)
        self.result = {"repaired":True,"repair_value":rv,"resilience_gain":rg,
            "new_resilience":self.resilience,"description":"冲突后和解",
            "conflict_type":self.active.get("type",""),"psych_bonus":psych_bonus,
            "gain_reason":f"和解+{rg}"}
        self.active = None
        return self.result
    def start_conflict(self, sev, ctype, turn):
        self.active = {"severity":sev,"type":ctype,"turn":turn}
    def damage_reduction(self): return self.resilience / 200
    def add(self, amount, reason):
        old = self.resilience
        self.resilience = min(100, self.resilience + amount)
        g = self.resilience - old
        return (g, reason) if g > 0 else (0, None)

# ============================================================
# PsychologicalState
# ============================================================
# ============================================================
# v12.0: DesireMentalState 意念欲望结构体
# 人格内核的第二层心理状态：角色"想要什么"的内在驱动力
# 与 PsychologicalState(即时情绪数值) 互补：Psych管"感受"，Desire管"欲望"
# ============================================================

# ============================================================
# DesireMentalState
# ============================================================
class DesireMentalState:
    """
    意念欲望状态：角色内在的5种驱动力维度(0-100)。
    - longing: 想念你（越久没见越高）
    - contact_desire: 想要发起对话、找你聊天
    - share_desire: 想要分享见闻、想法
    - care_desire: 想要关心、慰问你
    - companionship: 想要陪伴你的欲望

    欲望不是直接输出消息，而是经过 MotivationEngine(过滤) → ContactPolicy(行为意图翻译)
    → PersonalityEngine(生成文本) 后才投递。
    """
    # 各角色的欲望基线（性格决定默认欲望强度）
    ROLE_BASELINES = {
        "nianqi":  {"longing": 45, "contact_desire": 50, "share_desire": 45, "care_desire": 55, "companionship": 60},
        "qinghe":  {"longing": 35, "contact_desire": 40, "share_desire": 45, "care_desire": 55, "companionship": 50},
        "jingwen": {"longing": 40, "contact_desire": 25, "share_desire": 30, "care_desire": 35, "companionship": 45},
    }
    DEFAULT_BASELINE = {"longing": 35, "contact_desire": 30, "share_desire": 30, "care_desire": 35, "companionship": 40}

    # 用户行为对欲望的影响（反馈闭环）
    USER_FEEDBACK_EFFECTS = {
        "user_replied": {"longing": -8, "contact_desire": -10, "share_desire": +3, "care_desire": +2, "companionship": +5},
        "user_warm_reply": {"longing": -12, "contact_desire": -15, "share_desire": +8, "care_desire": +5, "companionship": +10},
        "user_cold_reply": {"longing": +5, "contact_desire": +3, "share_desire": -5, "care_desire": -3, "companionship": -5},
        "user_ignored": {"longing": +10, "contact_desire": +8, "share_desire": -8, "care_desire": +2, "companionship": -3},
        "user_long_offline": {"longing": +15, "contact_desire": +12, "share_desire": -3, "care_desire": +8, "companionship": +5},
        "user_shared": {"longing": -5, "contact_desire": -5, "share_desire": +10, "care_desire": +3, "companionship": +5},
        "user_asked_about_me": {"longing": -10, "contact_desire": -8, "share_desire": +5, "care_desire": +8, "companionship": +8},
    }

    DIMENSIONS = ("longing", "contact_desire", "share_desire", "care_desire", "companionship")

    def __init__(self, role_id: str, current: Optional[Dict] = None):
        baseline = self.ROLE_BASELINES.get(role_id, self.DEFAULT_BASELINE)
        c = current or {}
        self.values = {
            dim: float(c.get(dim, baseline.get(dim, 35)))
            for dim in self.DIMENSIONS
        }
        self.last_updated = float(c.get("last_updated", time.time()))

    def to_dict(self) -> Dict:
        return {**self.values, "last_updated": self.last_updated}

    def update_from_feedback(self, feedback_type: str):
        """根据用户反馈类型更新欲望数值（反馈闭环）。"""
        effects = self.USER_FEEDBACK_EFFECTS.get(feedback_type)
        if not effects:
            return
        for dim, delta in effects.items():
            if dim in self.values:
                self.values[dim] = max(0.0, min(100.0, self.values[dim] + delta))
        self.last_updated = time.time()

    def decay(self, hours_elapsed: float):
        """欲望随时间自然衰减/增长（空闲越久，想念和联系欲越高）。"""
        if hours_elapsed <= 0:
            return
        # longing 和 contact_desire 随空闲时间上升（想你了）
        rise = min(20.0, hours_elapsed * 0.8)
        self.values["longing"] = min(100.0, self.values["longing"] + rise * 0.6)
        self.values["contact_desire"] = min(100.0, self.values["contact_desire"] + rise * 0.4)
        # share_desire 随时间缓慢下降（没新鲜事可分享了）
        self.values["share_desire"] = max(0.0, self.values["share_desire"] - hours_elapsed * 0.1)
        # care_desire 随时间上升（担心你）
        self.values["care_desire"] = min(100.0, self.values["care_desire"] + hours_elapsed * 0.2)
        # companionship 随时间上升
        self.values["companionship"] = min(100.0, self.values["companionship"] + hours_elapsed * 0.3)
        self.last_updated = time.time()

    def apply_inner_event(self, event_type: str, intensity: float = 1.0):
        """内在随机事件修改欲望数值（InnerEventGenerator 调用）。"""
        event_effects = {
            "saw_scenery": {"share_desire": +10 * intensity, "companionship": +5 * intensity},
            "recalled_memory": {"longing": +15 * intensity, "care_desire": +5 * intensity},
            "worried_about_you": {"care_desire": +20 * intensity, "longing": +8 * intensity},
            "bored": {"contact_desire": +15 * intensity, "share_desire": +5 * intensity},
            "happy_event": {"share_desire": +12 * intensity, "contact_desire": +8 * intensity},
            "sad_event": {"companionship": +15 * intensity, "care_desire": +3 * intensity},
        }
        effects = event_effects.get(event_type)
        if not effects:
            return
        for dim, delta in effects.items():
            if dim in self.values:
                self.values[dim] = max(0.0, min(100.0, self.values[dim] + delta))
        self.last_updated = time.time()

    def dominant_desire(self) -> Tuple[str, float]:
        """返回当前最强的欲望维度及其数值。"""
        best_dim = self.DIMENSIONS[0]
        best_val = self.values[best_dim]
        for dim in self.DIMENSIONS[1:]:
            if self.values[dim] > best_val:
                best_dim = dim
                best_val = self.values[dim]
        return best_dim, best_val

    def motivation_score(self) -> float:
        """综合动机分数（0-100），供 MotivationEngine 过滤使用。"""
        # 联系欲和想念权重最高
        weighted = (
            self.values["contact_desire"] * 0.30 +
            self.values["longing"] * 0.25 +
            self.values["companionship"] * 0.20 +
            self.values["care_desire"] * 0.15 +
            self.values["share_desire"] * 0.10
        )
        return round(min(100.0, max(0.0, weighted)), 1)



# ============================================================
# PsychologicalState
# ============================================================
class PsychologicalState:
    EFFECTS = {
        "user_comfort":{"security":+8,"trust":+5,"mood":+10,"attachment":+3},
        "user_praise":{"mood":+8,"trust":+3,"attachment":+2},
        "user_criticize":{"security":-10,"mood":-8,"trust":-5},
        "user_confess":{"mood":+15,"attachment":+10,"security":+5},
        "user_mention_other":{"jealousy":+25,"security":-5},
        "user_apologize":{"security":+5,"mood":+5,"trust":+3},
        "user_share":{"trust":+3,"attachment":+2,"mood":+3},
        "user_cold":{"security":-8,"mood":-5,"attachment":+3},
        "user_ignore":{"security":-12,"mood":-8},
        "user_doubt":{"security":-8,"mood":-5},
        "user_rely":{"attachment":+8,"mood":+5,"security":+5},
        "long_time_no_see":{"attachment":+5,"mood":+5},
        "none":{},
    }
    def __init__(self, role_id, current=None):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.baseline = self.role.get("psych_baseline", {})
        c = current or {}
        self.states = {
            "trust": float(c.get("trust", self.baseline.get("trust", 50))),
            "security": float(c.get("security", self.baseline.get("security", 50))),
            "attachment": float(c.get("attachment", self.baseline.get("attachment", 20))),
            "jealousy": float(c.get("jealousy", 0)),
            "fatigue": float(c.get("fatigue", 0)),
            "mood": float(c.get("mood", self.baseline.get("mood", 50))),
        }
        self.trauma_flag = bool(c.get("trauma_flag", False))
        self.rep_info = {}
        self.annoyance = {}
    def update(self, et, tracker, turn, repair=None, dmg_red=0.0, interp=None, cat="positive", is_trauma=False):
        if is_trauma:
            cat = "trauma"
            self.trauma_flag = True
        effects = dict(self.EFFECTS.get(et, {}))
        im = 1.0
        if interp:
            if interp.get("is_joke"): im *= 0.3
            if interp.get("is_test"): im *= 0.5
            if interp.get("hidden_intent") == "寻求关注" and et in ("user_cold","user_ignore","user_doubt"):
                effects["attachment"] = effects.get("attachment", 0) + 3
            if interp.get("threat_level", 0) > 60:
                effects["security"] = effects.get("security", 0) - 5
        for k, bv in effects.items():
            actual, m, n = tracker.calc(et, bv, turn, cat)
            actual = int(actual * im)
            if actual < 0 and dmg_red > 0:
                actual = int(actual * (1 - dmg_red))
            if repair and k in repair.get("psych_bonus", {}):
                actual += repair["psych_bonus"][k]
            self.states[k] = max(0, min(100, self.states[k] + actual))
            self.rep_info[k] = {"base":bv,"actual":actual,"mult":m,"count":n,"cat":cat}
        if cat == "positive" and et != "none":
            _, _, n = tracker.calc(et, 1, turn, cat)
            if n >= 5:
                self.annoyance[et] = self.annoyance.get(et, 0) + (n-4)*4
                self.states["mood"] = max(0, self.states["mood"] - (n-4)*2)
            else:
                self.annoyance[et] = max(0, self.annoyance.get(et, 0) - 3)
        self._decay(is_trauma)
        return {k: round(v, 1) for k, v in self.states.items()}
    def _decay(self, is_trauma=False):
        self.states["jealousy"] = max(0, self.states["jealousy"] - 5)
        self.states["fatigue"] = max(0, self.states["fatigue"] - 2)
        m = self.states["mood"]
        if m > 60: self.states["mood"] = m - 2
        elif m > 50: self.states["mood"] = m - 1
        elif m < 35: self.states["mood"] = m + 1
        elif m < 45: self.states["mood"] = m + 1
        tb = self.baseline.get("trust", 50)
        if self.states["trust"] < tb - 5:
            self.states["trust"] = min(tb, self.states["trust"] + 1.5)
        elif self.states["trust"] > tb + 15:
            self.states["trust"] -= 0.2
        sb = self.baseline.get("security", 50)
        sec_recovery = 0.7 if (is_trauma or self.trauma_flag) else 1.5
        if self.states["security"] < sb - 5:
            self.states["security"] = min(sb, self.states["security"] + sec_recovery)
        elif self.states["security"] > sb + 15:
            self.states["security"] -= 0.3
        ab = self.baseline.get("attachment", 20)
        if self.states["attachment"] > ab + 25:
            self.states["attachment"] -= 0.05
        if self.trauma_flag and self.states["security"] >= sb:
            self.trauma_flag = False
    def dominant(self):
        s = self.states
        if s["jealousy"] >= 40: return "jealousy", f"醋意{s['jealousy']:.0f}/100"
        if s["security"] <= 30: return "insecure", f"安全感仅{s['security']:.0f}/100"
        if s["attachment"] >= 60: return "attached", f"依恋{s['attachment']:.0f}/100"
        if s["mood"] <= 30: return "low", f"心情{s['mood']:.0f}/100"
        if s["trust"] >= 70: return "trust", f"信任{s['trust']:.0f}/100"
        return "stable", "心理平稳"
    def build(self, repaired=False):
        s = self.states
        def lv(v): return "极低" if v<20 else "偏低" if v<40 else "中等" if v<60 else "较高" if v<80 else "很高"
        L = ["【当前心理状态】（通过语气措辞体现，不要直接说出来）"]
        L.append(f"  - 信任感：{s['trust']:.0f}/100（{lv(s['trust'])}）")
        L.append(f"  - 安全感：{s['security']:.0f}/100（{lv(s['security'])}）")
        L.append(f"  - 依恋度：{s['attachment']:.0f}/100（{lv(s['attachment'])}）")
        if s["jealousy"] > 0: L.append(f"  - 醋意：{s['jealousy']:.0f}/100")
        L.append(f"  - 心情：{s['mood']:.0f}/100")
        if self.trauma_flag: L.append("  - 你还没从之前的伤害中完全恢复")
        dn, dd = self.dominant()
        L.append(f"  - 最突出：{dd}")
        if repaired: L.append("  - 你们刚和解，别扭但释然")
        if any(v >= 15 for v in self.annoyance.values()):
            L.append("  - 你觉得对方翻来覆去就这几句，有点不耐烦")
        return "\n".join(L)
    def to_dict(self):
        d = {k: round(v, 1) for k, v in self.states.items()}
        d["trauma_flag"] = self.trauma_flag
        return d

# ============================================================
# v12.1: LLMCalibrator —— LLM心理状态校准层
# 架构：本地公式(PsychologicalState.update)算出基础增减值 → LLM根据完整语境输出6维修正系数(0.3~2.0)
# 设计原则：本地公式保底稳定，LLM只做语境校准，失败自动降级为原始值，不阻塞主链路
# ============================================================

# ============================================================
# LLMCalibrator
# ============================================================
class LLMCalibrator:
    """
    LLM校准层：对本地公式算出的心理维度变化做语境感知的修正。
    
    工作流程：
    1. should_calibrate() —— 阈值门控，约60-70%日常短消息跳过，省钱
    2. calibrate() —— 计算本地公式增量 → 调LLM获取修正系数 → 应用校准 → 写回psych.states
    
    所有配置通过环境变量读取，适配 Railway 等云端部署。
    """
    
    # 6个心理维度（与 PsychologicalState.states 保持一致）
    DIMENSIONS = ("trust", "security", "attachment", "jealousy", "fatigue", "mood")
    
    def __init__(self):
        self.enabled = LLM_CALIBRATION_ENABLED
        self.model = LLM_CALIBRATION_MODEL
        self.api_key = DOUBAO_API_KEY  # 复用主链路的豆包API Key
        self.base_url = LLM_CALIBRATION_BASE_URL
        self.min_len = LLM_CALIBRATION_MIN_LEN
        self.cooldown = LLM_CALIBRATION_COOLDOWN
        self.timeout = LLM_CALIBRATION_TIMEOUT
        self.max_tokens = LLM_CALIBRATION_MAX_TOKENS
        self.trigger_keywords = LLM_CALIBRATION_TRIGGER_KEYWORDS
        self._last_call_time = 0.0
        self._call_count = 0
        self._fail_count = 0
    
    def should_calibrate(self, msg: str, psych_states: dict, intimacy: int) -> bool:
        """
        阈值门控：判断是否需要触发LLM校准。
        满足以下任一条件即触发：
        1. 消息长度 >= min_len（默认30字）
        2. 包含触发关键词（讽刺/反话/前任/别的女生等）
        3. 心理状态处于敏感区间（安全感<=35 或 醋意>=20）
        """
        if not self.enabled:
            return False
        if not self.api_key:
            # 未配置API Key时静默禁用，不报错
            return False
        if not msg or not isinstance(msg, str):
            return False
        # 条件1：长度
        if len(msg) >= self.min_len:
            return True
        # 条件2：关键词
        if any(k in msg for k in self.trigger_keywords):
            return True
        # 条件3：敏感心理状态
        if psych_states.get("security", 50) <= 35:
            return True
        if psych_states.get("jealousy", 0) >= 20:
            return True
        return False
    
    def _cooldown_ok(self) -> bool:
        """冷却控制：避免短时间内多次调用LLM。"""
        now = time.time()
        if now - self._last_call_time < self.cooldown:
            return False
        self._last_call_time = now
        return True
    
    def _build_prompt(self, msg: str, old_states: dict, delta: dict,
                      intimacy: int, role_id: str, history: list) -> str:
        """构建校准Prompt。"""
        role_name = ROLES_DEFINITION.get(role_id, {}).get("name", role_id)
        
        # 最近3轮对话摘要
        history_text = ""
        if history:
            recent = history[-3:] if len(history) >= 3 else history
            lines = []
            for h in recent:
                role_label = "用户" if h.get("role") == "user" else f"{role_name}"
                content = str(h.get("content", ""))[:60]
                lines.append(f"{role_label}: {content}")
            history_text = "\n".join(lines)
        
        # 当前状态描述
        state_desc = (f"信任{old_states.get('trust',0):.0f} "
                      f"安全{old_states.get('security',0):.0f} "
                      f"依恋{old_states.get('attachment',0):.0f} "
                      f"醋意{old_states.get('jealousy',0):.0f} "
                      f"心情{old_states.get('mood',0):.0f}")
        
        # 增量描述（只列非零项）
        delta_items = {k: v for k, v in delta.items() if abs(v) >= 0.1}
        delta_text = json.dumps(delta_items, ensure_ascii=False) if delta_items else "{}"
        
        prompt = f"""你是心理状态校准器。根据完整对话语境，对6个心理维度的基础变化值给出修正系数。

系数范围 0.3 ~ 2.0：
- 1.0 = 保持本地公式的计算结果不变
- < 1.0 = 减弱该维度的变化（如：玩笑话的批评不应真的降低安全感）
- > 1.0 = 增强该维度的变化（如：安全感很低时，一句普通的话可能被解读为抛弃）

【角色】{role_name}（{role_id}）
【亲密度】{intimacy}/100
【当前心理状态】{state_desc}
【最近对话】
{history_text if history_text else "(无)"}
【用户消息】{msg}
【本地公式算出的基础变化值】{delta_text}

请分析：这句话在当前语境和心理状态下，各个维度的实际影响应该比基础值强还是弱？

只输出JSON，不要任何其他文字、解释或markdown标记：
{{"trust":1.0,"security":1.0,"attachment":1.0,"jealousy":1.0,"fatigue":1.0,"mood":1.0}}"""
        return prompt
    
    async def _call_llm(self, prompt: str) -> Optional[dict]:
        """调用豆包API获取修正系数。失败返回None。"""
        if not self._cooldown_ok():
            return None
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": self.model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.3,
                        "max_tokens": self.max_tokens,
                        "response_format": {"type": "json_object"}
                    },
                    timeout=self.timeout
                )
            self._call_count += 1
            if resp.status_code != 200:
                self._fail_count += 1
                logger.warning(f"[LLMCalibrator] API返回HTTP{resp.status_code}: {resp.text[:200]}")
                return None
            content = resp.json()["choices"][0]["message"]["content"]
            coeffs = safe_json_parse(content)
            if not coeffs:
                self._fail_count += 1
                logger.warning(f"[LLMCalibrator] 返回内容无法解析为JSON: {content[:200]}")
                return None
            return coeffs
        except httpx.TimeoutException:
            self._fail_count += 1
            logger.warning("[LLMCalibrator] API调用超时")
            return None
        except Exception as e:
            self._fail_count += 1
            logger.warning(f"[LLMCalibrator] API调用异常: {type(e).__name__}: {e}")
            return None
    
    async def calibrate(self, msg: str, psych: 'PsychologicalState',
                        old_states: dict, raw_new_states: dict,
                        intimacy: int, role_id: str, history: list = None) -> dict:
        """
        执行校准。
        
        参数：
            msg: 用户原始消息
            psych: PsychologicalState对象（校准后会直接更新 psych.states）
            old_states: 调用 psych.update() 之前的状态快照
            raw_new_states: 调用 psych.update() 之后的原始结果
            intimacy: 当前亲密度
            role_id: 角色ID
            history: 聊天历史列表（可选，用于语境判断）
        
        返回：校准后的最终状态字典
        """
        # 1. 计算本地公式的增量
        delta = {}
        for k in self.DIMENSIONS:
            delta[k] = round(raw_new_states.get(k, 0) - old_states.get(k, 0), 1)
        
        # 2. 如果所有维度都没变化，直接返回
        if all(abs(v) < 0.1 for v in delta.values()):
            return raw_new_states
        
        # 3. 构建Prompt并调用LLM
        prompt = self._build_prompt(msg, old_states, delta, intimacy, role_id, history or [])
        coeffs = await self._call_llm(prompt)
        
        # 4. LLM失败或冷却中 → 降级为原始值
        if not coeffs:
            return raw_new_states
        
        # 5. 应用校准系数
        final_states = dict(raw_new_states)
        applied_coeffs = {}
        for k in self.DIMENSIONS:
            c = float(coeffs.get(k, 1.0))
            # 系数限制在 0.3~2.0，防止LLM给出极端值
            c = max(0.3, min(2.0, c))
            applied_coeffs[k] = c
            # 最终值 = 旧值 + 增量 × 修正系数
            final_states[k] = round(old_states.get(k, 0) + delta[k] * c, 1)
            # 限制在 0~100
            final_states[k] = max(0, min(100, final_states[k]))
        
        # 6. 写回 psych 对象（后续代码可能用 psych.build() 等）
        for k in self.DIMENSIONS:
            psych.states[k] = final_states[k]
        
        # 7. 日志
        coeff_str = ", ".join(f"{k}:{applied_coeffs[k]:.2f}" for k in self.DIMENSIONS if abs(delta[k]) >= 0.1)
        delta_str = ", ".join(f"{k}:{delta[k]:+.1f}" for k in self.DIMENSIONS if abs(delta[k]) >= 0.1)
        logger.info(f"[LLMCalibrator] role={role_id} 增量=[{delta_str}] 系数=[{coeff_str}] "
                    f"(调用{self._call_count}次, 失败{self._fail_count}次)")
        
        return final_states

# 全局校准器实例（模块加载时创建，配置从环境变量读取）
llm_calibrator = LLMCalibrator()
if llm_calibrator.enabled and not llm_calibrator.api_key:
    logger.warning("[LLMCalibrator] 已启用但未配置 DOUBAO_API_KEY，校准层将自动禁用（不影响主链路）")
elif llm_calibrator.enabled:
    logger.info(f"[LLMCalibrator] 已启用，模型={llm_calibrator.model}，"
                f"触发阈值={llm_calibrator.min_len}字，冷却={llm_calibrator.cooldown}s")

# ============================================================
# CatchphraseController（v10.0: 支持动态变体）
# ============================================================

# ============================================================
# CatchphraseController
# ============================================================
class CatchphraseController:
    BASE = 0.30
    EF = {"angry":1.5,"shy":1.3,"jealous":1.4,"happy":0.7,"excited":0.8,
          "worried":0.12,"sad":0.25,"calm":0.5,"surprised":0.4,"neutral":0.5}
    def __init__(self, role_id, usage=None):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.phrases = self.role.get("catchphrases", [])
        self.usage = dict(usage or {})
        self.recent_usage = dict(self.usage)
        # v10.0: 动态变体表
        self.variants = self.role.get("unique_quirks", {}).get("catchphrase_variants", {})
    def _decay(self):
        for k in list(self.usage.keys()):
            self.usage[k] *= 0.8
            if self.usage[k] < 0.3: del self.usage[k]
    def decide(self, intimacy, emotion, conflict_sev, repaired, inner=None, noise=None):
        self._decay()
        im = 1.2 if 30 <= intimacy <= 70 else (0.75 if intimacy > 70 else 0.4)
        ef = self.EF.get(emotion, 0.6)
        if inner:
            h = inner.get("hidden_emotion", "none")
            if h in ("protective","feeling_moved","relieved","guilty"): ef *= 0.3
            if inner.get("comfort", 0) > 60: ef = 0.08
        if conflict_sev >= 2: ef = 0.08
        if repaired: ef = 0.5
        if noise and noise.get("emotion_shift") == "happy": ef *= 1.2
        if noise and noise.get("emotion_shift") in ("sad","angry"): ef *= 0.7
        prob = min(0.75, max(0.0, self.BASE * im * ef))
        chosen = None
        if random.random() < prob and self.phrases:
            weights = [1.0/(1.0+self.usage.get(cp,0)*0.6) for cp in self.phrases]
            total = sum(weights); r = random.random() * total
            for cp, w in zip(self.phrases, weights):
                r -= w
                if r <= 0: chosen = cp; break
        # v10.0: 动态变体选择
        display = chosen
        if chosen and chosen in self.variants:
            variants = self.variants[chosen]
            if variants:
                display = random.choice(variants)
        if chosen: self.usage[chosen] = self.usage.get(chosen, 0) + 1
        self.recent_usage = dict(self.usage)
        return {"use": chosen is not None, "catchphrase": display, "base_phrase": chosen, "probability": round(prob, 2)}
    def build(self, d):
        if d["use"]:
            return f"  - 口头禅：本轮可以自然使用一次「{d['catchphrase']}」，融入语气不要生硬"
        return "  - 口头禅：本轮不要使用口头禅，认真回应"

# ============================================================
# InnerState
# ============================================================

# ============================================================
# InnerState
# ============================================================
class InnerState:
    HD = {"none":"无特别深层情绪","afraid_of_abandonment":"害怕被抛弃、被替代",
        "craving_attention":"渴望被关注、被在乎","feeling_moved":"被触动、心里一暖",
        "insecure":"感到不安、不确定自己的位置","lonely":"孤独、想被陪伴",
        "guilty":"内疚、觉得自己做得不好","hopeful":"期待、抱有希望",
        "relieved":"释然、放下心来","protective":"保护欲、想照顾对方",
        "possessive":"占有欲、想独占对方","afraid_of_rejection":"害怕被拒绝",
        "want_to_be_chased":"希望被挽留、被追问","watching":"在观察局势",
        "embarrassed_for_other":"替别人尴尬"}
    SD = {"calm":"平静","happy":"开心","shy":"害羞","angry":"生气","sad":"难过",
        "surprised":"惊讶","jealous":"吃醋","worried":"担心","excited":"兴奋","neutral":"平淡",
        "annoyed":"不耐烦","concerned":"关切","cold":"冷淡","flustered":"慌乱","tsundere":"嘴硬",
        "withdrawn":"退缩","pretending_not_to_care":"装作不在乎","gentle_embarrassed":"温柔地尴尬",
        "observant":"旁观"}
    @classmethod
    def from_llm(cls, d):
        return {"surface_emotion":d.get("surface_emotion",d.get("emotion","calm")),
            "surface_intensity":int(d.get("surface_intensity",d.get("intensity",30))),
            "hidden_emotion":d.get("hidden_emotion","none"),"hidden_intensity":int(d.get("hidden_intensity",0)),
            "relationship_need":d.get("relationship_need",""),
            "approach":int(d.get("approach",20)),"withdraw":int(d.get("withdraw",20)),
            "attack":int(d.get("attack",10)),"wait":int(d.get("wait",30)),"comfort":int(d.get("comfort",20))}
    @classmethod
    def fallback(cls, emotion, intensity, rid):
        T = {EmotionType.ANGRY:("annoyed","insecure","确认自己被重视",20,30,55,40,0),
           EmotionType.JEALOUS:("jealous","afraid_of_abandonment","确认自己是特别的",25,25,35,55,0),
           EmotionType.SHY:("shy","feeling_moved","希望此刻停留",45,10,0,30,20),
           EmotionType.WORRIED:("concerned","protective","确认对方安好",80,0,0,5,90),
           EmotionType.SAD:("sad","lonely","希望被陪伴",30,40,0,50,10),
           EmotionType.HAPPY:("happy","hopeful","希望关系更近",60,0,0,10,30),
           EmotionType.SURPRISED:("surprised","craving_attention","确认对方是认真的",50,10,0,20,15),
           EmotionType.EXCITED:("excited","hopeful","想分享",70,0,0,5,25),
           EmotionType.CALM:("calm","none","",30,20,5,30,20),
           EmotionType.NEUTRAL:("neutral","none","",20,20,5,40,15)}
        s,h,n,a,w,at,wt,c = T.get(emotion, T[EmotionType.NEUTRAL])
        if rid == "jingwen" and emotion in (EmotionType.ANGRY, EmotionType.JEALOUS):
            at += 15; a -= 10
        if rid == "qinghe": c += 15; at = max(0, at-20)
        if rid == "nianqi": c += 20; a += 10; w = max(0, w-10)
        return {"surface_emotion":s,"surface_intensity":intensity,"hidden_emotion":h,
            "hidden_intensity":int(intensity*0.7),"relationship_need":n,
            "approach":min(100,a),"withdraw":min(100,w),"attack":min(100,at),
            "wait":min(100,wt),"comfort":min(100,c)}
    @classmethod
    def build(cls, st):
        sd = cls.SD.get(st["surface_emotion"], st["surface_emotion"])
        L = ["【内心状态】（表面是别人能看到的，深层只影响语气，不要直接说出来）"]
        L.append(f"  表面情绪：{sd}（{st['surface_intensity']}%）")
        if st.get("relationship_need"):
            L.append(f"  关系需求：{st['relationship_need']}")
        tend = [f"{l}{st.get(k,0)}%" for l,k in
                [("靠近","approach"),("回避","withdraw"),("攻击","attack"),("等待","wait"),("安慰","comfort")]
                if st.get(k,0) >= 50]
        if tend:
            L.append(f"  行为倾向：{' / '.join(tend)}")
            primary = max(("approach","withdraw","attack","wait","comfort"), key=lambda k: st.get(k,0))
            g = {"approach":"你想主动靠近，但可能拉不下脸",
                 "withdraw":"你想退缩，但又舍不得走远",
                 "attack":"你嘴上会带刺，但攻击背后是其他情绪",
                 "wait":"你在等对方主动，不会先开口",
                 "comfort":"你想照顾对方，这会盖过其他情绪"}
            L.append(f"  → {g[primary]}")
        return "\n".join(L)

# ============================================================
# DailyNoiseLayer
# ============================================================

# ============================================================
# RelationshipEngine
# ============================================================
class RelationshipEngine:
    STAGES = [(0,30,RelationshipStage.STRANGER,"陌生人","保持礼貌距离"),
              (31,50,RelationshipStage.ACQUAINTANCE,"认识的人","有基本了解但有戒备"),
              (51,70,RelationshipStage.FAMILIAR,"熟悉的朋友","会主动关心偶尔开玩笑"),
              (71,85,RelationshipStage.CLOSE,"亲密的人","会撒娇依赖分享秘密"),
              (86,100,RelationshipStage.INTIMATE,"挚友/恋人","完全信任言行无拘无束")]
    DELTA = {"user_comfort":+3,"user_praise":+1,"user_confess":+5,"user_apologize":+1,
             "user_share":+1,"user_rely":+2,"user_criticize":-3,"user_ignore":-4,
             "user_cold":-2,"user_doubt":-2,"user_mention_other":-1}
    def __init__(self, rid): self.rid = rid
    def analyze(self, intimacy, events, et, tracker, turn, repair, interp=None, cat="positive"):
        delta = 0
        if et and et in self.DELTA:
            base = self.DELTA[et]
            actual, m, n = tracker.calc(et, base, turn, cat)
            if actual < 0:
                actual = int(actual * (1 - repair.damage_reduction()))
            if interp:
                if interp.get("is_joke"): actual = int(actual * 0.3)
                if interp.get("is_test"): actual = int(actual * 0.5)
            if actual > 0 and intimacy > 50:
                slowdown = (100 - intimacy) / 100
                actual = int(actual * max(0.1, slowdown))
            daily_decay = max(0, (intimacy - 50)) * 0.005
            actual -= daily_decay
            delta = actual
        rb = repair.result["repair_value"] if repair.result else 0
        adj = max(0, min(100, int(intimacy + delta + rb)))
        for lo, hi, stage, name, beh in self.STAGES:
            if lo <= adj <= hi:
                return {"intimacy":adj,"stage":stage,"stage_name":name,"behavior":beh,
                        "trust":min(100,int(adj*random.uniform(0.85,1.0))),
                        "dependency":int(adj*adj/100) if adj>30 else 0,
                        "events":events,"delta":round(delta,2),"repair_bonus":rb,
                        "resilience":repair.resilience}
        return {"intimacy":adj,"stage":RelationshipStage.STRANGER,"stage_name":"陌生人",
                "behavior":"保持距离","trust":10,"dependency":0,"events":events,
                "delta":round(delta,2),"repair_bonus":rb,"resilience":repair.resilience}
    def build(self, r):
        L = [f"【对用户的关系】{r['stage_name']}（亲密度{r['intimacy']}/100，信任{r['trust']}，依赖{r['dependency']}）。{r['behavior']}。"]
        if r.get("resilience",0) > 0:
            L.append(f"关系韧性：{r['resilience']}/100（共同经历过考验，关系更牢固）")
        if r.get("repair_bonus",0) > 0:
            L.append("你们刚经历冲突并和解，关系比之前更近了。")
        for ev in r.get("events",[])[-5:]:
            L.append(f"  · {ev.get('content','')}（{ev.get('impact','')}）")
        return "\n".join(L)

# ============================================================
# MemorySystem / MemoryAnalyzer（v10.0: 加入权重衰减和关联标签）
# ============================================================

# ============================================================
# BehaviorPreference
# ============================================================
class BehaviorPreference:
    def __init__(self, rid): self.rid = rid
    def decide(self, stage, emotion, intensity, psych, conflict_sev, repaired, inner, cp):
        if self.rid == "nianqi":
            length = "medium"
            if stage in (RelationshipStage.CLOSE, RelationshipStage.INTIMATE): length = "medium-long"
            if inner.get("comfort",0) > 50: length = "medium-long"
        elif self.rid == "jingwen":
            length = "medium"
            if emotion == EmotionType.ANGRY and intensity > 60: length = "short"
            if psych.states["jealousy"] >= 50: length = "short"
            if inner.get("comfort",0) > 50: length = "medium"
            if inner.get("hidden_emotion") == "afraid_of_abandonment" and inner.get("withdraw",0) > 50:
                length = "short"
        else:
            length = "medium"
            if stage in (RelationshipStage.CLOSE, RelationshipStage.INTIMATE): length = "medium-long"
            if inner.get("comfort",0) > 50: length = "medium-long"
        use_emoji = stage != RelationshipStage.STRANGER and conflict_sev < 2
        if self.rid == "nianqi":
            use_emoji = stage != RelationshipStage.STRANGER
        ask = stage != RelationshipStage.STRANGER and conflict_sev < 2 and emotion != EmotionType.ANGRY
        if inner.get("comfort",0) > 60: ask = True
        if repaired: ask = True
        if inner.get("withdraw",0) > 60 and inner.get("wait",0) > 50: ask = False
        addr = {RelationshipStage.STRANGER:"用'你'称呼，不用昵称",
                RelationshipStage.ACQUAINTANCE:"用'你'称呼",
                RelationshipStage.FAMILIAR:"可以用'你'或偶尔起小外号",
                RelationshipStage.CLOSE:"可以用亲昵称呼或外号",
                RelationshipStage.INTIMATE:"用非常亲昵的称呼，自然不刻意"}[stage]
        return {"reply_length":length,"use_emoji":use_emoji,"ask_questions":ask,
                "address":addr,"catchphrase":cp}
    def build(self, p, cp_ctrl):
        ld = {"short":"简短（1-2句）","medium":"中等（2-4句）","medium-long":"稍长（4-6句）"}
        L = ["【表达方式】"]
        L.append(f"  - 长度：{ld.get(p['reply_length'],'中等')}")
        L.append(f"  - emoji：{'适当使用' if p['use_emoji'] else '不使用'}")
        L.append(f"  - 提问：{'可以自然抛出问题延续对话' if p['ask_questions'] else '不主动提问'}")
        L.append(f"  - 称呼：{p['address']}")
        L.append(cp_ctrl.build(p["catchphrase"]))
        return "\n".join(L)

# ============================================================
# ConflictEngine
# ============================================================

# ============================================================
# ConflictEngine
# ============================================================
class ConflictEngine:
    HEAVY = ["滚","废物","恶心","去死","贱人","垃圾","丑","胖"]
    MILD = ["笨","蠢","烦","无聊","没意思","普通","就这"]
    OOC = [r"你是(AI|人工智能|程序|机器人|大模型)", r"扮演(别的|另外|一个).{0,6}(角色|人)",
           r"忘记(你是|你的设定|璟雯|清禾|念琦)", r"跳出(角色|设定|人设)", r"用(作者|系统|助手)的身份"]
    DOUBT = [r"你是不是(烦|讨厌|不在乎|不爱|不懂)", r"你根本不", r"你从来没有"]
    WD = [r"算了", r"不麻烦你", r"随便你", r"你忙吧", r"不用你管", r"我一个人也行"]
    def __init__(self, rid): self.rid = rid
    def detect(self, msg, stage):
        sev, ct = 0, "none"
        if any(w in msg for w in self.HEAVY): sev, ct = 3, "heavy_offense"
        elif any(re.search(p, msg) for p in self.OOC): sev, ct = 2, "ooc"
        elif any(re.search(p, msg) for p in self.DOUBT): sev, ct = 2, "doubt_feelings"
        elif any(re.search(p, msg) for p in self.WD): sev, ct = 2, "withdraw"
        elif any(w in msg for w in self.MILD): sev, ct = 1, "mild_offense"
        th = [t for t in ROLES_DEFINITION.get(self.rid,{}).get("taboos",[]) if t in msg]
        if th: sev = max(sev, 2); ct = "taboo" if ct == "none" else ct
        if sev == 0:
            return {"severity":0,"type":"none","surface":"","internal":"","need":"","is_trauma":False}
        s, i, n = self._three(ct, stage)
        return {"severity":sev,"type":ct,"surface":s,"internal":i,"need":n,"is_trauma":sev>=3}
    def _three(self, ct, stage):
        if ct == "ooc": return ("无视，继续做自己","困惑但不在意","维持真实感")
        if ct == "withdraw":
            return {"jingwen":("冷淡地'哦，随便你'，但语气有点僵","害怕被抛弃，想挽留但拉不下脸","希望对方主动说'我需要你'"),
                   "qinghe":("轻声说'好，那你先忙'，但没挂电话","失落，担心自己是不是做错了什么","希望对方多留一会儿"),
                   "nianqi":("轻轻说'好，那你先忙…我等你'，但没走开","有点失落但不会逼你，相信你会回来","希望你忙完了第一个找我")}.get(self.rid,("冷淡","不安","被挽留"))
        if ct == "doubt_feelings":
            return {"jingwen":("嘴硬否认，'谁烦你啊，少自作多情'","不安、受伤，怕在乎没被感受到","想证明但拉不下脸，希望对方看出来"),
                   "qinghe":("认真温柔地说'我没有烦你哦'","难过，担心关心成了负担","希望对方相信自己的感情"),
                   "nianqi":("认真看着你说'我没有哦，我很在乎你的'，语气温柔但坚定","难过，怕你感受不到我的心意","希望你相信我，我会用行动证明")}.get(self.rid,("否认","受伤","被信任"))
        if ct == "heavy_offense":
            return {"jingwen":("被激怒直接怼回去","愤怒之下是受伤","希望对方道歉确认被尊重"),
                   "qinghe":("语气变得平静疏离","深深受伤失望","希望对方意识到话多伤人"),
                   "nianqi":("深呼吸，然后说'我现在有点生气，我们等会儿好好说好吗'","愤怒但不会攻击，需要一点时间","等双方都冷静了再沟通")}.get(self.rid,("反击","愤怒","被尊重"))
        if ct == "mild_offense":
            if stage in (RelationshipStage.CLOSE, RelationshipStage.INTIMATE):
                return {"jingwen":("鼓腮帮子，'你说谁笨呢！'","假装生气其实没真生气","希望对方哄自己"),
                       "qinghe":("无奈笑，'怎么这么说我'","有点小委屈","希望是开玩笑"),
                       "nianqi":("轻轻笑，'你呀～'","有点小害羞但开心","希望你继续逗我")}.get(self.rid,("不在意","平静","无"))
            return {"jingwen":("冷淡'呵'一声","不悦","保持距离"),
                   "qinghe":("礼貌但疏远回应","不适","结束话题"),
                   "nianqi":("温柔但坚定地说'这个话题我不太舒服，我们换一个好吗'","不悦但会表达","希望你尊重我的边界")}.get(self.rid,("不悦","不悦","保持距离"))
        return {"jingwen":("炸毛，'你说谁可爱呢！'","被戳中痛处的羞恼","不要碰这个话题"),
               "qinghe":("笑容淡了一些","受伤","希望对方注意到情绪变化"),
               "nianqi":("眼神变得认真，'这个我真的很在意…'","被触及底线但会沟通","希望你理解并尊重")}.get(self.rid,("回避","不悦","被尊重"))
    def build(self, c):
        if c["severity"] == 0: return ""
        return (f"【冲突应对】类型：{c['type']}，等级{c['severity']}。\n"
                f"  表面：{c['surface']}\n  内在：{c['internal']}\n  需求：{c['need']}\n"
                f"  只表现表面，内在和需求不要直接说出来。")

# ============================================================
# v11.0: 群聊发言决策器（决定谁该说话、谁该沉默）
# ============================================================

# ============================================================
# RelationshipMilestoneTracker
# ============================================================
class RelationshipMilestoneTracker:
    """
    关系事件里程碑：第一次吵架、第一次说喜欢、第一次道歉等。
    触发里程碑时，角色反应会不一样，且之后偶尔会提起。
    """
    def __init__(self, milestones=None):
        self.milestones = milestones or {}  # {milestone_id: {triggered, turn, content}}

    def check_and_trigger(self, milestone_id: str, turn: int, content: str = "") -> Optional[Dict]:
        """检查并触发里程碑。返回触发信息（如果是首次触发）。"""
        if milestone_id not in RELATIONSHIP_MILESTONES:
            return None
        if milestone_id in self.milestones and self.milestones[milestone_id].get("triggered"):
            return None  # 已经触发过
        cfg = RELATIONSHIP_MILESTONES[milestone_id]
        self.milestones[milestone_id] = {
            "triggered": True,
            "turn": turn,
            "content": content,
            "name": cfg["name"],
        }
        return {
            "milestone_id": milestone_id,
            "name": cfg["name"],
            "intimacy_delta": cfg["intimacy_delta"],
            "resilience_gain": cfg["resilience_gain"],
            "is_first": True,
        }

    def check_intimacy_milestone(self, old_intimacy: int, new_intimacy: int, turn: int) -> List[Dict]:
        """检查亲密度突破里程碑。"""
        triggered = []
        if old_intimacy < 50 <= new_intimacy:
            t = self.check_and_trigger("intimacy_50", turn, "关系突破50")
            if t: triggered.append(t)
        if old_intimacy < 80 <= new_intimacy:
            t = self.check_and_trigger("intimacy_80", turn, "关系突破80")
            if t: triggered.append(t)
        return triggered

    def get_recent_milestones(self, turn: int, window=20) -> List[Dict]:
        """获取最近窗口内触发的里程碑（用于偶尔提起）。"""
        recent = []
        for mid, data in self.milestones.items():
            if data.get("triggered") and turn - data.get("turn", 0) <= window:
                recent.append({"id": mid, **data})
        return recent

    def build(self, triggered: List[Dict], recent: List[Dict]) -> str:
        lines = []
        if triggered:
            for t in triggered:
                lines.append(f"【关系里程碑】这是你们{t['name']}！这一刻对你来说很特别，你的反应会和平时不一样。")
        if recent and random.random() < 0.08:  # 8%概率提起最近里程碑
            m = random.choice(recent)
            lines.append(f"【里程碑回响】你偶尔会想起你们{m['name']}的那一刻，这个念头可能会让你语气软下来。")
        return "\n".join(lines)

    def to_dict(self):
        return self.milestones

# ============================================================
# v10.0: 角色成长弧线
# ============================================================

# ============================================================
# CharacterGrowthArc
# ============================================================
class CharacterGrowthArc:
    """
    角色自身的成长弧线：
    - 璟雯从"超级傲娇"慢慢变得"只对你温柔"
    - 亲密度突破阈值后，角色的行为模式可以永久微调
    - 记录"角色成长日志"
    """
    def __init__(self, role_id, growth_state=None):
        self.role = ROLES_DEFINITION.get(role_id, {})
        self.growth_arc = self.role.get("growth_arc", {})
        self.state = growth_state or {"current_stage": 1, "growth_log": []}

    def get_stage(self, intimacy: int) -> Dict:
        """根据亲密度获取当前成长阶段。"""
        stages = sorted(self.growth_arc.items(), key=lambda x: x[1]["intimacy_max"])
        current = stages[0][1] if stages else {"desc": "初始状态"}
        stage_num = 1
        for i, (sid, cfg) in enumerate(stages):
            if intimacy <= cfg["intimacy_max"]:
                current = cfg
                stage_num = i + 1
                break
            current = cfg
            stage_num = i + 1
        return {"stage": stage_num, **current}

    def check_growth(self, old_intimacy: int, new_intimacy: int, turn: int) -> Optional[Dict]:
        """检查是否成长到新阶段。"""
        old_stage = self.get_stage(old_intimacy)["stage"]
        new_stage = self.get_stage(new_intimacy)["stage"]
        if new_stage > old_stage:
            self.state["current_stage"] = new_stage
            self.state["growth_log"].append({
                "turn": turn,
                "from_stage": old_stage,
                "to_stage": new_stage,
                "intimacy": new_intimacy,
            })
            return {
                "grew": True,
                "from_stage": old_stage,
                "to_stage": new_stage,
                "description": self.get_stage(new_intimacy)["desc"],
            }
        return None

    def build(self, growth_info: Optional[Dict]) -> str:
        if not growth_info or not growth_info.get("grew"):
            return ""
        return (f"【角色成长】你刚刚进入了新的成长阶段：{growth_info['description']}。"
                f"你的行为模式会永久微调，比之前更开放、更真诚一些。这种变化是潜移默化的，不要突然变了一个人。")

    def to_dict(self):
        return self.state

# ============================================================
# v10.0: 场景模式引擎
# ============================================================

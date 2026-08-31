"""
FlexiChrono group 模块
P4 序号1：人格服务器业务类拆分
自动从 personality_server.py 拆分，包含以下类：
- GroupSpeakerDecider
- GroupBrain
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

logger = logging.getLogger("group")


# ============================================================
# GroupSpeakerDecider
# ============================================================
class GroupSpeakerDecider:
    """
    群聊中每个角色根据以下因素决定是否发言：
    - 被提及/被@：高概率发言
    - 话题相关性：话题与角色相关则发言概率高
    - 性格外向度：外向角色更可能插话
    - 当前情绪：开心/生气更可能说话，悲伤/退缩更可能沉默
    - 沉默惯性：距上次发言越久，发言意愿越高
    - 全局频率控制：每角色每分钟最多发言N次
    """
    def __init__(self, role_ids: List[str]):
        self.role_ids = role_ids
        self.last_speak_turn: Dict[str, int] = {rid: 0 for rid in role_ids}
        self.speak_count: Dict[str, int] = defaultdict(int)
    def decide(self, msg: str, role_states: Dict, current_turn: int,
               mentioned_roles: Optional[List[str]] = None) -> Dict[str, Dict]:
        """
        决定每个角色是否发言。
        返回: {role_id: {"should_speak": bool, "priority": float, "reason": str}}
        """
        mentioned_roles = mentioned_roles or []
        decisions = {}
        for rid in self.role_ids:
            role = ROLES_DEFINITION.get(rid, {})
            state = role_states.get(rid, {})
            emotion = state.get("emotion", EmotionType.CALM)
            intensity = state.get("intensity", 20)
            inner = state.get("inner", {})
            # 1. 被提及分
            mention_score = 1.0 if rid in mentioned_roles else 0.0
            role_name = role.get("name", "")
            if role_name and role_name in msg:
                mention_score = max(mention_score, 0.8)
            # 2. 性格外向度（core_traits中包含外向相关词）
            traits = role.get("core_traits", [])
            extroversion = 0.3  # 默认中等
            if any(t in traits for t in ["情绪外露", "活泼", "开朗"]): extroversion = 0.7
            if any(t in traits for t in ["冷静", "寡言", "温柔", "知性"]): extroversion = 0.2
            # 3. 情绪加成
            emotion_bonus = 0.0
            if emotion in (EmotionType.ANGRY, EmotionType.JEALOUS, EmotionType.EXCITED):
                emotion_bonus = 0.2 + intensity / 200
            elif emotion in (EmotionType.SAD, EmotionType.WORRIED):
                emotion_bonus = -0.1
            if inner.get("withdraw", 0) > 60:
                emotion_bonus -= 0.2
            if inner.get("comfort", 0) > 60:
                emotion_bonus += 0.15
            # 4. 沉默惯性（越久没说话越想说）
            gap = current_turn - self.last_speak_turn.get(rid, 0)
            silence_bonus = min(0.3, gap * 0.05)
            # 5. 频率控制（最近3轮内说过话则降低意愿）
            recent_penalty = -0.3 if gap <= 2 else 0.0
            # 综合评分
            priority = mention_score * 0.5 + extroversion * 0.2 + emotion_bonus + silence_bonus + recent_penalty
            # 阈值判断
            threshold = 0.35
            should_speak = priority >= threshold or mention_score >= 0.8
            # 限制同时发言人数：最多2个角色主动发言（被@的除外）
            decisions[rid] = {
                "should_speak": should_speak,
                "priority": round(priority, 3),
                "mention_score": mention_score,
                "reason": f"提及={mention_score:.1f} 外向={extroversion:.1f} 情绪={emotion_bonus:+.1f} 沉默={silence_bonus:+.1f}"
            }
        # 限制主动发言人数（被提及的不受限）
        active_speakers = [rid for rid, d in decisions.items()
                          if d["should_speak"] and d["mention_score"] < 0.8]
        if len(active_speakers) > 2:
            active_speakers.sort(key=lambda rid: decisions[rid]["priority"], reverse=True)
            for rid in active_speakers[2:]:
                decisions[rid]["should_speak"] = False
                decisions[rid]["reason"] += " [人数限制→沉默]"
        return decisions
    def mark_spoken(self, role_id: str, turn: int):
        """标记角色已发言。"""
        self.last_speak_turn[role_id] = turn
        self.speak_count[role_id] += 1

# ============================================================
# v11.0: 对话质量轻量检测器（检测OOC/人设偏离）
# ============================================================

# ============================================================
# GroupBrain
# ============================================================
class GroupBrain:
    async def analyze_group_event(self, msg, history, role_ids, intimacy_map):
        rd = ""; rel = ""
        for rid in role_ids:
            r = ROLES_DEFINITION.get(rid, {})
            rd += f"- {rid}：{r.get('name','')}，{r.get('personality','')}\n"
            intim = intimacy_map.get(rid, 30)
            stage = "陌生人" if intim<=30 else "认识" if intim<=50 else "熟悉" if intim<=70 else "亲密"
            rel += f"- {r.get('name','')}与用户：{stage}（亲密度{intim}）\n"
        rel += "\n【角色间关系】\n"
        for i, a in enumerate(role_ids):
            for b in role_ids[i+1:]:
                rr = get_role_relation(a, b)
                an = ROLES_DEFINITION.get(a,{}).get("name",a)
                bn = ROLES_DEFINITION.get(b,{}).get("name",b)
                rel += f"- {an}↔{bn}：竞争{rr['rivalry']:.0%} 亲和{rr['affinity']:.0%}（{rr['dynamic']}）\n"
        recent = ""
        for h in history[-6:]:
            r = "用户" if h.get("role") == "user" else h.get("role","")
            recent += f"{r}：{h.get('content','')[:60]}\n"
        prompt = (
            f"分析群聊消息对每个角色的影响。\n\n【角色】\n{rd}\n【关系】\n{rel}\n"
            f"【近期对话】\n{recent}\n【用户消息】{msg}\n\n"
            f"返回JSON：event_type(comparison/question/share/greeting/conflict/tease/comfort/announcement/neutral),"
            f"summary(一句话),impacts(对象，键为角色ID，值含emotion,surface,hidden_emotion,intensity,event_type,reason)"
        )
        content = await smart_llm_call([{"role":"user","content":prompt}], temperature=0, max_tokens=600, json_mode=True)
        if not content:
            return {"event_type":"neutral","summary":msg[:30],"impacts":{
                rid:{"emotion":"calm","surface":"平静","hidden_emotion":"none","intensity":10,
                     "event_type":"none","reason":""} for rid in role_ids}}
        return safe_json_parse(content, GroupEventModel) or {"event_type":"neutral","summary":msg[:30],"impacts":{
            rid:{"emotion":"calm","surface":"平静","hidden_emotion":"none","intensity":10,
                 "event_type":"none","reason":""} for rid in role_ids}}
    def _interpersonal(self, role_states):
        percep = {rid: [] for rid in role_states}
        for rid, state in role_states.items():
            for oid, other in role_states.items():
                if rid == oid: continue
                oname = ROLES_DEFINITION.get(oid,{}).get("name",oid)
                oi = other.get("inner", {})
                he = oi.get("hidden_emotion","none")
                rr = get_role_relation(rid, oid)
                if he in ("afraid_of_abandonment","insecure","craving_attention"):
                    percep[rid].append(f"你察觉到{oname}有些不安，会注意分寸")
                elif he == "possessive" or oi.get("surface_emotion") == "jealous":
                    percep[rid].append(f"你察觉到{oname}在吃醋")
                    if rr["rivalry"] > 0.3 and rid == "jingwen":
                        percep[rid].append("你心里有点小得意，可能会故意逗她")
                    elif rr["affinity"] > 0.3:
                        percep[rid].append("你会适当收敛，不火上浇油")
                elif oi.get("comfort",0) > 60:
                    percep[rid].append(f"{oname}在关心用户，你不用抢着说话")
                    if rr["rivalry"] > 0.2:
                        percep[rid].append("但你也不想完全被比下去")
                elif oi.get("attack",0) > 60:
                    percep[rid].append(f"{oname}似乎要发作，你选择观望或打圆场")
                elif oi.get("withdraw",0) > 60:
                    percep[rid].append(f"你察觉到{oname}在退缩，可能需要你打圆场")
        return percep
    async def generate(self, msg, mem_ctx, history, intimacy_map, psych_in, event_hist,
                       active_conf, resilience_map, cp_usage, turn, use_llm=True):
        role_ids = [rid for rid in intimacy_map.keys() if rid in ROLES_DEFINITION][:3]
        if not role_ids: role_ids = list(ROLES_DEFINITION.keys())[:1]
        ge = await self.analyze_group_event(msg, history, role_ids, intimacy_map) if use_llm else {
            "event_type":"neutral","summary":msg[:30],
            "impacts":{rid:{"emotion":"calm","surface":"平静","hidden_emotion":"none",
                            "intensity":10,"event_type":"none","reason":""} for rid in role_ids}}
        role_states = {}
        new_psych = {}; new_cp = {}; new_ev = {}; new_active = {}; new_res = {}
        for rid in role_ids:
            role = ROLES_DEFINITION[rid]
            intimacy = intimacy_map.get(rid, 30)
            impact = ge.get("impacts",{}).get(rid, {})
            try: emotion = EmotionType(impact.get("emotion","calm"))
            except: emotion = EmotionType.CALM
            intensity = int(impact.get("intensity",20))
            et = impact.get("event_type","none")
            interp = {"surface":impact.get("reason",""),"hidden_intent":"无意识",
                      "relationship_meaning":impact.get("reason",""),"threat_level":0,
                      "is_test":False,"is_joke":False} if impact.get("reason") else None
            inner = {"surface_emotion":impact.get("surface","calm"),"surface_intensity":intensity,
                     "hidden_emotion":impact.get("hidden_emotion","none"),"hidden_intensity":intensity*2//3,
                     "relationship_need":"","approach":30,"withdraw":20,"attack":15,"wait":30,"comfort":20}
            if rid == "jingwen":
                inner["attack"] += 15; inner["approach"] -= 10
                if emotion == EmotionType.JEALOUS: inner["attack"] += 10; inner["wait"] += 15
            elif rid == "qinghe":
                inner["comfort"] += 20; inner["attack"] = max(0, inner["attack"]-15)
            elif rid == "nianqi":
                inner["comfort"] += 15; inner["approach"] += 10; inner["wait"] += 5
            rid_ev = event_hist.get(rid, {}) if isinstance(event_hist, dict) else {}
            psych = PsychologicalState(rid, psych_in.get(rid))
            tracker = EventHistoryTracker(rid_ev)
            rid_ac = active_conf.get(rid) if isinstance(active_conf, dict) else active_conf
            rid_res = resilience_map.get(rid, 0) if isinstance(resilience_map, dict) else (resilience_map if isinstance(resilience_map,(int,float)) else 0)
            repair = RelationshipRepairSystem(rid_ac, rid_res)
            cp_ctrl = CatchphraseController(rid, cp_usage.get(rid,{}) if isinstance(cp_usage, dict) else {})
            ce = ConflictEngine(rid)
            stage_name = "陌生人" if intimacy<=30 else "认识" if intimacy<=50 else "熟悉" if intimacy<=70 else "亲密"
            conflict = ce.detect(msg, stage_name)
            repair_result = repair.check(et, turn)
            just_repaired = repair_result is not None
            cat = EVENT_CATEGORY.get(et, "neutral")
            is_trauma = conflict.get("is_trauma", False)
            # v12.1: LLM校准层 —— 先保存旧状态快照，本地公式算基础值，复杂语境下由LLM输出修正系数
            old_psych_states = dict(psych.states)
            raw_new_psych = psych.update(et, tracker, turn, repair_result, repair.damage_reduction(), interp, cat, is_trauma)
            if llm_calibrator.should_calibrate(msg, old_psych_states, intimacy):
                new_psych[rid] = await llm_calibrator.calibrate(
                    msg, psych, old_psych_states, raw_new_psych, intimacy, rid, history)
            else:
                new_psych[rid] = raw_new_psych
            if et != "none": tracker.record(et, turn)
            new_ev[rid] = tracker.history
            cp_decision = cp_ctrl.decide(intimacy, emotion.value, conflict["severity"], just_repaired, inner)
            new_cp[rid] = cp_ctrl.recent_usage
            if conflict["severity"] >= 2 and not just_repaired:
                repair.start_conflict(conflict["severity"], conflict["type"], turn)
                new_active[rid] = repair.active
            else:
                new_active[rid] = None
            new_res[rid] = repair.resilience
            noise = DailyNoiseLayer().generate(rid, intensity)
            role_states[rid] = {"role":role,"emotion":emotion,"intensity":intensity,"et":et,
                "interp":interp,"inner":inner,"psych":psych,"conflict":conflict,
                "just_repaired":just_repaired,"cp":cp_decision,"noise":noise,
                "intimacy":intimacy,"impact_reason":impact.get("reason","")}
        perceptions = self._interpersonal(role_states)
        sections = ["═══════════════════════════════",
            "你正在同时扮演以下多个角色。每个角色都有独立心理状态，严格按各自状态回应。不是AI。",
            "═══════════════════════════════",
            f"【群聊事件】{ge.get('summary','')}"]
        for rid in role_ids:
            rs = role_states[rid]; role = rs["role"]; psych = rs["psych"]
            inner = rs["inner"]; s = psych.states
            sections.append(f"─────\n【{role['emoji']} {role['name']}】")
            sections.append(f"身份：{role['age']}{role['gender']}生，{role['personality']}。{role['speaking_style']}")
            sections.append(f"核心特质：{'、'.join(role.get('core_traits',[]))}")
            sections.append(f"与用户：亲密度{rs['intimacy']}，信任{s['trust']:.0f}，安全感{s['security']:.0f}，依恋{s['attachment']:.0f}")
            if s["jealousy"] > 0: sections.append(f"当前醋意：{s['jealousy']:.0f}")
            sections.append(f"表面情绪：{inner['surface_emotion']}（{inner['surface_intensity']}%）")
            bt = role.get("behavior_tendency", {})
            bh_key = "jealous" if rs["emotion"] == EmotionType.JEALOUS else ("worried" if rs["emotion"]==EmotionType.WORRIED else "default")
            sections.append(f"行为倾向：{bt.get(bh_key, bt.get('default','正常回应'))}")
            if rs["impact_reason"]: sections.append(f"你对这件事的感受：{rs['impact_reason']}")
            if rs["just_repaired"]: sections.append("你们刚和解，别扭但释然")
            if rs["conflict"]["severity"] > 0:
                sections.append(f"冲突：表面{rs['conflict']['surface']}")
            if rs["cp"]["use"]: sections.append(f"口头禅：本轮可以自然用一次「{rs['cp']['catchphrase']}」")
            else: sections.append("口头禅：本轮不用")
            if rs["noise"]: sections.append(f"日常状态：{rs['noise']['description']}")
            for p in perceptions.get(rid, []):
                sections.append(f"群体感知：{p}")
        if mem_ctx: sections.append(f"─────\n【共同记忆】\n{mem_ctx}")
        sections.append("═══════════════════════════════")
        sections.append("【输出规则】\n1. 用「角色名：对话内容」格式\n"
                        "2. 不是每个角色都必须说话，可以1-2个主要回应，其他人简短带过或沉默\n"
                        "3. 角色之间能感知彼此情绪并做出反应（竞争/亲和/打圆场）\n"
                        "4. 只输出对话，不要动作描写、心理旁白\n"
                        "5. 行为倾向只影响语气，不要直接解释心理\n6. 口头禅偶尔使用")
        prompt = "\n\n".join(sections)
        debug = {"mode":"group","group_event":ge,
            "role_states":{rid:{"emotion":rs["emotion"].value,"inner":rs["inner"]["hidden_emotion"],
                                "intimacy":rs["intimacy"],"conflict":rs["conflict"]["type"]} for rid,rs in role_states.items()},
            "new_psychological_state":new_psych,"new_event_history":new_ev,
            "new_catchphrase_usage":new_cp,"new_active_conflict":new_active,
            "new_resilience":new_res}
        return prompt, debug

# ============================================================
# v10.0: 微叙事引擎（角色"正在做什么"的持续微叙事流）
# ============================================================

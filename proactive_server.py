"""
主动消息后端 v10.0 - 端口 8003
对接人格后端 v11.0：动机引擎 / 内部事件生成 / 防骚扰策略 / 后台调度
v10.0: 配置项环境变量化 / 版本号对齐 / 兼容人格后端v11.0接口
"""
import asyncio
import json
import logging
import os
import random
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("proactive_server")

# ============================================================
# 配置
# ============================================================
PORT = int(os.getenv("PROACTIVE_PORT", "8003"))
PERSONALITY_SERVER_URL = os.getenv("PERSONALITY_SERVER_URL", "http://127.0.0.1:8002")
VECTOR_SERVER_URL = os.getenv("VECTOR_SERVER_URL", "http://127.0.0.1:8001")
MAIN_SERVER_URL = os.getenv("MAIN_SERVER_URL", "http://127.0.0.1:8000")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "change_me_internal_secret_2026")
VECTOR_API_TOKEN = os.getenv("VECTOR_API_TOKEN", "change_me_strong_secret_key_123456")

DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(DATA_DIR, "proactive.db")

# 调度器
SCHEDULER_INTERVAL_SECONDS = 60          # 每分钟检查一次
MAX_MESSAGES_PER_TICK = 3                # 每轮最多推送3条（防爆发）
SCORE_THRESHOLD_LIGHT = 60               # 60-80 发轻消息
SCORE_THRESHOLD_STRONG = 80              # 80+ 强烈想联系
SCORE_THRESHOLD_MEMORY_BONUS = 45        # 有强记忆事件时，45分以上即可触发

# 静默时段（北京时间，24小时制）：[SLEEP_START, SLEEP_END) 不主动推送
SLEEP_START_HOUR = 23
SLEEP_END_HOUR = 8
TIMEZONE_CST = timezone(timedelta(hours=8))

# 角色 daily_noise 镜像（与 personality_server 保持一致，用于动机情绪模拟）
ROLE_DAILY_NOISE = {
    "jingwen": ["刚才看小说看到一半被打断，有点不爽", "喝到了好喝的奶茶，心情不错",
                "今天扎了双马尾，总觉得怪怪的", "刚睡醒，还有点迷糊"],
    "qinghe":  ["刚泡了一壶花茶，很放松", "在看书，被打断有点无奈",
                "今天做了好吃的甜点，心情很好", "窗外下雨了，有点犯困"],
    "nianqi":  ["刚晒完被子，有阳光的味道", "在听一首很温柔的歌，忽然想到你",
                "刚洗完澡，很放松", "在修东西，手上忙着"],
}

# ============================================================
# SQLite 持久化
# ============================================================
def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS activities (
            user_id TEXT NOT NULL,
            role_id TEXT NOT NULL,
            last_active_at REAL NOT NULL,
            last_intimacy INTEGER DEFAULT 30,
            last_attachment REAL DEFAULT 20,
            last_psych TEXT DEFAULT '{}',
            PRIMARY KEY (user_id, role_id)
        );
        CREATE TABLE IF NOT EXISTS proactive_messages (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            role_id TEXT NOT NULL,
            content TEXT NOT NULL,
            reason_type TEXT NOT NULL,
            reason_detail TEXT DEFAULT '',
            motivation_score REAL DEFAULT 0,
            created_at REAL NOT NULL,
            delivered INTEGER DEFAULT 0,
            replied INTEGER DEFAULT 0,
            replied_at REAL
        );
        CREATE INDEX IF NOT EXISTS idx_msg_user ON proactive_messages(user_id, role_id);
        CREATE INDEX IF NOT EXISTS idx_msg_unreplied ON proactive_messages(user_id, role_id, replied);
        CREATE INDEX IF NOT EXISTS idx_msg_created ON proactive_messages(created_at);
        CREATE TABLE IF NOT EXISTS daily_counters (
            user_id TEXT NOT NULL,
            role_id TEXT NOT NULL,
            date TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, role_id, date)
        );
    """)
    conn.commit()
    conn.close()
    logger.info(f"SQLite初始化: {DB_PATH}")


# ============================================================
# 工具
# ============================================================
def now_cst() -> datetime:
    return datetime.now(TIMEZONE_CST)


def today_str() -> str:
    return now_cst().strftime("%Y-%m-%d")


def stage_of(intimacy: int) -> str:
    if intimacy <= 30: return "stranger"
    if intimacy <= 50: return "acquaintance"
    if intimacy <= 70: return "familiar"
    if intimacy <= 85: return "close"
    return "intimate"


# ============================================================
# v12.0: LongingEngine 欲望演算引擎
# 输入：心理状态 + 历史交互上下文（空闲时长、亲密度、依恋、时段、情绪）
# 输出：结构化动机对象 Motivation（不是直接输出消息！）
# 优先从人格后端拉取 DesireMentalState，失败则降级为本地计算
# ============================================================
class LongingEngine:
    """
    欲望演算引擎：将心理状态和交互上下文转化为结构化动机对象。
    输出包含：原始欲望5维度、主导欲望、综合动机分数、各维度分项。
    这是主动消息流水线的第一步，后续经过 MotivationEngine(过滤) → ContactPolicy(行为意图) → PersonalityEngine(生成文本)。
    """

    async def fetch_desire_from_personality(self, session_id: str, role_id: str) -> Optional[Dict[str, Any]]:
        """从人格后端拉取 DesireMentalState（v12.0 新增接口）。失败返回 None。"""
        if not session_id:
            return None
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    f"{PERSONALITY_SERVER_URL}/api/session/{session_id}/desire/{role_id}",
                    timeout=8.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success"):
                        return data
        except Exception as e:
            logger.debug(f"[LongingEngine] 从人格后端拉取欲望状态失败: {e}")
        return None

    def calc_local_desire(self, role_id: str, intimacy: int, attachment: float,
                           idle_hours: float, mood: str, hour: int) -> Dict[str, Any]:
        """
        本地计算欲望状态（降级方案，无 session_id 或人格后端不可用时使用）。
        基于空闲时长、亲密度、依恋、情绪、时段模拟5个欲望维度。
        """
        # 基础值（角色性格决定）
        role_base = {
            "nianqi":  {"longing": 45, "contact_desire": 50, "share_desire": 45, "care_desire": 55, "companionship": 60},
            "qinghe":  {"longing": 35, "contact_desire": 40, "share_desire": 45, "care_desire": 55, "companionship": 50},
            "jingwen": {"longing": 40, "contact_desire": 25, "share_desire": 30, "care_desire": 35, "companionship": 45},
        }.get(role_id, {"longing": 35, "contact_desire": 30, "share_desire": 30, "care_desire": 35, "companionship": 40})

        desire = dict(role_base)

        # 空闲时长影响：越久没联系，想念和联系欲越高
        if idle_hours > 3:
            rise = min(30.0, (idle_hours - 3) * 0.8)
            desire["longing"] = min(100, desire["longing"] + rise * 0.6)
            desire["contact_desire"] = min(100, desire["contact_desire"] + rise * 0.4)
            desire["care_desire"] = min(100, desire["care_desire"] + rise * 0.2)
            desire["companionship"] = min(100, desire["companionship"] + rise * 0.3)

        # 亲密度影响：亲密越高，陪伴欲和想念越高
        if intimacy >= 80:
            desire["companionship"] = min(100, desire["companionship"] + 10)
            desire["longing"] = min(100, desire["longing"] + 8)
        elif intimacy <= 30:
            desire["contact_desire"] = max(0, desire["contact_desire"] - 15)
            desire["care_desire"] = max(0, desire["care_desire"] - 10)

        # 依恋影响
        attach_bonus = (max(0, min(100, attachment)) / 100.0) * 15
        desire["longing"] = min(100, desire["longing"] + attach_bonus * 0.5)
        desire["companionship"] = min(100, desire["companionship"] + attach_bonus * 0.5)

        # 情绪影响
        mood_effects = {
            "lonely": {"longing": +15, "contact_desire": +10, "companionship": +12},
            "sad":    {"longing": +10, "care_desire": +5, "companionship": +8},
            "bored":  {"contact_desire": +15, "share_desire": +8},
            "happy":  {"share_desire": +12, "contact_desire": +5},
            "excited":{"share_desire": +15, "contact_desire": +8},
            "worried":{"care_desire": +15, "longing": +5},
        }
        for dim, delta in mood_effects.get(mood, {}).items():
            desire[dim] = max(0, min(100, desire[dim] + delta))

        # 时段影响：傍晚/夜里联系欲上升，深夜抑制
        if 19 <= hour < 23:
            desire["contact_desire"] = min(100, desire["contact_desire"] + 8)
            desire["companionship"] = min(100, desire["companionship"] + 5)
        elif hour >= 23 or hour < 8:
            desire["contact_desire"] = max(0, desire["contact_desire"] - 10)

        # 随机抖动
        for dim in desire:
            desire[dim] = round(max(0, min(100, desire[dim] + random.uniform(-5, 5))), 1)

        return desire

    def calc_motivation_score(self, desire: Dict[str, float]) -> float:
        """从5维欲望计算综合动机分数（0-100）。联系欲和想念权重最高。"""
        weighted = (
            desire.get("contact_desire", 0) * 0.30 +
            desire.get("longing", 0) * 0.25 +
            desire.get("companionship", 0) * 0.20 +
            desire.get("care_desire", 0) * 0.15 +
            desire.get("share_desire", 0) * 0.10
        )
        return round(min(100.0, max(0.0, weighted)), 1)

    def dominant_desire(self, desire: Dict[str, float]) -> Tuple[str, float]:
        """返回当前最强的欲望维度及其数值。"""
        dims = ("longing", "contact_desire", "share_desire", "care_desire", "companionship")
        best = dims[0]
        best_val = desire.get(best, 0)
        for d in dims[1:]:
            v = desire.get(d, 0)
            if v > best_val:
                best = d
                best_val = v
        return best, best_val

    async def calc(self, user_id: str, role_id: str, intimacy: int, attachment: float,
                   idle_hours: float, mood: str, hour: int,
                   session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        欲望演算主入口。输出结构化动机对象：
        {source, desire, dominant, dominant_value, motivation_score, breakdown}
        """
        # 1. 优先从人格后端拉取真实欲望状态
        remote = await self.fetch_desire_from_personality(session_id, role_id)
        if remote and remote.get("desire"):
            desire = {k: v for k, v in remote["desire"].items() if k != "last_updated"}
            return {
                "source": "personality_desire_state",
                "desire": desire,
                "dominant": remote.get("dominant", self.dominant_desire(desire)[0]),
                "dominant_value": remote.get("dominant_value", self.dominant_desire(desire)[1]),
                "motivation_score": remote.get("motivation_score", self.calc_motivation_score(desire)),
                "breakdown": desire,
            }

        # 2. 降级：本地计算欲望状态
        desire = self.calc_local_desire(role_id, intimacy, attachment, idle_hours, mood, hour)
        dominant, dom_val = self.dominant_desire(desire)
        score = self.calc_motivation_score(desire)
        return {
            "source": "local_calculation",
            "desire": desire,
            "dominant": dominant,
            "dominant_value": dom_val,
            "motivation_score": score,
            "breakdown": desire,
        }


# ============================================================
# MotivationEngine —— 计算「多想找你」(0-100)
# v12.0: 重构为过滤约束层 —— 对 LongingEngine 输出的原始欲望做现实约束压制
# ============================================================
class MotivationEngine:
    # 各阶段每日主动消息上限
    DAILY_LIMIT = {
        "stranger": 0, "acquaintance": 1, "familiar": 2, "close": 3, "intimate": 5
    }
    # 各阶段两条主动消息之间的最小间隔（小时）
    MIN_INTERVAL_HOURS = {
        "stranger": 24, "acquaintance": 8, "familiar": 4, "close": 2, "intimate": 1
    }

    def calc_idle_score(self, idle_hours: float) -> float:
        """空闲时长分：3h内0；3-24h缓升；24-72h冲到高峰（想你了）；72h后缓慢衰减（习惯了但惦记）"""
        if idle_hours < 3:
            return 0.0
        if idle_hours < 24:
            return 20.0 * (idle_hours - 3) / 21.0
        if idle_hours < 72:
            return 20.0 + 50.0 * (idle_hours - 24) / 48.0
        if idle_hours < 168:
            return 70.0 - 30.0 * (idle_hours - 72) / 96.0
        return 40.0

    def calc_time_score(self, hour: int) -> float:
        """时段分：傍晚/夜里最容易想找人；深夜抑制"""
        if 19 <= hour < 23:
            return 10.0
        if 9 <= hour < 12:
            return 5.0
        if 12 <= hour < 19:
            return 0.0
        if 8 <= hour < 9:
            return -10.0
        return -30.0  # 23:00 - 08:00

    def simulate_mood(self, role_id: str, idle_hours: float) -> str:
        """模拟角色此刻情绪（无对话时由日常+随机+空闲决定）"""
        r = random.random()
        # 空闲越久越容易感到 lonely/bored
        lonely_chance = min(0.4, 0.05 + idle_hours / 200.0)
        if r < lonely_chance:
            return "lonely"
        if r < lonely_chance + 0.15:
            return "bored"
        if r < lonely_chance + 0.30:
            return "sad"
        if r < lonely_chance + 0.55:
            return "happy"
        if r < lonely_chance + 0.70:
            return "excited"
        return "calm"

    def mood_score(self, mood: str) -> float:
        return {
            "lonely": 15.0, "sad": 15.0, "bored": 10.0,
            "excited": 12.0, "happy": 10.0, "worried": 8.0, "calm": 0.0,
        }.get(mood, 0.0)

    def calc(
        self,
        intimacy: int,
        attachment: float,
        idle_hours: float,
        memory_weight: float,
        mood: str,
        hour: int,
    ) -> Dict[str, Any]:
        stage = stage_of(intimacy)

        idle_s = self.calc_idle_score(idle_hours)
        attach_s = (max(0.0, min(100.0, attachment)) / 100.0) * 25.0
        mem_s = min(15.0, max(0.0, memory_weight) / 100.0 * 15.0)
        mood_s = self.mood_score(mood)
        time_s = self.calc_time_score(hour)

        # 亲密度基础：陌生人不该主动，亲密有加成
        if intimacy < 30:
            intimacy_base = -40.0
        elif intimacy < 50:
            intimacy_base = -10.0
        elif intimacy >= 80:
            intimacy_base = 10.0
        else:
            intimacy_base = 0.0

        raw = idle_s + attach_s + mem_s + mood_s + time_s + intimacy_base
        jitter = random.uniform(-8.0, 8.0)
        score = max(0.0, min(100.0, raw + jitter))

        return {
            "score": round(score, 1),
            "stage": stage,
            "breakdown": {
                "idle": round(idle_s, 1),
                "attachment": round(attach_s, 1),
                "memory": round(mem_s, 1),
                "mood": round(mood_s, 1),
                "time": round(time_s, 1),
                "intimacy_base": round(intimacy_base, 1),
                "jitter": round(jitter, 1),
            },
            "mood": mood,
        }

    def filter(self, motivation: Dict[str, Any], fatigue: float = 0.0,
               hour: int = 12, unreplied_count: int = 0, role_id: str = "nianqi",
               hours_since_last: float = 999.0) -> Dict[str, Any]:
        """
        v12.0: 动机过滤约束层 —— 对 LongingEngine 输出的原始欲望做现实约束压制。
        压制因子：疲劳、深夜时段、未回复条数、历史冷却、角色性格阈值。
        输出：{allowed, final_score, original_score, suppress_factors, reason}
        """
        original_score = motivation.get("motivation_score", 0.0)
        score = original_score
        suppress = []

        # 1. 疲劳压制：疲劳越高，动机越低
        if fatigue > 50:
            penalty = (fatigue - 50) * 0.5
            score -= penalty
            suppress.append(f"疲劳({fatigue:.0f})-{penalty:.1f}")

        # 2. 深夜时段压制：23:00-08:00 大幅降低动机
        if hour >= 23 or hour < 8:
            score -= 25
            suppress.append("深夜时段-25")
        elif hour < 9:
            score -= 10
            suppress.append("清晨-10")

        # 3. 未回复压制：有未回复消息时降低动机（别缠着人家）
        if unreplied_count >= 2:
            score -= 40
            suppress.append(f"未回复{unreplied_count}条-40")
        elif unreplied_count >= 1:
            score -= 15
            suppress.append(f"未回复{unreplied_count}条-15")

        # 4. 历史冷却：距上次主动消息太近，降低动机
        min_interval = self.MIN_INTERVAL_HOURS.get(stage_of(30), 4)  # 用默认阶段
        if hours_since_last < min_interval:
            score -= 30
            suppress.append(f"冷却中({hours_since_last:.1f}h<{min_interval}h)-30")

        # 5. 角色性格阈值：不同角色对主动联系的阈值不同
        role_threshold = {
            "nianqi": 35,    # 温柔依恋：安全型依恋，会主动靠近和陪伴
            "qinghe": 40,    # 温柔知性：较低动机就会主动关心
            "jingwen": 55,   # 傲娇：需要更高动机才会主动（嘴硬）
        }.get(role_id, 50)

        score = max(0.0, min(100.0, score))
        allowed = score >= role_threshold

        return {
            "allowed": allowed,
            "final_score": round(score, 1),
            "original_score": original_score,
            "threshold": role_threshold,
            "suppress_factors": suppress,
            "reason": "通过" if allowed else f"分数{score:.1f}<阈值{role_threshold}",
        }


# ============================================================
# InnerEventGenerator —— 生成内部触发事件
# ============================================================
class InnerEventGenerator:
    """根据动机分项和记忆，决定"因为什么想联系"，并给出 prompt 引导"""

    async def generate(
        self,
        role_id: str,
        role_name: str,
        idle_hours: float,
        motivation: Dict[str, Any],
        related_memory: Optional[Dict[str, Any]],
    ) -> Dict[str, str]:
        bd = motivation["breakdown"]
        mood = motivation["mood"]

        candidates: List[Dict[str, str]] = []

        # 1. 记忆触发：检索到高价值共同记忆
        if related_memory and bd["memory"] >= 8.0:
            summary = related_memory.get("summary", "")
            if summary:
                candidates.append({
                    "reason_type": "memory_recall",
                    "reason_detail": summary,
                    "weight": bd["memory"] + 10.0,
                })

        # 2. 想念：空闲久 + 依恋/孤独
        if idle_hours >= 24 and (bd["attachment"] >= 10.0 or mood == "lonely"):
            candidates.append({
                "reason_type": "missing_you",
                "reason_detail": f"已经{int(idle_hours)}小时没联系了，有点想对方",
                "weight": bd["idle"] * 0.5 + bd["attachment"],
            })

        # 3. 久未联系问候
        if idle_hours >= 48:
            candidates.append({
                "reason_type": "long_time_no_see",
                "reason_detail": f"好几天没聊了，想知道对方最近怎么样",
                "weight": bd["idle"] * 0.3,
            })

        # 4. 情绪需求：心情低落/开心想分享
        if mood in ("sad", "lonely") and idle_hours >= 6:
            candidates.append({
                "reason_type": "emotion_need",
                "reason_detail": "现在心情有点低落，想找对方说说话",
                "weight": bd["mood"] + 5.0,
            })
        elif mood in ("happy", "excited"):
            noise = random.choice(ROLE_DAILY_NOISE.get(role_id, ["刚遇到一件小事"]))
            candidates.append({
                "reason_type": "daily_share",
                "reason_detail": f"刚在{noise}，想分享给对方",
                "weight": bd["mood"],
            })

        # 5. 日常分享（基于 daily_noise，权重较低）
        if random.random() < 0.35:
            noise = random.choice(ROLE_DAILY_NOISE.get(role_id, ["刚在忙自己的事"]))
            candidates.append({
                "reason_type": "daily_share",
                "reason_detail": f"刚在{noise}，顺手想发给对方",
                "weight": 5.0,
            })

        # 6. 轻问候（兜底）
        candidates.append({
            "reason_type": "check_in",
            "reason_detail": "没什么特别的事，就是想问候一下",
            "weight": 2.0,
        })

        if not candidates:
            return {"reason_type": "check_in", "reason_detail": ""}

        # 加权随机
        total_w = sum(max(0.1, c["weight"]) for c in candidates)
        r = random.uniform(0, total_w)
        for c in candidates:
            r -= max(0.1, c["weight"])
            if r <= 0:
                return {"reason_type": c["reason_type"], "reason_detail": c["reason_detail"]}
        return candidates[0]

    async def apply_to_desire(self, role_id: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        v12.0: 生成内在随机事件并应用到欲望状态（修改 DesireMentalState 数值）。
        事件类型：saw_scenery / recalled_memory / worried_about_you / bored / happy_event / sad_event
        如果有 session_id，调人格后端接口修改真实欲望状态；否则仅返回事件信息（本地计算时使用）。
        """
        events = [
            ("saw_scenery", 0.20, "看到了好看的风景，想分享"),
            ("recalled_memory", 0.15, "突然想起了和对方的往事"),
            ("worried_about_you", 0.10, "有点担心对方最近怎么样"),
            ("bored", 0.25, "有点无聊，想找人聊天"),
            ("happy_event", 0.15, "遇到了开心的事，想分享"),
            ("sad_event", 0.10, "心情有点低落，想有人陪"),
        ]
        r = random.random()
        cumulative = 0
        chosen_event = "bored"
        chosen_desc = "有点无聊，想找人聊天"
        for event, prob, desc in events:
            cumulative += prob
            if r <= cumulative:
                chosen_event = event
                chosen_desc = desc
                break

        # 如果有 session_id，调人格后端应用内在事件到欲望状态
        applied = False
        if session_id:
            try:
                async with httpx.AsyncClient(timeout=8.0) as client:
                    resp = await client.post(
                        f"{PERSONALITY_SERVER_URL}/api/session/{session_id}/desire/{role_id}/inner_event",
                        json={"event_type": chosen_event, "intensity": random.uniform(0.5, 1.5)},
                        timeout=8.0,
                    )
                    if resp.status_code == 200:
                        applied = True
            except Exception as e:
                logger.debug(f"[InnerEvent] 应用内在事件到人格后端失败: {e}")

        return {
            "event_type": chosen_event,
            "event_desc": chosen_desc,
            "applied_to_desire": applied,
            "intensity": random.uniform(0.5, 1.5),
        }


# ============================================================
# ContactPolicy —— 防骚扰
# ============================================================
class ContactPolicy:
    def __init__(self, engine: MotivationEngine):
        self.engine = engine

    def _today_count(self, conn, user_id: str, role_id: str) -> int:
        row = conn.execute(
            "SELECT count FROM daily_counters WHERE user_id=? AND role_id=? AND date=?",
            (user_id, role_id, today_str())
        ).fetchone()
        return row["count"] if row else 0

    def _last_message(self, conn, user_id: str, role_id: str) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM proactive_messages WHERE user_id=? AND role_id=? ORDER BY created_at DESC LIMIT 1",
            (user_id, role_id)
        ).fetchone()

    def _unreplied_count(self, conn, user_id: str, role_id: str) -> int:
        return conn.execute(
            "SELECT COUNT(*) AS c FROM proactive_messages WHERE user_id=? AND role_id=? AND replied=0 AND delivered=1",
            (user_id, role_id)
        ).fetchone()["c"]

    def is_silent_hour(self) -> bool:
        h = now_cst().hour
        if SLEEP_START_HOUR <= h or h < SLEEP_END_HOUR:
            return True
        return False

    def check(self, conn, user_id: str, role_id: str, intimacy: int,
              score: float, has_memory_event: bool) -> Dict[str, Any]:
        """返回 {allowed, reason, threshold}"""
        stage = stage_of(intimacy)
        now_ts = time.time()

        # 0. 静默时段
        if self.is_silent_hour():
            return {"allowed": False, "reason": "silent_hour", "threshold": score}

        # 1. 陌生人不主动
        if stage == "stranger":
            return {"allowed": False, "reason": "stage_stranger", "threshold": score}

        # 2. 每日上限
        limit = self.engine.DAILY_LIMIT.get(stage, 1)
        used = self._today_count(conn, user_id, role_id)
        if used >= limit:
            return {"allowed": False, "reason": f"daily_limit({used}/{limit})", "threshold": score}

        # 3. 最小间隔
        last = self._last_message(conn, user_id, role_id)
        if last:
            min_interval = self.engine.MIN_INTERVAL_HOURS.get(stage, 4) * 3600
            if now_ts - last["created_at"] < min_interval:
                return {"allowed": False, "reason": "min_interval", "threshold": score}

        # 4. 未回复降频
        unreplied = self._unreplied_count(conn, user_id, role_id)
        threshold = SCORE_THRESHOLD_LIGHT
        if unreplied >= 2:
            return {"allowed": False, "reason": "two_unreplied", "threshold": threshold}
        if unreplied >= 1:
            threshold += 20  # 有一条未回复，门槛提高到80

        # 5. 强记忆事件可降低门槛
        if has_memory_event and score >= SCORE_THRESHOLD_MEMORY_BONUS:
            return {"allowed": True, "reason": "memory_trigger", "threshold": threshold}

        if score >= threshold:
            return {"allowed": True, "reason": "score_ok", "threshold": threshold}
        return {"allowed": False, "reason": f"score_too_low({score}<{threshold})", "threshold": threshold}

    def increment_daily(self, conn, user_id: str, role_id: str) -> None:
        d = today_str()
        conn.execute(
            "INSERT INTO daily_counters(user_id, role_id, date, count) VALUES(?,?,?,1) "
            "ON CONFLICT(user_id, role_id, date) DO UPDATE SET count = count + 1",
            (user_id, role_id, d)
        )
        conn.commit()

    def translate_intent(self, motivation: Dict[str, Any], role_id: str,
                         intimacy: int, idle_hours: float) -> Dict[str, Any]:
        """
        v12.0: 行为意图翻译层 —— 根据人格模板，把动机翻译成行为意图。
        【关键：同一动机，不同人格模板产出不同行为意图】
        温柔依恋：动机=想念 → 意图：直白的想念问候
        傲娇：动机=想念 → 意图：找借口搭话，嘴硬不承认想你
        活泼：动机=分享 → 意图：兴奋分享琐事
        清冷：动机=陪伴 → 意图：简短淡淡的近况问询
        """
        dominant = motivation.get("dominant", "longing")
        desire = motivation.get("desire", {})
        stage = stage_of(intimacy)

        # 角色行为风格模板
        role_style = {
            "jingwen": {  # 傲娇
                "longing": "excuse_to_talk",
                "contact_desire": "excuse_to_talk",
                "share_desire": "tsundere_share",
                "care_desire": "tsundere_care",
                "companionship": "casual_mention",
            },
            "qinghe": {  # 温柔
                "longing": "direct_missing",
                "contact_desire": "direct_greeting",
                "share_desire": "warm_share",
                "care_desire": "care_checkin",
                "companionship": "warm_company",
            },
            "nianqi": {  # 温柔依恋
                "longing": "direct_missing",
                "contact_desire": "warm_greeting",
                "share_desire": "gentle_share",
                "care_desire": "tender_care",
                "companionship": "close_company",
            },
        }.get(role_id, {
            "longing": "direct_greeting", "contact_desire": "direct_greeting",
            "share_desire": "share_story", "care_desire": "care_checkin",
            "companionship": "casual_mention",
        })

        intent_type = role_style.get(dominant, "casual_mention")

        # 根据意图类型生成详细描述和 prompt 引导
        intent_details = {
            "direct_missing": {
                "detail": f"已经{int(idle_hours)}小时没联系了，有点想对方，直接表达想念",
                "prompt_hint": "表达想念，语气温柔，不要太刻意",
            },
            "direct_greeting": {
                "detail": "想和对方聊天，直接打招呼问候",
                "prompt_hint": "自然的问候，问问对方在干嘛",
            },
            "excuse_to_talk": {
                "detail": "想对方但嘴硬，找个借口搭话（比如问个问题、说件小事）",
                "prompt_hint": "傲娇风格，找借口搭话，嘴硬不承认想对方，语气带刺但关心",
            },
            "tsundere_share": {
                "detail": "想分享但傲娇，用'顺便说一下'的语气分享",
                "prompt_hint": "傲娇分享，用'不是特意告诉你'的语气",
            },
            "tsundere_care": {
                "detail": "关心对方但嘴硬，用责备的语气表达关心",
                "prompt_hint": "傲娇关心，嘴上骂笨蛋但实际在关心",
            },
            "warm_share": {
                "detail": "温柔地分享一件小事或见闻",
                "prompt_hint": "温柔分享，语气亲切自然",
            },
            "care_checkin": {
                "detail": "关心对方近况，问问身体/心情/工作",
                "prompt_hint": "关心慰问，语气温柔体贴",
            },
            "warm_company": {
                "detail": "想陪伴对方，表达'我在呢'的感觉",
                "prompt_hint": "表达陪伴感，温柔安心",
            },
            "silent_longing": {
                "detail": "想念但不说，用简短的方式表达存在",
                "prompt_hint": "清冷风格，简短沉默的想念，不直白说想你",
            },
            "brief_checkin": {
                "detail": "简短问候，不多说",
                "prompt_hint": "清冷风格，简短问候，一两个字",
            },
            "concise_share": {
                "detail": "简洁地分享一件事",
                "prompt_hint": "清冷风格，简洁分享，不多废话",
            },
            "subtle_care": {
                "detail": "隐晦地表达关心，不直白",
                "prompt_hint": "清冷风格，隐晦关心，用行动而非语言",
            },
            "quiet_company": {
                "detail": "安静陪伴，不说话也在",
                "prompt_hint": "清冷风格，安静陪伴感",
            },
            "share_story": {
                "detail": "分享一件见闻或想法",
                "prompt_hint": "自然分享，语气轻松",
            },
            "warm_greeting": {
                "detail": "温柔地打招呼，自然地问对方在干嘛",
                "prompt_hint": "温柔自然的问候，像阳光一样温暖",
            },
            "gentle_share": {
                "detail": "温柔地分享一件小事，语气亲昵",
                "prompt_hint": "温柔分享，语气亲昵自然",
            },
            "tender_care": {
                "detail": "温柔地关心对方，问问身体和心情",
                "prompt_hint": "温柔体贴的关心，不唠叨但很暖心",
            },
            "close_company": {
                "detail": "表达想要陪伴的心情，自然地靠近",
                "prompt_hint": "表达陪伴感，温柔亲密，安全型依恋",
            },
            "casual_mention": {
                "detail": "随意提起一件事，不刻意",
                "prompt_hint": "随意自然，像突然想起",
            },
        }

        info = intent_details.get(intent_type, intent_details["casual_mention"])

        return {
            "intent_type": intent_type,
            "dominant_desire": dominant,
            "dominant_value": motivation.get("dominant_value", 0),
            "intent_detail": info["detail"],
            "prompt_hint": info["prompt_hint"],
            "role_style": role_id,
            "intimacy_stage": stage,
        }


# ============================================================
# 外部服务调用
# ============================================================
async def fetch_related_memory(user_id: str, role_id: str, query: str = "我们一起经历的事") -> Optional[Dict[str, Any]]:
    """从向量库检索一条高价值共同记忆"""
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                f"{VECTOR_SERVER_URL}/api/memory/search",
                json={"user_id": user_id, "query": query, "top_k": 5, "role_id": role_id},
                timeout=20.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    mems = data.get("memories", [])
                    if mems:
                        # 优先选情感/重要性高的，其次随机
                        def weight(m):
                            try:
                                return float(m.get("importance", 50))
                            except (TypeError, ValueError):
                                return 50.0
                        top = sorted(mems, key=weight, reverse=True)[:3]
                        return random.choice(top)
    except Exception as e:
        logger.warning(f"检索记忆失败: {e}")
    return None


async def generate_proactive_message(
    role_id: str, reason_type: str, reason_detail: str,
    related_memory: Optional[Dict[str, Any]], idle_hours: float,
    intimacy: int, mood: str, intent: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """调人格后端生成主动消息文本。v12.0: 支持传入行为意图(intent)引导生成风格。"""
    payload = {
        "role_id": role_id,
        "reason_type": reason_type,
        "reason_detail": reason_detail,
        "related_memory": related_memory,
        "idle_hours": round(idle_hours, 1),
        "intimacy": intimacy,
        "mood": mood,
    }
    if intent:
        payload["intent"] = {
            "intent_type": intent.get("intent_type"),
            "prompt_hint": intent.get("prompt_hint"),
            "dominant_desire": intent.get("dominant_desire"),
        }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{PERSONALITY_SERVER_URL}/api/proactive_generate",
                json=payload, timeout=60.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    return (data.get("content") or "").strip()
            else:
                logger.warning(f"人格后端主动生成失败: HTTP {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"调人格后端异常: {e}")
    return None


async def push_to_user(message_id: str, user_id: str, role_id: str, content: str) -> bool:
    """推送给主后端，返回是否在线投递成功"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{MAIN_SERVER_URL}/api/internal/proactive_push",
                json={
                    "message_id": message_id,
                    "user_id": user_id,
                    "role_id": role_id,
                    "content": content,
                },
                headers={"X-Internal-Token": INTERNAL_TOKEN},
                timeout=10.0,
            )
            if resp.status_code == 200:
                return bool(resp.json().get("delivered"))
    except Exception as e:
        logger.warning(f"推送主后端失败: {e}")
    return False


# ============================================================
# ProactiveScheduler
# ============================================================
class ProactiveScheduler:
    def __init__(self):
        self.engine = MotivationEngine()
        self.policy = ContactPolicy(self.engine)
        self.events = InnerEventGenerator()
        self.longing = LongingEngine()  # v12.0: 欲望演算引擎
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._last_tick_log = 0.0

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="proactive-scheduler")
            logger.info("ProactiveScheduler 已启动")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.tick()
            except Exception as e:
                logger.error(f"Scheduler tick 异常: {e}", exc_info=True)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=SCHEDULER_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def tick(self) -> Dict[str, Any]:
        conn = _get_db()
        sent = []
        try:
            rows = conn.execute("SELECT * FROM activities").fetchall()
            random.shuffle(rows)
            now_ts = time.time()
            cst = now_cst()
            hour = cst.hour

            for row in rows:
                if len(sent) >= MAX_MESSAGES_PER_TICK:
                    break
                user_id = row["user_id"]
                role_id = row["role_id"]
                intimacy = int(row["last_intimacy"] or 30)
                try:
                    psych = json.loads(row["last_psych"] or "{}")
                except Exception:
                    psych = {}
                attachment = float(row["last_attachment"] or psych.get("attachment", intimacy * 0.6))
                idle_hours = (now_ts - float(row["last_active_at"])) / 3600.0

                # 太近不触发
                if idle_hours < 3:
                    continue

                mood = self.engine.simulate_mood(role_id, idle_hours)
                fatigue = float(psych.get("fatigue", 0))
                unreplied = self.policy._unreplied_count(conn, user_id, role_id)
                last_msg = self.policy._last_message(conn, user_id, role_id)
                hours_since_last = ((now_ts - last_msg["created_at"]) / 3600.0) if last_msg else 999.0

                # ===== v12.0 新架构六步流程 =====
                # 第一步：LongingEngine 欲望演算（优先从人格后端拉取真实欲望状态，降级则本地计算）
                motivation = await self.longing.calc(
                    user_id, role_id, intimacy, attachment, idle_hours, mood, hour,
                    session_id=None)

                # 第二步：MotivationEngine 过滤约束（疲劳/深夜/未回复/冷却/角色性格阈值压制）
                filtered = self.engine.filter(
                    motivation, fatigue=fatigue, hour=hour,
                    unreplied_count=unreplied, role_id=role_id,
                    hours_since_last=hours_since_last)
                if not filtered["allowed"]:
                    continue

                # 第三步：ContactPolicy 防骚扰检查（每日上限/最小间隔/静默时段/陌生人限制）
                score = filtered["final_score"]
                decision = self.policy.check(conn, user_id, role_id, intimacy, score, False)
                if not decision["allowed"]:
                    continue

                # 第四步：ContactPolicy 行为意图翻译（同一动机，不同人格模板产出不同行为意图）
                intent = self.policy.translate_intent(motivation, role_id, intimacy, idle_hours)

                # 第五步：InnerEventGenerator 生成触发原因 + 检索相关记忆
                role_name = await self._get_role_name(role_id)
                related = await fetch_related_memory(user_id, role_id) if idle_hours >= 24 else None
                event = await self.events.generate(
                    role_id, role_name, idle_hours, motivation, related)

                # 第六步：PersonalityEngine 生成文本（传入行为意图引导生成风格）
                content = await generate_proactive_message(
                    role_id, event["reason_type"], event["reason_detail"],
                    related, idle_hours, intimacy, mood, intent=intent)
                if not content:
                    continue
                msg_id = uuid.uuid4().hex
                delivered = await push_to_user(msg_id, user_id, role_id, content)
                conn.execute(
                    "INSERT INTO proactive_messages(id,user_id,role_id,content,reason_type,reason_detail,"
                    "motivation_score,created_at,delivered,replied) VALUES(?,?,?,?,?,?,?,?,?,0)",
                    (msg_id, user_id, role_id, content, event["reason_type"],
                     event["reason_detail"], score, now_ts, 1 if delivered else 0))
                self.policy.increment_daily(conn, user_id, role_id)
                conn.commit()
                sent.append({
                    "message_id": msg_id, "user_id": user_id, "role_id": role_id,
                    "content": content, "reason_type": event["reason_type"],
                    "intent_type": intent["intent_type"],
                    "score": score, "delivered": delivered,
                })
                logger.info(f"主动消息[v12]: user={user_id} role={role_id} score={score} "
                            f"intent={intent['intent_type']} reason={event['reason_type']} "
                            f"delivered={delivered} content={content[:30]}")
                # 每条之间稍微间隔，避免瞬间打爆
                await asyncio.sleep(0.5)
        finally:
            conn.close()

        if sent or time.time() - self._last_tick_log > 1800:
            logger.info(f"Scheduler tick 完成，本轮发送 {len(sent)} 条")
            self._last_tick_log = time.time()
        return {"sent": sent, "ts": time.time()}

    async def _get_role_name(self, role_id: str) -> str:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{PERSONALITY_SERVER_URL}/api/roles", timeout=5.0)
                if resp.status_code == 200:
                    rd = resp.json()
                    if role_id in rd:
                        return rd[role_id].get("name", role_id)
        except Exception:
            pass
        return role_id


# ============================================================
# FastAPI
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info(f"💌 主动消息后端 v12.0 启动 - 端口 {PORT}")
    scheduler.start()
    yield
    await scheduler.stop()
    logger.info("主动消息后端关闭")


app = FastAPI(title="Proactive Server", version="12.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                   allow_methods=["*"], allow_headers=["*"])

scheduler = ProactiveScheduler()


async def verify_internal_token(x_internal_token: Optional[str] = Header(None)) -> bool:
    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="invalid internal token")
    return True


# ---------- 请求模型 ----------
class ActivityReport(BaseModel):
    user_id: str
    role_id: str
    intimacy: int = 30
    attachment: Optional[float] = None
    psych: Dict[str, Any] = {}


class MarkRepliedRequest(BaseModel):
    user_id: str
    role_id: Optional[str] = None


class MarkDeliveredRequest(BaseModel):
    message_id: str


class TriggerCheckRequest(BaseModel):
    user_id: Optional[str] = None  # 可选，只检查某用户


# ---------- 接口 ----------
@app.get("/health")
async def health():
    return {"status": "ok", "service": "proactive_server", "version": "12.0.0", "port": str(PORT)}


@app.post("/api/activity/report")
async def report_activity(req: ActivityReport):
    """主后端每次聊天后上报：更新最后活跃时间/亲密度/心理快照"""
    now_ts = time.time()
    psych_json = json.dumps(req.psych or {}, ensure_ascii=False)
    attachment = req.attachment if req.attachment is not None else float(req.intimacy) * 0.6
    conn = _get_db()
    try:
        conn.execute(
            "INSERT INTO activities(user_id,role_id,last_active_at,last_intimacy,last_attachment,last_psych) "
            "VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(user_id,role_id) DO UPDATE SET "
            "last_active_at=excluded.last_active_at, last_intimacy=excluded.last_intimacy, "
            "last_attachment=excluded.last_attachment, last_psych=excluded.last_psych",
            (req.user_id, req.role_id, now_ts, int(req.intimacy), float(attachment), psych_json))
        conn.commit()
    finally:
        conn.close()
    return {"success": True}


@app.post("/api/mark_replied")
async def mark_replied(req: MarkRepliedRequest):
    """用户回复了主动消息：标记最近一条未回复消息为已回复（返还额度在policy里体现为unreplied减少）"""
    conn = _get_db()
    try:
        if req.role_id:
            cur = conn.execute(
                "UPDATE proactive_messages SET replied=1, replied_at=? "
                "WHERE user_id=? AND role_id=? AND replied=0",
                (time.time(), req.user_id, req.role_id))
        else:
            cur = conn.execute(
                "UPDATE proactive_messages SET replied=1, replied_at=? "
                "WHERE user_id=? AND replied=0",
                (time.time(), req.user_id))
        conn.commit()
        return {"success": True, "updated": cur.rowcount}
    finally:
        conn.close()


# ---------- v12.0: 用户侧反馈闭环 ----------
class FeedbackRequest(BaseModel):
    user_id: str
    role_id: str
    feedback_type: str  # replied / warm_reply / cold_reply / ignored / long_offline
    session_id: Optional[str] = None

@app.post("/api/feedback")
async def user_feedback(req: FeedbackRequest):
    """v12.0: 用户侧反馈闭环 —— 用户回复/冷淡/忽略/离线时更新欲望数值。有 session_id 则同步人格后端。"""
    valid_types = ("replied", "warm_reply", "cold_reply", "ignored", "long_offline")
    if req.feedback_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"feedback_type must be one of {valid_types}")
    desire_updated = False
    desire_state = None
    if req.session_id:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    f"{PERSONALITY_SERVER_URL}/api/session/{req.session_id}/desire/{req.role_id}",
                    timeout=8.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success"):
                        current = data.get("desire", {})
                        effects = {
                            "replied": {"longing": -8, "contact_desire": -10, "share_desire": +3, "care_desire": +2, "companionship": +5},
                            "warm_reply": {"longing": -12, "contact_desire": -15, "share_desire": +8, "care_desire": +5, "companionship": +10},
                            "cold_reply": {"longing": +5, "contact_desire": +3, "share_desire": -5, "care_desire": -3, "companionship": -5},
                            "ignored": {"longing": +10, "contact_desire": +8, "share_desire": -8, "care_desire": +2, "companionship": -3},
                            "long_offline": {"longing": +15, "contact_desire": +12, "share_desire": -3, "care_desire": +8, "companionship": +5},
                        }
                        eff = effects.get(req.feedback_type, {})
                        updated = {}
                        for dim in ("longing", "contact_desire", "share_desire", "care_desire", "companionship"):
                            val = float(current.get(dim, 35)) + eff.get(dim, 0)
                            updated[dim] = round(max(0.0, min(100.0, val)), 1)
                        resp2 = await client.put(
                            f"{PERSONALITY_SERVER_URL}/api/session/{req.session_id}/desire/{req.role_id}",
                            json=updated, timeout=8.0)
                        if resp2.status_code == 200:
                            desire_updated = True
                            desire_state = updated
        except Exception as e:
            logger.warning(f"[Feedback] 同步欲望状态失败: {e}")
    return {"success": True, "feedback_type": req.feedback_type, "user_id": req.user_id,
            "role_id": req.role_id, "desire_updated": desire_updated, "desire_state": desire_state}

@app.get("/api/pending/{user_id}")
async def get_pending(user_id: str):
    """用户上线时拉取未投递的主动消息"""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT id, role_id, content, reason_type, created_at FROM proactive_messages "
            "WHERE user_id=? AND delivered=0 ORDER BY created_at ASC LIMIT 20",
            (user_id,)
        ).fetchall()
        return {"success": True, "messages": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.post("/api/mark_delivered")
async def mark_delivered(req: MarkDeliveredRequest):
    conn = _get_db()
    try:
        conn.execute("UPDATE proactive_messages SET delivered=1 WHERE id=?", (req.message_id,))
        conn.commit()
        return {"success": True}
    finally:
        conn.close()



@app.post("/api/internal/migrate_user")
async def migrate_user(req: Request, x_internal_token: Optional[str] = Header(None)):
    """将 old_user_id 的主动消息数据迁移到 new_user_id（QQ绑定账号时调用）"""
    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="invalid internal token")
    body = await req.json()
    old_uid = body.get("old_user_id", "")
    new_uid = body.get("new_user_id", "")
    if not old_uid or not new_uid or old_uid == new_uid:
        return {"success": False, "error": "参数无效"}
    conn = _get_db()
    try:
        # activities: 逐条处理，新用户已有同角色记录时保留较新的
        old_rows = conn.execute(
            "SELECT * FROM activities WHERE user_id=?", (old_uid,)
        ).fetchall()
        for row in old_rows:
            existing = conn.execute(
                "SELECT * FROM activities WHERE user_id=? AND role_id=?",
                (new_uid, row["role_id"])
            ).fetchone()
            if existing:
                # 保留 last_active_at 较新的那条
                if row["last_active_at"] > existing["last_active_at"]:
                    conn.execute(
                        "UPDATE activities SET last_active_at=?, last_intimacy=?, "
                        "last_attachment=?, last_psych=? WHERE user_id=? AND role_id=?",
                        (row["last_active_at"], row["last_intimacy"],
                         row["last_attachment"], row["last_psych"],
                         new_uid, row["role_id"]))
            else:
                conn.execute(
                    "INSERT INTO activities(user_id,role_id,last_active_at,last_intimacy,"
                    "last_attachment,last_psych) VALUES(?,?,?,?,?,?)",
                    (new_uid, row["role_id"], row["last_active_at"],
                     row["last_intimacy"], row["last_attachment"], row["last_psych"]))
        conn.execute("DELETE FROM activities WHERE user_id=?", (old_uid,))
        # proactive_messages: 直接改 user_id
        conn.execute("UPDATE proactive_messages SET user_id=? WHERE user_id=?", (new_uid, old_uid))
        # daily_counters: 合并
        old_counters = conn.execute(
            "SELECT * FROM daily_counters WHERE user_id=?", (old_uid,)
        ).fetchall()
        for c in old_counters:
            existing = conn.execute(
                "SELECT count FROM daily_counters WHERE user_id=? AND role_id=? AND date=?",
                (new_uid, c["role_id"], c["date"])
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE daily_counters SET count=count+? WHERE user_id=? AND role_id=? AND date=?",
                    (c["count"], new_uid, c["role_id"], c["date"]))
            else:
                conn.execute(
                    "INSERT INTO daily_counters(user_id,role_id,date,count) VALUES(?,?,?,?)",
                    (new_uid, c["role_id"], c["date"], c["count"]))
        conn.execute("DELETE FROM daily_counters WHERE user_id=?", (old_uid,))
        conn.commit()
        logger.info(f"主动消息数据迁移: {old_uid} -> {new_uid}")
        return {"success": True}
    except Exception as e:
        logger.error(f"主动消息数据迁移失败: {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

@app.get("/api/status/{user_id}")
async def get_status(user_id: str):
    """查看某用户各角色当前动机分数（调试/前端展示）"""
    conn = _get_db()
    try:
        rows = conn.execute("SELECT * FROM activities WHERE user_id=?", (user_id,)).fetchall()
        result = []
        cst = now_cst()
        hour = cst.hour
        now_ts = time.time()
        for row in rows:
            idle_hours = (now_ts - float(row["last_active_at"])) / 3600.0
            try:
                psych = json.loads(row["last_psych"] or "{}")
            except Exception:
                psych = {}
            attachment = float(row["last_attachment"] or psych.get("attachment", 30))
            mood = scheduler.engine.simulate_mood(row["role_id"], idle_hours)
            mot = scheduler.engine.calc(
                int(row["last_intimacy"] or 30), attachment, idle_hours, 0.0, mood, hour)
            used = scheduler.policy._today_count(conn, user_id, row["role_id"])
            unreplied = scheduler.policy._unreplied_count(conn, user_id, row["role_id"])
            result.append({
                "role_id": row["role_id"],
                "intimacy": row["last_intimacy"],
                "idle_hours": round(idle_hours, 1),
                "mood": mood,
                "motivation": mot,
                "today_sent": used,
                "unreplied": unreplied,
            })
        return {"success": True, "user_id": user_id, "roles": result, "silent_hour": scheduler.policy.is_silent_hour()}
    finally:
        conn.close()


@app.post("/api/trigger/check")
async def trigger_check(req: TriggerCheckRequest = TriggerCheckRequest()):
    """手动触发一次检查（调试用）"""
    result = await scheduler.tick()
    return {"success": True, **result}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("proactive_server:app", host="0.0.0.0", port=PORT, workers=1, log_level="info")

"""
P1 模块3：图像生成/自拍系统
基于豆包 Seedream（火山方舟）的角色自拍生成，支持亲密度判断、频率限制、角色外貌锁定。

功能：
1. 角色外貌特征管理（从YAML读取，确保每次生成是同一个人）
2. Seedream API 调用（火山方舟，和LLM共用API Key）
3. 亲密度判断（基于intimacy+心理状态+角色性格，决定是否愿意发自拍）
4. 频率限制（每用户每小时N张，角色主动发图每天M次）
5. 主动发图（与proactive_server绑定）

用法：
    from core.image_generator import get_selfie_system

    system = get_selfie_system()
    result = await system.handle_selfie_request(
        user_id="user123",
        role_id="nianqi",
        intimacy=65,
        psych_states={"trust": 70, "security": 65},
        scene="indoor",  # indoor/outdoor/bedroom/etc
    )
    # result = {"allowed": True, "image_url": "...", "message": "..."}
    # 或 {"allowed": False, "message": "拒绝理由"}
"""
import os
import time
import json
import random
import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta

import httpx
import yaml

logger = logging.getLogger("image_generator")

# ============================================================
# 配置（从环境变量读取）
# ============================================================
# Seedream 配置（和豆包LLM共用API Key）
SEEDREAM_API_KEY = os.getenv("SEEDREAM_API_KEY", os.getenv("DOUBAO_API_KEY", ""))
SEEDREAM_BASE_URL = os.getenv("SEEDREAM_BASE_URL", os.getenv("DOUBAO_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3"))
SEEDREAM_MODEL = os.getenv("SEEDREAM_MODEL", "")  # 推理接入点Endpoint ID

# 频率限制（环境变量可配置）
MAX_USER_IMAGES_PER_HOUR = int(os.getenv("MAX_USER_IMAGES_PER_HOUR", "5"))
MAX_PROACTIVE_IMAGES_PER_DAY = int(os.getenv("MAX_PROACTIVE_IMAGES_PER_DAY", "2"))

# 图片尺寸
SELFIE_SIZE = os.getenv("SELFIE_SIZE", "768x1024")  # 3:4 竖版自拍
LANDSCAPE_SIZE = os.getenv("LANDSCAPE_SIZE", "1024x768")  # 4:3 横版风景

# 数据目录
DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
IMAGE_CACHE_DIR = os.path.join(DATA_DIR, "image_cache")
RATE_LIMIT_FILE = os.path.join(DATA_DIR, "image_rate_limit.json")

# 角色配置目录
CHARACTERS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "characters")

# 确保目录存在
os.makedirs(IMAGE_CACHE_DIR, exist_ok=True)


# ============================================================
# 角色外貌特征预设（用户提供 + 自由发挥）
# ============================================================
# 每个角色的自拍阈值（亲密度达到这个值才大概率愿意发自拍）
ROLE_SELFIE_THRESHOLD = {
    "nianqi": 45,    # 温柔细腻，安全型依恋，熟悉后愿意给
    "qinghe": 55,    # 知性学姐，稳重，需要更亲密
    "jingwen": 50,   # 温柔古风少女，有点害羞
}

# 角色拒绝自拍时的理由（符合性格）
ROLE_SELFIE_REJECTIONS = {
    "nianqi": [
        "嗯...现在还不太好意思呢，等我们再熟一点好不好？",
        "哎呀，怎么突然要这个...今天状态不太好，下次吧？",
        "唔...被你这样要求有点害羞，等我准备好再给你看好不好？",
    ],
    "qinghe": [
        "真是的，怎么突然要这个...现在还不行哦。",
        "学弟/学妹不要闹，学姐今天没打扮，改天吧。",
        "嗯...这个要求有点突然，等我们关系再好一点再说吧。",
    ],
    "jingwen": [
        "这...太害羞了，下次好不好？",
        "不行啦...被你看到会很不好意思的。",
        "嗯...今天不太方便，改天我主动发给你好不好？",
    ],
}

# 角色同意自拍时的开场白
ROLE_SELFIE_AGREEMENTS = {
    "nianqi": [
        "好吧...只给你看一眼哦。",
        "嗯...那你不许笑我。",
        "好啦好啦，给你看就是了。",
    ],
    "qinghe": [
        "真是拿你没办法呢，只此一次哦。",
        "好吧...谁让是你呢。",
        "嗯...那就给你看看吧。",
    ],
    "jingwen": [
        "那...那你要好好收着哦。",
        "好吧...只给你一个人看。",
        "嗯...你要答应我不许给别人看。",
    ],
}


# ============================================================
# 角色外貌管理
# ============================================================
class AppearanceManager:
    """角色外貌特征管理，从YAML读取appearance字段，构建生图prompt。"""

    def __init__(self, characters_dir: str = CHARACTERS_DIR):
        self.characters_dir = characters_dir
        self._appearances: Dict[str, Dict] = {}
        self._load_all()

    def _load_all(self):
        """加载所有角色的外貌配置。"""
        if not os.path.exists(self.characters_dir):
            logger.warning(f"角色目录不存在: {self.characters_dir}")
            return

        for filename in os.listdir(self.characters_dir):
            if not filename.endswith(".yaml"):
                continue
            role_id = filename.replace(".yaml", "")
            filepath = os.path.join(self.characters_dir, filename)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                appearance = data.get("appearance", {})
                if appearance:
                    self._appearances[role_id] = appearance
                    logger.info(f"已加载角色外貌: {role_id}")
            except Exception as e:
                logger.warning(f"加载角色外貌失败 {role_id}: {e}")

    def get_appearance(self, role_id: str) -> Optional[Dict]:
        """获取角色外貌配置。"""
        return self._appearances.get(role_id)

    def build_appearance_prompt(self, role_id: str) -> str:
        """
        构建角色外貌描述prompt（用于生图时锁定角色特征）。
        确保每次生成的是同一个人。
        """
        appearance = self.get_appearance(role_id)
        if not appearance:
            logger.warning(f"角色 {role_id} 没有外貌配置，使用通用描述")
            return "beautiful young asian woman, detailed face"

        parts = []

        # 脸型
        if appearance.get("face"):
            parts.append(f"face: {appearance['face']}")

        # 眼睛
        if appearance.get("eyes"):
            parts.append(f"eyes: {appearance['eyes']}")

        # 眉毛
        if appearance.get("eyebrows"):
            parts.append(f"eyebrows: {appearance['eyebrows']}")

        # 鼻子
        if appearance.get("nose"):
            parts.append(f"nose: {appearance['nose']}")

        # 嘴唇
        if appearance.get("lips"):
            parts.append(f"lips: {appearance['lips']}")

        # 肤色
        if appearance.get("skin"):
            parts.append(f"skin: {appearance['skin']}")

        # 头发
        hair_parts = []
        if appearance.get("hair_color"):
            hair_parts.append(appearance["hair_color"])
        if appearance.get("hair_style"):
            hair_parts.append(appearance["hair_style"])
        if appearance.get("hair_length"):
            hair_parts.append(appearance["hair_length"])
        if hair_parts:
            parts.append(f"hair: {' '.join(hair_parts)}")

        # 身材
        body_parts = []
        if appearance.get("height"):
            body_parts.append(appearance["height"])
        if appearance.get("body"):
            body_parts.append(appearance["body"])
        if body_parts:
            parts.append(f"body: {' '.join(body_parts)}")

        # 整体风格
        if appearance.get("style"):
            parts.append(f"style: {appearance['style']}")

        return ", ".join(parts)

    def build_full_prompt(
        self,
        role_id: str,
        scene: str = "indoor",
        expression: str = "gentle smile",
        clothing: str = "casual",
        extra: str = "",
    ) -> str:
        """
        构建完整的生图prompt：角色外貌 + 场景 + 表情 + 服装 + 风格。

        Args:
            role_id: 角色ID
            scene: 场景（indoor/outdoor/bedroom/cafe/park等）
            expression: 表情（gentle smile/shy/happy等）
            clothing: 服装（casual/dress/homewear等）
            extra: 额外描述

        Returns:
            完整的生图prompt（英文，Seedream对英文prompt响应更好）
        """
        appearance = self.build_appearance_prompt(role_id)

        # 场景描述映射
        scene_map = {
            "indoor": "cozy indoor room, warm lighting",
            "outdoor": "outdoor, natural lighting",
            "bedroom": "bedroom, soft warm lighting",
            "cafe": "cafe background, warm atmosphere",
            "park": "park, greenery, natural light",
            "morning": "morning sunlight, just woke up",
            "night": "night, soft room lighting",
        }
        scene_desc = scene_map.get(scene, f"{scene} background")

        # 自拍构图
        composition = "selfie angle, looking at camera, upper body shot"

        # 质量描述
        quality = "high quality, detailed, 4K, anime style, beautiful detailed face, soft lighting"

        # 组合prompt
        prompt_parts = [
            f"1girl, {appearance}",
            composition,
            f"expression: {expression}",
            f"wearing {clothing} clothes",
            scene_desc,
            quality,
        ]
        if extra:
            prompt_parts.append(extra)

        full_prompt = ", ".join(prompt_parts)
        logger.debug(f"[ImageGen] 生成prompt: {full_prompt[:200]}...")
        return full_prompt


# ============================================================
# Seedream API 客户端
# ============================================================
class SeedreamClient:
    """豆包 Seedream 图像生成API客户端（火山方舟）。"""

    def __init__(
        self,
        api_key: str = SEEDREAM_API_KEY,
        base_url: str = SEEDREAM_BASE_URL,
        model: str = SEEDREAM_MODEL,
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.available = bool(api_key and model)
        if not self.available:
            logger.warning("[Seedream] 未配置 API Key 或 Model，图像生成不可用")

    async def generate(
        self,
        prompt: str,
        size: str = "1024x1024",
        negative_prompt: str = "",
        timeout: float = 60.0,
    ) -> Optional[str]:
        """
        生成图片。

        Args:
            prompt: 生图prompt
            size: 图片尺寸（如 1024x1024, 768x1024）
            negative_prompt: 反向提示词
            timeout: 超时时间

        Returns:
            图片URL，失败返回None
        """
        if not self.available:
            logger.warning("[Seedream] 不可用，跳过生成")
            return None

        payload = {
            "model": self.model,
            "prompt": prompt,
            "size": size,
            "response_format": "url",
            "watermark": False,
        }
        if negative_prompt:
            payload["negative_prompt"] = negative_prompt

        t0 = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    f"{self.base_url}/images/generations",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )

            dur = time.perf_counter() - t0

            if resp.status_code == 200:
                data = resp.json()
                image_url = data.get("data", [{}])[0].get("url", "")
                if image_url:
                    logger.info(f"[Seedream] 生成成功 耗时={dur:.2f}s size={size} url={image_url[:80]}...")
                    return image_url
                else:
                    logger.warning(f"[Seedream] 响应中没有图片URL: {json.dumps(data)[:200]}")
                    return None
            else:
                logger.warning(f"[Seedream] HTTP{resp.status_code}: {resp.text[:300]}")
                return None

        except httpx.TimeoutException:
            logger.warning(f"[Seedream] 超时 ({timeout}s)")
            return None
        except Exception as e:
            logger.warning(f"[Seedream] 异常: {type(e).__name__}: {e}")
            return None


# ============================================================
# 亲密度判断器
# ============================================================
class IntimacyJudger:
    """
    基于亲密度+心理状态+角色性格，判断角色是否愿意发自拍。

    判断逻辑：
    1. 亲密度 < 阈值-10：直接拒绝
    2. 阈值-10 <= 亲密度 < 阈值：30%概率同意（犹豫）
    3. 亲密度 >= 阈值：80%概率同意
    4. 心理状态加成：信任高+5%，安全感高+5%
    5. 角色害羞倾向：shy高的角色拒绝概率+10%
    """

    def __init__(self):
        self.thresholds = ROLE_SELFIE_THRESHOLD

    def judge(
        self,
        role_id: str,
        intimacy: int,
        psych_states: Optional[Dict[str, float]] = None,
    ) -> Tuple[bool, str]:
        """
        判断是否愿意发自拍。

        Args:
            role_id: 角色ID
            intimacy: 亲密度（0-100）
            psych_states: 心理状态（trust, security, attachment等）

        Returns:
            (是否同意, 理由/开场白)
        """
        threshold = self.thresholds.get(role_id, 50)
        psych_states = psych_states or {}

        # 基础概率
        if intimacy < threshold - 10:
            base_prob = 0.0  # 太陌生，直接拒绝
        elif intimacy < threshold:
            base_prob = 0.3  # 犹豫，30%概率
        else:
            base_prob = 0.8  # 熟悉，80%概率

        # 心理状态加成
        trust = psych_states.get("trust", 50)
        security = psych_states.get("security", 50)
        bonus = 0.0
        if trust >= 70:
            bonus += 0.05
        if security >= 70:
            bonus += 0.05

        # 最终概率
        final_prob = min(0.95, base_prob + bonus)

        # 随机判断
        allowed = random.random() < final_prob

        if allowed:
            message = random.choice(ROLE_SELFIE_AGREEMENTS.get(role_id, ["好，给你看。"]))
            logger.info(
                f"[SelfieJudge] {role_id} 同意发自拍 "
                f"intimacy={intimacy} threshold={threshold} prob={final_prob:.2f}"
            )
        else:
            message = random.choice(ROLE_SELFIE_REJECTIONS.get(role_id, ["现在不行。"]))
            logger.info(
                f"[SelfieJudge] {role_id} 拒绝发自拍 "
                f"intimacy={intimacy} threshold={threshold} prob={final_prob:.2f}"
            )

        return allowed, message


# ============================================================
# 频率限制器
# ============================================================
class RateLimiter:
    """
    图像生成频率限制。
    - 每用户每小时最多 MAX_USER_IMAGES_PER_HOUR 张
    - 每角色每天主动发图最多 MAX_PROACTIVE_IMAGES_PER_DAY 次
    """

    def __init__(self, rate_limit_file: str = RATE_LIMIT_FILE):
        self.rate_limit_file = rate_limit_file
        self._data: Dict[str, Any] = self._load()

    def _load(self) -> Dict:
        """加载频率限制数据。"""
        if os.path.exists(self.rate_limit_file):
            try:
                with open(self.rate_limit_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载频率限制文件失败: {e}")
        return {"user_hourly": {}, "proactive_daily": {}}

    def _save(self):
        """保存频率限制数据。"""
        try:
            with open(self.rate_limit_file, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存频率限制文件失败: {e}")

    def _cleanup_old(self):
        """清理过期的记录。"""
        now = time.time()
        one_hour_ago = now - 3600
        one_day_ago = now - 86400

        # 清理每小时用户记录
        self._data["user_hourly"] = {
            k: v for k, v in self._data.get("user_hourly", {}).items()
            if v.get("timestamp", 0) > one_hour_ago
        }

        # 清理每天主动发图记录
        self._data["proactive_daily"] = {
            k: v for k, v in self._data.get("proactive_daily", {}).items()
            if v.get("date") == datetime.now().strftime("%Y-%m-%d")
        }

    def check_user_limit(self, user_id: str) -> Tuple[bool, str]:
        """
        检查用户是否超过每小时图片限制。

        Returns:
            (是否允许, 理由)
        """
        self._cleanup_old()
        key = f"{user_id}"
        record = self._data["user_hourly"].get(key, {"count": 0, "timestamp": time.time()})

        if record["count"] >= MAX_USER_IMAGES_PER_HOUR:
            msg = f"每小时最多生成{MAX_USER_IMAGES_PER_HOUR}张图片，请稍后再试"
            logger.info(f"[RateLimit] 用户 {user_id} 超过限制: {record['count']}/{MAX_USER_IMAGES_PER_HOUR}")
            return False, msg

        return True, ""

    def increment_user(self, user_id: str):
        """记录用户生成了一张图片。"""
        key = f"{user_id}"
        record = self._data["user_hourly"].get(key, {"count": 0, "timestamp": time.time()})
        record["count"] += 1
        record["timestamp"] = time.time()
        self._data["user_hourly"][key] = record
        self._save()

    def check_proactive_limit(self, role_id: str) -> Tuple[bool, str]:
        """
        检查角色是否超过每天主动发图限制。

        Returns:
            (是否允许, 理由)
        """
        self._cleanup_old()
        today = datetime.now().strftime("%Y-%m-%d")
        key = f"{role_id}_{today}"
        record = self._data["proactive_daily"].get(key, {"count": 0, "date": today})

        if record["count"] >= MAX_PROACTIVE_IMAGES_PER_DAY:
            msg = f"每天最多主动发{MAX_PROACTIVE_IMAGES_PER_DAY}张图片"
            logger.info(f"[RateLimit] 角色 {role_id} 主动发图超过限制: {record['count']}/{MAX_PROACTIVE_IMAGES_PER_DAY}")
            return False, msg

        return True, ""

    def increment_proactive(self, role_id: str):
        """记录角色主动发了一张图片。"""
        today = datetime.now().strftime("%Y-%m-%d")
        key = f"{role_id}_{today}"
        record = self._data["proactive_daily"].get(key, {"count": 0, "date": today})
        record["count"] += 1
        self._data["proactive_daily"][key] = record
        self._save()


# ============================================================
# 自拍系统整合
# ============================================================
class SelfieSystem:
    """自拍系统：整合外貌管理、API调用、亲密度判断、频率限制。"""

    def __init__(self):
        self.appearance = AppearanceManager()
        self.client = SeedreamClient()
        self.judger = IntimacyJudger()
        self.rate_limiter = RateLimiter()

    async def handle_selfie_request(
        self,
        user_id: str,
        role_id: str,
        intimacy: int,
        psych_states: Optional[Dict[str, float]] = None,
        scene: str = "indoor",
        expression: str = "gentle smile",
        clothing: str = "casual",
    ) -> Dict[str, Any]:
        """
        处理用户的自拍请求。

        Args:
            user_id: 用户ID
            role_id: 角色ID
            intimacy: 亲密度（0-100）
            psych_states: 心理状态
            scene: 场景
            expression: 表情
            clothing: 服装

        Returns:
            {
                "allowed": bool,
                "message": str,  # 同意时的开场白或拒绝理由
                "image_url": str,  # 同意时的图片URL
                "error": str,  # 错误信息
            }
        """
        # 1. 检查频率限制
        allowed, reason = self.rate_limiter.check_user_limit(user_id)
        if not allowed:
            return {"allowed": False, "message": reason, "image_url": "", "error": "rate_limit"}

        # 2. 亲密度判断
        is_allowed, message = self.judger.judge(role_id, intimacy, psych_states)
        if not is_allowed:
            return {"allowed": False, "message": message, "image_url": "", "error": "intimacy_too_low"}

        # 3. 检查API可用性
        if not self.client.available:
            return {
                "allowed": False,
                "message": "图片生成功能暂时不可用，稍后再试吧。",
                "image_url": "",
                "error": "api_unavailable",
            }

        # 4. 构建prompt并生成
        prompt = self.appearance.build_full_prompt(
            role_id=role_id,
            scene=scene,
            expression=expression,
            clothing=clothing,
        )

        # 反向提示词：确保动漫风格，排除写实
        negative_prompt = "realistic, photo, 3d render, low quality, blurry, deformed, ugly"

        image_url = await self.client.generate(
            prompt=prompt,
            size=SELFIE_SIZE,
            negative_prompt=negative_prompt,
        )

        if not image_url:
            return {
                "allowed": False,
                "message": "图片生成失败了，稍后再试好不好？",
                "image_url": "",
                "error": "generation_failed",
            }

        # 5. 记录频率限制
        self.rate_limiter.increment_user(user_id)

        return {
            "allowed": True,
            "message": message,
            "image_url": image_url,
            "error": "",
        }

    async def generate_proactive_image(
        self,
        user_id: str,
        role_id: str,
        intimacy: int,
        scene: str = "indoor",
        expression: str = "happy",
        clothing: str = "casual",
    ) -> Dict[str, Any]:
        """
        生成主动发图（与proactive_server绑定）。
        主动发图不需要亲密度判断（角色主动发的），但有每天次数限制。

        Args:
            user_id: 用户ID
            role_id: 角色ID
            intimacy: 亲密度（用于决定图片内容的亲密程度）
            scene: 场景
            expression: 表情
            clothing: 服装

        Returns:
            同 handle_selfie_request
        """
        # 1. 检查主动发图频率限制
        allowed, reason = self.rate_limiter.check_proactive_limit(role_id)
        if not allowed:
            return {"allowed": False, "message": "", "image_url": "", "error": "proactive_limit"}

        # 2. 检查API可用性
        if not self.client.available:
            return {"allowed": False, "message": "", "image_url": "", "error": "api_unavailable"}

        # 3. 根据亲密度调整内容（高亲密可以更随意）
        if intimacy >= 70:
            expression = expression or "happy"
            clothing = clothing or "homewear"
        else:
            expression = expression or "gentle smile"
            clothing = clothing or "casual"

        # 4. 构建prompt并生成
        prompt = self.appearance.build_full_prompt(
            role_id=role_id,
            scene=scene,
            expression=expression,
            clothing=clothing,
        )

        negative_prompt = "realistic, photo, 3d render, low quality, blurry, deformed, ugly"

        image_url = await self.client.generate(
            prompt=prompt,
            size=SELFIE_SIZE,
            negative_prompt=negative_prompt,
        )

        if not image_url:
            return {"allowed": False, "message": "", "image_url": "", "error": "generation_failed"}

        # 5. 记录频率限制
        self.rate_limiter.increment_proactive(role_id)

        return {
            "allowed": True,
            "message": "",
            "image_url": image_url,
            "error": "",
        }

    def get_status(self) -> Dict[str, Any]:
        """获取系统状态（用于调试）。"""
        return {
            "api_available": self.client.available,
            "model": self.client.model,
            "roles_with_appearance": list(self.appearance._appearances.keys()),
            "selfie_thresholds": ROLE_SELFIE_THRESHOLD,
            "rate_limit": {
                "max_user_per_hour": MAX_USER_IMAGES_PER_HOUR,
                "max_proactive_per_day": MAX_PROACTIVE_IMAGES_PER_DAY,
            },
            "sizes": {
                "selfie": SELFIE_SIZE,
                "landscape": LANDSCAPE_SIZE,
            },
        }


# ============================================================
# 全局单例
# ============================================================
_selfie_system: Optional[SelfieSystem] = None


def get_selfie_system() -> SelfieSystem:
    """获取自拍系统单例。"""
    global _selfie_system
    if _selfie_system is None:
        _selfie_system = SelfieSystem()
    return _selfie_system

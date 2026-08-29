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

# 图片尺寸（P3修复：火山云Seedream 5.0要求至少368万像素，768x1024只有78万像素会400错误）
SELFIE_SIZE = os.getenv("SELFIE_SIZE", "1728x2304")  # 3:4 竖版自拍，约398万像素
LANDSCAPE_SIZE = os.getenv("LANDSCAPE_SIZE", "2304x1728")  # 4:3 横版风景，约398万像素

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

# ============================================================
# P2 新增：两种自拍模式 + 上下文场景识别
# 借鉴 Clawra：Mirror（镜子自拍/全身/服装）+ Direct（直接自拍/特写/场景）
# ============================================================
from enum import Enum

class SelfieMode(str, Enum):
    """自拍模式"""
    MIRROR = "mirror"  # 镜子自拍，适合全身/服装展示
    DIRECT = "direct"  # 直接自拍，适合特写/场景/表情
    AUTO = "auto"      # 自动检测

# Mirror模式关键词（服装/全身/镜子）
MIRROR_KEYWORDS = [
    "outfit", "wearing", "clothes", "dress", "suit", "fashion",
    "full-body", "mirror", "reflection",
    "全身", "穿搭", "衣服", "裙子", "套装", "镜子", "换装", "造型",
    "穿什么", "今天穿", "搭配",
]

# Direct模式关键词（场景/特写/表情）
DIRECT_KEYWORDS = [
    "cafe", "restaurant", "beach", "park", "city", "street",
    "close-up", "portrait", "face", "eyes", "smile", "selfie",
    "咖啡馆", "餐厅", "饭店", "海滩", "海边", "公园", "街上", "城市",
    "特写", "脸", "眼睛", "笑", "自拍", "照片", "相片",
    "在干嘛", "在哪", "在做什么", "看看你", "发张",
]

# 场景关键词映射（中文场景 -> 英文描述）
SCENE_KEYWORDS = {
    "咖啡馆": "cozy cafe with warm lighting",
    "咖啡店": "cozy cafe with warm lighting",
    "咖啡厅": "cozy cafe with warm lighting",
    "餐厅": "nice restaurant",
    "饭店": "nice restaurant",
    "食堂": "university cafeteria",
    "海滩": "sunny beach",
    "海边": "sunny beach",
    "公园": "peaceful park with greenery",
    "街上": "busy city street",
    "城市": "city street at night",
    "家里": "cozy home living room",
    "房间": "cozy bedroom",
    "卧室": "cozy bedroom",
    "宿舍": "cozy dorm room",
    "学校": "university campus",
    "图书馆": "quiet library",
    "健身房": "gym",
    "超市": "supermarket",
    "商场": "shopping mall",
    "电影院": "cinema",
    "早上": "morning sunlight, just woke up",
    "早晨": "morning sunlight, just woke up",
    "晚上": "night, soft room lighting",
    "夜晚": "night, soft room lighting",
    "睡前": "bedroom, night, cozy lighting",
    "刚起床": "bedroom, morning sunlight, just woke up",
}

# 服装关键词映射
CLOTHING_KEYWORDS = {
    "裙子": "cute dress",
    "连衣裙": "elegant dress",
    "卫衣": "casual hoodie",
    "校服": "school uniform",
    "制服": "uniform",
    "T恤": "casual t-shirt",
    "毛衣": "cozy sweater",
    "外套": "stylish jacket",
    "大衣": "warm coat",
    "睡衣": "cute pajamas",
    "家居服": "cozy homewear",
    "运动服": "sportswear",
    "牛仔裤": "jeans and casual top",
    "短裤": "shorts and casual top",
    "汉服": "traditional hanfu",
    "洛丽塔": "lolita dress",
    "JK": "JK uniform",
    "女仆": "maid outfit",
}

# 表情关键词映射
EXPRESSION_KEYWORDS = {
    "笑": "happy smile",
    "开心": "happy smile",
    "高兴": "happy smile",
    "害羞": "shy blush",
    "羞涩": "shy blush",
    "脸红": "shy blush",
    "生气": "slightly pouting",
    "傲娇": "tsundere expression, slight blush",
    "难过": "sad expression",
    "伤心": "sad expression",
    "困": "sleepy expression",
    "累": "tired expression",
    "惊讶": "surprised expression",
    "调皮": "playful wink",
    "眨眼": "playful wink",
    "嘟嘴": "pouting",
    "委屈": "pouting, teary eyes",
}


def detect_selfie_mode(message: str) -> SelfieMode:
    """
    从用户消息中自动检测自拍模式。
    借鉴 Clawra 的关键词检测逻辑。
    P2 修复：先检测Mirror（服装/全身/穿搭），再检测Direct（特写/场景）。
    因为"穿裙子"、"穿搭"等服装相关请求应该用Mirror模式展示全身。
    """
    msg_lower = message.lower()
    
    # 先检测 Mirror 关键词（服装/全身/穿搭，优先级更高）
    for kw in MIRROR_KEYWORDS:
        if kw.lower() in msg_lower:
            return SelfieMode.MIRROR
    
    # 再检测 Direct 关键词（特写/场景/表情）
    for kw in DIRECT_KEYWORDS:
        if kw.lower() in msg_lower:
            return SelfieMode.DIRECT
    
    # 默认 Direct 模式（大多数自拍请求都是特写）
    return SelfieMode.DIRECT


def extract_scene(message: str) -> str:
    """从用户消息中提取场景描述。"""
    for kw, scene_desc in SCENE_KEYWORDS.items():
        if kw in message:
            return scene_desc
    return ""  # 没检测到特定场景


def extract_clothing(message: str) -> str:
    """从用户消息中提取服装描述。"""
    for kw, clothing_desc in CLOTHING_KEYWORDS.items():
        if kw in message:
            return clothing_desc
    return ""  # 没检测到特定服装


def extract_expression(message: str) -> str:
    """从用户消息中提取表情描述。"""
    for kw, expr_desc in EXPRESSION_KEYWORDS.items():
        if kw in message:
            return expr_desc
    return ""  # 没检测到特定表情


def is_selfie_request(message: str) -> bool:
    """
    判断用户消息是否是自拍请求。
    触发词：发自拍、发张照片、看看你、你长什么样、发张照、自拍、照片等
    """
    selfie_triggers = [
        "发自拍", "发张自拍", "发张照片", "发张照", "发个照片", "发个自拍",
        "看看你", "看你", "你长什么样", "你长啥样", "你的照片", "你的自拍",
        "自拍", "照片", "相片", "照骗",
        "发图", "发张图", "发个图",
        "现在在干嘛", "你在干嘛", "你在哪", "你在做什么",
    ]
    msg = message.lower()
    for trigger in selfie_triggers:
        if trigger in msg:
            return True
    return False

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
        mode: SelfieMode = SelfieMode.DIRECT,
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

        # 质量描述
        quality = "high quality, detailed, 4K, anime style, beautiful detailed face, soft lighting, masterpiece"

        if mode == SelfieMode.MIRROR:
            # Mirror模式：全身照，镜子自拍，服装展示
            # 借鉴 Clawra: "make a pic of this person, but ... the person is taking a mirror selfie"
            composition = "full body shot, taking a mirror selfie, mirror reflection, holding phone in front of mirror"
            prompt_parts = [
                f"1girl, {appearance}",
                composition,
                f"expression: {expression}",
                f"wearing {clothing}",
                f"at {scene_desc}",
                quality,
            ]
        else:
            # Direct模式：特写，直接自拍，眼神接触
            # 借鉴 Clawra: "a close-up selfie taken by herself at ... direct eye contact with camera"
            composition = (
                "close-up selfie, direct eye contact with camera, looking straight into lens, "
                "eyes centered and clearly visible, phone held at arm's length, face fully visible, "
                "upper body shot"
            )
            prompt_parts = [
                f"1girl, {appearance}",
                composition,
                f"expression: {expression}",
                f"wearing {clothing}",
                f"at {scene_desc}",
                quality,
            ]

        if extra:
            prompt_parts.append(extra)

        full_prompt = ", ".join(prompt_parts)
        logger.debug(f"[ImageGen][{mode.value}] 生成prompt: {full_prompt[:200]}...")
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
            # P3 修复：打印请求参数，方便排查400错误
            logger.info(f"[Seedream] 请求参数: model={self.model} size={size} "
                       f"response_format={payload.get('response_format')} watermark={payload.get('watermark')}")
            
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
                    logger.warning(f"[Seedream] 响应中没有图片URL: {json.dumps(data)[:500]}")
                    return None
            else:
                # P3 修复：打印完整响应体，方便排查400错误
                logger.error(f"[Seedream] HTTP{resp.status_code} 完整响应: {resp.text[:1000]}")
                
                # P3 修复：400错误时自动降级size到1024x1024重试一次
                if resp.status_code == 400 and size != "2048x2048":
                    logger.warning(f"[Seedream] 400错误，降级size到2048x2048重试...")
                    fallback_payload = {**payload, "size": "2048x2048"}
                    try:
                        async with httpx.AsyncClient(timeout=timeout) as client:
                            resp2 = await client.post(
                                f"{self.base_url}/images/generations",
                                headers={
                                    "Authorization": f"Bearer {self.api_key}",
                                    "Content-Type": "application/json",
                                },
                                json=fallback_payload,
                            )
                        if resp2.status_code == 200:
                            data2 = resp2.json()
                            image_url2 = data2.get("data", [{}])[0].get("url", "")
                            if image_url2:
                                logger.info(f"[Seedream] 降级重试成功 size=2048x2048 url={image_url2[:80]}...")
                                return image_url2
                        else:
                            logger.error(f"[Seedream] 降级重试也失败 HTTP{resp2.status_code}: {resp2.text[:500]}")
                    except Exception as e2:
                        logger.warning(f"[Seedream] 降级重试异常: {type(e2).__name__}: {e2}")
                
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

    async def handle_selfie_from_message(
        self,
        user_id: str,
        role_id: str,
        message: str,
        intimacy: int,
        psych_states: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        P2 新增：从用户原始消息直接处理自拍请求。
        自动检测自拍模式、提取场景/服装/表情，然后生成图片。

        Args:
            user_id: 用户ID
            role_id: 角色ID
            message: 用户原始消息
            intimacy: 亲密度
            psych_states: 心理状态

        Returns:
            同 handle_selfie_request
        """
        # 1. 自动检测自拍模式
        mode = detect_selfie_mode(message)
        logger.info(f"[SelfieP2] 自动检测模式: {mode.value} (消息: {message[:30]}...)")

        # 2. 从消息中提取场景、服装、表情
        scene = extract_scene(message) or "indoor"
        clothing = extract_clothing(message) or "casual"
        expression = extract_expression(message) or "gentle smile"

        logger.info(f"[SelfieP2] 提取上下文: scene={scene}, clothing={clothing}, expression={expression}")

        # 3. 调用标准处理流程
        return await self.handle_selfie_request(
            user_id=user_id,
            role_id=role_id,
            intimacy=intimacy,
            psych_states=psych_states,
            scene=scene,
            expression=expression,
            clothing=clothing,
            mode=mode,
        )

    async def handle_selfie_request(
        self,
        user_id: str,
        role_id: str,
        intimacy: int,
        psych_states: Optional[Dict[str, float]] = None,
        scene: str = "indoor",
        expression: str = "gentle smile",
        clothing: str = "casual",
        mode: SelfieMode = SelfieMode.DIRECT,
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

        # 4. 构建prompt并生成（P2：支持两种自拍模式）
        prompt = self.appearance.build_full_prompt(
            role_id=role_id,
            scene=scene,
            expression=expression,
            clothing=clothing,
            mode=mode,
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

        # 4. 构建prompt并生成（P2：主动发图默认Direct模式）
        prompt = self.appearance.build_full_prompt(
            role_id=role_id,
            scene=scene,
            expression=expression,
            clothing=clothing,
            mode=SelfieMode.DIRECT,
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
            "p2_features": {
                "dual_modes": True,
                "context_detection": True,
                "modes": ["mirror (全身/服装)", "direct (特写/场景)"],
                "scene_keywords": len(SCENE_KEYWORDS),
                "clothing_keywords": len(CLOTHING_KEYWORDS),
                "expression_keywords": len(EXPRESSION_KEYWORDS),
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

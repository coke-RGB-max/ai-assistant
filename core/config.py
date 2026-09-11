"""
人格服务器配置模块
所有环境变量和配置常量集中管理，便于维护和修改。
"""
import os

# ============================================================
# 配置
# ============================================================
DOUBAO_API_KEY = os.getenv("DOUBAO_API_KEY", "")
DOUBAO_BASE_URL = os.getenv("DOUBAO_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3")
DOUBAO_MODEL = os.getenv("DOUBAO_MODEL", "")

# v10.0: Kimi 联网搜索配置（A线主搜）
KIMI_API_KEY = os.getenv("KIMI_API_KEY", "")
KIMI_BASE_URL = os.getenv("KIMI_BASE_URL", "https://api.moonshot.cn/v1")
KIMI_MODEL = os.getenv("KIMI_MODEL", "moonshot-v1-8k")
KIMI_SEARCH_MODEL = os.getenv("KIMI_SEARCH_MODEL", "moonshot-v1-search")  # 支持联网搜索的模型

# ============================================================
# v13.0: 火山「豆包搜索」独立联网搜索（Kimi 失败时的兜底 / B线）
# 这是一个独立的搜索服务（open.feedcoopapi.com），不是方舟自带 web_search 插件。
# 注意：此 API Key 要在火山引擎「联网搜索控制台」单独创建，
#       与方舟大模型的 DOUBAO_API_KEY 不通用。
# 免费额度：每个火山账号每月 500 次，每月 1 号重置，默认 5 QPS。
# ============================================================
DOUBAO_SEARCH_API_KEY = os.getenv("DOUBAO_SEARCH_API_KEY", "")
DOUBAO_SEARCH_URL = os.getenv(
    "DOUBAO_SEARCH_URL",
    "https://open.feedcoopapi.com/search_api/web_search",
)
DOUBAO_SEARCH_TYPE = os.getenv("DOUBAO_SEARCH_TYPE", "web")
# web=返回站点列表(自己拼带来源文本，精细可控)；web_summary=额外给一段 LLM 总结
DOUBAO_SEARCH_COUNT = int(os.getenv("DOUBAO_SEARCH_COUNT", "8"))
DOUBAO_SEARCH_TIMERANGE = os.getenv("DOUBAO_SEARCH_TIMERANGE", "")
# 空=不限时间；可选 OneDay / OneWeek / OneMonth / OneYear
DOUBAO_SEARCH_TIMEOUT = float(os.getenv("DOUBAO_SEARCH_TIMEOUT", "10.0"))

# Kimi 搜索超时（秒）：超时即判失败，立刻走豆包兜底，避免用户干等
KIMI_SEARCH_TIMEOUT = float(os.getenv("KIMI_SEARCH_TIMEOUT", "10.0"))

# 成本护栏：本容器每日联网搜索总次数上限（Kimi + 豆包合计），
# 防止异常刷量 / 死循环烧钱。到顶后当日不再发起任何搜索，直接走离线 B 线。
SEARCH_DAILY_LIMIT = int(os.getenv("SEARCH_DAILY_LIMIT", "50"))

# 注意：不要使用通用的 PORT 环境变量！云端平台（Sealos/Render/Railway等）
# 通常会注入 PORT=8080 作为外部访问端口，会导致人格后端错误监听 8080 而非 8002。
# 各后端服务使用各自专用的环境变量，与 voice_server(VOICE_PORT)/proactive(PROACTIVE_PORT) 保持一致。
PORT = int(os.getenv("PERSONALITY_PORT", "8002"))
DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(DATA_DIR, "personality_sessions.db")
RATE_LIMIT_PER_MINUTE = 30
SESSION_TIMEOUT_SECONDS = 7 * 24 * 3600

CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")
CORS_CREDENTIALS = os.getenv("CORS_CREDENTIALS", "false").lower() == "true"

LLM_ANALYSIS_MIN_LEN = 25
LLM_HIGH_VALUE_KEYWORDS = ["喜欢","爱你","告白","分手","再见","永远","承诺","约定","生日",
    "对不起","原谅","滚","废物","恶心","讨厌我","不在乎我","不懂我","算了","不麻烦你",
    "随便你","别的女生","别的男生","前女友","前男友","累","难受","生病","哭","孤独",
    "撑不住","压力大","你是不是烦","你根本不","你从来没有","我一个人也行"]

# v10.0: 知识路由阈值
KNOWLEDGE_ROUTER_MIN_LEN = 8  # 太短的消息不走知识路由
KNOWLEDGE_ROUTER_ENABLED = os.getenv("KNOWLEDGE_ROUTER_ENABLED", "true").lower() == "true"
# v11.0: 新增配置
ROLE_CONCURRENCY_LOCK = True  # 角色级并发锁，防止同角色多会话状态冲突
MEMORY_DECAY_INTERVAL_HOURS = 24  # 记忆衰减定时任务间隔（小时）
MAX_PROMPT_TOKENS = 1200  # 系统prompt目标上限（字符数近似）
USER_PROFILE_EXTRACT_INTERVAL = 5  # 每N轮对话提取一次用户画像
QUALITY_CHECK_ENABLED = True  # 对话质量轻量检测
EMOTION_CONTAGION_DECAY = 0.3  # 群聊情绪传染衰减系数
MEMORY_ARCHIVE_THRESHOLD = 500  # 单session记忆超过此数触发归档

# ============================================================
# v12.1: LLM心理状态校准层（方案B：本地公式算基础值 + LLM输出修正系数）
# 所有参数均通过环境变量配置，适配 Railway 等云端部署
# ============================================================
LLM_CALIBRATION_ENABLED = os.getenv("LLM_CALIBRATION_ENABLED", "true").lower() == "true"
LLM_CALIBRATION_MODEL = os.getenv("LLM_CALIBRATION_MODEL", "doubao-seed-2.0-mini")
# API Key 复用 DOUBAO_API_KEY（与主生成链路共用），无需额外配置
LLM_CALIBRATION_BASE_URL = os.getenv("LLM_CALIBRATION_BASE_URL",
    os.getenv("DOUBAO_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3"))
LLM_CALIBRATION_MIN_LEN = int(os.getenv("LLM_CALIBRATION_MIN_LEN", "30"))   # 短消息不触发
LLM_CALIBRATION_COOLDOWN = float(os.getenv("LLM_CALIBRATION_COOLDOWN", "2.0"))  # 两次校准最小间隔(秒)
LLM_CALIBRATION_TIMEOUT = float(os.getenv("LLM_CALIBRATION_TIMEOUT", "15.0"))    # API超时(秒)
LLM_CALIBRATION_MAX_TOKENS = int(os.getenv("LLM_CALIBRATION_MAX_TOKENS", "100")) # 输出token上限
# 触发校准的关键词（含这些词的短消息也会触发）
LLM_CALIBRATION_TRIGGER_KEYWORDS = [
    "讽刺","反话","开玩笑","呵呵","哦","随便","算了","别这样",
    "别的女生","别的男生","前女友","前男友","前任","她比你","他比你",
    "你是不是","你根本","你从来","无所谓","都行","怪我","我的错",
]



# 配置校验：启动时检查必要的配置
def validate_config() -> list:
    """检查必要配置，返回警告列表（不阻塞启动）。"""
    warnings = []
    if not DOUBAO_API_KEY:
        warnings.append("DOUBAO_API_KEY 未配置，LLM 调用将不可用")
    if not DOUBAO_MODEL:
        warnings.append("DOUBAO_MODEL 未配置")
    if not DOUBAO_SEARCH_API_KEY:
        warnings.append("DOUBAO_SEARCH_API_KEY 未配置：Kimi 失败时将无兜底搜索")
    return warnings

#!/usr/bin/env python3
"""
FlexiChrono 统一启动器 - 单进程拉起全部 5 个后端
P4 序号6：引导式部署向导
  - 首次运行自动检测 .env，不存在则启动交互式配置向导
  - 也可使用 python3 launcher.py --setup 手动启动向导
  - 向导逐步输入 API Key、模型名称、QQ配置等，自动生成 .env
  - 自动检测 Python 依赖，缺失时提示安装命令
  - 配置完成后自动启动所有服务

用法:
  python3 launcher.py          # 正常启动（首次自动进入向导）
  python3 launcher.py --setup  # 强制重新配置
  python3 launcher.py --no-setup  # 跳过向导，直接启动（.env必须已存在）
"""
import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def load_dotenv(env_path: Path):
    """轻量 .env 加载器（不依赖 python-dotenv）
    只设置尚未存在的环境变量，已有的环境变量（如 Docker 注入的）优先。
    """
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


# ============================================================
# P4 序号6：引导式部署向导
# ============================================================

# 必需的 Python 依赖（用于启动前检测）
REQUIRED_PACKAGES = {
    "fastapi": "fastapi",
    "uvicorn": "uvicorn",
    "httpx": "httpx",
    "pydantic": "pydantic",
    "edge_tts": "edge-tts",
    "pyyaml": "pyyaml",
    "bcrypt": "bcrypt",
}

# 可选依赖（有额外功能时需要）
OPTIONAL_PACKAGES = {
    "chromadb": "chromadb",  # P4: 本地向量存储
}


def print_banner():
    """打印欢迎横幅。"""
    print("\n" + "=" * 60)
    print("  🎀 FlexiChrono 引导式部署向导")
    print("=" * 60)
    print("  这个向导会帮你配置所有必要的 API Key 和参数，")
    print("  完成后自动生成 .env 文件并启动所有服务。")
    print("  按 Ctrl+C 可随时退出向导。")
    print("=" * 60 + "\n")


def ask_input(prompt: str, default: str = "", required: bool = False, secret: bool = False) -> str:
    """
    交互式输入。
    Args:
        prompt: 提示文字
        default: 默认值（直接回车使用）
        required: 是否必填（空值会重复询问）
        secret: 是否为密钥（输入时不显示，简单用getpass）
    Returns:
        用户输入的值
    """
    while True:
        default_str = f" [默认: {default}]" if default else ""
        required_str = " *" if required else ""
        full_prompt = f"  {prompt}{default_str}{required_str}: "

        if secret:
            try:
                import getpass
                value = getpass.getpass(full_prompt).strip()
            except Exception:
                value = input(full_prompt).strip()
        else:
            value = input(full_prompt).strip()

        if not value and default:
            return default
        if not value and required:
            print("    ⚠️  此项为必填，请输入有效内容。")
            continue
        return value


def ask_yes_no(prompt: str, default: bool = False) -> bool:
    """是/否选择。"""
    default_str = "Y/n" if default else "y/N"
    while True:
        value = input(f"  {prompt} [{default_str}]: ").strip().lower()
        if not value:
            return default
        if value in ("y", "yes", "是"):
            return True
        if value in ("n", "no", "否"):
            return False
        print("    请输入 y 或 n")


def check_dependencies() -> dict:
    """
    检测 Python 依赖是否安装。
    Returns:
        {"missing": [...], "missing_optional": [...]}
    """
    missing = []
    for module_name, pip_name in REQUIRED_PACKAGES.items():
        try:
            __import__(module_name)
        except ImportError:
            missing.append(pip_name)

    missing_optional = []
    for module_name, pip_name in OPTIONAL_PACKAGES.items():
        try:
            __import__(module_name)
        except ImportError:
            missing_optional.append(pip_name)

    return {"missing": missing, "missing_optional": missing_optional}


def generate_env_file(config: dict, env_path: Path) -> bool:
    """
    根据配置生成 .env 文件。
    Args:
        config: 配置字典
        env_path: .env 文件路径
    Returns:
        是否成功
    """
    lines = [
        "# ============================================================",
        "# FlexiChrono 环境配置文件",
        f"# 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "# 由 launcher.py --setup 引导式向导生成",
        "# ============================================================",
        "",
        "# ---- 数据目录 ----",
        f'DATA_DIR="{config.get("data_dir", ".")}"',
        "",
        "# ---- 1级主力模型：豆包（火山方舟）----",
        f'DOUBAO_API_KEY="{config.get("doubao_api_key", "")}"',
        f'DOUBAO_BASE_URL="https://ark.cn-beijing.volces.com/api/v3"',
        f'DOUBAO_MODEL="{config.get("doubao_model", "")}"',
        "",
    ]

    # Kimi（可选）
    if config.get("kimi_api_key"):
        lines.extend([
            "# ---- 2级降级：Kimi（官方）----",
            f'KIMI_API_KEY="{config["kimi_api_key"]}"',
            f'KIMI_BASE_URL="https://api.moonshot.cn/v1"',
            f'KIMI_MODEL="{config.get("kimi_model", "moonshot-v1-8k")}"',
            "",
        ])

    # DeepSeek 火山方舟（可选）
    if config.get("deepseek_volc_model"):
        lines.extend([
            "# ---- 2级降级：DeepSeek（火山方舟）----",
            '# API Key 默认和豆包共用（DEEPSEEK_VOLC_API_KEY 可单独设置）',
            f'DEEPSEEK_VOLC_MODEL="{config["deepseek_volc_model"]}"',
            "",
        ])

    # 千问火山方舟（可选）
    if config.get("qwen_volc_model"):
        lines.extend([
            "# ---- 3级兜底：千问（火山方舟）----",
            '# API Key 默认和豆包共用（QWEN_VOLC_API_KEY 可单独设置）',
            f'QWEN_VOLC_MODEL="{config["qwen_volc_model"]}"',
            "",
        ])

    # ASR 语音识别
    if config.get("asr_api_key"):
        lines.extend([
            "# ---- ASR 语音识别（SiliconFlow SenseVoice）----",
            f'ASR_API_KEY="{config["asr_api_key"]}"',
            f'ASR_BASE_URL="https://api.siliconflow.cn/v1"',
            f'ASR_MODEL="FunAudioLLM/SenseVoiceSmall"',
            f'ASR_LANGUAGE="zh"',
            "",
        ])

    # TTS 引擎
    tts_engine = config.get("tts_engine", "edge-tts")
    lines.extend([
        "# ---- TTS 语音合成 ----",
        f'TTS_ENGINE="{tts_engine}"',
        f'TTS_FORMAT="mp3"',
        "",
    ])

    if tts_engine == "fish-audio" and config.get("fish_audio_api_key"):
        lines.extend([
            "# ---- Fish-Audio 语音克隆 ----",
            f'FISH_AUDIO_API_KEY="{config["fish_audio_api_key"]}"',
            f'FISH_AUDIO_BASE_URL="https://api.fish.audio/v1"',
            "# 每个角色的说话人ID（在 Fish-Audio 控制台创建）",
            f'NIANQI_FISH_REFERENCE_ID="{config.get("nianqi_fish_ref", "")}"',
            f'QINGHE_FISH_REFERENCE_ID="{config.get("qinghe_fish_ref", "")}"',
            f'JINGWEN_FISH_REFERENCE_ID="{config.get("jingwen_fish_ref", "")}"',
            "",
        ])

    # NapCat QQ 接入
    if config.get("napcat_http_url"):
        lines.extend([
            "# ---- NapCat QQ 接入 ----",
            f'NAPCAT_HTTP_URL="{config["napcat_http_url"]}"',
            f'NAPCAT_ACCESS_TOKEN="{config.get("napcat_access_token", "")}"',
            f'QQ_WEBHOOK_SECRET="{config.get("qq_webhook_secret", "")}"',
            "",
        ])

    # 主动发图
    if config.get("proactive_auto_image"):
        lines.extend([
            "# ---- 主动联系自动发图 ----",
            'PROACTIVE_AUTO_IMAGE="true"',
            f'PROACTIVE_IMAGE_INTIMACY_THRESHOLD="{config.get("proactive_image_threshold", "50")}"',
            "",
        ])

    # 内部通信密钥
    lines.extend([
        "# ---- 内部服务通信密钥（各服务间调用鉴权）----",
        f'INTERNAL_TOKEN="{config.get("internal_token", "change_me_internal_secret_2026")}"',
        "",
        "# ---- 管理员默认密码（首次启动生效，可在管理后台修改）----",
        f'ADMIN_DEFAULT_PASSWORD="{config.get("admin_password", "admin123")}"',
        "",
    ])

    try:
        with open(env_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        # 设置文件权限为仅所有者可读写（保护 API Key）
        try:
            os.chmod(env_path, 0o600)
        except Exception:
            pass
        return True
    except Exception as e:
        print(f"    ❌ 写入 .env 文件失败: {e}")
        return False


def run_setup_wizard(env_path: Path) -> bool:
    """
    运行引导式部署向导。
    Args:
        env_path: .env 文件路径
    Returns:
        是否成功完成配置
    """
    print_banner()

    # 检测现有配置
    existing_config = {}
    if env_path.exists():
        print(f"  📋 检测到已存在的 .env 文件: {env_path}")
        overwrite = ask_yes_no("是否覆盖现有配置？", default=False)
        if not overwrite:
            print("  保留现有配置，跳过向导。")
            return True
        # 读取现有配置作为默认值
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, _, value = line.partition("=")
                        existing_config[key.strip()] = value.strip().strip('"').strip("'")
        except Exception:
            pass

    print("\n" + "─" * 50)
    print("  第 1 步：基础配置")
    print("─" * 50)

    data_dir = ask_input(
        "数据存储目录（用户数据/会话/记忆都存在这里）",
        default=existing_config.get("DATA_DIR", str(Path.cwd())),
        required=True,
    )
    admin_password = ask_input(
        "管理员默认密码",
        default=existing_config.get("ADMIN_DEFAULT_PASSWORD", "admin123"),
        required=True,
    )
    internal_token = ask_input(
        "内部服务通信密钥（随机字符串即可，各服务间调用鉴权用）",
        default=existing_config.get("INTERNAL_TOKEN", os.urandom(16).hex()),
        required=True,
    )

    print("\n" + "─" * 50)
    print("  第 2 步：豆包 API（主力模型，必填）")
    print("─" * 50)
    print("  获取地址: https://console.volcengine.com/ark/region:ark+cn-beijing/apiKey")
    print("  模型名称在火山方舟「在线推理」页面查看，如 ep-2024xxxxxx")
    doubao_api_key = ask_input(
        "豆包 API Key",
        default=existing_config.get("DOUBAO_API_KEY", ""),
        required=True,
        secret=True,
    )
    doubao_model = ask_input(
        "豆包模型名称（推理接入点ID，如 ep-xxxxxx）",
        default=existing_config.get("DOUBAO_MODEL", ""),
        required=True,
    )

    print("\n" + "─" * 50)
    print("  第 3 步：降级模型（可选，主力模型挂了自动切换）")
    print("─" * 50)
    print("  不配置也能用，只是主力模型挂了就没兜底。建议至少配一个。")

    kimi_api_key = ""
    kimi_model = ""
    if ask_yes_no("是否配置 Kimi（月之暗面）作为2级降级？", default=False):
        kimi_api_key = ask_input("Kimi API Key", default=existing_config.get("KIMI_API_KEY", ""), required=True, secret=True)
        kimi_model = ask_input("Kimi 模型", default=existing_config.get("KIMI_MODEL", "moonshot-v1-8k"), required=True)

    deepseek_volc_model = ""
    if ask_yes_no("是否配置 DeepSeek（火山方舟）作为2级降级？", default=False):
        deepseek_volc_model = ask_input(
            "DeepSeek 火山方舟模型名称（推理接入点ID）",
            default=existing_config.get("DEEPSEEK_VOLC_MODEL", ""),
            required=True,
        )

    qwen_volc_model = ""
    if ask_yes_no("是否配置千问（火山方舟）作为3级兜底？", default=False):
        qwen_volc_model = ask_input(
            "千问火山方舟模型名称（推理接入点ID）",
            default=existing_config.get("QWEN_VOLC_MODEL", ""),
            required=True,
        )

    print("\n" + "─" * 50)
    print("  第 4 步：语音识别（ASR，可选）")
    print("─" * 50)
    print("  不配置则无法使用语音消息功能。")
    asr_api_key = ""
    if ask_yes_no("是否配置语音识别（SiliconFlow SenseVoice，免费额度够用）？", default=False):
        print("  获取地址: https://cloud.siliconflow.cn/account/ak")
        asr_api_key = ask_input("SiliconFlow API Key", default=existing_config.get("ASR_API_KEY", ""), required=True, secret=True)

    print("\n" + "─" * 50)
    print("  第 5 步：语音合成（TTS）")
    print("─" * 50)
    print("  edge-tts: 免费，微软音色，无需API Key（推荐新手）")
    print("  fish-audio: 语音克隆，高质量专属音色，需要API Key")
    print("  openai: OpenAI兼容TTS接口")
    print("  gpt-sovits: 本地部署GPT-SoVITS（需要GPU服务器）")
    tts_engine = ask_input(
        "选择 TTS 引擎 (edge-tts/fish-audio/openai/gpt-sovits)",
        default=existing_config.get("TTS_ENGINE", "edge-tts"),
        required=True,
    )

    fish_audio_api_key = ""
    nianqi_fish_ref = ""
    qinghe_fish_ref = ""
    jingwen_fish_ref = ""
    if tts_engine == "fish-audio":
        print("\n  Fish-Audio 语音克隆配置:")
        print("  获取地址: https://fish.audio/console/api-keys")
        fish_audio_api_key = ask_input("Fish-Audio API Key", default=existing_config.get("FISH_AUDIO_API_KEY", ""), required=True, secret=True)
        print("\n  每个角色需要一个说话人ID（在 Fish-Audio 控制台创建音色后获得）")
        nianqi_fish_ref = ask_input("念琦的说话人ID", default=existing_config.get("NIANQI_FISH_REFERENCE_ID", ""))
        qinghe_fish_ref = ask_input("清禾的说话人ID", default=existing_config.get("QINGHE_FISH_REFERENCE_ID", ""))
        jingwen_fish_ref = ask_input("璟雯的说话人ID", default=existing_config.get("JINGWEN_FISH_REFERENCE_ID", ""))

    print("\n" + "─" * 50)
    print("  第 6 步：QQ 接入（NapCat，可选）")
    print("─" * 50)
    print("  不配置则只能用网页端聊天。")
    napcat_http_url = ""
    napcat_access_token = ""
    qq_webhook_secret = ""
    if ask_yes_no("是否配置 NapCat QQ 机器人接入？", default=False):
        print("  NapCat 部署教程: https://github.com/NapNeko/NapCatQQ")
        napcat_http_url = ask_input(
            "NapCat HTTP API 地址（如 http://127.0.0.1:3000）",
            default=existing_config.get("NAPCAT_HTTP_URL", ""),
            required=True,
        )
        napcat_access_token = ask_input(
            "NapCat Access Token（NapCat配置中的access_token，没有就留空）",
            default=existing_config.get("NAPCAT_ACCESS_TOKEN", ""),
            secret=True,
        )
        qq_webhook_secret = ask_input(
            "Webhook 签名密钥（NapCat配置中的secret，没有就留空）",
            default=existing_config.get("QQ_WEBHOOK_SECRET", ""),
            secret=True,
        )

    print("\n" + "─" * 50)
    print("  第 7 步：主动发图（可选）")
    print("─" * 50)
    proactive_auto_image = False
    proactive_image_threshold = "50"
    if ask_yes_no("是否启用主动联系时自动发图（角色主动找你聊天时发自拍）？", default=False):
        proactive_auto_image = True
        proactive_image_threshold = ask_input(
            "主动发图的亲密度阈值（0-100，低于此值不发图）",
            default=existing_config.get("PROACTIVE_IMAGE_INTIMACY_THRESHOLD", "50"),
            required=True,
        )

    # 汇总配置
    config = {
        "data_dir": data_dir,
        "admin_password": admin_password,
        "internal_token": internal_token,
        "doubao_api_key": doubao_api_key,
        "doubao_model": doubao_model,
        "kimi_api_key": kimi_api_key,
        "kimi_model": kimi_model,
        "deepseek_volc_model": deepseek_volc_model,
        "qwen_volc_model": qwen_volc_model,
        "asr_api_key": asr_api_key,
        "tts_engine": tts_engine,
        "fish_audio_api_key": fish_audio_api_key,
        "nianqi_fish_ref": nianqi_fish_ref,
        "qinghe_fish_ref": qinghe_fish_ref,
        "jingwen_fish_ref": jingwen_fish_ref,
        "napcat_http_url": napcat_http_url,
        "napcat_access_token": napcat_access_token,
        "qq_webhook_secret": qq_webhook_secret,
        "proactive_auto_image": proactive_auto_image,
        "proactive_image_threshold": proactive_image_threshold,
    }

    # 确认配置
    print("\n" + "=" * 60)
    print("  📋 配置确认")
    print("=" * 60)
    print(f"  数据目录: {data_dir}")
    print(f"  管理员密码: {'*' * len(admin_password)}")
    print(f"  豆包模型: {doubao_model}")
    print(f"  Kimi: {'已配置' if kimi_api_key else '未配置'}")
    print(f"  DeepSeek火山: {'已配置' if deepseek_volc_model else '未配置'}")
    print(f"  千问火山: {'已配置' if qwen_volc_model else '未配置'}")
    print(f"  语音识别: {'已配置' if asr_api_key else '未配置'}")
    print(f"  TTS引擎: {tts_engine}")
    print(f"  QQ接入: {'已配置' if napcat_http_url else '未配置'}")
    print(f"  主动发图: {'启用' if proactive_auto_image else '禁用'}")
    print("=" * 60)

    if not ask_yes_no("确认以上配置并生成 .env 文件？", default=True):
        print("  已取消配置。")
        return False

    # 生成 .env
    print(f"\n  📝 正在生成 .env 文件: {env_path}")
    if not generate_env_file(config, env_path):
        return False
    print("  ✅ .env 文件已生成（权限已设置为 600，仅所有者可读写）")

    # 检测依赖
    print("\n" + "─" * 50)
    print("  依赖检测")
    print("─" * 50)
    deps = check_dependencies()
    if deps["missing"]:
        print(f"  ⚠️  缺少必需依赖: {', '.join(deps['missing'])}")
        print(f"  请执行: pip install {' '.join(deps['missing'])}")
        if not ask_yes_no("是否现在自动安装缺失的依赖？", default=True):
            print("  请手动安装后再启动。")
            return False
        print("  正在安装依赖...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + deps["missing"])
            print("  ✅ 依赖安装完成")
        except Exception as e:
            print(f"  ❌ 依赖安装失败: {e}")
            print("  请手动执行: pip install " + " ".join(deps["missing"]))
            return False
    else:
        print("  ✅ 所有必需依赖已安装")

    if deps["missing_optional"]:
        print(f"  💡 缺少可选依赖（不影响基础功能）: {', '.join(deps['missing_optional'])}")
        print(f"     需要时执行: pip install {' '.join(deps['missing_optional'])}")

    print("\n" + "=" * 60)
    print("  🎉 配置完成！即将启动所有服务...")
    print("=" * 60)
    print(f"  网页端地址: http://127.0.0.1:8000")
    print(f"  管理后台:   http://127.0.0.1:8000/admin.html")
    print(f"  管理员账号: admin / {admin_password}")
    print("=" * 60 + "\n")
    return True


# ============================================================
# 服务启动逻辑
# ============================================================

# 先加载 .env（本地开发用；Docker 中由 docker-compose env_file 注入，不冲突）
SCRIPT_DIR = Path(__file__).parent.resolve()
ENV_PATH = SCRIPT_DIR / ".env"

# 解析命令行参数
SKIP_SETUP = "--no-setup" in sys.argv
FORCE_SETUP = "--setup" in sys.argv

# 如果强制设置，或者 .env 不存在且没有跳过设置，则运行向导
if FORCE_SETUP or (not ENV_PATH.exists() and not SKIP_SETUP):
    try:
        success = run_setup_wizard(ENV_PATH)
        if not success:
            print("  配置未完成，退出。")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n  已取消配置。")
        sys.exit(0)

# 加载 .env（向导生成后或已存在）
load_dotenv(ENV_PATH)

# 服务定义：(名称, 脚本文件, 端口, 环境变量覆盖)
# 显式注入各服务专用 PORT 环境变量，防止云端平台注入的 PORT=8080 劫持内部服务端口
SERVICES = [
    ("vector",      "vector_server.py",      8001, {"VECTOR_PORT": "8001"}),
    ("personality", "personality_server.py", 8002, {"PERSONALITY_PORT": "8002"}),
    ("proactive",   "proactive_server.py",   8003, {"PROACTIVE_PORT": "8003"}),
    ("voice",       "voice_server.py",       8004, {"VOICE_PORT": "8004"}),
    ("main",        "main.py",               8000, {"MAIN_PORT": "8000"}),
]

DATA_DIR = os.getenv("DATA_DIR", str(SCRIPT_DIR))
PYTHON = sys.executable

# 确保数据目录存在
os.makedirs(DATA_DIR, exist_ok=True)

processes = {}
shutdown_event = asyncio.Event()


def log(name: str, msg: str):
    print(f"[{name:>11}] {msg}", flush=True)


async def read_stream(name, stream):
    """异步读取子进程输出流，带服务名前缀"""
    while True:
        try:
            line = await stream.readline()
        except asyncio.LimitOverrunError:
            chunks = []
            while True:
                try:
                    chunk = await stream.read(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    if b"\n" in chunk:
                        break
                except Exception:
                    break
            line = b"".join(chunks)
        except Exception as e:
            log(name, f"[read_stream] 读取异常: {type(e).__name__}: {e}")
            await asyncio.sleep(0.1)
            continue
        if not line:
            break
        try:
            text = line.decode("utf-8", errors="replace").rstrip()
        except Exception:
            text = repr(line)
        if text:
            log(name, text)


def is_port_in_use(port: int) -> bool:
    """检测端口是否已被占用。"""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", port))
            return False
        except OSError:
            return True


async def run_service(name, script, port, env_overrides):
    """启动单个服务并监控其输出"""
    if is_port_in_use(port):
        log(name, f"端口 {port} 已被占用，跳过启动（服务可能已在运行）")
        return None
    env = os.environ.copy()
    env["DATA_DIR"] = DATA_DIR
    env.update(env_overrides)
    cmd = [PYTHON, str(SCRIPT_DIR / script)]
    log(name, f"启动 {script} (端口 {port})")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
            cwd=str(SCRIPT_DIR),
            limit=1024 * 1024,
        )
    except Exception as e:
        log(name, f"启动失败: {e}")
        return None
    processes[name] = proc
    asyncio.create_task(read_stream(name, proc.stdout))
    return proc


async def monitor_process(name, proc):
    """监控进程，退出时记录并可选重启"""
    await proc.wait()
    if name in processes:
        del processes[name]
    if shutdown_event.is_set():
        log(name, f"已停止 (退出码 {proc.returncode})")
        return
    if proc.returncode == 0:
        log(name, f"正常退出 (退出码 0)，不重启")
        return
    log(name, f"异常退出 (退出码 {proc.returncode})，5秒后重启...")
    await asyncio.sleep(5)
    if shutdown_event.is_set():
        return
    for svc_name, script, port, env_overrides in SERVICES:
        if svc_name == name:
            new_proc = await run_service(name, script, port, env_overrides)
            if new_proc:
                asyncio.create_task(monitor_process(name, new_proc))
            break


async def health_check():
    """启动后等待所有端口就绪"""
    import httpx
    await asyncio.sleep(2)
    for name, script, port, _ in SERVICES:
        for attempt in range(15):
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    r = await client.get(f"http://127.0.0.1:{port}/health")
                    if r.status_code == 200:
                        log(name, f"健康检查通过 (端口 {port})")
                        break
            except Exception:
                pass
            await asyncio.sleep(1)
        else:
            log(name, f"健康检查超时 (端口 {port})，但继续运行")


async def shutdown():
    """优雅关闭所有子进程"""
    shutdown_event.set()
    log("launcher", "收到关闭信号，停止所有服务...")
    for name, proc in list(processes.items()):
        try:
            proc.terminate()
        except Exception:
            pass
    try:
        await asyncio.wait_for(
            asyncio.gather(*[proc.wait() for proc in processes.values()], return_exceptions=True),
            timeout=10.0,
        )
    except asyncio.TimeoutError:
        log("launcher", "强制杀死未退出的进程...")
        for name, proc in list(processes.items()):
            try:
                proc.kill()
            except Exception:
                pass
    log("launcher", "所有服务已停止")


def signal_handler():
    asyncio.create_task(shutdown())


async def main():
    log("launcher", "=" * 60)
    log("launcher", f"FlexiChrono 统一启动器 v2.0")
    log("launcher", f"Python: {PYTHON}")
    log("launcher", f"数据目录: {DATA_DIR}")
    log("launcher", f"服务数量: {len(SERVICES)}")
    log("launcher", f"配置文件: {ENV_PATH} {'(存在)' if ENV_PATH.exists() else '(不存在)'}")
    log("launcher", "=" * 60)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

    for name, script, port, env_overrides in SERVICES:
        proc = await run_service(name, script, port, env_overrides)
        if proc:
            asyncio.create_task(monitor_process(name, proc))
        await asyncio.sleep(0.5)

    asyncio.create_task(health_check())
    log("launcher", "全部服务已启动，按 Ctrl+C 停止")
    await shutdown_event.wait()
    await shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

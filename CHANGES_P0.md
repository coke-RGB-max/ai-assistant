# FlexiChrono P0 架构改造交付说明

## 改造目标
借鉴 AIRI（三层架构/可插拔组件）、Clawra（SOUL.md 人格即文件）、GirlfriendGPT（工程规范）等开源项目的优点，对现有5个后端服务进行架构解耦，解决硬编码、重复代码、安全隐患等问题。

---

## 一、改动总览

| 改造项 | 借鉴来源 | 状态 |
|---|---|---|
| 角色配置 YAML 化 | Clawra SOUL.md | ✅ 完成 |
| common/ 共享库 | AIRI 可插拔组件 | ✅ 完成 |
| 密码 bcrypt 哈希 | 通用安全实践 | ✅ 完成 |
| 人格服务器底层模块拆分 | AIRI 三层架构 | ✅ 完成 |

---

## 二、新增文件清单

### 1. 角色配置（characters/）
```
characters/
├── __init__.py
├── index.yaml          # 角色索引（顺序、默认角色）
├── loader.py           # 角色加载器（启动时自动加载YAML，支持热重载）
├── nianqi.yaml         # 念琦角色配置（5078字节）
├── qinghe.yaml         # 清禾角色配置（4046字节）
└── jingwen.yaml        # 璟雯角色配置（4167字节）
```

**每个角色YAML包含24个属性**：id、name、emoji、gender、age、personality、description、speaking_style、core_traits、values、catchphrases、taboos、emotion_tendency、conflict_style、psych_baseline、behavior_tendency、daily_noise、intimacy_prompts、micro_narratives、topic_pool、unique_quirks、jealousy_stages、nickname_evolution、growth_arc

### 2. 公共库（common/）
```
common/
├── __init__.py
├── step_timer.py       # 耗时统计工具（原5个服务各写一遍，现在统一）
├── logger.py           # 统一日志配置
├── service_client.py   # 统一服务间HTTP客户端（带熔断、重试、健康检查）
└── security.py         # 安全工具（bcrypt密码哈希、token生成、API Key掩码）
```

### 3. 人格服务器核心模块（core/）
```
core/
├── __init__.py
├── config.py           # 配置常量（所有环境变量集中管理）
├── utils.py            # 工具函数（StepTimer、时间/天气感知、枚举、JSON解析）
└── llm.py              # LLM调用（豆包主模型、Kimi联网搜索、流式调用、阈值判断）
```

### 4. 预留模块目录（后续逐步填充）
```
emotion/    # 情绪系统（EmotionBlender、EmotionEngine等）
psych/      # 心理状态（PsychologicalState、LLMCalibrator、InnerState等）
memory/     # 记忆系统（MemorySystem、MemoryAnalyzer、AssociativeMemory等）
knowledge/  # 知识路由（KnowledgeRouter）
api/        # API路由
```

---

## 三、修改文件清单

### 1. personality_server.py（人格服务器）
- **移除**：硬编码的 ROLES_DEFINITION 字典（9465字符，3个角色共24×3=72个属性）
- **移除**：配置区（环境变量定义）
- **移除**：工具函数区（StepTimer、时间感知、枚举、JSON解析等）
- **移除**：LLM调用区（smart_llm_call、kimi_search_call等）
- **新增**：`from characters.loader import load_all_roles, ...` 角色加载
- **新增**：`from core.config import *` 配置导入
- **新增**：`from core.utils import *` 工具导入
- **新增**：`from core.llm import *` LLM导入
- **新增**：`get_role_definition()` 和 `reload_role_definitions()` 函数
- **结果**：文件从 219979 字符 → 191686 字符，减少 28KB

### 2. main.py（主后端）
- **新增**：`from common.security import hash_password, verify_password`
- **修改**：`UserDB._ensure_file()` - 管理员默认密码改为 bcrypt 哈希
- **修改**：`UserDB.authenticate()` - 改为 bcrypt 验证，**旧明文密码自动升级为哈希**
- **修改**：`UserDB.register()` - 密码改为 bcrypt 哈希存储
- **修改**：`UserDB.reset_password()` - 密码改为 bcrypt 哈希存储
- **修改**：`UserDB.ensure_tmp_user()` - QQ临时用户密码改为 bcrypt 哈希
- **向后兼容**：旧 userdb.json 中的明文密码在首次登录时自动升级，无需手动迁移

---

## 四、关键特性说明

### 1. 角色配置热重载
改人设不需要改代码、不需要重新部署：
```python
from characters.loader import reload_roles
reload_roles()  # 管理员触发，重新加载所有YAML
```
后续可以加一个管理员API接口，在Web后台直接改人设并热重载。

### 2. 密码安全升级
- 新用户密码自动用 bcrypt 哈希（工作因子12）
- 旧用户明文密码在首次登录时自动升级为哈希，用户无感知
- 超过72字节的密码自动用 SHA256 预处理（bcrypt 限制）
- bcrypt 不可用时降级为 PBKDF2（不推荐生产使用）

### 3. 统一服务客户端（common/service_client.py）
后续可以逐步替换各服务中的 httpx 直接调用：
```python
from common.service_client import ServiceClient

client = ServiceClient(
    name="人格后端",
    base_url="http://127.0.0.1:8002",
    timeout=30.0,
    max_retries=2,
    fail_threshold=3,
    recover_interval=60.0,
)
resp = await client.post("/api/generate", json={...})
```
特性：自动重试（指数退避）、熔断器（连续失败3次熔断60秒）、超时控制、统一耗时日志。

### 4. 人格服务器渐进式拆分
当前只拆分了最独立的底层模块（config/utils/llm），业务类（50+个）和API路由保留在 personality_server.py 中。这样风险最低，后续可以逐步把情绪、心理、记忆等模块抽到 emotion/、psych/、memory/ 目录。

---

## 五、Railway 部署注意事项

### 1. 必须做：挂载 Volume 持久化数据
Railway 文件系统是临时的，重启/重新部署会清空所有数据：
- 进入 Railway 服务 → Settings → Volumes
- 挂载路径填 `/data`
- 环境变量添加：`DATA_DIR=/data`
- 这样 userdb.json、personality_sessions.db、proactive.db 都会持久化

### 2. 依赖更新
requirements.txt 需要新增：
```
pyyaml>=6.0
bcrypt>=4.0
```

### 3. 启动命令
launcher.py 会自动加载 .env 并启动所有5个服务：
```
python launcher.py
```
Railway 的 PORT 环境变量不会影响内部服务端口（各服务用专用的 MAIN_PORT/PERSONALITY_PORT 等）。

---

## 六、后续改造建议（P1/P2）

### P1 — 依赖与能力增强
1. **多模型抽象层**：LLM 调用从写死豆包改为可切换 DeepSeek/千问/Ollama，主模型挂了自动降级
2. **向量存储双后端**：DashVector（云端）+ ChromaDB（本地），配置切换
3. **图像生成模块**：借鉴 Clawra，加"发自拍"功能，角色配置锁定外貌特征
4. **TTS 多引擎**：edge-tts（免费兜底）+ Fish-Speech/GPT-SoVITS（高质量）

### P2 — 体验升级
1. **人格服务器完整拆分**：把50+个业务类按模块抽到 emotion/psych/memory/knowledge/api
2. **Web 前端 Live2D 形象**：借鉴 AIRI，角色说话时嘴型同步、情绪表情变化
3. **插件/技能系统**：角色可以通过技能调用外部工具（查天气、设提醒等）
4. **引导式部署**：launcher.py 加首次运行向导，交互式输入 API Key 自动生成 .env

---

## 七、验证结果

- ✅ 所有 Python 文件语法检查通过
- ✅ 所有模块导入测试通过
- ✅ 角色 YAML 加载测试通过（3个角色，各24个属性）
- ✅ 密码哈希功能测试通过（注册、登录、旧明文自动升级）
- ✅ 人格服务器关键类验证通过（PsychologicalState、LLMCalibrator、PersonalityEngine 等10个核心类）
- ✅ FastAPI app 创建成功
- ✅ 向后兼容：旧 userdb.json 明文密码自动升级

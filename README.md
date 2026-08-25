# FlexiChrono 全栈后端（云端部署版）

5 个后端合并为一个 Docker 容器，单进程运行，内部 HTTP 调用零改动。

## 架构

```
┌─────────────────────────────────────────────────┐
│              Docker 容器 (单进程)                 │
│                                                   │
│  launcher.py (统一启动器 + 进程监控 + 自动重启)   │
│    ├── vector_server.py      :8001  记忆/向量    │
│    ├── personality_server.py :8002  人格/LLM     │
│    ├── proactive_server.py   :8003  主动消息     │
│    ├── voice_server.py       :8004  语音ASR+TTS  │
│    └── main.py               :8000  主后端/QQ/WS │
│                                                   │
│  ffmpeg (AMR↔WAV 转码)  +  edge-tts (语音合成)  │
└─────────────────────────────────────────────────┘
         │                    │
    ./data (volume)     NapCat (宿主机/外部)
    userdb.json
    personality_sessions.db
    proactive.db
```

## 快速部署

### 1. 准备环境

```bash
# 安装 Docker 和 Docker Compose
# 确保有 2GB+ 内存，2GB+ 磁盘空间
```

### 2. 配置环境变量（API Key 等敏感信息）

```bash
# 复制模板
cp .env.example .env

# 编辑 .env，填入真实 API Key
nano .env
```

必须配置的项：

| 变量 | 说明 | 获取地址 |
|------|------|----------|
| `NAPCAT_HTTP_URL` | NapCat HTTP API 地址 | 见下方 NapCat 配置 |
| `DOUBAO_API_KEY` | 豆包 LLM API Key | https://console.volcengine.com/ark |
| `DOUBAO_MODEL` | 豆包推理接入点 ID | 豆包控制台创建 |
| `ASR_API_KEY` | SiliconFlow 语音识别 Key | https://cloud.siliconflow.cn/ |
| `KIMI_API_KEY` | Kimi 联网搜索 Key | https://platform.moonshot.cn/ |

可选配置：

| 变量 | 说明 |
|------|------|
| `DASHVECTOR_API_KEY` | 阿里云向量库 Key，不填降级为伪向量 |
| `DEEPSEEK_API_KEY` | DeepSeek 记忆摘要，不填则不可用 |
| `TTS_ENGINE` | `edge-tts`（免费）或 `openai` |

### 3. 构建并启动

```bash
# 构建镜像（首次约 3-5 分钟，需要下载 ffmpeg 和 Python 依赖）
docker compose build

# 后台启动
docker compose up -d

# 查看日志（确认 5 个服务都启动成功）
docker compose logs -f
```

启动成功后日志应出现：
```
[   launcher] ============================================================
[   launcher] FlexiChrono 统一启动器
[   launcher] 数据目录: /data
[   launcher] 服务数量: 5
[   launcher] ============================================================
[     vector] 启动 vector_server.py (端口 8001)
[personality] 启动 personality_server.py (端口 8002)
[  proactive] 启动 proactive_server.py (端口 8003)
[      voice] 启动 voice_server.py (端口 8004)
[       main] 启动 main.py (端口 8000)
[     vector] 健康检查通过 (端口 8001)
...
```

如果某个 API Key 没配，启动日志会有 `⚠️` 警告。

### 4. 验证

```bash
# 主后端健康检查
curl http://localhost:8000/

# 人格后端健康检查
curl http://localhost:8002/health

# 语音后端健康检查
curl http://localhost:8004/health
```

## 数据持久化

所有数据文件存储在宿主机 `./data/` 目录（挂载到容器 `/data`）：

| 文件 | 说明 |
|------|------|
| `userdb.json` | 用户账号、QQ绑定、亲密度、session ID |
| `personality_sessions.db` | 人格会话状态、心理状态 |
| `proactive.db` | 主动消息调度、活动记录 |

**备份**：直接复制 `./data/` 目录即可。
**迁移**：把 `./data/` 目录复制到新服务器，重新 `docker compose up -d`。

## 本地开发运行（不用 Docker）

```bash
# 安装依赖
pip install -r requirements.txt

# 安装 ffmpeg（系统级）
sudo apt install ffmpeg   # Ubuntu/Debian
brew install ffmpeg       # macOS

# 配置环境变量
cp .env.example .env
nano .env

# 启动全部服务（launcher.py 会自动加载 .env）
python3 launcher.py

# 或单独启动某个服务调试（需先 export 环境变量或 source .env）
python3 main.py
python3 personality_server.py
```

## 端口说明

| 端口 | 服务 | 外部访问 |
|------|------|----------|
| 8000 | 主后端 | ✅ QQ Webhook / WebSocket / 管理API |
| 8001 | 记忆后端 | ❌ 仅内部调用 |
| 8002 | 人格后端 | ❌ 仅内部调用 |
| 8003 | 主动后端 | ❌ 仅内部调用 |
| 8004 | 语音后端 | ❌ 仅内部调用 |

云端部署时，**只需对外开放 8000 端口**（配置防火墙/安全组）。

## NapCat 配置

NapCat 需要同时开启两个能力：

1. **HTTP 上报（post_url）**：接收 QQ 消息
   - URL: `http://你的服务器IP:8000/api/qq/webhook`

2. **HTTP API**：发送 QQ 消息
   - 端口默认 3000，在 `.env` 的 `NAPCAT_HTTP_URL` 中配置

如果 NapCat 和 Docker 在同一台机器：
- Windows/macOS: `NAPCAT_HTTP_URL=http://host.docker.internal:3000`
- Linux: 需要加 `network_mode: host` 或用实际 IP

## GitHub 安全说明

- `.env` 文件包含真实 API Key，**已被 .gitignore 忽略，不会提交**
- `.env.example` 是模板，可以安全提交
- `docker-compose.yml` 不含任何密钥，可以安全提交
- 所有 `.py` 文件中的硬编码 Key 已全部移除，改为 `os.getenv()` 读取
- `data/` 目录（数据库文件）也已被 .gitignore 忽略

## 常见问题

**Q: 语音消息识别不出来？**
A: 检查 `.env` 中 `ASR_API_KEY` 是否正确，容器内执行 `curl https://api.siliconflow.cn/v1/models -H "Authorization: Bearer $ASR_API_KEY"` 验证。

**Q: QQ 消息发不出去？**
A: 日志出现 `NAPCAT_HTTP_URL 未配置` 说明没配。确认 `.env` 中 `NAPCAT_HTTP_URL` 正确，且 NapCat 的 HTTP API 已开启。

**Q: 记忆检索返回空？**
A: 没配 `DASHVECTOR_API_KEY` 时会用伪向量降级（能运行但检索不准）。建议配置阿里云 DashVector。

**Q: 某个服务崩溃了？**
A: launcher.py 会自动检测并在 5 秒后重启崩溃的服务。日志会显示 `异常退出，5秒后重启`。

**Q: 如何更新代码？**
A: 修改代码后执行 `docker compose build && docker compose up -d`，数据在 volume 中不会丢失。

**Q: 改了 .env 怎么生效？**
A: 执行 `docker compose up -d`（会自动重建容器并加载新配置）。

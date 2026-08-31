# FlexiChrono 全栈后端 Docker 镜像
# 包含：主后端 + 人格后端 + 记忆后端 + 主动后端 + 语音后端
# 依赖：ffmpeg(语音转码) + edge-tts(语音合成) + Python 依赖
# P0 架构改造：新增 characters/ common/ core/ 三个目录，必须显式复制进容器

FROM python:3.10-slim

# 避免 apt 交互式提示
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# 安装系统依赖：ffmpeg(语音AMR/WAV转码) + curl(健康检查) + build-essential(部分pip包编译)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 先复制 requirements 并安装依赖（利用 Docker 缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制所有后端代码（P0改造：必须包含子目录，不能只用 COPY *.py）
COPY *.py ./
COPY index.html ./
COPY characters/ ./characters/
COPY common/ ./common/
COPY core/ ./core/

# 数据目录（SQLite / userdb.json 持久化挂载点）
RUN mkdir -p /data
ENV DATA_DIR=/data

# 暴露端口：8000主后端 8001记忆 8002人格 8003主动 8004语音
EXPOSE 8000 8001 8002 8003 8004

# 健康检查
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/ || exit 1

# 启动统一启动器
CMD ["python3", "launcher.py", "--no-setup"]
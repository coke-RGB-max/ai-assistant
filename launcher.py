#!/usr/bin/env python3
"""
FlexiChrono 统一启动器 - 单进程拉起全部 5 个后端
用法: python3 launcher.py
内部 HTTP 调用完全不变(127.0.0.1:8000~8004)，架构零改动。
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


# 先加载 .env（本地开发用；Docker 中由 docker-compose env_file 注入，不冲突）
SCRIPT_DIR = Path(__file__).parent.resolve()
load_dotenv(SCRIPT_DIR / ".env")

# 服务定义：(名称, 脚本文件, 端口, 环境变量覆盖)
SERVICES = [
    ("vector",      "vector_server.py",      8001, {}),
    ("personality", "personality_server.py", 8002, {}),
    ("proactive",   "proactive_server.py",   8003, {}),
    ("voice",       "voice_server.py",       8004, {}),
    ("main",        "main.py",               8000, {}),
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
        line = await stream.readline()
        if not line:
            break
        try:
            text = line.decode("utf-8", errors="replace").rstrip()
        except Exception:
            text = repr(line)
        if text:
            log(name, text)


async def run_service(name, script, port, env_overrides):
    """启动单个服务并监控其输出"""
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
        )
    except Exception as e:
        log(name, f"启动失败: {e}")
        return None

    processes[name] = proc

    # 异步读取输出
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
    log(name, f"异常退出 (退出码 {proc.returncode})，5秒后重启...")
    await asyncio.sleep(5)
    if shutdown_event.is_set():
        return
    # 找到服务定义并重启
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
    # 等待最多 10 秒
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
    log("launcher", f"FlexiChrono 统一启动器")
    log("launcher", f"Python: {PYTHON}")
    log("launcher", f"数据目录: {DATA_DIR}")
    log("launcher", f"服务数量: {len(SERVICES)}")
    log("launcher", "=" * 60)

    # 注册信号处理
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

    # 按顺序启动所有服务
    for name, script, port, env_overrides in SERVICES:
        proc = await run_service(name, script, port, env_overrides)
        if proc:
            asyncio.create_task(monitor_process(name, proc))
        await asyncio.sleep(0.5)  # 错开启动，避免资源争抢

    # 健康检查
    asyncio.create_task(health_check())

    log("launcher", "全部服务已启动，按 Ctrl+C 停止")

    # 等待关闭信号
    await shutdown_event.wait()
    await shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

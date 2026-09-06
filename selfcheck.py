"""
FlexiChrono 启动自检（selfcheck）
==================================================================
在 launcher 拉起 5 个服务之前运行，把“运行到一半才崩”的问题提前
拦截在启动阶段。检查三部分：

  1. 全量导入：所有本地模块必须可导入（语法错误 / 幽灵导入 /
     模块级 NameError / 漏装第三方依赖，全部在此暴露）
  2. 路由枚举：每个 FastAPI 服务必须成功注册非空路由
  3. 跨模块契约：针对历史事故的防回归断言（拆分回退、mood 字段、
     群路由符号、角色加载等）

用法：
    python selfcheck.py            # 人类可读输出，退出码 0=通过 / 1=失败
    python selfcheck.py --json     # 机器可读
设计原则：只做静态/导入级检查与纯函数契约，不连数据库、不发网络请求。
"""
import importlib
import inspect
import os
import sys
import traceback

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

# 领域包 / 共享底座（轻量，必须先于服务导入）
DOMAIN_MODULES = [
    "core.config", "core.utils", "core.llm", "core.roles", "core.storage",
    "emotion", "psych", "memory", "knowledge", "group", "quality", "topic", "scene",
    "characters.loader", "plugins",
]
# 5 个 FastAPI 服务（重，依赖第三方库）
SERVER_MODULES = [
    "personality_server", "proactive_server", "vector_server",
    "voice_server", "main",
]
# requirements.txt 中声明的第三方包名（用于区分“内部断裂”与“缺依赖”）
THIRD_PARTY = {
    "fastapi", "uvicorn", "httpx", "pydantic", "yaml", "pinecone", "edge_tts",
    "aiohttp", "websockets", "jieba", "bcrypt", "multipart", "dotenv",
    "pydantic_core", "starlette", "anyio", "sniffio", "certifi", "idna",
    "h11", "httpcore", "typing_extensions", "annotated_types",
}

results = []  # (level, module, msg)  level: ok/warn/fail


def record(level, module, msg=""):
    results.append((level, module, msg))
    mark = {"ok": "OK  ", "warn": "WARN", "fail": "FAIL"}[level]
    print(f"[{mark}] {module}" + (f" — {msg}" if msg else ""))


def try_import(name):
    try:
        return importlib.import_module(name), None
    except ModuleNotFoundError as e:
        missing = getattr(e, "name", None) or str(e)
        kind = "缺第三方依赖" if missing.split(".")[0] in THIRD_PARTY else "内部导入断裂"
        return None, f"{kind}: 缺少模块 '{missing}'\n{traceback.format_exc()}"
    except BaseException as e:  # 模块级代码任何异常都要拦截（NameError/SyntaxError/...）
        return None, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"


def check_imports():
    print("\n=== 1. 本地模块全量导入 ===")
    for name in DOMAIN_MODULES + SERVER_MODULES:
        mod, err = try_import(name)
        if err is None:
            record("ok", name, "导入成功")
        else:
            # 领域包断裂一律失败；服务缺第三方依赖在精简环境下降级为 warn
            top = err.split(":")[0]
            if name in SERVER_MODULES and top == "缺第三方依赖":
                record("warn", name, err.splitlines()[0] + "（部署镜像内不应出现）")
            else:
                record("fail", name, err)


def check_routes():
    print("\n=== 2. FastAPI 路由枚举 ===")
    for name in SERVER_MODULES:
        mod = sys.modules.get(name)
        if mod is None:
            continue
        app = getattr(mod, "app", None)
        routes = getattr(app, "routes", None)
        if app is None or routes is None:
            record("fail", name, "模块内找不到 FastAPI 实例 app")
            continue
        n = len([r for r in routes if hasattr(r, "methods") or hasattr(r, "path")])
        if n > 0:
            record("ok", name, f"注册 {n} 条路由")
        else:
            record("fail", name, "路由数为 0")


def check_contracts():
    print("\n=== 3. 跨模块契约（历史事故防回归）===")
    # 契约 A：personality 领域引擎必须来自拆分模块，不得回退到内联副本
    ps = sys.modules.get("personality_server")
    if ps is not None:
        expect = {
            "EmotionEngine": "emotion", "PsychologicalState": "psych",
            "MemorySystem": "memory", "KnowledgeRouter": "knowledge",
            "GroupBrain": "group", "QualityChecker": "quality",
            "TopicInitiator": "topic", "SceneModeEngine": "scene",
        }
        bad = []
        for cls, pkg in expect.items():
            obj = getattr(ps, cls, None)
            if obj is None:
                bad.append(f"{cls} 不存在")
            elif getattr(obj, "__module__", "").split(".")[0] != pkg:
                bad.append(f"{cls} 来自 {obj.__module__}（应为 {pkg}，疑似回退内联）")
        record("ok" if not bad else "fail", "personality 领域类归位",
                "全部来自拆分模块" if not bad else "; ".join(bad))
        # 共享底座必须唯一
        import core.roles as cr
        if ps.ROLES_DEFINITION is cr.ROLES_DEFINITION:
            record("ok", "角色表全局唯一", "personality 与 core.roles 同一对象")
        else:
            record("fail", "角色表全局唯一", "personality 与 core.roles 不是同一对象，热重载将不同步")

    # 契约 B：proactive 欲望引擎必须返回顶层 mood（KeyError: 'mood' 防回归）
    pro = sys.modules.get("proactive_server")
    if pro is not None and hasattr(pro, "LongingEngine"):
        src = inspect.getsource(pro.LongingEngine.calc)
        record("ok" if '"mood"' in src or "'mood'" in src else "fail",
               "proactive.LongingEngine.calc", "返回结构含 mood" if ('"mood"' in src or "'mood'" in src)
               else "calc 返回缺少顶层 mood，会触发 KeyError")

    # 契约 C：群路由 7 个必备符号（ModuleNotFoundError 防回归）
    try:
        gr = importlib.import_module("core.napcat.group_router")
        need = ["GroupRoleConfig", "GroupConfig", "GroupConfigManager",
                "GroupMessageContext", "GroupMessageRouter",
                "get_group_config_manager", "get_group_router"]
        miss = [s for s in need if not hasattr(gr, s)]
        record("ok" if not miss else "fail", "core.napcat.group_router",
               "7 个导出符号齐全" if not miss else f"缺少: {miss}")
    except BaseException as e:
        record("fail", "core.napcat.group_router", f"{type(e).__name__}: {e}")

    # 契约 D：角色配置至少加载到一个角色
    try:
        from characters.loader import get_all_roles
        roles = get_all_roles()
        record("ok" if roles else "fail", "characters.loader",
               f"加载 {len(roles)} 个角色: {list(roles.keys())}" if roles else "未加载到任何角色")
    except BaseException as e:
        record("fail", "characters.loader", f"{type(e).__name__}: {e}")


def main():
    as_json = "--json" in sys.argv
    print("=" * 64)
    print("FlexiChrono 启动自检")
    print("=" * 64)
    check_imports()
    check_routes()
    check_contracts()
    fails = [r for r in results if r[0] == "fail"]
    warns = [r for r in results if r[0] == "warn"]
    print("\n" + "=" * 64)
    if as_json:
        import json
        print(json.dumps({"fail": len(fails), "warn": len(warns),
                          "results": results}, ensure_ascii=False))
    if fails:
        print(f"自检未通过：{len(fails)} 项失败，{len(warns)} 项警告。拒绝启动，请先修复上述 FAIL。")
        sys.exit(1)
    print(f"自检通过：{len(results)-len(warns)} 项正常，{len(warns)} 项警告（不阻塞）。")
    sys.exit(0)


if __name__ == "__main__":
    main()

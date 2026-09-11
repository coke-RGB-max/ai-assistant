"""
人格服务器 LLM 调用模块
豆包主模型、Kimi 联网搜索、火山豆包搜索兜底、流式调用、阈值判断。
"""
import asyncio
import time
import random
import json
import datetime
import logging
from typing import Optional, List, Dict, Any

import httpx

from core.config import (
    DOUBAO_API_KEY, DOUBAO_BASE_URL, DOUBAO_MODEL,
    KIMI_API_KEY, KIMI_BASE_URL, KIMI_MODEL, KIMI_SEARCH_MODEL,
    LLM_ANALYSIS_MIN_LEN, LLM_HIGH_VALUE_KEYWORDS,
    DOUBAO_SEARCH_API_KEY, DOUBAO_SEARCH_URL, DOUBAO_SEARCH_TYPE,
    DOUBAO_SEARCH_COUNT, DOUBAO_SEARCH_TIMERANGE, DOUBAO_SEARCH_TIMEOUT,
    KIMI_SEARCH_TIMEOUT, SEARCH_DAILY_LIMIT,
)
from core.utils import safe_json_parse

logger = logging.getLogger("personality_llm")

from core.llm_router import get_llm_router_sync

# 全局路由器实例（延迟初始化，避免循环导入）
_llm_router = None


def _get_router():
    """获取路由器单例。"""
    global _llm_router
    if _llm_router is None:
        _llm_router = get_llm_router_sync()
    return _llm_router


# ============================================================
# v13.0: 每日搜索成本护栏（全局单容器计数，Kimi+豆包合计）
# ============================================================
_search_counter = {"date": "", "count": 0}


def _search_budget_ok() -> bool:
    """当日搜索次数未到上限则放行。"""
    today = datetime.date.today().isoformat()
    if _search_counter["date"] != today:
        _search_counter["date"] = today
        _search_counter["count"] = 0
    ok = _search_counter["count"] < SEARCH_DAILY_LIMIT
    if not ok:
        logger.warning(f"[搜索护栏] 当日已达上限 {SEARCH_DAILY_LIMIT} 次，本日不再联网")
    return ok


def _search_budget_commit():
    """实际发起一次搜索后计数。"""
    today = datetime.date.today().isoformat()
    if _search_counter["date"] != today:
        _search_counter["date"] = today
        _search_counter["count"] = 0
    _search_counter["count"] += 1


# ============================================================
# P1: 多模型智能降级调用（原 smart_llm_call，函数签名不变）
# 优先级：豆包(1级) → Kimi/DeepSeek(2级轮流) → 千问(3级)
# 连续失败3次自动降级，降级后60秒探测恢复
# ============================================================
async def smart_llm_call(messages, temperature=0.9, max_tokens=500, timeout=60.0,
                         max_retries=3, json_mode=False):
    """
    多模型智能降级调用。

    保留原函数签名，内部通过 LLMRouter 自动选择可用模型。
    max_retries 参数保留兼容（路由内部有自己的重试+降级逻辑）。
    """
    router = _get_router()
    t0 = time.perf_counter()

    result = await router.chat(
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        json_mode=json_mode,
    )

    dur = time.perf_counter() - t0
    if result is not None:
        logger.info(f"[LLM][路由] 成功 耗时={dur:.2f}s 输出长度={len(result)} json_mode={json_mode}")
    else:
        logger.warning(f"[LLM][路由] 所有模型调用失败 耗时={dur:.2f}s")
    return result

# ============================================================
# v11.0: 流式LLM调用（SSE推送token）
# ============================================================
async def smart_llm_stream_call(messages, temperature=0.9, max_tokens=500, timeout=60.0):
    """流式调用豆包API，异步yield每个token片段。失败时yield error事件。"""
    payload = {"model":DOUBAO_MODEL,"messages":messages,"temperature":temperature,
               "max_tokens":max_tokens,"stream":True}
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", f"{DOUBAO_BASE_URL}/chat/completions",
                headers={"Authorization":f"Bearer {DOUBAO_API_KEY}","Content-Type":"application/json"},
                json=payload) as resp:
                if resp.status_code != 200:
                    err_text = await resp.aread()
                    logger.error(f"[LLM流式] HTTP{resp.status_code}: {err_text[:200]}")
                    yield f"data: {json.dumps({'type':'error','error':f'HTTP {resp.status_code}'})}\n\n"
                    return
                async for line in resp.aiter_lines():
                    if not line or not line.startswith("data: "):
                        continue
                    data = line[6:]
                    if data == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                        delta = chunk.get("choices",[{}])[0].get("delta",{})
                        content = delta.get("content","")
                        if content:
                            yield f"data: {json.dumps({'type':'token','content':content})}\n\n"
                    except json.JSONDecodeError:
                        continue
        dur = time.perf_counter() - t0
        logger.info(f"[LLM流式] 完成 耗时={_fmt_ms(dur)}")
        yield f"data: {json.dumps({'type':'done'})}\n\n"
    except httpx.TimeoutException:
        logger.error("[LLM流式] 超时")
        yield f"data: {json.dumps({'type':'error','error':'timeout'})}\n\n"
    except Exception as e:
        logger.error(f"[LLM流式] 异常: {e}", exc_info=True)
        yield f"data: {json.dumps({'type':'error','error':str(e)})}\n\n"


def _fmt_ms(seconds: float) -> str:
    return f"{seconds * 1000:.0f}ms"


# ============================================================
# v13.0: 火山「豆包搜索」独立搜索（兜底线 B）
# 调独立搜索接口，拿回结构化结果，自己拼成带来源的文本。
# 与方舟自带 web_search 插件不同：这里完全由程序控制搜什么、要哪些来源。
# ============================================================
async def doubao_search_call(query: str, count: Optional[int] = None,
                             timeout: Optional[float] = None) -> Optional[str]:
    """调用火山豆包搜索，返回带来源标题/摘要/链接的文本；失败返回 None。"""
    if not DOUBAO_SEARCH_API_KEY:
        logger.warning("[豆包搜索] 未配置 DOUBAO_SEARCH_API_KEY，跳过兜底搜索")
        return None
    if not _search_budget_ok():
        return None

    payload: Dict[str, Any] = {
        "Query": query[:100],
        "SearchType": DOUBAO_SEARCH_TYPE or "web",
        "Count": count or DOUBAO_SEARCH_COUNT,
        "Filter": {"NeedContent": False, "NeedUrl": True},
        "NeedSummary": True,  # 取 Summary（500~1000字，官方推荐喂 LLM）
    }
    if DOUBAO_SEARCH_TIMERANGE:
        payload["TimeRange"] = DOUBAO_SEARCH_TIMERANGE

    _search_budget_commit()
    try:
        async with httpx.AsyncClient(timeout=timeout or DOUBAO_SEARCH_TIMEOUT) as client:
            resp = await client.post(
                DOUBAO_SEARCH_URL,
                headers={
                    "Authorization": f"Bearer {DOUBAO_SEARCH_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        if resp.status_code != 200:
            logger.warning(f"[豆包搜索] HTTP{resp.status_code}: {resp.text[:200]}")
            return None

        data = resp.json() or {}
        result = data.get("Result") or {}
        items = result.get("WebResults") or []
        if not items:
            logger.info(f"[豆包搜索] 无结果，query={query[:30]}")
            return None

        lines: List[str] = []
        for i, it in enumerate(items, 1):
            title = (it.get("Title") or "").strip()
            site = (it.get("SiteName") or "").strip()
            url = (it.get("Url") or "").strip()
            summary = (it.get("Summary") or it.get("Snippet") or "").strip()
            pub = (it.get("PublishTime") or "")[:10]
            auth = (it.get("AuthInfoDes") or "").strip()
            if not summary:
                continue
            meta = f"来源：{site or '未知站点'}"
            if pub:
                meta += f" · {pub}"
            if auth:
                meta += f" · {auth}"
            if url:
                meta += f" · {url}"
            lines.append(f"[{i}] {title}\n{meta}\n{summary}")
            if len(lines) >= (count or DOUBAO_SEARCH_COUNT):
                break

        if not lines:
            return None
        text = "\n\n".join(lines)
        logger.info(f"[豆包搜索] 成功 query={query[:30]} 条数={len(lines)} 长度={len(text)}")
        return text
    except Exception as e:
        logger.warning(f"[豆包搜索] 异常: {e}")
        return None


# ============================================================
# v13.0: Kimi 联网搜索（A线主搜）→ 失败自动走豆包搜索兜底
# 对外签名不变，所有现有调用方（知识路由、情绪模块）自动获得兜底能力。
# ============================================================
async def kimi_search_call(query: str, max_tokens: int = 800,
                           timeout: Optional[float] = None) -> Optional[str]:
    """联网搜索：先走 Kimi，失败/为空/超时再走火山豆包搜索。

    返回一段带来源的文本；两条线都失败才返回 None（上层走离线 B 线）。
    """
    if not _search_budget_ok():
        return None

    # ---- 第一级：Kimi ----
    kimi_ok = False
    if KIMI_API_KEY:
        payload = {
            "model": KIMI_SEARCH_MODEL,
            "messages": [
                {"role": "system", "content": "你是一个联网搜索助手。请根据用户问题搜索最新信息，并给出简洁准确的摘要回答。只回答事实，不要加个人观点。"},
                {"role": "user", "content": query}
            ],
            "temperature": 0.3,
            "max_tokens": max_tokens,
        }
        try:
            async with httpx.AsyncClient(timeout=timeout or KIMI_SEARCH_TIMEOUT) as client:
                resp = await client.post(
                    f"{KIMI_BASE_URL}/chat/completions",
                    headers={"Authorization": f"Bearer {KIMI_API_KEY}", "Content-Type": "application/json"},
                    json=payload,
                )
            if resp.status_code == 200:
                content = (resp.json().get("choices", [{}])[0].get("message", {})
                           .get("content", "") or "").strip()
                if content:
                    _search_budget_commit()
                    logger.info(f"[Kimi] 联网搜索成功 query={query[:30]} 长度={len(content)}")
                    return content
                logger.warning("[Kimi] 返回空内容，转豆包兜底")
            else:
                logger.warning(f"[Kimi] 搜索失败 HTTP{resp.status_code}: {resp.text[:200]}，转豆包兜底")
        except httpx.TimeoutException:
            logger.warning(f"[Kimi] 搜索超时({KIMI_SEARCH_TIMEOUT}s)，转豆包兜底")
        except Exception as e:
            logger.warning(f"[Kimi] 搜索异常: {e}，转豆包兜底")
    else:
        logger.info("[Kimi] 未配置 KIMI_API_KEY，直接走豆包搜索兜底")

    # ---- 第二级：火山豆包搜索 ----
    return await doubao_search_call(query)


# ============================================================
# v8.1: LLM调用阈值判断
# ============================================================
def should_use_llm_analysis(msg: str) -> bool:
    if len(msg) >= LLM_ANALYSIS_MIN_LEN: return True
    return any(k in msg for k in LLM_HIGH_VALUE_KEYWORDS)

# ============================================================

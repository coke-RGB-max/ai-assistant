"""
人格服务器 LLM 调用模块
豆包主模型、Kimi 联网搜索、流式调用、阈值判断。
"""
import asyncio
import time
import random
import logging
from typing import Optional, List, Dict, Any

import httpx

from core.config import (
    DOUBAO_API_KEY, DOUBAO_BASE_URL, DOUBAO_MODEL,
    KIMI_API_KEY, KIMI_BASE_URL, KIMI_MODEL, KIMI_SEARCH_MODEL,
    LLM_ANALYSIS_MIN_LEN, LLM_HIGH_VALUE_KEYWORDS,
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

# ============================================================
# v10.0: Kimi 联网搜索客户端（A线）
# ============================================================
async def kimi_search_call(query: str, max_tokens: int = 800, timeout: int = 30) -> Optional[str]:
    """调用 Kimi 联网搜索模型，返回搜索结果摘要。"""
    if not KIMI_API_KEY:
        logger.warning("[Kimi] 未配置 KIMI_API_KEY，跳过联网搜索")
        return None
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
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(f"{KIMI_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {KIMI_API_KEY}", "Content-Type": "application/json"},
                json=payload)
        if resp.status_code == 200:
            content = resp.json()["choices"][0]["message"]["content"]
            logger.info(f"[Kimi] 联网搜索成功，query={query[:30]}，结果长度={len(content)}")
            return content
        else:
            logger.warning(f"[Kimi] 搜索失败 HTTP{resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        logger.warning(f"[Kimi] 搜索异常: {e}")
        return None

# ============================================================
# v8.1: LLM调用阈值判断
# ============================================================
def should_use_llm_analysis(msg: str) -> bool:
    if len(msg) >= LLM_ANALYSIS_MIN_LEN: return True
    return any(k in msg for k in LLM_HIGH_VALUE_KEYWORDS)

# ============================================================

"""
记忆后端 v2.0 - 端口 8001
对接人格后端 v11.0：向量存储/检索、DashVector、Embedding、DeepSeek记忆摘要分析
v2.0: API Key环境变量化 / 版本号对齐 / 移除硬编码密钥
"""
# Windows UTF-8 强制修复（必须位于所有 import 之前）
import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import os
import asyncio
import hashlib
import json
import logging
import time
import uuid
import re
from typing import Any, Dict, List, Optional
import httpx
from fastapi import Body, Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager

# ===================== 配置区（合并两套全部配置 =====================
HOST = "0.0.0.0"
PORT = int(os.getenv("VECTOR_PORT", "8001"))
SUB_VECTOR_API_TOKEN = os.getenv("VECTOR_API_TOKEN", "change_me_strong_secret_key_123456")
DOUBAO_API_KEY = os.getenv("DOUBAO_API_KEY", "")
DOUBAO_EMBEDDING_URL = os.getenv("DOUBAO_EMBEDDING_URL", "https://ark.cn-beijing.volces.com/api/v3/embeddings/multimodal")
DOUBAO_EMBEDDING_MODEL = os.getenv("DOUBAO_EMBEDDING_MODEL", "ep-20260820233627-pjl5h")
DASHVECTOR_API_KEY = os.getenv("DASHVECTOR_API_KEY", "")
DASHVECTOR_ENDPOINT = os.getenv("DASHVECTOR_ENDPOINT", "https://vrs-cn-voj4x54l70001d.dashvector.cn-beijing.aliyuncs.com")
DASHVECTOR_COLLECTION = os.getenv("DASHVECTOR_COLLECTION", "flexichrono")
DASHVECTOR_SESSION_COLLECTION = "flexichrono_session"
DASHVECTOR_DIMENSION = 2048
REQUEST_TIMEOUT = 60
LOG_MAX_CHARS = 2000

# 新版独有的 DeepSeek 配置
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
# ==========================================================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vector_server")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🧠 记忆后端 v2.0 启动 - 端口 {PORT}")
    if not DASHVECTOR_API_KEY:
        logger.warning("⚠️ DASHVECTOR_API_KEY 未配置！向量检索将降级为伪向量模式（不推荐生产使用）")
    if not DOUBAO_API_KEY:
        logger.warning("⚠️ DOUBAO_API_KEY 未配置！Embedding 功能将不可用")
    yield
    logger.info("记忆后端关闭")

app = FastAPI(title="Vector Server", version="2.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------- 【全部来自旧版sub_vector_service 底层工具函数】 --------------------------
def truncate_text(text: Any, max_len: int = LOG_MAX_CHARS) -> str:
    text = str(text)
    if len(text) <= max_len:
        return text
    return text[:max_len] + f"... [截断 {len(text) - max_len} 字符]"


async def safe_api_request(
    service_name: str,
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = REQUEST_TIMEOUT,
) -> Optional[httpx.Response]:
    headers = headers or {}
    method = method.upper()
    print(f"[Sub-Req] {service_name} | {method} {url}")
    start = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            if method == "GET":
                resp = await client.get(url, headers=headers, params=json_body)
            else:
                resp = await client.request(method, url, headers=headers, json=json_body)
        elapsed = time.perf_counter() - start
        print(f"[Sub-Req] {service_name} done, {elapsed:.2f}s status={resp.status_code}")
        return resp
    except Exception as exc:
        print(f"[Sub-Req] {service_name} error: {exc!r}")
        return None


def pseudo_embedding(text: str, dim: int = 2048) -> List[float]:
    digest = hashlib.sha256(text.encode("utf-8", errors="ignore")).digest()
    vector = [0.0] * dim
    for i in range(dim):
        seed = digest[i % len(digest)] ^ ((i * 131) & 0xFF)
        vector[i] = (seed / 255.0) * 2.0 - 1.0
    return vector


async def get_embedding(text: str) -> List[float]:
    if not DOUBAO_API_KEY or not DOUBAO_EMBEDDING_MODEL:
        return pseudo_embedding(text, DASHVECTOR_DIMENSION)
    headers = {
        "Authorization": f"Bearer {DOUBAO_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {"model": DOUBAO_EMBEDDING_MODEL, "input": [{"type": "text", "text": text}]}
    resp = await safe_api_request("Doubao-Embedding", "POST", DOUBAO_EMBEDDING_URL, headers, body)
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            # 修复：Ark embedding API返回的data是数组
            emb_data = data.get("data", [])
            if isinstance(emb_data, list) and len(emb_data) > 0:
                return emb_data[0].get("embedding", [])
            elif isinstance(emb_data, dict):
                return emb_data.get("embedding", [])
            return []
        except Exception as exc:
            print(f"[Sub-Embedding] 解析失败: {exc!r}")
    return pseudo_embedding(text, DASHVECTOR_DIMENSION)


def dashvector_business_ok(resp: Optional[httpx.Response], label: str) -> bool:
    if resp is None:
        print(f"[DashVector-{label}] 未获得 HTTP 响应")
        return False
    print(f"[DashVector-{label}] HTTP={resp.status_code}, body={truncate_text(resp.text)}")
    if resp.status_code != 200:
        return False
    try:
        data = resp.json()
    except Exception:
        return False
    code = data.get("code")
    if code != 0 and str(code) != "0":
        message = data.get("message") or data.get("msg") or ""
        print(f"[DashVector-{label}] 业务失败 code={code!r}, message={message!r}")
        return False
    return True

# ===================== 鉴权依赖（旧版） =====================
async def verify_token(x_vector_token: Optional[str] = Header(None)) -> bool:
    if x_vector_token != SUB_VECTOR_API_TOKEN:
        raise HTTPException(status_code=403, detail="token invalid")
    return True

# -------------------------- 【旧版4个原始向量接口保留，兼容老调用】 --------------------------
@app.post("/api/vector/search_memory")
async def api_search_memory(_: bool = Depends(verify_token), payload: Dict[str, Any] = Body(...)):
    username = payload["username"]
    conv_id = payload["conversation_id"]
    query_text = payload["query_text"]
    top_k = int(payload.get("top_k", 2))
    vector = await get_embedding(query_text)
    url = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{DASHVECTOR_COLLECTION}/query"
    headers = {
        "dashvector-auth-token": DASHVECTOR_API_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "vector": vector,
        "topk": top_k,
        "include_metadata": True,
        "filter": f'conversation_id = "{conv_id}"',
    }
    resp = await safe_api_request("DashVector-search_memory", "POST", url, headers, body)
    if not dashvector_business_ok(resp, "search_memory"):
        return {"ok": True, "data": []}
    try:
        data = resp.json()
        output = data.get("result", {}).get("output", []) or data.get("output", []) or []
        items = []
        for item in output:
            if isinstance(item, dict):
                items.append(item.get("fields") or item.get("metadata") or {})
        return {"ok": True, "data": items}
    except Exception as exc:
        print(f"[Sub] search_memory 结果解析失败: {exc!r}")
        return {"ok": True, "data": []}


@app.post("/api/vector/insert_memory")
async def api_insert_memory(_: bool = Depends(verify_token), payload: Dict[str, Any] = Body(...)):
    username = payload["username"]
    role_id = payload["role_id"]
    conv_id = payload["conversation_id"]
    user_content = payload.get("user_content", "")
    summary = payload["summary"]
    affinity_delta = int(payload.get("affinity_delta", 0))
    vector = await get_embedding(summary)
    vector_len = len(vector) if isinstance(vector, list) else 0
    print(f"[Sub] insert_memory embedding 维度: {vector_len}")
    if not vector or vector_len != DASHVECTOR_DIMENSION:
        print(f"[Sub] insert_memory 向量维度不符，期望 {DASHVECTOR_DIMENSION}，实际 {vector_len}")
        return {"ok": False}
    url = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{DASHVECTOR_COLLECTION}/docs/upsert"
    headers = {
        "dashvector-auth-token": DASHVECTOR_API_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "docs": [
            {
                "id": str(uuid.uuid4()),
                "vector": vector,
                "fields": {
                    "username": username,
                    "role_id": role_id,
                    "conversation_id": conv_id,
                    "summary": summary,
                    "source": user_content[:500],
                    "affinity_delta": affinity_delta,
                    "created_at": time.time(),
                },
            }
        ],
    }
    resp = await safe_api_request("DashVector-insert_memory", "POST", url, headers, body)
    ok = dashvector_business_ok(resp, "insert_memory")
    return {"ok": ok}


@app.post("/api/vector/insert_session_vector")
async def api_insert_session_vector(_: bool = Depends(verify_token), payload: Dict[str, Any] = Body(...)):
    username = payload["username"]
    conv_id = payload["conversation_id"]
    role = payload["role"]
    text = payload["text"]
    vector = await get_embedding(text)
    vector_len = len(vector) if isinstance(vector, list) else 0
    print(f"[Sub] insert_session_vector embedding 维度: {vector_len}")
    if not vector or vector_len != DASHVECTOR_DIMENSION:
        print(f"[Sub] insert_session_vector 向量维度不符，期望 {DASHVECTOR_DIMENSION}，实际 {vector_len}")
        return {"ok": False}
    url = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{DASHVECTOR_SESSION_COLLECTION}/docs/upsert"
    headers = {
        "dashvector-auth-token": DASHVECTOR_API_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "docs": [
            {
                "id": str(uuid.uuid4()),
                "vector": vector,
                "fields": {
                    "username": username,
                    "conversation_id": conv_id,
                    "role": role,
                    "text": text,
                    "created_at": time.time(),
                },
            }
        ],
    }
    print(f"[DEBUG] insert_session_vector body: {json.dumps(body, ensure_ascii=False)}")
    resp = await safe_api_request("DashVector-insert_session_vector", "POST", url, headers, body)
    ok = dashvector_business_ok(resp, "insert_session_vector")
    return {"ok": ok}


@app.post("/api/vector/search_session_history")
async def api_search_session_history(_: bool = Depends(verify_token), payload: Dict[str, Any] = Body(...)):
    username = payload["username"]
    conv_id = payload["conversation_id"]
    query_text = payload["query_text"]
    top_k = int(payload.get("top_k", 3))
    vector = await get_embedding(query_text)
    url = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{DASHVECTOR_SESSION_COLLECTION}/query"
    headers = {
        "dashvector-auth-token": DASHVECTOR_API_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "vector": vector,
        "topk": top_k,
        "include_metadata": True,
        "filter": f'conversation_id = "{conv_id}"',
    }
    resp = await safe_api_request("DashVector-search_session_history", "POST", url, headers, body)
    if not dashvector_business_ok(resp, "search_session_history"):
        return {"ok": True, "data": []}
    try:
        data = resp.json()
        output = data.get("result", {}).get("output", []) or data.get("output", []) or []
        items = []
        for item in output:
            if isinstance(item, dict):
                items.append(item.get("fields") or item.get("metadata") or {})
        print(f"[DEBUG] search_session_history 返回 {len(items)} 条: {json.dumps(items, ensure_ascii=False)[:500]}")
        return {"ok": True, "data": items}
    except Exception as exc:
        print(f"[Sub] search_session_history 结果解析失败: {exc!r}")
        return {"ok": True, "data": []}


@app.get("/")
async def root() -> Dict[str, str]:
    return {"status": "ok", "service": "Sub-Vector-Service"}


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"status": "ok", "service": "vector_server", "version": "2.0.0", "port": str(PORT)}

# -------------------------- 【新版独有的全部代码：DeepSeek记忆分析 + /api/memory/* 接口全部保留】 --------------------------
DEEPSEEK_SYSTEM_PROMPT = (
    "你是一个对话分析助手。请分析以下对话，提取关键信息。\n"
    "\n"
    "请严格按以下 JSON 格式返回（不要返回其他内容）：\n"
    "\n"
    "{\n"
    '    "summary": "30-60字的简短记忆摘要，描述用户说了什么、透露了什么信息",\n'
    '    "intimacy_change": -5到5之间的整数，正面表示关系变好，负面表示关系变差\n'
    "}\n"
    "\n"
    "规则：\n"
    "- summary 要简洁，只记录对后续对话有参考价值的信息\n"
    "- intimacy_change 根据用户语气、态度来判断：友好热情=+1~3，冷漠攻击=-1~-3，特别亲密=+4~5，明显敌意=-4~-5"
)

async def analyze_memory_with_deepseek(
    user_message: str,
    assistant_reply: str,
    role_names: List[str],
    existing_memories: str
) -> Optional[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=60.0) as client:
        for attempt in range(3):
            try:
                headers = {
                    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                    "Content-Type": "application/json"
                }
                user_prompt = (
                    f"角色：{', '.join(role_names)}\n"
                    f"用户消息：{user_message}\n"
                    f"角色回复：{assistant_reply}\n"
                    f"已有记忆：{existing_memories if existing_memories else '无'}\n"
                    "\n请分析并返回 JSON。"
                )
                payload = {
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": DEEPSEEK_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 300,
                }
                response = await client.post(
                    f"{DEEPSEEK_BASE_URL}/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=60.0
                )
                if response.status_code == 200:
                    data = response.json()
                    content = data["choices"][0]["message"]["content"]
                    json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?\s*```', content, re.DOTALL)
                    if json_match:
                        content = json_match.group(1).strip()
                    result = json.loads(content)
                    result["intimacy_change"] = max(-5, min(5, result.get("intimacy_change", 0)))
                    return result
                else:
                    logger.warning(f"DeepSeek API 错误: {response.status_code}")
                    if attempt < 2:
                        await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"DeepSeek API 异常: {e}")
                if attempt < 2:
                    await asyncio.sleep(2)
    return None

class SearchMemoryRequest(BaseModel):
    user_id: str
    query: str
    top_k: int = 5
    role_id: str = ""
    conversation_id: str = ""

class SearchMemoryResponse(BaseModel):
    success: bool
    memories: List[Dict[str, Any]] = []
    context_text: str = ""

class AddMemoryRequest(BaseModel):
    user_id: str
    user_message: str
    assistant_reply: str
    role_names: List[str] = []
    role_id: str = ""
    conversation_id: str = ""

class AddMemoryResponse(BaseModel):
    success: bool
    summary: str = ""
    intimacy_change: int = 0

class ClearMemoryRequest(BaseModel):
    user_id: str

class AddDirectMemoryRequest(BaseModel):
    user_id: str
    content: str
    memory_type: str = "episodic"
    importance: int = 50
    reason: str = ""
    source: str = ""
    role_id: str = ""
    conversation_id: str = ""

class AddDirectMemoryResponse(BaseModel):
    success: bool
    summary: str = ""
    intimacy_change: int = 0

@app.post("/api/memory/search", response_model=SearchMemoryResponse)
async def search_memory(request: SearchMemoryRequest):
    try:
        query_embedding = await get_embedding(request.query)
        # 这里调用旧版底层能力，注意：旧版接口强依赖 conversation_id，新版这组接口没有conv_id，所以直接裸查向量
        url = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{DASHVECTOR_COLLECTION}/query"
        headers = {
            "dashvector-auth-token": DASHVECTOR_API_KEY,
            "Content-Type": "application/json",
        }
        # 按 user_id / role_id / conversation_id 构造过滤条件，避免多用户、多角色记忆串台
        filter_parts = []
        if request.user_id:
            filter_parts.append(f'username = "{request.user_id}"')
        if request.role_id:
            filter_parts.append(f'role_id = "{request.role_id}"')
        if request.conversation_id:
            filter_parts.append(f'conversation_id = "{request.conversation_id}"')
        body = {
            "vector": query_embedding,
            "topk": request.top_k,
            "include_metadata": True
        }
        if filter_parts:
            body["filter"] = " and ".join(filter_parts)
        resp = await safe_api_request("DashVector-search", "POST", url, headers, body)
        if not dashvector_business_ok(resp, "search"):
            return SearchMemoryResponse(success=False)
        data = resp.json()
        output = data.get("result", {}).get("output", []) or data.get("output", []) or []
        memories = []
        for item in output:
            memories.append(item.get("fields") or {})
        context_parts = [f"- {m.get('summary','')}" for m in memories]
        context_text = "\n".join(context_parts) if context_parts else ""
        return SearchMemoryResponse(success=True, memories=memories, context_text=context_text)
    except Exception as e:
        logger.error(f"记忆检索失败: {e}")
        return SearchMemoryResponse(success=False)


@app.post("/api/memory/add", response_model=AddMemoryResponse)
async def add_memory(request: AddMemoryRequest):
    try:
        existing_embedding = await get_embedding(request.user_message)
        # 查询已有记忆
        url = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{DASHVECTOR_COLLECTION}/query"
        headers = {
            "dashvector-auth-token": DASHVECTOR_API_KEY,
            "Content-Type": "application/json",
        }
        body = {
            "vector": existing_embedding,
            "topk":5,
            "include_metadata": True
        }
        # 查询已有记忆时按用户+角色过滤，避免把别人的记忆塞进 DeepSeek prompt
        existing_filter_parts = []
        if request.user_id:
            existing_filter_parts.append(f'username = "{request.user_id}"')
        if request.role_id:
            existing_filter_parts.append(f'role_id = "{request.role_id}"')
        if existing_filter_parts:
            body["filter"] = " and ".join(existing_filter_parts)
        resp_q = await safe_api_request("DashVector-query-existing", "POST", url, headers, body)
        existing_text = ""
        if dashvector_business_ok(resp_q,"query-existing"):
            q_data = resp_q.json()
            q_out = q_data.get("result",{}).get("output",[]) or q_data.get("output",[])
            existing_text = "\n".join([(x.get("fields") or {}).get("summary","") for x in q_out])

        analysis = await analyze_memory_with_deepseek(
            request.user_message, request.assistant_reply,
            request.role_names, existing_text
        )
        if analysis:
            summary = analysis.get("summary", "")
            intimacy_change = analysis.get("intimacy_change", 0)
            if summary:
                embedding = await get_embedding(summary)
                payload_insert = {
                    "username": request.user_id,
                    "role_id": request.role_id,
                    "conversation_id": request.conversation_id,
                    "user_content": request.user_message,
                    "summary": summary,
                    "affinity_delta": intimacy_change
                }
                # 直接调用内部逻辑，复用旧版写入逻辑
                vector_len = len(embedding) if isinstance(embedding, list) else 0
                if embedding and vector_len == DASHVECTOR_DIMENSION:
                    url_ins = f"{DASHVECTOR_ENDPOINT}/v1/collections/{DASHVECTOR_COLLECTION}/docs/upsert"
                    body_ins = {
                        "docs": [
                            {
                                "id": str(uuid.uuid4()),
                                "vector": embedding,
                                "fields": {
                                    "username": request.user_id,
                                    "role_id": request.role_id,
                                    "conversation_id": request.conversation_id,
                                    "summary": summary,
                                    "source": request.user_message[:500],
                                    "affinity_delta": intimacy_change,
                                    "created_at": time.time(),
                                },
                            }
                        ],
                    }
                    await safe_api_request("DashVector-insert_memory", "POST", url_ins, headers, body_ins)
            return AddMemoryResponse(success=True, summary=summary, intimacy_change=intimacy_change)
        else:
            return AddMemoryResponse(success=False)
    except Exception as e:
        logger.error(f"记忆添加失败: {e}")
        return AddMemoryResponse(success=False)



@app.post("/api/memory/add_direct", response_model=AddDirectMemoryResponse)
async def add_direct_memory(request: AddDirectMemoryRequest):
    """直存已分析好的记忆（人格后端memory_candidate），无需DeepSeek重复分析"""
    try:
        if not request.content or not request.content.strip():
            return AddDirectMemoryResponse(success=False)
        # importance映射到亲密度变化
        if request.importance >= 80:
            affinity = 3
        elif request.importance >= 60:
            affinity = 1
        else:
            affinity = 0
        embedding = await get_embedding(request.content)
        vector_len = len(embedding) if isinstance(embedding, list) else 0
        if not embedding or vector_len != DASHVECTOR_DIMENSION:
            return AddDirectMemoryResponse(success=False)
        url = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{DASHVECTOR_COLLECTION}/docs/upsert"
        headers = {
            "dashvector-auth-token": DASHVECTOR_API_KEY,
            "Content-Type": "application/json",
        }
        body = {
            "docs": [{
                "id": str(uuid.uuid4()),
                "vector": embedding,
                "fields": {
                    "username": request.user_id,
                    "role_id": request.role_id,
                    "conversation_id": request.conversation_id,
                    "summary": request.content,
                    "source": request.source[:500] if request.source else "",
                    "affinity_delta": affinity,
                    "memory_type": request.memory_type,
                    "importance": request.importance,
                    "reason": request.reason,
                    "created_at": time.time(),
                },
            }]
        }
        resp = await safe_api_request("DashVector-add_direct", "POST", url, headers, body)
        ok = dashvector_business_ok(resp, "add_direct")
        return AddDirectMemoryResponse(success=ok, summary=request.content, intimacy_change=affinity)
    except Exception as e:
        logger.error(f"记忆直存失败: {e}")
        return AddDirectMemoryResponse(success=False)



@app.post("/api/memory/migrate_user")
async def migrate_user(payload: Dict[str, Any] = Body(...)):
    """将 old_user_id 名下的所有记忆迁移到 new_user_id（QQ绑定账号时调用）"""
    old_uid = payload.get("old_user_id", "")
    new_uid = payload.get("new_user_id", "")
    if not old_uid or not new_uid or old_uid == new_uid:
        return {"success": False, "error": "参数无效"}
    migrated = 0
    headers = {
        "dashvector-auth-token": DASHVECTOR_API_KEY,
        "Content-Type": "application/json",
    }
    for collection in [DASHVECTOR_COLLECTION, DASHVECTOR_SESSION_COLLECTION]:
        # 用伪向量 + 过滤条件拉取旧用户的所有文档
        dummy_vector = pseudo_embedding(f"migrate_{old_uid}_{collection}", DASHVECTOR_DIMENSION)
        url_q = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{collection}/query"
        body_q = {
            "vector": dummy_vector,
            "topk": 1000,
            "include_metadata": True,
            "filter": f'username = "{old_uid}"',
        }
        resp_q = await safe_api_request("DashVector-migrate-query", "POST", url_q, headers, body_q)
        if not dashvector_business_ok(resp_q, "migrate-query"):
            continue
        try:
            q_data = resp_q.json()
            output = q_data.get("result", {}).get("output", []) or q_data.get("output", []) or []
        except Exception:
            output = []
        if not output:
            continue
        # 逐条重新 upsert，替换 username
        docs_to_upsert = []
        old_ids = []
        for item in output:
            fields = item.get("fields") or item.get("metadata") or {}
            vec = item.get("vector")
            if not vec:
                # 没有向量则用 summary/text 重新生成
                text_src = fields.get("summary") or fields.get("text") or ""
                if not text_src:
                    continue
                vec = await get_embedding(text_src)
            new_fields = dict(fields)
            new_fields["username"] = new_uid
            old_id = item.get("id") or item.get("docid")
            new_id = str(uuid.uuid4())
            if old_id:
                old_ids.append(old_id)
            docs_to_upsert.append({"id": new_id, "vector": vec, "fields": new_fields})
        if docs_to_upsert:
            url_ins = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{collection}/docs/upsert"
            body_ins = {"docs": docs_to_upsert}
            resp_ins = await safe_api_request("DashVector-migrate-upsert", "POST", url_ins, headers, body_ins)
            if dashvector_business_ok(resp_ins, "migrate-upsert"):
                migrated += len(docs_to_upsert)
                # 删除旧文档
                if old_ids:
                    url_del = f"{DASHVECTOR_ENDPOINT.rstrip('/')}/v1/collections/{collection}/docs/delete"
                    body_del = {"ids": old_ids}
                    await safe_api_request("DashVector-migrate-delete", "POST", url_del, headers, body_del)
    logger.info(f"记忆迁移: {old_uid} -> {new_uid}, 共 {migrated} 条")
    return {"success": True, "migrated": migrated}

@app.post("/api/memory/clear")
async def clear_memory(request: ClearMemoryRequest):
    return {"success": True, "message": "清除操作暂不支持（DashVector 需按 ID 删除）"}


if __name__ == "__main__":
    # P3 修复：启动前检测端口是否已被占用，避免与旧实例冲突导致无限崩溃重启
    import socket
    def _is_port_in_use(port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((HOST, port))
                return False
            except OSError:
                return True
    if _is_port_in_use(PORT):
        print(f"[vector_server] 端口 {PORT} 已被占用，可能已有实例在运行，正常退出。", flush=True)
        import sys
        sys.exit(0)  # 退出码0表示正常退出，launcher不会重启

    import uvicorn
    uvicorn.run("vector_server:app", host=HOST, port=PORT, workers=1, log_level="info")

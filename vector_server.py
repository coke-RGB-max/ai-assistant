"""
记忆后端 v3.0 - 端口 8001
Pinecone 版本：从 DashVector 迁移到 Pinecone Serverless
对接人格后端 v11.0：向量存储/检索、Pinecone、Embedding、DeepSeek记忆摘要分析

v3.0 改动：
- 底层向量库从 DashVector 换成 Pinecone Serverless
- 所有密钥/配置环境变量化
- 保留原有全部 API 接口，人格后端无需修改
- filter 语法从 DashVector 风格改为 Pinecone 风格
- 新增按 filter 直接删除能力（Pinecone 原生支持，不需要先查ID）
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

# Pinecone 官方 SDK
try:
    from pinecone import Pinecone
except ImportError:
    Pinecone = None  # 启动时会检查并提示安装

# ===================== 配置区（全部环境变量化 =====================
HOST = "0.0.0.0"
PORT = int(os.getenv("VECTOR_PORT", "8001"))
SUB_VECTOR_API_TOKEN = os.getenv("VECTOR_API_TOKEN", "change_me_strong_secret_key_123456")

# 豆包 Embedding 配置（保持不变）
DOUBAO_API_KEY = os.getenv("DOUBAO_API_KEY", "")
DOUBAO_EMBEDDING_URL = os.getenv("DOUBAO_EMBEDDING_URL", "https://ark.cn-beijing.volces.com/api/v3/embeddings/multimodal")
DOUBAO_EMBEDDING_MODEL = os.getenv("DOUBAO_EMBEDDING_MODEL", "ep-20260820233627-pjl5h")

# Pinecone 配置（关键信息全部环境变量）
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY", "")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "flexichrono")
# 会话历史用同一个 Index，靠 metadata 的 memory_type 字段区分
# （Pinecone 免费版最多5个Index，省着用；也可以单独建一个 Index，改这个变量名就行）
PINECONE_SESSION_INDEX_NAME = os.getenv("PINECONE_SESSION_INDEX_NAME", "flexichrono")
PINECONE_DIMENSION = 2048

# Namespace 配置：默认用空字符串（默认namespace），靠 filter 做用户隔离
# 如果用户数少（<100），可以改成按用户分 namespace，把 USE_NAMESPACE_PER_USER 设为 true
USE_NAMESPACE_PER_USER = os.getenv("USE_NAMESPACE_PER_USER", "false").lower() == "true"

REQUEST_TIMEOUT = 60
LOG_MAX_CHARS = 2000

# DeepSeek 配置（保持不变）
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", os.getenv("DOUBAO_API_KEY", ""))
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
DEEPSEEK_OFFICIAL_API_KEY = os.getenv("DEEPSEEK_OFFICIAL_API_KEY", "")
DEEPSEEK_OFFICIAL_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_OFFICIAL_MODEL = "deepseek-chat"
# ==========================================================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vector_server")

# Pinecone 客户端全局单例
_pinecone_client = None
_pinecone_index = None
_pinecone_session_index = None


def get_pinecone_index(session: bool = False):
    """获取 Pinecone Index 实例（懒加载）"""
    global _pinecone_client, _pinecone_index, _pinecone_session_index

    if not PINECONE_API_KEY:
        return None

    if _pinecone_client is None:
        if Pinecone is None:
            logger.error("❌ 未安装 pinecone SDK，请执行: pip install pinecone")
            return None
        _pinecone_client = Pinecone(api_key=PINECONE_API_KEY)

    index_name = PINECONE_SESSION_INDEX_NAME if session else PINECONE_INDEX_NAME

    if session:
        if _pinecone_session_index is None:
            _pinecone_session_index = _pinecone_client.Index(index_name)
        return _pinecone_session_index
    else:
        if _pinecone_index is None:
            _pinecone_index = _pinecone_client.Index(index_name)
        return _pinecone_index


def get_namespace(username: str = "") -> str:
    """根据配置决定 namespace：按用户分 或 统一默认namespace"""
    if USE_NAMESPACE_PER_USER and username:
        return f"user_{username}"
    return ""  # 默认 namespace


def build_pinecone_filter(username: str = "", role_id: str = "", conversation_id: str = "", extra: Dict = None) -> Dict:
    """
    构造 Pinecone filter 条件。
    Pinecone filter 语法示例: {"username": {"$eq": "xxx"}, "created_at": {"$lt": 1234567890}}
    支持 $eq, $ne, $in, $nin, $gt, $gte, $lt, $lte, $and, $or
    """
    conditions = []
    if username:
        conditions.append({"username": {"$eq": username}})
    if role_id:
        conditions.append({"role_id": {"$eq": role_id}})
    if conversation_id:
        conditions.append({"conversation_id": {"$eq": conversation_id}})
    if extra:
        for k, v in extra.items():
            conditions.append({k: v})

    if not conditions:
        return {}
    if len(conditions) == 1:
        return conditions[0]
    return {"$and": conditions}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🧠 记忆后端 v3.0 (Pinecone) 启动 - 端口 {PORT}")
    if not PINECONE_API_KEY:
        logger.warning("⚠️ PINECONE_API_KEY 未配置！向量功能不可用")
    else:
        # 预热连接，验证 Index 是否存在
        try:
            idx = get_pinecone_index()
            if idx:
                stats = idx.describe_index_stats()
                logger.info(f"✅ Pinecone 连接成功，Index: {PINECONE_INDEX_NAME}, 总向量数: {stats.get('total_vector_count', 0)}")
        except Exception as e:
            logger.error(f"❌ Pinecone 连接失败: {e}")
    if not DOUBAO_API_KEY:
        logger.warning("⚠️ DOUBAO_API_KEY 未配置！Embedding 将降级为伪向量模式")
    yield
    logger.info("记忆后端关闭")


app = FastAPI(title="Vector Server (Pinecone)", version="3.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------- 底层工具函数 --------------------------
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
    """通用 HTTP 请求封装（仅用于 Embedding 和 DeepSeek，Pinecone 走 SDK）"""
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


import hashlib


def pseudo_embedding(text: str, dim: int = 2048) -> List[float]:
    digest = hashlib.sha256(text.encode("utf-8", errors="ignore")).digest()
    vector = [0.0] * dim
    for i in range(dim):
        seed = digest[i % len(digest)] ^ ((i * 131) & 0xFF)
        vector[i] = (seed / 255.0) * 2.0 - 1.0
    return vector


async def get_embedding(text: str) -> List[float]:
    """获取文本向量（豆包 Embedding，失败降级为伪向量）"""
    if not DOUBAO_API_KEY or not DOUBAO_EMBEDDING_MODEL:
        return pseudo_embedding(text, PINECONE_DIMENSION)
    headers = {
        "Authorization": f"Bearer {DOUBAO_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {"model": DOUBAO_EMBEDDING_MODEL, "input": [{"type": "text", "text": text}]}
    resp = await safe_api_request("Doubao-Embedding", "POST", DOUBAO_EMBEDDING_URL, headers, body)
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            emb_data = data.get("data", [])
            if isinstance(emb_data, list) and len(emb_data) > 0:
                return emb_data[0].get("embedding", [])
            elif isinstance(emb_data, dict):
                return emb_data.get("embedding", [])
            return []
        except Exception as exc:
            print(f"[Sub-Embedding] 解析失败: {exc!r}")
    return pseudo_embedding(text, PINECONE_DIMENSION)


def pinecone_upsert(index, vectors: List[Dict], namespace: str = "") -> bool:
    """Pinecone 批量写入封装"""
    if index is None:
        logger.error("[Pinecone] Index 未初始化，写入失败")
        return False
    try:
        index.upsert(vectors=vectors, namespace=namespace)
        return True
    except Exception as e:
        logger.error(f"[Pinecone] upsert 失败: {e}")
        return False


def pinecone_query(index, vector: List[float], top_k: int = 5, filter: Dict = None, namespace: str = "") -> List[Dict]:
    """Pinecone 检索封装，返回 metadata 列表"""
    if index is None:
        return []
    try:
        results = index.query(
            vector=vector,
            top_k=top_k,
            include_metadata=True,
            filter=filter if filter else None,
            namespace=namespace,
        )
        matches = results.get("matches", [])
        return [m.get("metadata", {}) for m in matches]
    except Exception as e:
        logger.error(f"[Pinecone] query 失败: {e}")
        return []


def pinecone_delete(index, ids: List[str] = None, filter: Dict = None, namespace: str = "", delete_all: bool = False) -> bool:
    """Pinecone 删除封装：支持按ID删、按filter删、清空namespace"""
    if index is None:
        return False
    try:
        if delete_all:
            index.delete(delete_all=True, namespace=namespace)
        elif ids:
            index.delete(ids=ids, namespace=namespace)
        elif filter:
            index.delete(filter=filter, namespace=namespace)
        return True
    except Exception as e:
        logger.error(f"[Pinecone] delete 失败: {e}")
        return False


# ===================== 鉴权依赖 =====================
async def verify_token(x_vector_token: Optional[str] = Header(None)) -> bool:
    if x_vector_token != SUB_VECTOR_API_TOKEN:
        raise HTTPException(status_code=403, detail="token invalid")
    return True


# -------------------------- 旧版4个原始向量接口（保持兼容） --------------------------
@app.post("/api/vector/search_memory")
async def api_search_memory(_: bool = Depends(verify_token), payload: Dict[str, Any] = Body(...)):
    username = payload["username"]
    conv_id = payload["conversation_id"]
    query_text = payload["query_text"]
    top_k = int(payload.get("top_k", 2))

    vector = await get_embedding(query_text)
    index = get_pinecone_index()
    namespace = get_namespace(username)
    filter_cond = build_pinecone_filter(conversation_id=conv_id)

    items = pinecone_query(index, vector, top_k=top_k, filter=filter_cond, namespace=namespace)
    return {"ok": True, "data": items}


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

    if not vector or vector_len != PINECONE_DIMENSION:
        print(f"[Sub] insert_memory 向量维度不符，期望 {PINECONE_DIMENSION}，实际 {vector_len}")
        return {"ok": False}

    index = get_pinecone_index()
    namespace = get_namespace(username)
    doc_id = str(uuid.uuid4())

    vectors = [{
        "id": doc_id,
        "values": vector,
        "metadata": {
            "username": username,
            "role_id": role_id,
            "conversation_id": conv_id,
            "summary": summary,
            "source": user_content[:500],
            "affinity_delta": affinity_delta,
            "memory_type": "summary",
            "created_at": time.time(),
            "last_accessed": time.time(),
            "access_count": 0,
        },
    }]

    ok = pinecone_upsert(index, vectors, namespace=namespace)
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

    if not vector or vector_len != PINECONE_DIMENSION:
        print(f"[Sub] insert_session_vector 向量维度不符，期望 {PINECONE_DIMENSION}，实际 {vector_len}")
        return {"ok": False}

    index = get_pinecone_index(session=True)
    namespace = get_namespace(username)
    doc_id = str(uuid.uuid4())

    vectors = [{
        "id": doc_id,
        "values": vector,
        "metadata": {
            "username": username,
            "conversation_id": conv_id,
            "role": role,
            "text": text,
            "memory_type": "session",
            "created_at": time.time(),
            # 会话片段7天后过期（由清理任务删除）
            "expire_at": time.time() + 7 * 24 * 3600,
        },
    }]

    print(f"[DEBUG] insert_session_vector metadata: {json.dumps(vectors[0]['metadata'], ensure_ascii=False)[:500]}")
    ok = pinecone_upsert(index, vectors, namespace=namespace)
    return {"ok": ok}


@app.post("/api/vector/search_session_history")
async def api_search_session_history(_: bool = Depends(verify_token), payload: Dict[str, Any] = Body(...)):
    username = payload["username"]
    conv_id = payload["conversation_id"]
    query_text = payload["query_text"]
    top_k = int(payload.get("top_k", 3))

    vector = await get_embedding(query_text)
    index = get_pinecone_index(session=True)
    namespace = get_namespace(username)
    filter_cond = build_pinecone_filter(conversation_id=conv_id)

    items = pinecone_query(index, vector, top_k=top_k, filter=filter_cond, namespace=namespace)
    print(f"[DEBUG] search_session_history 返回 {len(items)} 条: {json.dumps(items, ensure_ascii=False)[:500]}")
    return {"ok": True, "data": items}


@app.get("/")
async def root() -> Dict[str, str]:
    return {"status": "ok", "service": "Vector-Server-Pinecone"}


@app.get("/health")
async def health() -> Dict[str, Any]:
    index = get_pinecone_index()
    total_vectors = 0
    if index:
        try:
            stats = index.describe_index_stats()
            total_vectors = stats.get("total_vector_count", 0)
        except Exception:
            pass
    return {
        "status": "ok",
        "service": "vector_server",
        "version": "3.0.0-pinecone",
        "port": str(PORT),
        "pinecone_index": PINECONE_INDEX_NAME,
        "total_vectors": total_vectors,
    }


# -------------------------- 新版：DeepSeek记忆分析 + /api/memory/* 接口 --------------------------
DEEPSEEK_SYSTEM_PROMPT = (
    "你是一个对话分析助手。请分析以下对话，提取关键信息。\n"
    "\n"
    "请严格按以下 JSON 格式返回（不要返回其他内容）：\n"
    "\n"
    "{\n"
    '    "summary": "30-60字的简短记忆摘要，描述用户说了什么、透露了什么信息",\n'
    '    "intimacy_change": -5到5之间的整数，正面表示关系变好，负面表示关系变差,\n'
    '    "importance": 0到100之间的整数，表示这条记忆的重要程度（用户偏好/个人信息=80+，日常闲聊=30以下）\n'
    "}\n"
    "\n"
    "规则：\n"
    "- summary 要简洁，只记录对后续对话有参考价值的信息\n"
    "- intimacy_change 根据用户语气、态度来判断：友好热情=+1~3，冷漠攻击=-1~-3，特别亲密=+4~5，明显敌意=-4~-5\n"
    "- importance：用户明确说的个人信息、偏好、重要事件给80-100；一般话题50-70；纯闲聊10-30"
)


async def analyze_memory_with_deepseek(
    user_message: str,
    assistant_reply: str,
    role_names: List[str],
    existing_memories: str
) -> Optional[Dict[str, Any]]:
    """优先用火山方舟的 DeepSeek API，失败时 fallback 到官方 API。"""
    user_prompt = (
        f"角色：{', '.join(role_names)}\n"
        f"用户消息：{user_message}\n"
        f"角色回复：{assistant_reply}\n"
        f"已有记忆：{existing_memories if existing_memories else '无'}\n"
        "\n请分析并返回 JSON。"
    )

    endpoints = [
        {"name": "火山方舟", "base_url": DEEPSEEK_BASE_URL, "api_key": DEEPSEEK_API_KEY, "model": DEEPSEEK_MODEL},
    ]
    if DEEPSEEK_OFFICIAL_API_KEY:
        endpoints.append({"name": "DeepSeek官方", "base_url": DEEPSEEK_OFFICIAL_BASE_URL,
                          "api_key": DEEPSEEK_OFFICIAL_API_KEY, "model": DEEPSEEK_OFFICIAL_MODEL})

    async with httpx.AsyncClient(timeout=60.0) as client:
        for endpoint in endpoints:
            for attempt in range(2):
                try:
                    headers = {
                        "Authorization": f"Bearer {endpoint['api_key']}",
                        "Content-Type": "application/json"
                    }
                    payload = {
                        "model": endpoint["model"],
                        "messages": [
                            {"role": "system", "content": DEEPSEEK_SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt}
                        ],
                        "temperature": 0.3,
                        "max_tokens": 300,
                    }
                    response = await client.post(
                        f"{endpoint['base_url'].rstrip('/')}/chat/completions",
                        headers=headers,
                        json=payload,
                        timeout=60.0
                    )
                    if response.status_code == 200:
                        data = response.json()
                        resp_content = data["choices"][0]["message"]["content"]
                        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?\s*```', resp_content, re.DOTALL)
                        if json_match:
                            resp_content = json_match.group(1).strip()
                        result = json.loads(resp_content)
                        result["intimacy_change"] = max(-5, min(5, result.get("intimacy_change", 0)))
                        result["importance"] = max(0, min(100, result.get("importance", 50)))
                        logger.info(f"[DeepSeek] {endpoint['name']} 记忆分析成功, importance={result['importance']}")
                        return result
                    else:
                        logger.warning(f"[DeepSeek] {endpoint['name']} API 错误: {response.status_code} {response.text[:200]}")
                        if attempt < 1:
                            await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"[DeepSeek] {endpoint['name']} API 异常: {type(e).__name__}: {e}")
                    if attempt < 1:
                        await asyncio.sleep(2)
            if endpoint != endpoints[-1]:
                logger.warning(f"[DeepSeek] {endpoint['name']} 失败，尝试下一个端点...")
        logger.warning("[DeepSeek] 所有端点都失败，返回 None")
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
    role_id: str = ""


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
        index = get_pinecone_index()
        namespace = get_namespace(request.user_id)
        filter_cond = build_pinecone_filter(
            username=request.user_id,
            role_id=request.role_id,
            conversation_id=request.conversation_id,
        )

        memories = pinecone_query(index, query_embedding, top_k=request.top_k, filter=filter_cond, namespace=namespace)

        # 异步更新命中记忆的访问时间（不阻塞返回）
        if memories:
            asyncio.create_task(_update_access_time(memories, namespace))

        context_parts = [f"- {m.get('summary', '')}" for m in memories]
        context_text = "\n".join(context_parts) if context_parts else ""
        return SearchMemoryResponse(success=True, memories=memories, context_text=context_text)
    except Exception as e:
        logger.error(f"记忆检索失败: {e}")
        return SearchMemoryResponse(success=False)


async def _update_access_time(memories: List[Dict], namespace: str):
    """后台更新记忆的最后访问时间和访问次数（Pinecone 没有部分更新，需要重新upsert）"""
    index = get_pinecone_index()
    if index is None or not memories:
        return
    try:
        # Pinecone 更新 metadata 需要重新 upsert 整个向量
        # 注意：这里只更新 metadata，向量值从原始数据里取（如果有的话）
        # 由于 query 时 include_values 默认 False，我们拿不到向量，所以这里只做计数日志
        # 真正的访问时间更新建议在 cleanup 任务里批量做
        logger.info(f"[Access-Track] 命中 {len(memories)} 条记忆，访问时间待批量更新")
    except Exception as e:
        logger.error(f"[Access-Track] 更新失败: {e}")


@app.post("/api/memory/add", response_model=AddMemoryResponse)
async def add_memory(request: AddMemoryRequest):
    try:
        existing_embedding = await get_embedding(request.user_message)
        index = get_pinecone_index()
        namespace = get_namespace(request.user_id)

        # 查询已有记忆（给 DeepSeek 做参考）
        existing_filter = build_pinecone_filter(username=request.user_id, role_id=request.role_id)
        existing_memories = pinecone_query(index, existing_embedding, top_k=5, filter=existing_filter, namespace=namespace)
        existing_text = "\n".join([m.get("summary", "") for m in existing_memories])

        analysis = await analyze_memory_with_deepseek(
            request.user_message, request.assistant_reply,
            request.role_names, existing_text
        )

        if analysis:
            summary = analysis.get("summary", "")
            intimacy_change = analysis.get("intimacy_change", 0)
            importance = analysis.get("importance", 50)

            if summary:
                embedding = await get_embedding(summary)
                vector_len = len(embedding) if isinstance(embedding, list) else 0

                if embedding and vector_len == PINECONE_DIMENSION:
                    doc_id = str(uuid.uuid4())
                    vectors = [{
                        "id": doc_id,
                        "values": embedding,
                        "metadata": {
                            "username": request.user_id,
                            "role_id": request.role_id,
                            "conversation_id": request.conversation_id,
                            "summary": summary,
                            "source": request.user_message[:500],
                            "affinity_delta": intimacy_change,
                            "memory_type": "summary",
                            "importance": importance,
                            "created_at": time.time(),
                            "last_accessed": time.time(),
                            "access_count": 0,
                        },
                    }]
                    pinecone_upsert(index, vectors, namespace=namespace)

            return AddMemoryResponse(success=True, summary=summary, intimacy_change=intimacy_change)
        else:
            return AddMemoryResponse(success=False)
    except Exception as e:
        logger.error(f"记忆添加失败: {e}")
        return AddMemoryResponse(success=False)


@app.post("/api/memory/add_direct", response_model=AddDirectMemoryResponse)
async def add_direct_memory(request: AddDirectMemoryRequest):
    """直存已分析好的记忆，无需DeepSeek重复分析"""
    try:
        if not request.content or not request.content.strip():
            return AddDirectMemoryResponse(success=False)

        if request.importance >= 80:
            affinity = 3
        elif request.importance >= 60:
            affinity = 1
        else:
            affinity = 0

        embedding = await get_embedding(request.content)
        vector_len = len(embedding) if isinstance(embedding, list) else 0
        if not embedding or vector_len != PINECONE_DIMENSION:
            return AddDirectMemoryResponse(success=False)

        index = get_pinecone_index()
        namespace = get_namespace(request.user_id)
        doc_id = str(uuid.uuid4())

        vectors = [{
            "id": doc_id,
            "values": embedding,
            "metadata": {
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
                "last_accessed": time.time(),
                "access_count": 0,
            },
        }]

        ok = pinecone_upsert(index, vectors, namespace=namespace)
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
    index = get_pinecone_index()
    if index is None:
        return {"success": False, "error": "Pinecone 未连接"}

    # Pinecone 没有按 filter 拉取全量的接口，需要用 query + 大 top_k 近似拉取
    # 注意：这是近似迁移，特别老的记忆可能漏，生产环境建议用分页 fetch
    dummy_vector = pseudo_embedding(f"migrate_{old_uid}", PINECONE_DIMENSION)
    old_namespace = get_namespace(old_uid)
    new_namespace = get_namespace(new_uid)

    try:
        # 拉取旧用户的所有记忆（topk 设大一点，Pinecone 最大 10000）
        results = index.query(
            vector=dummy_vector,
            top_k=5000,
            include_metadata=True,
            include_values=True,
            filter={"username": {"$eq": old_uid}},
            namespace=old_namespace,
        )
        matches = results.get("matches", [])

        if not matches:
            return {"success": True, "migrated": 0, "note": "旧用户无记忆"}

        # 批量写入新用户
        new_vectors = []
        old_ids = []
        for m in matches:
            metadata = dict(m.get("metadata", {}))
            metadata["username"] = new_uid
            new_vectors.append({
                "id": str(uuid.uuid4()),
                "values": m.get("values", []),
                "metadata": metadata,
            })
            old_ids.append(m["id"])

        # 分批写入（每批 100 条）
        for i in range(0, len(new_vectors), 100):
            batch = new_vectors[i:i+100]
            index.upsert(vectors=batch, namespace=new_namespace)
            migrated += len(batch)

        # 删除旧数据
        for i in range(0, len(old_ids), 100):
            index.delete(ids=old_ids[i:i+100], namespace=old_namespace)

        logger.info(f"记忆迁移: {old_uid} -> {new_uid}, 共 {migrated} 条")
        return {"success": True, "migrated": migrated}
    except Exception as e:
        logger.error(f"记忆迁移失败: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/memory/clear")
async def clear_memory(request: ClearMemoryRequest):
    """清除指定用户的所有记忆（Pinecone 支持按 filter 直接删除，不需要先查ID）"""
    index = get_pinecone_index()
    if index is None:
        return {"success": False, "error": "Pinecone 未连接"}

    namespace = get_namespace(request.user_id)
    filter_cond = build_pinecone_filter(username=request.user_id, role_id=request.role_id)

    try:
        # 如果按用户分 namespace，直接清空整个 namespace 更快
        if USE_NAMESPACE_PER_USER:
            index.delete(delete_all=True, namespace=namespace)
            deleted = "namespace"
        else:
            # 否则按 filter 删除
            index.delete(filter=filter_cond, namespace=namespace)
            deleted = "filter"

        # 同时清理会话库
        session_index = get_pinecone_index(session=True)
        if session_index:
            if USE_NAMESPACE_PER_USER:
                session_index.delete(delete_all=True, namespace=namespace)
            else:
                session_index.delete(filter=filter_cond, namespace=namespace)

        logger.info(f"记忆清除: user={request.user_id}, 方式={deleted}")
        return {"success": True, "message": f"已清除用户 {request.user_id} 的记忆", "method": deleted}
    except Exception as e:
        logger.error(f"记忆清除失败: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/memory/delete")
async def delete_memory(payload: Dict[str, Any] = Body(...)):
    """按 ID 列表删除指定记忆"""
    user_id = payload.get("user_id", "")
    memory_ids = payload.get("memory_ids", [])
    if not memory_ids:
        return {"success": False, "error": "memory_ids 不能为空"}

    index = get_pinecone_index()
    if index is None:
        return {"success": False, "error": "Pinecone 未连接"}

    namespace = get_namespace(user_id)
    ok = pinecone_delete(index, ids=memory_ids, namespace=namespace)
    return {"success": ok, "deleted": len(memory_ids) if ok else 0}


@app.post("/api/memory/cleanup_expired")
async def cleanup_expired(payload: Dict[str, Any] = Body(...)):
    """
    清理过期记忆（由外部定时任务每天调用）
    1. 会话库：删除 expire_at < now 的片段
    2. 主记忆库：删除 last_accessed < 30天前 AND importance < 30 的冷记忆
    """
    user_id = payload.get("user_id", "")  # 不传则清理所有用户
    index = get_pinecone_index()
    session_index = get_pinecone_index(session=True)
    if index is None:
        return {"success": False, "error": "Pinecone 未连接"}

    namespace = get_namespace(user_id) if user_id else ""
    now = time.time()
    thirty_days_ago = now - 30 * 24 * 3600
    deleted_session = 0
    deleted_cold = 0

    try:
        # 1. 清理过期会话片段
        if session_index:
            session_filter = {"expire_at": {"$lt": now}}
            if user_id:
                session_filter = {"$and": [session_filter, {"username": {"$eq": user_id}}]}
            # Pinecone delete by filter 直接删，不需要先查
            session_index.delete(filter=session_filter, namespace=namespace)
            deleted_session = -1  # filter 删除不返回具体数量，记为 -1 表示已执行

        # 2. 清理冷记忆（30天未访问 + 低重要性）
        cold_filter = {
            "$and": [
                {"last_accessed": {"$lt": thirty_days_ago}},
                {"importance": {"$lt": 30}},
            ]
        }
        if user_id:
            cold_filter["$and"].append({"username": {"$eq": user_id}})
        index.delete(filter=cold_filter, namespace=namespace)
        deleted_cold = -1

        logger.info(f"过期清理完成: 会话={deleted_session}, 冷记忆={deleted_cold}")
        return {
            "success": True,
            "deleted_session": deleted_session,
            "deleted_cold_memory": deleted_cold,
            "note": "-1 表示按 filter 执行了删除，Pinecone 不返回具体删除数量",
        }
    except Exception as e:
        logger.error(f"过期清理失败: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/memory/stats")
async def memory_stats(user_id: str = ""):
    """查看记忆统计信息"""
    index = get_pinecone_index()
    if index is None:
        return {"success": False, "error": "Pinecone 未连接"}

    try:
        stats = index.describe_index_stats()
        total = stats.get("total_vector_count", 0)
        namespaces = stats.get("namespaces", {})

        result = {
            "success": True,
            "index": PINECONE_INDEX_NAME,
            "total_vectors": total,
            "namespace_count": len(namespaces),
        }

        if user_id:
            namespace = get_namespace(user_id)
            ns_info = namespaces.get(namespace, {})
            result["user"] = user_id
            result["user_vector_count"] = ns_info.get("vector_count", 0)

        return result
    except Exception as e:
        logger.error(f"统计获取失败: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    # 启动前检测端口是否已被占用
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
        sys.exit(0)

    import uvicorn
    uvicorn.run("vector_server:app", host=HOST, port=PORT, workers=1, log_level="info")

"""
MongoDB layer for X Profile Scraper — async via motor.

Collections:
    jobs             — job lifecycle tracking + distributed lock
    profile_results  — per-username results (incremental writes, batch-flushed)
    profile_cache    — TTL cache for recently fetched profiles

Multi-worker safe: jobs use atomic findOneAndUpdate for ownership transfer.
"""

import asyncio
import logging
import os
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Set

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

_PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(_PROJECT_ROOT / ".env")

logger = logging.getLogger("x_scraper")

_client: Optional[AsyncIOMotorClient] = None
_db: Optional[AsyncIOMotorDatabase] = None

MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://127.0.0.1:27017/")
MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "x_scraper")
PROFILE_CACHE_TTL: int = int(os.getenv("PROFILE_CACHE_TTL", "3600"))
JOB_HEARTBEAT_TIMEOUT: int = int(os.getenv("JOB_HEARTBEAT_TIMEOUT", "60"))

WORKER_ID = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def init_db() -> AsyncIOMotorDatabase:
    """初始化 Motor 客户端并创建索引。server startup 时调用。"""
    global _client, _db
    uri = MONGODB_URI or os.getenv("MONGODB_URI", "mongodb://127.0.0.1:27017/")
    db_name = MONGODB_DATABASE or os.getenv("MONGODB_DATABASE", "x_scraper")
    _client = AsyncIOMotorClient(uri)
    _db = _client[db_name]
    logger.info("MongoDB connected: %s / %s (worker=%s)", uri.split("@")[-1], db_name, WORKER_ID)

    # indexes
    await _db.jobs.create_index(
        [("status", 1), ("created_at", -1)],
        background=True,
    )
    await _db.jobs.create_index(
        [("status", 1), ("heartbeat_at", 1)],
        background=True,
    )
    await _db.profile_results.create_index(
        [("job_id", 1), ("username", 1)],
        unique=True,
        background=True,
    )
    await _db.profile_results.create_index(
        [("job_id", 1), ("status", 1)],
        background=True,
    )
    await _db.profile_results.create_index(
        [("job_id", 1), ("fetched_at", 1)],
        background=True,
    )
    ttl = PROFILE_CACHE_TTL or int(os.getenv("PROFILE_CACHE_TTL", "3600"))
    await _db.profile_cache.create_index(
        "cached_at",
        expireAfterSeconds=ttl,
        background=True,
    )
    logger.info("MongoDB indexes ensured (cache TTL=%ds)", ttl)
    return _db


def get_db() -> AsyncIOMotorDatabase:
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db


async def close_db():
    global _client, _db
    if _client:
        _client.close()
        _client = None
        _db = None


# ───────────────── Jobs ─────────────────


async def create_job(usernames: List[str], concurrency: int) -> str:
    """创建 job，返回 job_id。usernames 已去重。"""
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    doc = {
        "_id": job_id,
        "status": "submitted",
        "usernames": usernames,
        "total": len(usernames),
        "done": 0,
        "failed": 0,
        "concurrency": concurrency,
        "owner": None,
        "heartbeat_at": None,
        "created_at": _utcnow(),
        "started_at": None,
        "completed_at": None,
        "error": None,
    }
    await get_db().jobs.insert_one(doc)
    logger.info("job created: %s total=%d concurrency=%d", job_id, len(usernames), concurrency)
    return job_id


async def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return await get_db().jobs.find_one({"_id": job_id})


async def list_jobs(
    status: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    query = {}
    if status:
        query["status"] = status
    cursor = get_db().jobs.find(query).sort("created_at", -1).limit(limit)
    return await cursor.to_list(length=limit)


async def update_job(job_id: str, **fields) -> None:
    """更新 job 任意字段。"""
    await get_db().jobs.update_one({"_id": job_id}, {"$set": fields})


async def increment_job_progress(job_id: str, success_delta: int = 0, fail_delta: int = 0) -> None:
    """原子递增 done/failed 计数（支持批量）。"""
    inc: Dict[str, int] = {}
    if success_delta or fail_delta:
        inc["done"] = success_delta + fail_delta
    if fail_delta:
        inc["failed"] = fail_delta
    if not inc:
        return
    await get_db().jobs.update_one({"_id": job_id}, {"$inc": inc})


async def claim_job(job_id: str) -> bool:
    """尝试占用一个 submitted 的 job，原子操作。
    成功返回 True（本 worker 拥有），失败返回 False（其他 worker 已抢到）。
    """
    res = await get_db().jobs.find_one_and_update(
        {"_id": job_id, "status": "submitted"},
        {"$set": {
            "status": "running",
            "owner": WORKER_ID,
            "started_at": _utcnow(),
            "heartbeat_at": _utcnow(),
        }},
    )
    return res is not None


async def claim_orphaned_job() -> Optional[Dict[str, Any]]:
    """抢占一个心跳超时的 running job（用于多 worker 场景下其他 worker 崩溃）。
    
    返回被抢占的 job doc，无可用则返回 None。
    """
    cutoff = _utcnow() - timedelta(seconds=JOB_HEARTBEAT_TIMEOUT)
    res = await get_db().jobs.find_one_and_update(
        {
            "status": "running",
            "$or": [
                {"heartbeat_at": {"$lt": cutoff}},
                {"heartbeat_at": None},
            ],
        },
        {"$set": {
            "owner": WORKER_ID,
            "heartbeat_at": _utcnow(),
        }},
        return_document=True,
    )
    return res


async def claim_next_submitted_job() -> Optional[Dict[str, Any]]:
    """抢占下一个 submitted 状态的 job（多 worker 任务分发）。"""
    res = await get_db().jobs.find_one_and_update(
        {"status": "submitted"},
        {"$set": {
            "status": "running",
            "owner": WORKER_ID,
            "started_at": _utcnow(),
            "heartbeat_at": _utcnow(),
        }},
        sort=[("created_at", 1)],
        return_document=True,
    )
    return res


async def heartbeat_job(job_id: str) -> bool:
    """更新 job 心跳，如果 owner 不是本 worker 则失败（说明被抢占）。"""
    res = await get_db().jobs.update_one(
        {"_id": job_id, "owner": WORKER_ID},
        {"$set": {"heartbeat_at": _utcnow()}},
    )
    return res.modified_count > 0


async def find_my_jobs() -> List[Dict[str, Any]]:
    """查询当前 worker 拥有的 running jobs。"""
    cursor = get_db().jobs.find({"status": "running", "owner": WORKER_ID}).sort("created_at", 1)
    return await cursor.to_list(length=1000)


# ───────────────── Profile Results ─────────────────


async def save_profile_result(
    job_id: str,
    username: str,
    profile: Optional[Dict] = None,
    error: Optional[str] = None,
) -> None:
    """Upsert 单条 profile 结果（保留单条接口供 fallback 使用）。"""
    status = "success" if profile else "failed"
    await get_db().profile_results.update_one(
        {"job_id": job_id, "username": username},
        {"$set": {
            "status": status,
            "profile": profile,
            "error": error,
            "fetched_at": _utcnow(),
        }},
        upsert=True,
    )


async def save_profile_results_bulk(
    job_id: str,
    items: List[Dict[str, Any]],
) -> None:
    """批量 upsert profile 结果。
    
    每个 item: {"username": str, "profile": dict|None, "error": str|None}
    """
    if not items:
        return
    from pymongo import UpdateOne
    now = _utcnow()
    ops = []
    for it in items:
        username = it["username"]
        profile = it.get("profile")
        error = it.get("error")
        status = "success" if profile else "failed"
        ops.append(
            UpdateOne(
                {"job_id": job_id, "username": username},
                {"$set": {
                    "status": status,
                    "profile": profile,
                    "error": error,
                    "fetched_at": now,
                }},
                upsert=True,
            )
        )
    await get_db().profile_results.bulk_write(ops, ordered=False)


async def get_completed_usernames(job_id: str) -> Set[str]:
    """获取已完成的 username 集合（续传用）。"""
    cursor = get_db().profile_results.find(
        {"job_id": job_id},
        {"username": 1, "_id": 0},
    )
    docs = await cursor.to_list(length=None)
    return {d["username"] for d in docs}


async def get_job_results(
    job_id: str,
    skip: int = 0,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """分页获取 job 结果。"""
    cursor = (
        get_db()
        .profile_results.find({"job_id": job_id}, {"_id": 0})
        .skip(skip)
        .limit(limit)
    )
    return await cursor.to_list(length=limit)


async def count_job_results(job_id: str) -> int:
    return await get_db().profile_results.count_documents({"job_id": job_id})


async def get_new_results_since(
    job_id: str,
    after: datetime,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """获取某时间点之后的新结果（SSE 轮询 fallback 用）。"""
    cursor = (
        get_db()
        .profile_results.find(
            {"job_id": job_id, "fetched_at": {"$gt": after}},
            {"_id": 0},
        )
        .sort("fetched_at", 1)
        .limit(limit)
    )
    return await cursor.to_list(length=limit)


async def watch_job_results(job_id: str) -> AsyncIterator[Dict[str, Any]]:
    """通过 Mongo Change Streams 订阅 job 新结果（实时推送）。
    
    需要 MongoDB 是 replica set 或 sharded cluster；standalone 不支持。
    若不支持则抛 OperationFailure，调用方应回退到轮询。
    """
    pipeline = [
        {"$match": {
            "operationType": {"$in": ["insert", "update"]},
            "fullDocument.job_id": job_id,
        }},
    ]
    async with get_db().profile_results.watch(
        pipeline,
        full_document="updateLookup",
    ) as stream:
        async for change in stream:
            doc = change.get("fullDocument")
            if doc:
                doc.pop("_id", None)
                yield doc


# ───────────────── Profile Cache ─────────────────


async def get_cached_profile(username: str) -> Optional[Dict[str, Any]]:
    doc = await get_db().profile_cache.find_one({"_id": username})
    if doc:
        return doc.get("profile")
    return None


async def set_cached_profile(username: str, profile: Dict[str, Any]) -> None:
    await get_db().profile_cache.update_one(
        {"_id": username},
        {"$set": {"profile": profile, "cached_at": _utcnow()}},
        upsert=True,
    )


async def get_cached_profiles_bulk(usernames: List[str]) -> Dict[str, Dict[str, Any]]:
    """批量查询缓存，返回 {username: profile} 字典。"""
    if not usernames:
        return {}
    cursor = get_db().profile_cache.find({"_id": {"$in": usernames}})
    out: Dict[str, Dict[str, Any]] = {}
    async for doc in cursor:
        if doc.get("profile"):
            out[doc["_id"]] = doc["profile"]
    return out


async def set_cached_profiles_bulk(items: List[Dict[str, Any]]) -> None:
    """批量写缓存。每个 item: {"username": str, "profile": dict}"""
    if not items:
        return
    from pymongo import UpdateOne
    now = _utcnow()
    ops = [
        UpdateOne(
            {"_id": it["username"]},
            {"$set": {"profile": it["profile"], "cached_at": now}},
            upsert=True,
        )
        for it in items if it.get("profile")
    ]
    if ops:
        await get_db().profile_cache.bulk_write(ops, ordered=False)

"""
X (Twitter) Scraper MCP Server — async job + worker pool + metrics + multi-worker safe.

Endpoints:
    MCP (for AI Agents):
        POST /mcp                     — MCP Streamable HTTP protocol

    REST API — Profile:
        POST /api/profiles            — ≤threshold 同步返回 / >threshold 自动异步 job
        POST /api/jobs                — 强制创建异步 job
        GET  /api/jobs                — 列出所有 job
        GET  /api/jobs/{id}           — 查 job 进度
        GET  /api/jobs/{id}/results   — 分页获取结果
        POST /api/jobs/{id}/cancel    — 取消 job
        GET  /api/jobs/{id}/stream    — SSE 实时推送结果（Change Streams 优先）

    REST API — Other:
        POST /api/tweets              — Get user tweets
        POST /api/followers           — Get user followers
        POST /api/following           — Get user following

    Operations:
        GET  /health                  — Health check
        GET  /metrics                 — Prometheus metrics

Run:
    python server.py
    # 多 worker 部署 (推荐生产环境):
    uvicorn server:app --host 0.0.0.0 --port 22222 --workers 4
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(_PROJECT_ROOT / ".env")

sys.path.insert(0, str(_PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("x_scraper")

from fastmcp import FastMCP
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.types import ASGIApp, Receive, Scope, Send

import db
import metrics
import apify_client
from scrape_profile import (
    get_profiles as _get_profiles,
    submit_job as _submit_job,
    cancel_job as _cancel_job,
    resume_interrupted_jobs as _resume_jobs,
    orphan_claimer_loop as _orphan_loop,
    ASYNC_JOB_THRESHOLD,
)
from scrape_tweets import get_latest_tweets as _get_latest_tweets
from scrape_followers_followings_list import get_followers as _get_followers
from scrape_followers_followings_list import get_following as _get_following

SERVER_API_KEY = os.getenv("X_SCRAPER_API_KEY", "")
TOOL_TIMEOUT = int(os.getenv("X_SCRAPER_TOOL_TIMEOUT", "300"))
ORPHAN_SCAN_INTERVAL = int(os.getenv("ORPHAN_SCAN_INTERVAL", "30"))


def _check_auth(request: Request) -> bool:
    if not SERVER_API_KEY:
        return True
    auth = request.headers.get("authorization", "")
    return auth == f"Bearer {SERVER_API_KEY}"


# Wire up apify metrics hook
apify_client.set_metrics_hook(metrics.record_apify_call)


# Init metrics + register lifespan for DB / job resume / orphan claimer
metrics.init_metrics()


from contextlib import asynccontextmanager
from typing import AsyncIterator

_orphan_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def app_lifespan(server: "FastMCP") -> AsyncIterator[None]:
    """FastMCP lifespan: init DB + resume own jobs + start orphan claimer."""
    global _orphan_task
    try:
        await db.init_db()
        resumed = await _resume_jobs()
        if resumed:
            logger.info("resumed %d own job(s) on startup", resumed)
        _orphan_task = asyncio.create_task(_orphan_loop(ORPHAN_SCAN_INTERVAL))
        logger.info(
            "startup done; worker_id=%s; orphan claimer started (interval=%ds)",
            db.WORKER_ID, ORPHAN_SCAN_INTERVAL,
        )
    except Exception as e:
        logger.error("startup failed: %s", e)
    try:
        yield
    finally:
        if _orphan_task and not _orphan_task.done():
            _orphan_task.cancel()
            try:
                await _orphan_task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await db.close_db()
        except Exception:
            pass


mcp = FastMCP("x-scraper", lifespan=app_lifespan)


# ───────────────── Health & Metrics ─────────────────


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request):
    db_status = "unknown"
    try:
        d = db.get_db()
        await d.command("ping")
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {e}"
    return JSONResponse({
        "status": "healthy",
        "service": "x-scraper",
        "worker_id": db.WORKER_ID,
        "mongodb": db_status,
        "endpoints": {
            "mcp": "/mcp",
            "rest_profile": ["/api/profiles", "/api/jobs", "/api/jobs/{id}",
                             "/api/jobs/{id}/results", "/api/jobs/{id}/stream", "/api/jobs/{id}/cancel"],
            "rest_other": ["/api/tweets", "/api/followers", "/api/following"],
            "ops": ["/health", "/metrics"],
        },
    })


@mcp.custom_route("/metrics", methods=["GET"])
async def metrics_endpoint(request: Request):
    body, ctype = metrics.render_metrics()
    return Response(content=body, media_type=ctype)


# ───────────────── MCP Tools (for AI Agents) ─────────────────


@mcp.tool
async def get_x_profiles(usernames: list[str], concurrency: int = 50) -> dict:
    """Fetch one or more X (Twitter) user profiles concurrently.

    For small batches (≤threshold), returns results directly.
    For large batches (>threshold), submits an async job and returns job_id.

    Args:
        usernames: List of X usernames without @ (e.g. ["elonmusk"] or ["elonmusk", "jack"]).
        concurrency: Max concurrent requests (default 50, max 200).
    """
    usernames = [u.replace("@", "").strip() for u in usernames if u.strip()]
    if len(usernames) <= ASYNC_JOB_THRESHOLD:
        batch_timeout = max(60, len(usernames) * 10)
        return await asyncio.wait_for(
            _get_profiles(usernames, concurrency=concurrency),
            timeout=batch_timeout,
        )
    job_id = await _submit_job(usernames, concurrency=concurrency)
    return {"job_id": job_id, "total": len(usernames), "message": "async job submitted"}


@mcp.tool
async def submit_x_profile_job(usernames: list[str], concurrency: int = 50) -> dict:
    """Submit an async job to fetch X profiles in bulk.

    Always creates an async job regardless of batch size.
    Use get_x_job_status to check progress.

    Args:
        usernames: List of X usernames without @.
        concurrency: Max concurrent requests (default 50, max 200).
    """
    usernames = [u.replace("@", "").strip() for u in usernames if u.strip()]
    job_id = await _submit_job(usernames, concurrency=concurrency)
    return {"job_id": job_id, "total": len(usernames), "message": "job submitted"}


@mcp.tool
async def get_x_job_status(job_id: str) -> dict:
    """Check the status and progress of a profile scraping job.

    Args:
        job_id: The job ID returned by submit_x_profile_job or get_x_profiles.
    """
    job = await db.get_job(job_id)
    if not job:
        return {"error": f"job {job_id} not found"}
    return _serialize_job(job)


@mcp.tool
async def get_x_tweets(username: str, max_results: int = 0) -> dict:
    """Get X (Twitter) user's latest tweets.

    Args:
        username: X username without the @ symbol.
        max_results: Number of tweets to fetch (0 = all).
    """
    return await asyncio.wait_for(_get_latest_tweets(username, max_results), timeout=TOOL_TIMEOUT)


@mcp.tool
async def get_x_followers(username: str, max_count: int = 0) -> dict:
    """Get followers of an X (Twitter) user.

    Args:
        username: X username without the @ symbol.
        max_count: Number of followers to fetch (0 = all).
    """
    return await asyncio.wait_for(_get_followers(username, max_count), timeout=TOOL_TIMEOUT)


@mcp.tool
async def get_x_following(username: str, max_count: int = 0) -> dict:
    """Get all accounts that an X (Twitter) user is following.

    Args:
        username: X username without the @ symbol.
        max_count: Number of following to fetch (0 = all).
    """
    return await asyncio.wait_for(_get_following(username, max_count), timeout=TOOL_TIMEOUT)


# ───────────────── REST API helpers ─────────────────


async def _parse_body(request: Request) -> dict:
    try:
        return await request.json()
    except Exception:
        return {}


def _serialize_job(job: dict) -> dict:
    """将 job doc 序列化为 JSON 安全格式（不包含 usernames 大列表）。"""
    job = dict(job)  # copy
    job["_id"] = str(job.get("_id", ""))
    for k in ("created_at", "started_at", "completed_at", "heartbeat_at"):
        if isinstance(job.get(k), datetime):
            job[k] = job[k].isoformat()
    job.pop("usernames", None)
    return job


# ───────────────── REST API — Profile ─────────────────


@mcp.custom_route("/api/profiles", methods=["POST"])
async def api_profiles(request: Request):
    """POST /api/profiles
    小批量（≤ threshold）同步返回结果。
    大批量自动创建异步 job，返回 job_id。
    Body: {"usernames": [...], "concurrency": 50}
      or: {"username": "user1"}
    """
    t = metrics.TimedHTTP("/api/profiles", "POST")
    with t:
        body = await _parse_body(request)
        usernames = body.get("usernames", [])
        if not usernames:
            single = body.get("username", "").replace("@", "").strip()
            if single:
                usernames = [single]
        if not isinstance(usernames, list) or not usernames:
            t.set_status(400)
            return JSONResponse({"error": "usernames (list) or username (str) is required"}, status_code=400)
        usernames = [u.replace("@", "").strip() for u in usernames if u.strip()]
        if not usernames:
            t.set_status(400)
            return JSONResponse({"error": "usernames list is empty"}, status_code=400)

        concurrency = body.get("concurrency", 50)

        if len(usernames) <= ASYNC_JOB_THRESHOLD:
            batch_timeout = max(60, len(usernames) * 10)
            try:
                result = await asyncio.wait_for(
                    _get_profiles(usernames, concurrency=concurrency),
                    timeout=batch_timeout,
                )
                t.set_status(200)
                return JSONResponse(result)
            except asyncio.TimeoutError:
                t.set_status(504)
                return JSONResponse({"error": "timeout"}, status_code=504)
            except Exception as e:
                t.set_status(500)
                return JSONResponse({"error": str(e)}, status_code=500)

        try:
            job_id = await _submit_job(usernames, concurrency=concurrency)
            t.set_status(202)
            return JSONResponse({
                "job_id": job_id,
                "total": len(set(u.replace("@", "").strip() for u in usernames)),
                "message": "async job submitted, use GET /api/jobs/{id} to check progress",
            }, status_code=202)
        except Exception as e:
            t.set_status(500)
            return JSONResponse({"error": str(e)}, status_code=500)


# ───────────────── REST API — Jobs ─────────────────


@mcp.custom_route("/api/jobs", methods=["POST"])
async def api_create_job(request: Request):
    """POST /api/jobs — 强制创建异步 job。"""
    t = metrics.TimedHTTP("/api/jobs", "POST")
    with t:
        body = await _parse_body(request)
        usernames = body.get("usernames", [])
        if not isinstance(usernames, list) or not usernames:
            t.set_status(400)
            return JSONResponse({"error": "usernames (list) is required"}, status_code=400)
        usernames = [u.replace("@", "").strip() for u in usernames if u.strip()]
        if not usernames:
            t.set_status(400)
            return JSONResponse({"error": "usernames list is empty"}, status_code=400)
        concurrency = body.get("concurrency", 50)
        try:
            job_id = await _submit_job(usernames, concurrency=concurrency)
            t.set_status(202)
            return JSONResponse({
                "job_id": job_id,
                "total": len(set(usernames)),
                "message": "job submitted",
            }, status_code=202)
        except Exception as e:
            t.set_status(500)
            return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/jobs", methods=["GET"])
async def api_list_jobs(request: Request):
    """GET /api/jobs?status=running&limit=50"""
    t = metrics.TimedHTTP("/api/jobs", "GET")
    with t:
        status = request.query_params.get("status")
        limit = int(request.query_params.get("limit", "50"))
        jobs = await db.list_jobs(status=status, limit=limit)
        t.set_status(200)
        return JSONResponse([_serialize_job(j) for j in jobs])


@mcp.custom_route("/api/jobs/{job_id}", methods=["GET"])
async def api_get_job(request: Request):
    t = metrics.TimedHTTP("/api/jobs/{id}", "GET")
    with t:
        job_id = request.path_params["job_id"]
        job = await db.get_job(job_id)
        if not job:
            t.set_status(404)
            return JSONResponse({"error": "job not found"}, status_code=404)
        t.set_status(200)
        return JSONResponse(_serialize_job(job))


@mcp.custom_route("/api/jobs/{job_id}/results", methods=["GET"])
async def api_job_results(request: Request):
    """GET /api/jobs/{id}/results?skip=0&limit=100 — 分页获取结果。"""
    t = metrics.TimedHTTP("/api/jobs/{id}/results", "GET")
    with t:
        job_id = request.path_params["job_id"]
        job = await db.get_job(job_id)
        if not job:
            t.set_status(404)
            return JSONResponse({"error": "job not found"}, status_code=404)
        skip = int(request.query_params.get("skip", "0"))
        limit = int(request.query_params.get("limit", "100"))
        results = await db.get_job_results(job_id, skip=skip, limit=limit)
        total_results = await db.count_job_results(job_id)
        for r in results:
            if isinstance(r.get("fetched_at"), datetime):
                r["fetched_at"] = r["fetched_at"].isoformat()
        t.set_status(200)
        return JSONResponse({
            "job_id": job_id,
            "skip": skip,
            "limit": limit,
            "total_results": total_results,
            "results": results,
        })


@mcp.custom_route("/api/jobs/{job_id}/cancel", methods=["POST"])
async def api_cancel_job(request: Request):
    """POST /api/jobs/{id}/cancel — 取消运行中的 job（仅本 worker 拥有的）。"""
    t = metrics.TimedHTTP("/api/jobs/{id}/cancel", "POST")
    with t:
        job_id = request.path_params["job_id"]
        cancelled = _cancel_job(job_id)
        if cancelled:
            t.set_status(200)
            return JSONResponse({"job_id": job_id, "message": "cancel signal sent"})
        job = await db.get_job(job_id)
        if not job:
            t.set_status(404)
            return JSONResponse({"error": "job not found"}, status_code=404)
        t.set_status(200)
        return JSONResponse({
            "job_id": job_id,
            "status": job["status"],
            "owner": job.get("owner"),
            "message": f"job not running on this worker (owner={job.get('owner')})",
        })


@mcp.custom_route("/api/jobs/{job_id}/stream", methods=["GET"])
async def api_job_stream(request: Request):
    """GET /api/jobs/{id}/stream — SSE 实时推送结果。
    
    优先用 Mongo Change Streams（需要 replica set），失败回退到轮询。
    """
    job_id = request.path_params["job_id"]
    job = await db.get_job(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)

    async def event_generator():
        # 先回放已有的进度
        current_job = await db.get_job(job_id)
        if current_job:
            progress = {
                "type": "progress",
                "status": current_job["status"],
                "total": current_job["total"],
                "done": current_job["done"],
                "failed": current_job["failed"],
            }
            yield f"event: progress\ndata: {json.dumps(progress)}\n\n"
            if current_job["status"] in ("completed", "failed"):
                yield f"event: done\ndata: {json.dumps({'status': current_job['status']})}\n\n"
                return

        # 尝试 Change Streams
        change_stream_failed = False
        try:
            stream_iter = db.watch_job_results(job_id).__aiter__()
        except Exception as e:
            logger.info("change streams not available, falling back to polling: %s", e)
            change_stream_failed = True

        if not change_stream_failed:
            try:
                last_progress_check = time.monotonic()
                while True:
                    try:
                        # Wait for next result with timeout (so we can also check job status)
                        next_task = asyncio.create_task(stream_iter.__anext__())
                        done, _ = await asyncio.wait({next_task}, timeout=2.0)
                        if next_task in done:
                            doc = next_task.result()
                            if isinstance(doc.get("fetched_at"), datetime):
                                doc["fetched_at"] = doc["fetched_at"].isoformat()
                            yield f"data: {json.dumps(doc, ensure_ascii=False)}\n\n"
                        else:
                            next_task.cancel()
                            try:
                                await next_task
                            except (asyncio.CancelledError, Exception):
                                pass

                        # progress check every ~2s
                        if time.monotonic() - last_progress_check >= 2.0:
                            last_progress_check = time.monotonic()
                            current_job = await db.get_job(job_id)
                            if current_job:
                                progress = {
                                    "type": "progress",
                                    "status": current_job["status"],
                                    "total": current_job["total"],
                                    "done": current_job["done"],
                                    "failed": current_job["failed"],
                                }
                                yield f"event: progress\ndata: {json.dumps(progress)}\n\n"
                                if current_job["status"] in ("completed", "failed"):
                                    yield f"event: done\ndata: {json.dumps({'status': current_job['status']})}\n\n"
                                    return
                    except StopAsyncIteration:
                        break
                return
            except Exception as e:
                logger.warning("change stream loop failed (fallback to polling): %s", e)

        # ── Polling fallback ──
        cursor = datetime.now(timezone.utc)
        while True:
            current_job = await db.get_job(job_id)
            if not current_job:
                yield f"event: error\ndata: job not found\n\n"
                break
            new_results = await db.get_new_results_since(job_id, cursor, limit=100)
            for r in new_results:
                if isinstance(r.get("fetched_at"), datetime):
                    cursor = r["fetched_at"]
                    r["fetched_at"] = r["fetched_at"].isoformat()
                yield f"data: {json.dumps(r, ensure_ascii=False)}\n\n"
            progress = {
                "type": "progress",
                "status": current_job["status"],
                "total": current_job["total"],
                "done": current_job["done"],
                "failed": current_job["failed"],
            }
            yield f"event: progress\ndata: {json.dumps(progress)}\n\n"
            if current_job["status"] in ("completed", "failed"):
                yield f"event: done\ndata: {json.dumps({'status': current_job['status']})}\n\n"
                break
            await asyncio.sleep(2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ───────────────── REST API — Other ─────────────────


@mcp.custom_route("/api/tweets", methods=["POST"])
async def api_tweets(request: Request):
    t = metrics.TimedHTTP("/api/tweets", "POST")
    with t:
        body = await _parse_body(request)
        username = body.get("username", "").replace("@", "").strip()
        if not username:
            t.set_status(400)
            return JSONResponse({"error": "username is required"}, status_code=400)
        max_results = body.get("max_results", 0)
        try:
            result = await asyncio.wait_for(_get_latest_tweets(username, max_results), timeout=TOOL_TIMEOUT)
            t.set_status(200)
            return JSONResponse(result)
        except asyncio.TimeoutError:
            t.set_status(504)
            return JSONResponse({"error": "timeout"}, status_code=504)
        except Exception as e:
            t.set_status(500)
            return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/followers", methods=["POST"])
async def api_followers(request: Request):
    t = metrics.TimedHTTP("/api/followers", "POST")
    with t:
        body = await _parse_body(request)
        username = body.get("username", "").replace("@", "").strip()
        if not username:
            t.set_status(400)
            return JSONResponse({"error": "username is required"}, status_code=400)
        max_count = body.get("max_count", 0)
        try:
            result = await asyncio.wait_for(_get_followers(username, max_count), timeout=TOOL_TIMEOUT)
            t.set_status(200)
            return JSONResponse(result)
        except asyncio.TimeoutError:
            t.set_status(504)
            return JSONResponse({"error": "timeout"}, status_code=504)
        except Exception as e:
            t.set_status(500)
            return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/following", methods=["POST"])
async def api_following(request: Request):
    t = metrics.TimedHTTP("/api/following", "POST")
    with t:
        body = await _parse_body(request)
        username = body.get("username", "").replace("@", "").strip()
        if not username:
            t.set_status(400)
            return JSONResponse({"error": "username is required"}, status_code=400)
        max_count = body.get("max_count", 0)
        try:
            result = await asyncio.wait_for(_get_following(username, max_count), timeout=TOOL_TIMEOUT)
            t.set_status(200)
            return JSONResponse(result)
        except asyncio.TimeoutError:
            t.set_status(504)
            return JSONResponse({"error": "timeout"}, status_code=504)
        except Exception as e:
            t.set_status(500)
            return JSONResponse({"error": str(e)}, status_code=500)


# ───────────────── App setup ─────────────────


_starlette_app = mcp.http_app()

_starlette_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class _AuthMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        path = request.url.path
        if path in ("/health", "/metrics") or _check_auth(request):
            await self.app(scope, receive, send)
            return

        logger.warning("unauthorized request path=%s", path)
        resp = JSONResponse({"error": "unauthorized"}, status_code=401)
        await resp(scope, receive, send)


_starlette_app.add_middleware(_AuthMiddleware)

app = _starlette_app


if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=22222)

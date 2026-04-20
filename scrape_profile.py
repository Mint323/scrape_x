"""
X (Twitter) Profile Scraper — 通过 scrape.do 代理抓取 X 页面并解析 __INITIAL_STATE__。

支持:
    - 单用户 / 小批量同步返回（共享 httpx client）
    - 大批量异步 job（worker pool + MongoDB 持久化、自适应并发、缓存、断点续传、批量写、多 worker 协作）

Usage:
    python scrape_profile.py <username>                    # 单用户 profile
    python scrape_profile.py <username> --json             # JSON 输出
    python scrape_profile.py user1 user2 user3 --json      # 批量
    python scrape_profile.py -f usernames.txt --json --concurrency 100

环境变量:
    SCRAPE_DO_TOKEN          - scrape.do API token（必须）
    REQUEST_TIMEOUT          - 请求超时秒数（默认 30）
    SCRAPE_CONCURRENCY       - 默认并发数（默认 50）
    ASYNC_JOB_THRESHOLD      - 超过此数量自动走异步 job（默认 50）
    JOB_BULK_FLUSH_SIZE      - Mongo 批量写入条数（默认 50）
    JOB_BULK_FLUSH_INTERVAL  - Mongo 批量写入最大间隔秒（默认 5）
    JOB_HEARTBEAT_INTERVAL   - 心跳间隔秒（默认 15）
    ADAPTIVE_WINDOW_SECONDS  - 自适应窗口秒（默认 30）
    ADAPTIVE_429_THRESHOLD   - 429 触发降速比例（默认 0.1）
    ADAPTIVE_GROWTH_FACTOR   - 加速倍率（默认 1.2）
"""

import argparse
import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(_PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("x_scraper")

SCRAPE_DO_TOKEN: str = os.getenv("SCRAPE_DO_TOKEN", "").strip()
REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
DEFAULT_CONCURRENCY: int = int(os.getenv("SCRAPE_CONCURRENCY", "50"))
ASYNC_JOB_THRESHOLD: int = int(os.getenv("ASYNC_JOB_THRESHOLD", "50"))

JOB_BULK_FLUSH_SIZE: int = int(os.getenv("JOB_BULK_FLUSH_SIZE", "50"))
JOB_BULK_FLUSH_INTERVAL: float = float(os.getenv("JOB_BULK_FLUSH_INTERVAL", "5"))
JOB_HEARTBEAT_INTERVAL: int = int(os.getenv("JOB_HEARTBEAT_INTERVAL", "15"))

ADAPTIVE_WINDOW_SECONDS: float = float(os.getenv("ADAPTIVE_WINDOW_SECONDS", "30"))
ADAPTIVE_429_THRESHOLD: float = float(os.getenv("ADAPTIVE_429_THRESHOLD", "0.1"))
ADAPTIVE_GROWTH_FACTOR: float = float(os.getenv("ADAPTIVE_GROWTH_FACTOR", "1.2"))

MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 3


# ───────────────── 基础设施 ─────────────────


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _check_scrape_do_token() -> str:
    token = SCRAPE_DO_TOKEN or os.getenv("SCRAPE_DO_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "SCRAPE_DO_TOKEN is not set. "
            "Add it to .env or export it before running."
        )
    return token


# ───────────────── 自适应并发控制 ─────────────────


class AdaptiveConcurrency:
    """根据 429 比例自动调整并发数的信号量包装器。"""

    def __init__(
        self,
        initial: int,
        min_conc: int = 5,
        max_conc: int = 200,
        window_size: float = ADAPTIVE_WINDOW_SECONDS,
        threshold: float = ADAPTIVE_429_THRESHOLD,
        growth_factor: float = ADAPTIVE_GROWTH_FACTOR,
    ):
        self.current = initial
        self.min_conc = min_conc
        self.max_conc = max_conc
        self._semaphore = asyncio.Semaphore(initial)
        self._429_count = 0
        self._success_count = 0
        self._window_start = time.monotonic()
        self._window_size = window_size
        self._threshold = threshold
        self._growth = growth_factor

    async def acquire(self):
        await self._semaphore.acquire()

    def release(self):
        self._semaphore.release()

    def report_429(self):
        self._429_count += 1
        self._maybe_adjust()

    def report_success(self):
        self._success_count += 1
        self._maybe_adjust()

    def _maybe_adjust(self):
        elapsed = time.monotonic() - self._window_start
        if elapsed < self._window_size:
            return
        total = self._429_count + self._success_count
        if total == 0:
            self._reset_window()
            return
        ratio_429 = self._429_count / total
        if ratio_429 > self._threshold:
            new = max(self.min_conc, self.current // 2)
        elif ratio_429 == 0 and total > 20:
            new = min(self.max_conc, int(self.current * self._growth))
        else:
            new = self.current
        if new != self.current:
            logger.info(
                "concurrency adjusted: %d → %d (429_ratio=%.2f, window_total=%d)",
                self.current, new, ratio_429, total,
            )
            delta = new - self.current
            if delta > 0:
                for _ in range(delta):
                    self._semaphore.release()
            self.current = new
        self._reset_window()

    def _reset_window(self):
        self._429_count = 0
        self._success_count = 0
        self._window_start = time.monotonic()


# ───────────────── HTTP 请求 ─────────────────


async def _scrape_do_get(
    url: str,
    timeout: Optional[int] = None,
    client: Optional[httpx.AsyncClient] = None,
    semaphore: Optional[asyncio.Semaphore] = None,
    adaptive: Optional[AdaptiveConcurrency] = None,
) -> httpx.Response:
    """通过 scrape.do 代理发起 GET 请求（带重试，async）。"""
    try:
        from metrics import record_scrape_do_call
    except ImportError:
        record_scrape_do_call = None

    token = _check_scrape_do_token()
    encoded_url = quote(url, safe="")
    proxy_url = (
        f"https://api.scrape.do?token={token}"
        f"&url={encoded_url}"
        f"&super=true"
    )
    req_timeout = timeout or REQUEST_TIMEOUT

    async def _do_request(c: httpx.AsyncClient) -> httpx.Response:
        last_err: Optional[Exception] = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = await c.get(proxy_url, timeout=req_timeout)
                if record_scrape_do_call:
                    record_scrape_do_call(resp.status_code)
                if resp.status_code == 200:
                    if adaptive:
                        adaptive.report_success()
                    return resp
                if resp.status_code in (429, 403):
                    if adaptive:
                        adaptive.report_429()
                    wait = RETRY_BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        "HTTP %d from scrape.do, retrying in %ds (%d/%d)",
                        resp.status_code, wait, attempt + 1, MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                    continue
                return resp
            except httpx.HTTPError as e:
                last_err = e
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        "Request failed: %s, retrying in %ds (%d/%d)",
                        type(e).__name__, wait, attempt + 1, MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                else:
                    raise
        if last_err:
            raise last_err
        return resp  # type: ignore

    if adaptive:
        await adaptive.acquire()
        try:
            if client:
                return await _do_request(client)
            async with httpx.AsyncClient() as c:
                return await _do_request(c)
        finally:
            adaptive.release()
    elif semaphore:
        async with semaphore:
            if client:
                return await _do_request(client)
            async with httpx.AsyncClient() as c:
                return await _do_request(c)
    else:
        if client:
            return await _do_request(client)
        async with httpx.AsyncClient() as c:
            return await _do_request(c)


# ───────────────── 解析 ─────────────────


def _extract_initial_state(html: str) -> Dict[str, Any]:
    match = re.search(r"window\.__INITIAL_STATE__\s*=\s*", html)
    if not match:
        raise ValueError("未找到 __INITIAL_STATE__，页面结构可能已变更")
    decoder = json.JSONDecoder()
    state, _ = decoder.raw_decode(html, match.end())
    return state


def _extract_user(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    users = state.get("entities", {}).get("users", {}).get("entities", {})
    if not users:
        return None
    return next(iter(users.values()))


def _build_profile(raw: Dict[str, Any]) -> Dict[str, Any]:
    desc = (raw.get("description") or "").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    url_entities = raw.get("entities", {}).get("url", {}).get("urls", [])
    avatar = (raw.get("profile_image_url_https") or "").replace("_normal", "_400x400")
    pinned = raw.get("pinned_tweet_ids_str", [])
    pro = raw.get("professional") or {}
    screen_name = raw.get("screen_name")

    return {
        "name": raw.get("name"),
        "screen_name": screen_name,
        "id": raw.get("id_str"),
        "id_str": raw.get("id_str"),
        "description": desc,
        "location": raw.get("location"),
        "url": url_entities[0].get("expanded_url") if url_entities else raw.get("url"),
        "followers_count": raw.get("followers_count") or raw.get("normal_followers_count"),
        "friends_count": raw.get("friends_count"),
        "following_count": raw.get("friends_count"),
        "statuses_count": raw.get("statuses_count"),
        "tweet_count": raw.get("statuses_count"),
        "favourites_count": raw.get("favourites_count"),
        "media_count": raw.get("media_count"),
        "listed_count": raw.get("listed_count"),
        "created_at": raw.get("created_at"),
        "verified": raw.get("verified"),
        "is_blue_verified": raw.get("is_blue_verified"),
        "protected": raw.get("protected"),
        "profile_banner_url": raw.get("profile_banner_url"),
        "profile_image_url": avatar,
        "profile_image_url_https": avatar,
        "pinned_tweet_ids_str": pinned,
        "professional_type": pro.get("professional_type"),
        "professional_categories": [c.get("name") for c in pro.get("category", []) if c.get("name")],
    }


# ───────────────── 核心 API（同步小批量） ─────────────────


async def get_profile(username: str) -> Dict[str, Any]:
    """获取单个 X 用户 profile。"""
    try:
        from metrics import record_profile
    except ImportError:
        record_profile = None

    result: Dict[str, Any] = {
        "username": username,
        "profile": None,
        "success": False,
        "error": None,
        "fetched_at": utcnow_iso(),
    }
    logger.info("get_profile username=%s", username)
    try:
        resp = await _scrape_do_get(f"https://x.com/{username}", timeout=60)
        if resp.status_code != 200:
            result["error"] = f"HTTP {resp.status_code}"
            if record_profile:
                record_profile("failed")
            return result

        state = _extract_initial_state(resp.text)
        raw_user = _extract_user(state)
        if not raw_user:
            result["error"] = "用户数据未找到，可能用户不存在或已被封禁"
            if record_profile:
                record_profile("failed")
            return result

        result["profile"] = _build_profile(raw_user)
        result["success"] = True
        if record_profile:
            record_profile("success")
    except Exception as e:
        logger.error("get_profile failed username=%s error=%s", username, e)
        result["error"] = str(e)
        if record_profile:
            record_profile("failed")
    return result


async def get_profiles(
    usernames: List[str],
    concurrency: int = 0,
) -> Dict[str, Any]:
    """小批量同步并发获取 profile。worker pool 模式，避免大批量 OOM。"""
    try:
        from metrics import record_profile
    except ImportError:
        record_profile = None

    max_conc = concurrency if concurrency > 0 else DEFAULT_CONCURRENCY
    logger.info("get_profiles count=%d concurrency=%d", len(usernames), max_conc)

    limits = httpx.Limits(
        max_connections=max_conc,
        max_keepalive_connections=max_conc,
    )

    queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
    results: List[Dict[str, Any]] = []
    results_lock = asyncio.Lock()
    done_count = 0
    total = len(usernames)

    async with httpx.AsyncClient(limits=limits) as client:

        async def _worker() -> None:
            nonlocal done_count
            while True:
                username = await queue.get()
                if username is None:
                    queue.task_done()
                    return
                result: Dict[str, Any] = {
                    "username": username,
                    "profile": None,
                    "success": False,
                    "error": None,
                    "fetched_at": utcnow_iso(),
                }
                try:
                    resp = await _scrape_do_get(
                        f"https://x.com/{username}",
                        timeout=60,
                        client=client,
                    )
                    if resp.status_code != 200:
                        result["error"] = f"HTTP {resp.status_code}"
                    else:
                        state = _extract_initial_state(resp.text)
                        raw_user = _extract_user(state)
                        if not raw_user:
                            result["error"] = "用户数据未找到，可能用户不存在或已被封禁"
                        else:
                            result["profile"] = _build_profile(raw_user)
                            result["success"] = True
                except Exception as e:
                    logger.error("get_profile failed username=%s error=%s", username, e)
                    result["error"] = str(e)
                finally:
                    if record_profile:
                        record_profile("success" if result["success"] else "failed")
                    async with results_lock:
                        results.append(result)
                        done_count += 1
                        if total > 1 and (done_count % 50 == 0 or done_count == total):
                            logger.info("progress: %d/%d done", done_count, total)
                    queue.task_done()

        # enqueue
        for u in usernames:
            queue.put_nowait(u)
        # sentinels
        for _ in range(max_conc):
            queue.put_nowait(None)

        workers = [asyncio.create_task(_worker()) for _ in range(max_conc)]
        await asyncio.gather(*workers)

    success_count = sum(1 for r in results if r["success"])
    return {
        "total": total,
        "success_count": success_count,
        "fail_count": total - success_count,
        "results": results,
        "fetched_at": utcnow_iso(),
    }


# ───────────────── Job 引擎（异步大批量） ─────────────────

# 运行中的 job task 引用（仅本 worker），用于本地取消
_running_jobs: Dict[str, asyncio.Task] = {}


async def _bulk_writer(
    job_id: str,
    buffer_queue: asyncio.Queue,
    flush_size: int,
    flush_interval: float,
) -> None:
    """后台批量写入 worker：从 buffer_queue 收 result，批量 flush 到 Mongo。"""
    import db
    try:
        from metrics import record_profile
    except ImportError:
        record_profile = None

    buffer: List[Dict[str, Any]] = []
    cache_buffer: List[Dict[str, Any]] = []
    last_flush = time.monotonic()

    async def flush():
        nonlocal buffer, cache_buffer, last_flush
        if not buffer:
            last_flush = time.monotonic()
            return
        items = buffer
        cache_items = cache_buffer
        buffer = []
        cache_buffer = []
        last_flush = time.monotonic()

        success = sum(1 for it in items if it.get("profile"))
        fail = len(items) - success

        try:
            await db.save_profile_results_bulk(job_id, items)
            if cache_items:
                await db.set_cached_profiles_bulk(cache_items)
            await db.increment_job_progress(job_id, success_delta=success, fail_delta=fail)
            if record_profile:
                for _ in range(success):
                    record_profile("success")
                for _ in range(fail):
                    record_profile("failed")
        except Exception as e:
            logger.error("bulk flush failed for job %s: %s", job_id, e)

    while True:
        try:
            item = await asyncio.wait_for(buffer_queue.get(), timeout=flush_interval)
        except asyncio.TimeoutError:
            await flush()
            continue

        if item is None:
            buffer_queue.task_done()
            await flush()
            return

        buffer.append(item)
        if item.get("from_cache") is False and item.get("profile"):
            cache_buffer.append({"username": item["username"], "profile": item["profile"]})
        buffer_queue.task_done()

        if len(buffer) >= flush_size or (time.monotonic() - last_flush) >= flush_interval:
            await flush()


async def _heartbeat_loop(job_id: str, interval: int) -> None:
    """周期性更新 job 心跳，防止被其他 worker 抢占。"""
    import db
    while True:
        try:
            await asyncio.sleep(interval)
            ok = await db.heartbeat_job(job_id)
            if not ok:
                logger.warning("job %s lost ownership (heartbeat failed); aborting", job_id)
                # 告知调用者：通过 cancel 自身 task 实现
                cur = asyncio.current_task()
                if cur:
                    cur.cancel()
                return
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error("heartbeat error for job %s: %s", job_id, e)


async def run_job(job_id: str) -> None:
    """后台执行 job：worker pool + 缓存 + 批量持久化 + 心跳 + 断点续传。"""
    import db
    try:
        from metrics import record_job_event, inc_active_jobs
    except ImportError:
        record_job_event = inc_active_jobs = lambda *a, **kw: None

    inc_active_jobs(1)
    record_job_event("started")
    heartbeat_task: Optional[asyncio.Task] = None
    writer_task: Optional[asyncio.Task] = None

    try:
        job = await db.get_job(job_id)
        if not job:
            logger.error("run_job: job %s not found", job_id)
            return

        usernames = job["usernames"]
        conc = job["concurrency"]
        total = job["total"]

        logger.info("job %s started: total=%d concurrency=%d", job_id, total, conc)

        # 断点续传：跳过已完成的
        completed = await db.get_completed_usernames(job_id)
        pending = [u for u in usernames if u not in completed]
        if completed:
            logger.info("job %s resuming: %d already done, %d pending", job_id, len(completed), len(pending))
            await db.update_job(job_id, done=len(completed))

        if not pending:
            await db.update_job(job_id, status="completed", completed_at=db._utcnow())
            record_job_event("completed")
            logger.info("job %s already completed (all cached)", job_id)
            return

        # 启动心跳
        heartbeat_task = asyncio.create_task(_heartbeat_loop(job_id, JOB_HEARTBEAT_INTERVAL))

        # 批量预热缓存
        cached_map = await db.get_cached_profiles_bulk(pending)
        if cached_map:
            logger.info("job %s cache hit: %d/%d", job_id, len(cached_map), len(pending))

        adaptive = AdaptiveConcurrency(initial=conc, max_conc=conc)
        limits = httpx.Limits(max_connections=conc, max_keepalive_connections=conc)

        # 写 buffer + writer
        buffer_queue: asyncio.Queue = asyncio.Queue(maxsize=JOB_BULK_FLUSH_SIZE * 4)
        writer_task = asyncio.create_task(
            _bulk_writer(job_id, buffer_queue, JOB_BULK_FLUSH_SIZE, JOB_BULK_FLUSH_INTERVAL)
        )

        # worker pool
        work_queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        for u in pending:
            work_queue.put_nowait(u)
        for _ in range(conc):
            work_queue.put_nowait(None)

        async with httpx.AsyncClient(limits=limits) as client:

            async def _worker() -> None:
                while True:
                    username = await work_queue.get()
                    if username is None:
                        work_queue.task_done()
                        return
                    profile = None
                    error = None
                    from_cache = False
                    try:
                        cached = cached_map.get(username)
                        if cached:
                            profile = cached
                            from_cache = True
                        else:
                            resp = await _scrape_do_get(
                                f"https://x.com/{username}",
                                timeout=60,
                                client=client,
                                adaptive=adaptive,
                            )
                            if resp.status_code != 200:
                                error = f"HTTP {resp.status_code}"
                            else:
                                state = _extract_initial_state(resp.text)
                                raw_user = _extract_user(state)
                                if not raw_user:
                                    error = "用户数据未找到"
                                else:
                                    profile = _build_profile(raw_user)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        error = str(e)
                        logger.error("job %s fetch failed: %s error=%s", job_id, username, e)

                    await buffer_queue.put({
                        "username": username,
                        "profile": profile,
                        "error": error,
                        "from_cache": from_cache,
                    })
                    work_queue.task_done()

            workers = [asyncio.create_task(_worker()) for _ in range(conc)]
            await asyncio.gather(*workers)

        # 通知 writer 收尾
        await buffer_queue.put(None)
        await writer_task
        writer_task = None

        await db.update_job(job_id, status="completed", completed_at=db._utcnow())
        record_job_event("completed")
        logger.info("job %s completed: total=%d", job_id, total)

    except asyncio.CancelledError:
        await db.update_job(job_id, status="failed", error="cancelled", completed_at=db._utcnow())
        record_job_event("cancelled")
        logger.warning("job %s cancelled", job_id)
        raise
    except Exception as e:
        try:
            await db.update_job(job_id, status="failed", error=str(e), completed_at=db._utcnow())
        except Exception:
            pass
        record_job_event("failed")
        logger.error("job %s failed: %s", job_id, e)
    finally:
        if heartbeat_task and not heartbeat_task.done():
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
        if writer_task and not writer_task.done():
            writer_task.cancel()
            try:
                await writer_task
            except (asyncio.CancelledError, Exception):
                pass
        _running_jobs.pop(job_id, None)
        inc_active_jobs(-1)


async def submit_job(usernames: List[str], concurrency: int = 0) -> str:
    """提交异步 job，返回 job_id。"""
    import db
    try:
        from metrics import record_job_event
    except ImportError:
        record_job_event = lambda *a, **kw: None

    conc = concurrency if concurrency > 0 else DEFAULT_CONCURRENCY
    seen = set()
    deduped = []
    for u in usernames:
        u = u.replace("@", "").strip()
        if u and u not in seen:
            seen.add(u)
            deduped.append(u)

    job_id = await db.create_job(deduped, conc)
    record_job_event("created")
    # 立即尝试在本 worker 启动；其他 worker 也会尝试 claim，由 Mongo 决定
    claimed = await db.claim_job(job_id)
    if claimed:
        task = asyncio.create_task(run_job(job_id))
        _running_jobs[job_id] = task
    return job_id


def cancel_job(job_id: str) -> bool:
    """取消运行中的 job（仅本 worker）。"""
    task = _running_jobs.get(job_id)
    if task and not task.done():
        task.cancel()
        return True
    return False


async def resume_interrupted_jobs() -> int:
    """启动时：恢复本 worker 之前拥有的 running job + 抢占 orphaned job。"""
    import db

    resumed = 0
    # 1. 恢复自己之前的 jobs（同一 worker_id 重启的极少见情况）
    my_jobs = await db.find_my_jobs()
    for job in my_jobs:
        job_id = job["_id"]
        if job_id in _running_jobs:
            continue
        logger.info("resuming own job: %s", job_id)
        task = asyncio.create_task(run_job(job_id))
        _running_jobs[job_id] = task
        resumed += 1
    return resumed


async def orphan_claimer_loop(interval: int = 30) -> None:
    """后台 worker：定期扫描并抢占 orphaned job + submitted job。
    
    在 server 启动后用 asyncio.create_task 启动。
    """
    import db

    while True:
        try:
            # 1. 抢占 orphan
            orphan = await db.claim_orphaned_job()
            if orphan:
                job_id = orphan["_id"]
                logger.warning("claimed orphaned job: %s (prev_owner=%s)", job_id, orphan.get("owner"))
                task = asyncio.create_task(run_job(job_id))
                _running_jobs[job_id] = task
                continue  # 立即扫下一个

            # 2. 抢占 submitted（队列里的）
            new_job = await db.claim_next_submitted_job()
            if new_job:
                job_id = new_job["_id"]
                if job_id not in _running_jobs:
                    logger.info("claimed submitted job: %s", job_id)
                    task = asyncio.create_task(run_job(job_id))
                    _running_jobs[job_id] = task
                    continue

            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error("orphan_claimer_loop error: %s", e)
            await asyncio.sleep(interval)


# ───────────────── CLI helpers ─────────────────


def print_profile(profile: Dict):
    if not profile:
        print("  No profile data")
        return
    print(f"  Name:            {profile.get('name', 'N/A')}")
    print(f"  Screen name:     @{profile.get('screen_name', 'N/A')}")
    print(f"  ID:              {profile.get('id', 'N/A')}")
    print(f"  Bio:             {profile.get('description', 'N/A')}")
    print(f"  Location:        {profile.get('location', 'N/A')}")
    print(f"  URL:             {profile.get('url', 'N/A')}")
    print(f"  Followers:       {profile.get('followers_count', 'N/A')}")
    print(f"  Following:       {profile.get('friends_count') or profile.get('following_count', 'N/A')}")
    print(f"  Tweets:          {profile.get('statuses_count') or profile.get('tweet_count', 'N/A')}")
    print(f"  Favourites:      {profile.get('favourites_count', 'N/A')}")
    print(f"  Media:           {profile.get('media_count', 'N/A')}")
    print(f"  Listed:          {profile.get('listed_count', 'N/A')}")
    print(f"  Created:         {profile.get('created_at', 'N/A')}")
    print(f"  Verified:        {profile.get('verified', 'N/A')}")
    print(f"  Blue verified:   {profile.get('is_blue_verified', 'N/A')}")
    print(f"  Protected:       {profile.get('protected', 'N/A')}")
    print(f"  Banner:          {str(profile.get('profile_banner_url', 'N/A'))[:60]}")
    print(f"  Avatar:          {str(profile.get('profile_image_url', 'N/A'))[:60]}")
    pinned = profile.get("pinned_tweet_ids_str", [])
    if pinned:
        print(f"  Pinned tweet:    https://x.com/{profile.get('screen_name')}/status/{pinned[0]}")
    if profile.get("professional_type"):
        print(f"  Pro type:        {profile['professional_type']}")
    cats = profile.get("professional_categories", [])
    if cats:
        print(f"  Pro categories:  {', '.join(cats)}")


async def async_main():
    parser = argparse.ArgumentParser(description="Scrape X (Twitter) user profile(s) via scrape.do")
    parser.add_argument("username", nargs="*", help="X username(s) (without @)")
    parser.add_argument("-f", "--file", help="从文件读取用户名（每行一个）")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    parser.add_argument("--concurrency", type=int, default=0,
                        help=f"并发数（默认 {DEFAULT_CONCURRENCY}，最高 200）")
    args = parser.parse_args()

    usernames: List[str] = []
    for u in (args.username or []):
        usernames.append(u.replace("@", "").strip())
    if args.file:
        with open(args.file, "r", encoding="utf-8") as fh:
            for line in fh:
                name = line.strip().replace("@", "")
                if name and not name.startswith("#"):
                    usernames.append(name)

    if not usernames:
        parser.error("请提供至少一个用户名，或用 -f 指定文件")

    if len(usernames) > 1:
        batch_result = await get_profiles(usernames, concurrency=args.concurrency)

        if args.json:
            print(json.dumps(batch_result, ensure_ascii=False, indent=2))
            return

        print()
        print(f"{'=' * 60}")
        print(f"  Batch Profile: {batch_result['success_count']}/{batch_result['total']} succeeded")
        print(f"{'=' * 60}")
        for r in batch_result["results"]:
            print(f"\n--- @{r['username']} ---")
            if r["error"]:
                print(f"  ERROR: {r['error']}")
            else:
                print_profile(r["profile"])
        print()
        return

    username = usernames[0]
    result = await get_profile(username)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    print()
    print(f"{'=' * 60}")
    print(f"  @{username}")
    print(f"{'=' * 60}")

    if result.get("error"):
        print(f"  ERROR: {result['error']}")
        return

    print(f"\n--- Profile ---")
    print_profile(result["profile"])
    print()


if __name__ == "__main__":
    asyncio.run(async_main())

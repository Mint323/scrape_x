"""
Unified async Apify actor runner with retry, sync/async fallback, and Prometheus metrics.

All Apify API calls go through ``await run_actor(actor_id, payload, ...)``.
Token is read from env ``APIFY_API_TOKEN`` (no default — must be set).
"""

import asyncio
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("x_scraper")

APIFY_TOKEN: str = os.getenv("APIFY_API_TOKEN", "")
BASE_URL = "https://api.apify.com/v2"

APIFY_MAX_RETRIES: int = int(os.getenv("APIFY_MAX_RETRIES", "3"))
APIFY_RETRY_BASE: float = float(os.getenv("APIFY_RETRY_BASE", "2.0"))

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Optional metrics hook — set by metrics module; called as record(actor, status, elapsed, cost_usd)
_metrics_hook: Optional[Any] = None


def set_metrics_hook(hook):
    """Register a callable: record(actor: str, status: str, elapsed: float, cost_usd: float)"""
    global _metrics_hook
    _metrics_hook = hook


def _emit_metric(actor: str, status: str, elapsed: float, cost_usd: float = 0.0):
    if _metrics_hook is None:
        return
    try:
        _metrics_hook(actor, status, elapsed, cost_usd)
    except Exception as e:
        logger.debug("metrics hook failed: %s", e)


def _check_token() -> str:
    token = APIFY_TOKEN or os.getenv("APIFY_API_TOKEN", "")
    if not token:
        raise RuntimeError(
            "APIFY_API_TOKEN is not set. "
            "Add it to .env or export it before starting the service."
        )
    return token


def _sync_url(actor_id: str) -> str:
    return f"{BASE_URL}/acts/{actor_id}/run-sync-get-dataset-items"


def _async_url(actor_id: str) -> str:
    return f"{BASE_URL}/acts/{actor_id}/runs"


async def _run_sync_once(
    client: httpx.AsyncClient,
    actor_id: str,
    payload: Dict[str, Any],
    token: str,
    timeout: int,
) -> List[Dict]:
    resp = await client.post(
        _sync_url(actor_id),
        params={"token": token},
        json=payload,
        timeout=timeout,
    )
    if resp.status_code in (200, 201, 202):
        return resp.json()
    if resp.status_code in _RETRYABLE_STATUS:
        raise httpx.ConnectError(
            f"Retryable HTTP {resp.status_code}: {resp.text[:200]}"
        )
    raise RuntimeError(
        f"Apify sync call failed ({resp.status_code}): {resp.text[:300]}"
    )


async def _run_async_once(
    client: httpx.AsyncClient,
    actor_id: str,
    payload: Dict[str, Any],
    token: str,
    timeout: int,
) -> tuple[List[Dict], float]:
    """Returns (items, cost_usd)."""
    resp = await client.post(
        _async_url(actor_id),
        params={"token": token},
        json=payload,
        timeout=30,
    )
    if resp.status_code in _RETRYABLE_STATUS:
        raise httpx.ConnectError(
            f"Retryable HTTP {resp.status_code}: {resp.text[:200]}"
        )
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Apify async run failed ({resp.status_code}): {resp.text[:300]}"
        )

    run_data = resp.json().get("data", resp.json())
    run_id = run_data.get("id")
    dataset_id = run_data.get("defaultDatasetId")
    logger.info("Apify run started: run_id=%s dataset=%s", run_id, dataset_id)

    cost_usd = 0.0
    deadline = time.time() + timeout
    while time.time() < deadline:
        status_resp = await client.get(
            f"{BASE_URL}/actor-runs/{run_id}",
            params={"token": token},
            timeout=15,
        )
        run_info = status_resp.json().get("data", {})
        status = run_info.get("status", "UNKNOWN")
        if status == "SUCCEEDED":
            cost_usd = float(run_info.get("usageTotalUsd") or 0)
            if cost_usd:
                logger.info("Apify run %s cost: $%.4f", run_id, cost_usd)
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Actor run {status} (run_id={run_id})")
        await asyncio.sleep(4)
    else:
        raise RuntimeError(f"Timed out waiting for actor run {run_id}")

    items_resp = await client.get(
        f"{BASE_URL}/datasets/{dataset_id}/items",
        params={"token": token, "format": "json"},
        timeout=60,
    )
    if items_resp.status_code != 200:
        raise RuntimeError(
            f"Dataset fetch failed ({items_resp.status_code}): {items_resp.text[:200]}"
        )
    return items_resp.json(), cost_usd


async def run_actor(
    actor_id: str,
    payload: Dict[str, Any],
    timeout: int = 120,
) -> List[Dict]:
    """Run an Apify actor with retry, sync→async fallback.

    Retry policy:
        1. Try sync up to APIFY_MAX_RETRIES with exponential backoff on transient errors.
        2. If all sync attempts hit ConnectError/Timeout → fall back to async polling
           (also retried up to APIFY_MAX_RETRIES).

    Args:
        actor_id: Apify actor identifier (e.g. "scrape.badger~twitter-user-scraper").
        payload: Actor input JSON.
        timeout: Seconds to wait for the sync call before falling back.
    """
    token = _check_token()
    actor_short = actor_id.split("~")[-1]
    start = time.time()

    async with httpx.AsyncClient() as client:
        # ── Sync attempts with retry ──
        last_exc: Optional[Exception] = None
        for attempt in range(APIFY_MAX_RETRIES):
            try:
                items = await _run_sync_once(client, actor_id, payload, token, timeout)
                elapsed = time.time() - start
                logger.info(
                    "actor=%s mode=sync items=%d elapsed=%.1fs attempt=%d",
                    actor_short, len(items), elapsed, attempt + 1,
                )
                _emit_metric(actor_short, "success", elapsed, 0.0)
                return items
            except (httpx.ReadTimeout, httpx.ConnectError, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
                last_exc = e
                if attempt < APIFY_MAX_RETRIES - 1:
                    wait = APIFY_RETRY_BASE * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        "actor=%s sync attempt %d/%d failed: %s; retry in %.1fs",
                        actor_short, attempt + 1, APIFY_MAX_RETRIES, e, wait,
                    )
                    await asyncio.sleep(wait)

        logger.warning(
            "actor=%s all %d sync attempts failed, falling back to async polling",
            actor_short, APIFY_MAX_RETRIES,
        )

        # ── Async polling with retry ──
        for attempt in range(APIFY_MAX_RETRIES):
            try:
                items, cost = await _run_async_once(
                    client, actor_id, payload, token, timeout=timeout + 120,
                )
                elapsed = time.time() - start
                logger.info(
                    "actor=%s mode=async items=%d elapsed=%.1fs attempt=%d cost=$%.4f",
                    actor_short, len(items), elapsed, attempt + 1, cost,
                )
                _emit_metric(actor_short, "success", elapsed, cost)
                return items
            except (httpx.ReadTimeout, httpx.ConnectError, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
                last_exc = e
                if attempt < APIFY_MAX_RETRIES - 1:
                    wait = APIFY_RETRY_BASE * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        "actor=%s async attempt %d/%d failed: %s; retry in %.1fs",
                        actor_short, attempt + 1, APIFY_MAX_RETRIES, e, wait,
                    )
                    await asyncio.sleep(wait)

        elapsed = time.time() - start
        _emit_metric(actor_short, "failed", elapsed, 0.0)
        raise RuntimeError(
            f"Apify actor {actor_short} failed after {APIFY_MAX_RETRIES} sync + "
            f"{APIFY_MAX_RETRIES} async attempts: {last_exc}"
        )


def utcnow_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()

"""
Prometheus metrics for X scraper service.

Exposes counters / histograms tracking:
    - HTTP request count and latency (per endpoint, per status)
    - Apify actor calls (per actor, per status, cost in USD)
    - Job lifecycle (created / running / completed / failed)
    - Profile fetches (success / fail)
    - Mongo cache hits

Endpoint: GET /metrics  (text/plain Prometheus format)
"""

from __future__ import annotations

import logging
import time
from typing import Optional

logger = logging.getLogger("x_scraper")

try:
    from prometheus_client import (
        Counter,
        Gauge,
        Histogram,
        CollectorRegistry,
        generate_latest,
        CONTENT_TYPE_LATEST,
    )
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False
    logger.warning("prometheus_client not installed; /metrics will return empty body")
    CONTENT_TYPE_LATEST = "text/plain"


_registry: Optional["CollectorRegistry"] = None

# Metric handles
http_requests_total = None
http_request_duration_seconds = None
apify_calls_total = None
apify_call_duration_seconds = None
apify_cost_usd_total = None
jobs_total = None
jobs_active = None
profiles_total = None
profile_cache_hits_total = None
scrape_do_calls_total = None


def init_metrics() -> None:
    """初始化所有 metrics（幂等）。在 server 启动时调用一次。"""
    global _registry
    global http_requests_total, http_request_duration_seconds
    global apify_calls_total, apify_call_duration_seconds, apify_cost_usd_total
    global jobs_total, jobs_active
    global profiles_total, profile_cache_hits_total, scrape_do_calls_total

    if not _AVAILABLE or _registry is not None:
        return

    _registry = CollectorRegistry()

    http_requests_total = Counter(
        "x_scraper_http_requests_total",
        "HTTP requests by endpoint and status",
        ["endpoint", "method", "status"],
        registry=_registry,
    )
    http_request_duration_seconds = Histogram(
        "x_scraper_http_request_duration_seconds",
        "HTTP request latency",
        ["endpoint", "method"],
        buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
        registry=_registry,
    )

    apify_calls_total = Counter(
        "x_scraper_apify_calls_total",
        "Apify actor calls by actor and status",
        ["actor", "status"],
        registry=_registry,
    )
    apify_call_duration_seconds = Histogram(
        "x_scraper_apify_call_duration_seconds",
        "Apify actor call latency",
        ["actor"],
        buckets=(0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
        registry=_registry,
    )
    apify_cost_usd_total = Counter(
        "x_scraper_apify_cost_usd_total",
        "Cumulative Apify cost in USD by actor",
        ["actor"],
        registry=_registry,
    )

    jobs_total = Counter(
        "x_scraper_jobs_total",
        "Jobs by event type",
        ["event"],  # created, started, completed, failed, cancelled
        registry=_registry,
    )
    jobs_active = Gauge(
        "x_scraper_jobs_active",
        "Currently running jobs on this worker",
        registry=_registry,
    )

    profiles_total = Counter(
        "x_scraper_profiles_total",
        "Profile fetch attempts by status",
        ["status"],  # success, failed, cache_hit
        registry=_registry,
    )
    profile_cache_hits_total = Counter(
        "x_scraper_profile_cache_hits_total",
        "Profile cache hits",
        registry=_registry,
    )
    scrape_do_calls_total = Counter(
        "x_scraper_scrape_do_calls_total",
        "scrape.do calls by HTTP status",
        ["status"],
        registry=_registry,
    )


def _ensure_init():
    if _AVAILABLE and _registry is None:
        init_metrics()


# ───────────────── Hooks (called by other modules) ─────────────────


def record_http_request(endpoint: str, method: str, status: int, duration: float) -> None:
    _ensure_init()
    if not _AVAILABLE:
        return
    http_requests_total.labels(endpoint=endpoint, method=method, status=str(status)).inc()
    http_request_duration_seconds.labels(endpoint=endpoint, method=method).observe(duration)


def record_apify_call(actor: str, status: str, elapsed: float, cost_usd: float) -> None:
    _ensure_init()
    if not _AVAILABLE:
        return
    apify_calls_total.labels(actor=actor, status=status).inc()
    apify_call_duration_seconds.labels(actor=actor).observe(elapsed)
    if cost_usd > 0:
        apify_cost_usd_total.labels(actor=actor).inc(cost_usd)


def record_job_event(event: str) -> None:
    _ensure_init()
    if not _AVAILABLE:
        return
    jobs_total.labels(event=event).inc()


def inc_active_jobs(delta: int = 1) -> None:
    _ensure_init()
    if not _AVAILABLE:
        return
    if delta >= 0:
        jobs_active.inc(delta)
    else:
        jobs_active.dec(-delta)


def record_profile(status: str) -> None:
    """status: success / failed / cache_hit"""
    _ensure_init()
    if not _AVAILABLE:
        return
    profiles_total.labels(status=status).inc()
    if status == "cache_hit":
        profile_cache_hits_total.inc()


def record_scrape_do_call(status: int) -> None:
    _ensure_init()
    if not _AVAILABLE:
        return
    scrape_do_calls_total.labels(status=str(status)).inc()


def render_metrics() -> tuple[bytes, str]:
    """Generate Prometheus text format. Returns (body, content_type)."""
    _ensure_init()
    if not _AVAILABLE:
        return (b"# prometheus_client not installed\n", "text/plain")
    return (generate_latest(_registry), CONTENT_TYPE_LATEST)


# ───────────────── Decorator helper ─────────────────


class TimedHTTP:
    """Context manager: with TimedHTTP('/api/profiles', 'POST') as t: ...; t.set_status(200)"""

    def __init__(self, endpoint: str, method: str):
        self.endpoint = endpoint
        self.method = method
        self.status = 500
        self._start = 0.0

    def __enter__(self):
        self._start = time.monotonic()
        return self

    def __exit__(self, exc_type, exc, tb):
        duration = time.monotonic() - self._start
        if exc_type is not None:
            self.status = 500
        record_http_request(self.endpoint, self.method, self.status, duration)
        return False

    def set_status(self, status: int) -> None:
        self.status = status

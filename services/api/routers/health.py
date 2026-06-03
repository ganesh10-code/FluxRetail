"""
Production-style /health endpoint for FluxRetail API.

Verifies:
  1. PostgreSQL connectivity — live SELECT 1 query with latency measurement
  2. Redis connectivity — PING command with latency measurement
  3. Kafka broker availability — AdminClient metadata fetch
  4. Latest event timestamp from Redis (set by Kafka consumer on each event)
  5. Pipeline heartbeat freshness from Redis

Response shape:
  {
    "status": "healthy" | "degraded" | "unhealthy",
    "timestamp": "ISO-8601",
    "components": {
      "postgres":           {"status": "ok",      "latency_ms": 3.2},
      "redis":              {"status": "ok",      "latency_ms": 0.8},
      "kafka":              {"status": "ok",      "detail": "3 topics"},
      "event_feed":         {"status": "warning", "detail": "STALE_FEED: last event 90s ago"},
      "pipeline_heartbeat": {"status": "warning", "detail": "No heartbeat in 45s"}
    },
    "latest_event_at":        "ISO-8601 or null",
    "pipeline_heartbeat_at":  "ISO-8601 or null",
    "warnings": ["STALE_FEED"]
  }

Status rules:
  - HEALTHY:   all components ok, no warnings
  - DEGRADED:  some components ok, some warnings (but core services up)
  - UNHEALTHY: postgres or redis is down
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import redis.asyncio as aioredis
import structlog
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import APIRouter, Request
from sqlalchemy import text

from config import settings
from consumers.kafka_consumer import REDIS_LATEST_EVENT_KEY, REDIS_PIPELINE_HEARTBEAT_KEY
from db.session import AsyncSessionLocal
from schemas.events import ComponentHealth, HealthResponse, HealthStatus

logger = structlog.get_logger(__name__)

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Full system health check",
    description=(
        "Verifies connectivity to PostgreSQL, Redis, and Kafka. "
        "Reports STALE_FEED if no events received within the configured timeout. "
        "Reports pipeline heartbeat freshness."
    ),
)
async def health_check(request: Request) -> HealthResponse:
    """
    Full production-style health check.

    Each component is checked independently so a single failure
    does not prevent other checks from completing.
    """
    components: dict[str, ComponentHealth] = {}
    warnings: list[str] = []
    latest_event_at: datetime | None = None
    pipeline_heartbeat_at: datetime | None = None

    # Get the shared Redis client from app state
    redis_client: aioredis.Redis = request.app.state.redis

    # ── 1. PostgreSQL ────────────────────────────────────────────────────────
    components["postgres"] = await _check_postgres()

    # ── 2. Redis ─────────────────────────────────────────────────────────────
    components["redis"] = await _check_redis(redis_client)

    # ── 3. Kafka ─────────────────────────────────────────────────────────────
    components["kafka"] = await _check_kafka()

    # ── 4. Event feed freshness ───────────────────────────────────────────────
    feed_health, latest_event_at = await _check_event_feed(redis_client)
    components["event_feed"] = feed_health
    if feed_health.status == "warning":
        warnings.append("STALE_FEED")

    # ── 5. Pipeline heartbeat ─────────────────────────────────────────────────
    heartbeat_health, pipeline_heartbeat_at = await _check_pipeline_heartbeat(redis_client)
    components["pipeline_heartbeat"] = heartbeat_health
    if heartbeat_health.status == "warning":
        warnings.append("PIPELINE_STALE")

    # ── Determine overall status ──────────────────────────────────────────────
    core_statuses = [
        components["postgres"].status,
        components["redis"].status,
    ]
    all_statuses = [c.status for c in components.values()]

    if "error" in core_statuses:
        overall = HealthStatus.UNHEALTHY
    elif "error" in all_statuses or "warning" in all_statuses:
        overall = HealthStatus.DEGRADED
    else:
        overall = HealthStatus.HEALTHY

    response = HealthResponse(
        status=overall,
        timestamp=datetime.now(timezone.utc),
        components=components,
        latest_event_at=latest_event_at,
        pipeline_heartbeat_at=pipeline_heartbeat_at,
        warnings=warnings,
    )

    logger.info(
        "health_check",
        status=overall,
        warnings=warnings,
        postgres=components["postgres"].status,
        redis=components["redis"].status,
        kafka=components["kafka"].status,
    )

    return response


async def _check_postgres() -> ComponentHealth:
    """Verify PostgreSQL connectivity with a live query and measure latency."""
    t0 = time.monotonic()
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("SELECT 1"))
        latency_ms = (time.monotonic() - t0) * 1000
        return ComponentHealth(
            status="ok",
            detail="reachable",
            latency_ms=round(latency_ms, 2),
        )
    except Exception as exc:
        logger.error("health_postgres_failed", error=str(exc))
        return ComponentHealth(
            status="error",
            detail=f"Connection failed: {type(exc).__name__}",
        )


async def _check_redis(redis_client: aioredis.Redis) -> ComponentHealth:
    """Verify Redis connectivity with PING and measure latency."""
    t0 = time.monotonic()
    try:
        response = await redis_client.ping()
        latency_ms = (time.monotonic() - t0) * 1000
        if response:
            return ComponentHealth(
                status="ok",
                detail="PONG",
                latency_ms=round(latency_ms, 2),
            )
        return ComponentHealth(status="error", detail="Unexpected PING response")
    except Exception as exc:
        logger.error("health_redis_failed", error=str(exc))
        return ComponentHealth(
            status="error",
            detail=f"Connection failed: {type(exc).__name__}",
        )


async def _check_kafka() -> ComponentHealth:
    """Verify Kafka broker availability by fetching topic metadata."""
    t0 = time.monotonic()
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        request_timeout_ms=5000,
    )
    try:
        await admin.start()
        topics = await admin.list_topics()
        latency_ms = (time.monotonic() - t0) * 1000
        return ComponentHealth(
            status="ok",
            detail=f"{len(topics)} topics available",
            latency_ms=round(latency_ms, 2),
        )
    except Exception as exc:
        logger.error("health_kafka_failed", error=str(exc))
        return ComponentHealth(
            status="error",
            detail=f"Broker unreachable: {type(exc).__name__}",
        )
    finally:
        try:
            await admin.close()
        except Exception:
            pass


async def _check_event_feed(
    redis_client: aioredis.Redis,
) -> tuple[ComponentHealth, datetime | None]:
    """
    Check event feed freshness.

    Reads the latest event timestamp stored in Redis by the Kafka consumer.
    Returns STALE_FEED warning if no events within the configured timeout.
    """
    timeout = settings.health_stale_feed_timeout_seconds
    try:
        raw = await redis_client.get(REDIS_LATEST_EVENT_KEY)
        if raw is None:
            return (
                ComponentHealth(
                    status="warning",
                    detail="No events received yet (pipeline may not have started)",
                ),
                None,
            )

        latest_ts = datetime.fromisoformat(raw.decode())
        now = datetime.now(timezone.utc)
        age_seconds = (now - latest_ts).total_seconds()

        if age_seconds > timeout:
            return (
                ComponentHealth(
                    status="warning",
                    detail=(
                        f"STALE_FEED: last event {int(age_seconds)}s ago "
                        f"(threshold: {timeout}s)"
                    ),
                ),
                latest_ts,
            )

        return (
            ComponentHealth(
                status="ok",
                detail=f"Last event {int(age_seconds)}s ago",
            ),
            latest_ts,
        )

    except Exception as exc:
        logger.error("health_event_feed_check_failed", error=str(exc))
        return (
            ComponentHealth(
                status="warning",
                detail=f"Feed check failed: {type(exc).__name__}",
            ),
            None,
        )


async def _check_pipeline_heartbeat(
    redis_client: aioredis.Redis,
) -> tuple[ComponentHealth, datetime | None]:
    """
    Check pipeline process heartbeat freshness.

    The pipeline service writes its heartbeat to Redis on every processed frame.
    If the heartbeat is stale, the pipeline may have crashed or stalled.
    """
    timeout = settings.health_pipeline_heartbeat_timeout_seconds
    try:
        raw = await redis_client.get(REDIS_PIPELINE_HEARTBEAT_KEY)
        if raw is None:
            return (
                ComponentHealth(
                    status="warning",
                    detail="No pipeline heartbeat found (pipeline not started yet)",
                ),
                None,
            )

        heartbeat_ts = datetime.fromisoformat(raw.decode())
        now = datetime.now(timezone.utc)
        age_seconds = (now - heartbeat_ts).total_seconds()

        if age_seconds > timeout:
            return (
                ComponentHealth(
                    status="warning",
                    detail=(
                        f"Pipeline heartbeat stale: {int(age_seconds)}s ago "
                        f"(threshold: {timeout}s)"
                    ),
                ),
                heartbeat_ts,
            )

        return (
            ComponentHealth(
                status="ok",
                detail=f"Pipeline active: heartbeat {int(age_seconds)}s ago",
            ),
            heartbeat_ts,
        )

    except Exception as exc:
        logger.error("health_heartbeat_check_failed", error=str(exc))
        return (
            ComponentHealth(
                status="warning",
                detail=f"Heartbeat check failed: {type(exc).__name__}",
            ),
            None,
        )

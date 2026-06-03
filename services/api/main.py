"""
FluxRetail API Service — FastAPI application entry point.

Lifespan:
  startup:  initialise Redis, start Kafka consumer background task
  shutdown: gracefully cancel Kafka consumer, close Redis connection pool
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

import redis.asyncio as aioredis
import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from consumers.kafka_consumer import run_consumer
from logging_config import configure_logging
from routers import events, health, metrics
from websocket.manager import ws_manager

configure_logging(level=settings.log_level, fmt=settings.log_format)
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: startup and shutdown logic."""
    logger.info("api_startup", store_id=settings.store_id)

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_client = aioredis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=False,
    )
    app.state.redis = redis_client
    logger.info("redis_connected", url=settings.redis_url)

    # ── Kafka consumer background task ─────────────────────────────────────────
    def handle_consumer_result(task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.exception("kafka_consumer_task_crashed_fatally", error=str(exc))

    consumer_task = asyncio.create_task(run_consumer(redis_client))
    consumer_task.add_done_callback(handle_consumer_result)
    logger.info("kafka_consumer_task_started")

    yield  # ── Application runs here ───────────────────────────────────────────

    # ── Shutdown ──────────────────────────────────────────────────────────────
    logger.info("api_shutdown")
    consumer_task.cancel()
    try:
        await asyncio.wait_for(consumer_task, timeout=10.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    await redis_client.aclose()
    logger.info("redis_closed")


# ── Application factory ────────────────────────────────────────────────────────
app = FastAPI(
    title="FluxRetail API",
    description=(
        "Real-time retail analytics API. "
        "Consumes visitor events from Kafka, persists to PostgreSQL, "
        "and streams live KPIs via WebSocket."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── CORS ───────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ────────────────────────────────────────────────────────────────────
app.include_router(health.router)
app.include_router(events.router, prefix="/api/v1")
app.include_router(metrics.router, prefix="/api/v1")


# ── WebSocket endpoint ─────────────────────────────────────────────────────────
@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket) -> None:
    """
    WebSocket endpoint for real-time event and KPI streaming.

    Clients receive two message types:
      - {"message_type": "event", "payload": {...}}  — every retail event
      - {"message_type": "kpi",   "payload": {...}}  — KPI snapshot every 2 s
    """
    await ws_manager.connect(websocket)
    try:
        while True:
            # Keep the connection alive; the consumer pushes data to clients
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as exc:
        logger.warning("websocket_error", error=str(exc))
        ws_manager.disconnect(websocket)


# ── Root redirect ──────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def root() -> dict:
    return {"service": "FluxRetail API", "version": "1.0.0", "docs": "/docs"}

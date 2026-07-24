"""
Async Kafka consumer for the FluxRetail API service.

Consumes from fluxretail.events and:
  1. Persists each event to PostgreSQL
  2. Broadcasts raw events immediately via WebSocket
  3. Accumulates in-memory KPI state
  4. Broadcasts compact KPI updates every 2 seconds
  5. Updates Redis with latest event timestamp (for /health staleness check)

Occupancy correctness rules:
  - ENTRY event  → add visitor_id to active set
  - EXIT event   → remove visitor_id from active set
  - All others   → do NOT touch active set (prevents inflation from zone events)
  - Stale sessions (no update for >120 s) are pruned in to_kpi_dict()
  - active_visitors is always capped at total_visitors_today
  - Deduplication uses a set (O(1)) — no size cap needed
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import redis.asyncio as aioredis
import structlog
from aiokafka import AIOKafkaConsumer

from config import settings
from db.session import AsyncSessionLocal
from models.event import EventModel
from schemas.events import RetailEventIngest
from websocket.manager import ws_manager

logger = structlog.get_logger(__name__)

# Redis key for latest event timestamp (read by /health endpoint)
REDIS_LATEST_EVENT_KEY = "fluxretail:latest_event_ts"
REDIS_PIPELINE_HEARTBEAT_KEY = "fluxretail:pipeline_heartbeat"

# KPI broadcast interval in seconds
KPI_BROADCAST_INTERVAL = 2.0

# Active session stale timeout in seconds
_ACTIVE_SESSION_TTL_SECONDS = 120


class KPIAccumulator:
    """
    In-memory KPI state accumulator.

    Aggregates event stream into compact store metrics that are
    broadcast to the dashboard every KPI_BROADCAST_INTERVAL seconds.

    Occupancy invariant:
        active_visitors ≤ total_visitors_today at all times.
    """

    def __init__(self) -> None:
        self.total_events: int = 0
        self.visitor_count: int = 0
        self.zone_counts: dict[str, int] = defaultdict(int)
        self.event_type_counts: dict[str, int] = defaultdict(int)
        self.conversions: int = 0
        self.last_event_ts: datetime | None = None

        # Occupancy tracking — only ENTRY/EXIT mutate these
        self._active_visitor_ids: set[str] = set()
        self._exited_visitor_ids: set[str] = set()
        self._all_visitor_ids: set[str] = set()
        # Maps visitor_id → last event wall-clock time (for stale pruning)
        self._active_visitor_last_seen: dict[str, datetime] = {}

        # Conversion tracking — set ensures we count each visitor exactly once
        self._converted_visitor_ids: set[str] = set()

        # Deduplication — set for O(1) lookup; no size cap needed
        self._processed_event_ids: set[str] = set()

    def ingest(self, event: dict[str, Any]) -> None:
        """Update accumulators from a new event."""
        event_id = event.get("event_id")
        if not event_id:
            return

        # Deduplication — skip events already seen in this session
        if event_id in self._processed_event_ids:
            return
        self._processed_event_ids.add(event_id)

        self.total_events += 1
        event_type = event.get("event_type", "")
        visitor_id = event.get("visitor_id", "")
        zone_id = event.get("zone_id")
        metadata = event.get("metadata", {})
        now = datetime.now(timezone.utc)

        self.event_type_counts[event_type] += 1

        # Track total unique visitors (all-time for this session)
        if visitor_id:
            self._all_visitor_ids.add(visitor_id)
            self.visitor_count = len(self._all_visitor_ids)

        # ── Occupancy: only ENTRY increments, only EXIT decrements ──────────
        if event_type == "ENTRY":
            self._active_visitor_ids.add(visitor_id)
            self._active_visitor_last_seen[visitor_id] = now
            self._exited_visitor_ids.discard(visitor_id)
        elif event_type == "EXIT":
            self._active_visitor_ids.discard(visitor_id)
            self._active_visitor_last_seen.pop(visitor_id, None)
            self._exited_visitor_ids.add(visitor_id)
        else:
            # Zone events (ZONE_ENTER, ZONE_DWELL, ZONE_EXIT): refresh last-seen
            # for visitors already active, but do NOT auto-add new visitors.
            if visitor_id and visitor_id in self._active_visitor_ids:
                self._active_visitor_last_seen[visitor_id] = now

        if zone_id:
            self.zone_counts[zone_id] += 1

        # Track conversions — count each visitor at most once
        if (
            metadata.get("conversion_status") == "CONVERTED"
            and visitor_id
            and visitor_id not in self._converted_visitor_ids
        ):
            self._converted_visitor_ids.add(visitor_id)
            self.conversions = len(self._converted_visitor_ids)

        self.last_event_ts = now

    def to_kpi_dict(self) -> dict[str, Any]:
        """Return a KPI snapshot, pruning stale active sessions first."""
        now = datetime.now(timezone.utc)
        stale_limit = now - timedelta(seconds=_ACTIVE_SESSION_TTL_SECONDS)

        # Expire active visitors who haven't had an event update recently
        stale_vids = [
            vid for vid, ts in list(self._active_visitor_last_seen.items())
            if ts < stale_limit
        ]
        for vid in stale_vids:
            self._active_visitor_ids.discard(vid)
            self._active_visitor_last_seen.pop(vid, None)
            logger.debug("active_session_expired", visitor_id=vid)

        # Invariant: active_visitors must never exceed total_visitors_today
        active = min(len(self._active_visitor_ids), self.visitor_count)

        peak_zone = (
            max(self.zone_counts, key=lambda k: self.zone_counts[k])
            if self.zone_counts
            else None
        )
        conversion_rate = (self.conversions / max(self.visitor_count, 1)) * 100
        return {
            "total_events": self.total_events,
            "total_visitors_today": self.visitor_count,
            "active_visitors": active,
            "conversions": self.conversions,
            "conversion_rate": round(conversion_rate, 1),
            "peak_zone": peak_zone,
            "zone_counts": dict(self.zone_counts),
            "event_type_counts": dict(self.event_type_counts),
            "last_event_at": self.last_event_ts.isoformat() if self.last_event_ts else None,
        }


# Module-level KPI state shared by Kafka consumer and API routes
kpi_state = KPIAccumulator()


async def persist_event(event_dict: dict[str, Any]) -> None:
    """Persist a Kafka event to PostgreSQL."""
    event_id = event_dict.get("event_id")
    logger.info("db_insertion_attempt", event_id=event_id, event_type=event_dict.get("event_type"))
    async with AsyncSessionLocal() as db:
        try:
            event_ts = datetime.fromisoformat(event_dict["timestamp"])
            db_event = EventModel(
                event_id=event_dict["event_id"],
                store_id=event_dict["store_id"],
                camera_id=event_dict["camera_id"],
                visitor_id=event_dict["visitor_id"],
                event_type=event_dict["event_type"],
                zone_id=event_dict.get("zone_id"),
                dwell_ms=event_dict.get("dwell_ms", 0),
                is_staff=event_dict.get("is_staff", False),
                confidence=event_dict.get("confidence", 1.0),
                event_metadata=event_dict.get("metadata", {}),
                event_timestamp=event_ts,
            )
            db.add(db_event)
            await db.commit()
            logger.info("db_insertion_success", event_id=event_id)
        except Exception as exc:
            await db.rollback()
            logger.exception("db_insertion_failed", event_id=event_id, error=str(exc))


async def run_consumer(redis_client: aioredis.Redis) -> None:
    """
    Main Kafka consumer loop.

    Runs as a background asyncio task from the FastAPI lifespan.
    Resilient to transport-level crashes and individual message processing failures.
    """
    # Start periodic KPI broadcaster task
    broadcast_task = asyncio.create_task(_kpi_broadcast_loop())

    try:
        while True:
            logger.info("initializing_kafka_consumer", bootstrap_servers=settings.kafka_bootstrap_servers)
            consumer = AIOKafkaConsumer(
                settings.kafka_events_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=settings.kafka_consumer_group,
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
            )

            # Retry loop for Kafka startup
            connected = False
            for attempt in range(30):
                try:
                    await consumer.start()
                    logger.info("kafka_consumer_started", topic=settings.kafka_events_topic)
                    connected = True
                    break
                except Exception as exc:
                    logger.warning("kafka_consumer_not_ready", attempt=attempt, error=str(exc))
                    await asyncio.sleep(5)
            
            if not connected:
                logger.error("kafka_consumer_failed_to_start")
                await asyncio.sleep(10)
                continue

            try:
                async for msg in consumer:
                    logger.info("kafka_message_received", offset=msg.offset, key=msg.key)
                    
                    try:
                        event_dict: dict[str, Any] = msg.value
                    except Exception as exc:
                        logger.exception("kafka_message_deserialization_failed", offset=msg.offset, error=str(exc))
                        continue

                    # Schema validation
                    try:
                        validated = RetailEventIngest.model_validate(event_dict)
                        logger.info("schema_validation_success", event_id=validated.event_id)
                    except Exception as exc:
                        logger.exception("schema_validation_failed", raw_payload=event_dict, error=str(exc))
                        continue

                    # 1. Update in-memory KPI state
                    try:
                        kpi_state.ingest(event_dict)
                        logger.info("kpi_state_updated", event_id=validated.event_id)
                    except Exception as exc:
                        logger.exception("kpi_state_update_failed", event_id=validated.event_id, error=str(exc))

                    # 2. Broadcast raw event immediately
                    try:
                        logger.info("websocket_broadcasting", event_id=validated.event_id, active_connections=ws_manager.active_connections)
                        await ws_manager.broadcast_event(event_dict)
                        logger.info("websocket_broadcast_success", event_id=validated.event_id)
                    except Exception as exc:
                        logger.exception("websocket_broadcast_failed", event_id=validated.event_id, error=str(exc))

                    # 3. Persist to PostgreSQL
                    asyncio.create_task(persist_event(event_dict))

                    # 4. Update Redis latest-event timestamp (for /health staleness check)
                    try:
                        await redis_client.set(
                            REDIS_LATEST_EVENT_KEY,
                            datetime.now(timezone.utc).isoformat(),
                            ex=3600,  # TTL: 1 hour
                        )
                    except Exception as exc:
                        logger.warning("redis_heartbeat_update_failed", error=str(exc))

                    logger.debug(
                        "event_consumed_finished",
                        event_type=event_dict.get("event_type"),
                        visitor_id=event_dict.get("visitor_id"),
                        zone_id=event_dict.get("zone_id"),
                    )

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("kafka_consumer_loop_crashed", error=str(exc))
                try:
                    await consumer.stop()
                except Exception:
                    pass
                await asyncio.sleep(5)  # Grace period / backoff before reconnecting
            else:
                # If consumer finished cleanly
                break

    except asyncio.CancelledError:
        logger.info("kafka_consumer_cancelled")
    finally:
        broadcast_task.cancel()
        try:
            await consumer.stop()
        except Exception:
            pass
        logger.info("kafka_consumer_stopped")


async def _kpi_broadcast_loop() -> None:
    """Broadcast compact KPI updates every KPI_BROADCAST_INTERVAL seconds."""
    while True:
        try:
            await asyncio.sleep(KPI_BROADCAST_INTERVAL)
            if ws_manager.active_connections > 0:
                await ws_manager.broadcast_kpi(kpi_state.to_kpi_dict())
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("kpi_broadcast_error", error=str(exc))

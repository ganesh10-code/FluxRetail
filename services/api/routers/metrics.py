"""
Store metrics, funnel, heatmap, and anomaly endpoints.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

import structlog
from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from consumers.kafka_consumer import kpi_state
from db.session import get_db
from models.event import EventModel
from models.session import SessionModel
from schemas.events import (
    AnomalyResponse,
    FunnelResponse,
    HeatmapResponse,
    StoreMetricsResponse,
)

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/stores", tags=["Metrics"])


@router.get(
    "/{store_id}/metrics",
    response_model=StoreMetricsResponse,
    summary="Get store KPI metrics",
)
async def get_store_metrics(
    store_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    hours: int = Query(default=24, ge=1, le=168),
) -> StoreMetricsResponse:
    """Retrieve aggregated KPI metrics for a store over the past N hours."""
    now = datetime.now(timezone.utc)
    period_start = now - timedelta(hours=hours)

    # Total unique visitors (ENTRY events)
    visitor_stmt = (
        select(func.count(func.distinct(EventModel.visitor_id)))
        .where(EventModel.store_id == store_id)
        .where(EventModel.event_type == "ENTRY")
        .where(EventModel.event_timestamp >= period_start)
    )
    visitor_result = await db.execute(visitor_stmt)
    total_visitors: int = visitor_result.scalar() or 0

    # Average dwell time from ZONE_DWELL events
    dwell_stmt = (
        select(func.avg(EventModel.dwell_ms))
        .where(EventModel.store_id == store_id)
        .where(EventModel.event_type == "ZONE_DWELL")
        .where(EventModel.event_timestamp >= period_start)
    )
    dwell_result = await db.execute(dwell_stmt)
    avg_dwell_ms: float = dwell_result.scalar() or 0.0

    # Peak zone by ZONE_ENTER count
    zone_stmt = (
        select(EventModel.zone_id, func.count(EventModel.id).label("cnt"))
        .where(EventModel.store_id == store_id)
        .where(EventModel.event_type == "ZONE_ENTER")
        .where(EventModel.event_timestamp >= period_start)
        .where(EventModel.zone_id.isnot(None))
        .group_by(EventModel.zone_id)
        .order_by(func.count(EventModel.id).desc())
        .limit(1)
    )
    zone_result = await db.execute(zone_stmt)
    peak_zone_row = zone_result.first()
    peak_zone: str | None = peak_zone_row[0] if peak_zone_row else None

    # Zone breakdown — all ZONE_ENTER counts grouped by zone
    zone_breakdown_stmt = (
        select(EventModel.zone_id, func.count(EventModel.id).label("cnt"))
        .where(EventModel.store_id == store_id)
        .where(EventModel.event_type == "ZONE_ENTER")
        .where(EventModel.event_timestamp >= period_start)
        .where(EventModel.zone_id.isnot(None))
        .group_by(EventModel.zone_id)
    )
    zone_breakdown_result = await db.execute(zone_breakdown_stmt)
    zone_breakdown: dict[str, int] = {
        row[0]: row[1] for row in zone_breakdown_result.fetchall()
    }

    # Conversion rate — visitors who entered the billing zone dynamically defined in config
    from config_loader import load_store_config
    try:
        store_config = load_store_config(store_id)
        billing_zone_id = store_config.get_billing_zone_id()
    except Exception:
        billing_zone_id = "checkout"

    converted_stmt = (
        select(func.count(func.distinct(EventModel.visitor_id)))
        .where(EventModel.store_id == store_id)
        .where(EventModel.zone_id == billing_zone_id)
        .where(EventModel.event_timestamp >= period_start)
    )
    converted_result = await db.execute(converted_stmt)
    converted_count: int = converted_result.scalar() or 0
    conversion_rate = (converted_count / max(total_visitors, 1)) * 100

    return StoreMetricsResponse(
        store_id=store_id,
        period_start=period_start,
        period_end=now,
        total_visitors=total_visitors,
        avg_dwell_seconds=round(avg_dwell_ms / 1000, 1),
        peak_zone=peak_zone,
        conversion_rate=round(conversion_rate, 1),
        zone_breakdown=zone_breakdown,
    )


@router.get(
    "/{store_id}/funnel",
    response_model=FunnelResponse,
    summary="Get zone conversion funnel",
)
async def get_store_funnel(
    store_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    hours: int = Query(default=24, ge=1, le=168),
) -> FunnelResponse:
    """Zone-by-zone conversion funnel showing visitor flow through store zones."""
    now = datetime.now(timezone.utc)
    period_start = now - timedelta(hours=hours)

    from config_loader import load_store_config
    try:
        store_config = load_store_config(store_id)
        # 1. Entry stage first
        entry_stmt = (
            select(func.count(func.distinct(EventModel.visitor_id)))
            .where(EventModel.store_id == store_id)
            .where(EventModel.event_type == "ENTRY")
            .where(EventModel.event_timestamp >= period_start)
        )
        entry_result = await db.execute(entry_stmt)
        stages = [{"zone_id": "Entry", "visitor_count": entry_result.scalar() or 0}]

        # 2. Middle retail zones
        billing_zone_id = store_config.get_billing_zone_id()
        for zone_id in store_config.zones:
            if zone_id == billing_zone_id:
                continue
            stmt = (
                select(func.count(func.distinct(EventModel.visitor_id)))
                .where(EventModel.store_id == store_id)
                .where(EventModel.zone_id == zone_id)
                .where(EventModel.event_timestamp >= period_start)
            )
            result = await db.execute(stmt)
            stages.append({"zone_id": store_config.get_zone_display_name(zone_id), "visitor_count": result.scalar() or 0})

        # 3. Billing zone last
        billing_stmt = (
            select(func.count(func.distinct(EventModel.visitor_id)))
            .where(EventModel.store_id == store_id)
            .where(EventModel.zone_id == billing_zone_id)
            .where(EventModel.event_timestamp >= period_start)
        )
        billing_result = await db.execute(billing_stmt)
        stages.append({"zone_id": store_config.get_zone_display_name(billing_zone_id), "visitor_count": billing_result.scalar() or 0})
    except Exception as exc:
        logger.exception("funnel_load_failed", error=str(exc))
        # Fallback to defaults if store config fails to load
        zones_ordered = [
            "ENTRY_ZONE",
            "CENTER_ZONE",
            "MAKEUP_ZONE",
            "SKINCARE_ZONE",
            "BILLING_ZONE",
        ]
        stages = []
        for zone in zones_ordered:
            stmt = (
                select(func.count(func.distinct(EventModel.visitor_id)))
                .where(EventModel.store_id == store_id)
                .where(EventModel.zone_id == zone)
                .where(EventModel.event_timestamp >= period_start)
            )
            result = await db.execute(stmt)
            count: int = result.scalar() or 0
            stages.append({"zone_id": zone, "visitor_count": count})

    return FunnelResponse(store_id=store_id, stages=stages)


@router.get(
    "/{store_id}/heatmap",
    response_model=HeatmapResponse,
    summary="Get zone dwell heatmap data",
)
async def get_heatmap(
    store_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    hours: int = Query(default=24, ge=1, le=168),
) -> HeatmapResponse:
    """Average dwell time per zone — used to drive heatmap visualisation on the dashboard."""
    now = datetime.now(timezone.utc)
    period_start = now - timedelta(hours=hours)

    stmt = (
        select(
            EventModel.zone_id,
            func.avg(EventModel.dwell_ms).label("avg_dwell_ms"),
            func.count(EventModel.id).label("event_count"),
        )
        .where(EventModel.store_id == store_id)
        .where(EventModel.event_type == "ZONE_DWELL")
        .where(EventModel.event_timestamp >= period_start)
        .where(EventModel.zone_id.isnot(None))
        .group_by(EventModel.zone_id)
    )
    result = await db.execute(stmt)
    rows = result.fetchall()

    zones = [
        {
            "zone_id": row[0],
            "avg_dwell_seconds": round((row[1] or 0) / 1000, 1),
            "event_count": row[2],
        }
        for row in rows
    ]

    return HeatmapResponse(store_id=store_id, zones=zones)


@router.get(
    "/{store_id}/anomalies",
    response_model=AnomalyResponse,
    summary="Get operational anomaly flags",
)
async def get_anomalies(
    store_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    hours: int = Query(default=1, ge=1, le=24),
) -> AnomalyResponse:
    """
    Detect operational anomalies from recent event patterns in the database.

    Anomaly rules:
    - QUEUE_CONGESTION:    >3 visitors at billing counter in last 10 minutes (severity: high)
    - ZONE_OVERCROWDING:   >5 visitors in a retail zone in last 10 minutes (severity: medium)
    - LONG_DWELL:          avg dwell time > 60s in any zone in last 1 hour (severity: low)
    - LOW_CONVERSION:      conversion rate < 20% in last 1 hour (minimum 5 entries) (severity: medium)
    """
    now = datetime.now(timezone.utc)
    anomalies: list[dict[str, Any]] = []

    from config_loader import load_store_config
    try:
        store_config = load_store_config(store_id)
        billing_zone_id = store_config.get_billing_zone_id()
    except Exception:
        billing_zone_id = "checkout"
        store_config = None

    # 1. Queue Congestion (High Severity)
    billing_window = now - timedelta(minutes=10)
    billing_stmt = (
        select(func.count(func.distinct(EventModel.visitor_id)))
        .where(EventModel.store_id == store_id)
        .where(EventModel.zone_id == billing_zone_id)
        .where(EventModel.event_type == "ZONE_ENTER")
        .where(EventModel.event_timestamp >= billing_window)
    )
    billing_result = await db.execute(billing_stmt)
    billing_visitors: int = billing_result.scalar() or 0
    if billing_visitors > 3:
        anomalies.append({
            "type": "QUEUE_CONGESTION",
            "severity": "high",
            "detail": f"{billing_visitors} visitors at billing counter in last 10 minutes",
            "detected_at": now.isoformat(),
            "affected_zone": store_config.get_zone_display_name(billing_zone_id) if store_config else billing_zone_id.replace('_', ' ').title(),
        })

    if store_config:
        # 2. Zone Overcrowding (Medium Severity)
        for zone in store_config.zones:
            if zone == billing_zone_id:
                continue
            zone_stmt = (
                select(func.count(func.distinct(EventModel.visitor_id)))
                .where(EventModel.store_id == store_id)
                .where(EventModel.zone_id == zone)
                .where(EventModel.event_type == "ZONE_ENTER")
                .where(EventModel.event_timestamp >= billing_window)
            )
            zone_result = await db.execute(zone_stmt)
            zone_visitors = zone_result.scalar() or 0
            if zone_visitors > 5:
                anomalies.append({
                    "type": "ZONE_OVERCROWDING",
                    "severity": "medium",
                    "detail": f"{store_config.get_zone_display_name(zone)} zone has {zone_visitors} active shoppers in the last 10 minutes",
                    "detected_at": now.isoformat(),
                    "affected_zone": store_config.get_zone_display_name(zone),
                })

        # 3. Long Dwell (Low Severity)
        dwell_window = now - timedelta(hours=1)
        for zone in store_config.zones:
            dwell_stmt = (
                select(func.avg(EventModel.dwell_ms))
                .where(EventModel.store_id == store_id)
                .where(EventModel.zone_id == zone)
                .where(EventModel.event_timestamp >= dwell_window)
            )
            dwell_res = await db.execute(dwell_stmt)
            avg_dwell = dwell_res.scalar() or 0.0
            if avg_dwell > 60000:
                anomalies.append({
                    "type": "LONG_DWELL",
                    "severity": "low",
                    "detail": f"Shoppers dwelling for an average of {round(avg_dwell / 1000, 1)}s in {store_config.get_zone_display_name(zone)}",
                    "detected_at": now.isoformat(),
                    "affected_zone": store_config.get_zone_display_name(zone),
                })

        # 4. Low Conversion (Medium Severity)
        conversion_window = now - timedelta(hours=1)
        entries_stmt = (
            select(func.count(func.distinct(EventModel.visitor_id)))
            .where(EventModel.store_id == store_id)
            .where(EventModel.event_type == "ENTRY")
            .where(EventModel.event_timestamp >= conversion_window)
        )
        entries_res = await db.execute(entries_stmt)
        total_ents = entries_res.scalar() or 0
        if total_ents >= 5:
            conv_stmt = (
                select(func.count(func.distinct(EventModel.visitor_id)))
                .where(EventModel.store_id == store_id)
                .where(EventModel.zone_id == billing_zone_id)
                .where(EventModel.event_timestamp >= conversion_window)
            )
            conv_res = await db.execute(conv_stmt)
            conv_count = conv_res.scalar() or 0
            conv_rate = (conv_count / total_ents) * 100
            if conv_rate < 20.0:
                anomalies.append({
                    "type": "LOW_CONVERSION",
                    "severity": "medium",
                    "detail": f"Low checkout conversion rate of {round(conv_rate, 1)}% (out of {total_ents} visitors in the last hour)",
                    "detected_at": now.isoformat(),
                    "affected_zone": store_config.get_zone_display_name(billing_zone_id) if store_config else billing_zone_id.replace('_', ' ').title(),
                })

    return AnomalyResponse(store_id=store_id, anomalies=anomalies)

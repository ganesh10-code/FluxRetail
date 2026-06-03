"""
Events router — manual event ingestion and recent event retrieval.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.session import get_db
from models.event import EventModel
from schemas.events import RetailEventIngest, RetailEventResponse

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/events", tags=["Events"])


@router.post(
    "/ingest",
    response_model=RetailEventResponse,
    summary="Manually ingest a retail event",
    status_code=201,
)
async def ingest_event(
    event: RetailEventIngest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> RetailEventResponse:
    """Manually inject a retail event into the database (for testing / manual data entry)."""
    db_event = EventModel(
        event_id=event.event_id,
        store_id=event.store_id,
        camera_id=event.camera_id,
        visitor_id=event.visitor_id,
        event_type=event.event_type.value,
        zone_id=event.zone_id,
        dwell_ms=event.dwell_ms,
        is_staff=event.is_staff,
        confidence=event.confidence,
        event_metadata=event.metadata,
        event_timestamp=event.timestamp,
    )
    db.add(db_event)
    await db.flush()
    await db.refresh(db_event)
    logger.info(
        "event_manually_ingested",
        event_id=event.event_id,
        event_type=event.event_type,
    )
    return RetailEventResponse.model_validate(db_event)


@router.get(
    "/recent",
    response_model=list[RetailEventResponse],
    summary="Get recent retail events",
)
async def get_recent_events(
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = Query(default=50, ge=1, le=500),
    store_id: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
) -> list[RetailEventResponse]:
    """Retrieve the most recent retail events, with optional filters."""
    stmt = select(EventModel).order_by(desc(EventModel.event_timestamp)).limit(limit)
    if store_id:
        stmt = stmt.where(EventModel.store_id == store_id)
    if event_type:
        stmt = stmt.where(EventModel.event_type == event_type)
    result = await db.execute(stmt)
    events = result.scalars().all()
    return [RetailEventResponse.model_validate(e) for e in events]

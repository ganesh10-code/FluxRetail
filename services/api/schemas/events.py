"""
Pydantic v2 schemas for API request/response validation.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventTypeEnum(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    ZONE_ENTER = "ZONE_ENTER"
    ZONE_EXIT = "ZONE_EXIT"
    ZONE_DWELL = "ZONE_DWELL"


class RetailEventIngest(BaseModel):
    """Schema for manually ingesting a retail event via POST /events/ingest."""

    event_id: str
    store_id: str
    camera_id: str
    visitor_id: str
    event_type: EventTypeEnum
    timestamp: datetime
    zone_id: str | None = None
    dwell_ms: int = 0
    is_staff: bool = False
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RetailEventResponse(BaseModel):
    """Schema for event responses."""

    id: int
    event_id: str
    store_id: str
    camera_id: str
    visitor_id: str
    event_type: str
    zone_id: str | None
    dwell_ms: int
    is_staff: bool
    confidence: float
    event_metadata: dict[str, Any]
    event_timestamp: datetime
    created_at: datetime

    model_config = {"from_attributes": True}


class StoreMetricsResponse(BaseModel):
    store_id: str
    period_start: datetime
    period_end: datetime
    total_visitors: int
    avg_dwell_seconds: float
    peak_zone: str | None
    conversion_rate: float
    zone_breakdown: dict[str, int]


class FunnelResponse(BaseModel):
    store_id: str
    stages: list[dict[str, Any]]


class HeatmapResponse(BaseModel):
    store_id: str
    zones: list[dict[str, Any]]


class AnomalyResponse(BaseModel):
    store_id: str
    anomalies: list[dict[str, Any]]


class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class ComponentHealth(BaseModel):
    status: str  # "ok" | "error" | "warning"
    detail: str = ""
    latency_ms: float | None = None


class HealthResponse(BaseModel):
    status: HealthStatus
    timestamp: datetime
    version: str = "1.0.0"
    components: dict[str, ComponentHealth]
    latest_event_at: datetime | None = None
    pipeline_heartbeat_at: datetime | None = None
    warnings: list[str] = Field(default_factory=list)

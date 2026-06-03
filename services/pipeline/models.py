"""
Shared data models for the FluxRetail pipeline service.

All models are Pydantic v2 for validation, serialisation, and
type-safe passing between pipeline stages.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """All event types that the pipeline can emit."""

    ENTRY = "ENTRY"
    EXIT = "EXIT"
    ZONE_ENTER = "ZONE_ENTER"
    ZONE_EXIT = "ZONE_EXIT"
    ZONE_DWELL = "ZONE_DWELL"


class Detection(BaseModel):
    """Single person detection from YOLOv8."""

    bbox: list[float]  # [x1, y1, x2, y2] in pixel coordinates
    confidence: float
    class_id: int = 0  # 0 = person

    @property
    def centroid(self) -> tuple[float, float]:
        """Returns (cx, cy) centroid of the bounding box."""
        x1, y1, x2, y2 = self.bbox
        return ((x1 + x2) / 2, (y1 + y2) / 2)


class TrackedPerson(BaseModel):
    """A detection enriched with a stable ByteTrack track ID."""

    track_id: int
    bbox: list[float]  # [x1, y1, x2, y2]
    confidence: float
    frame_idx: int
    timestamp: datetime

    @property
    def centroid(self) -> tuple[float, float]:
        x1, y1, x2, y2 = self.bbox
        return ((x1 + x2) / 2, (y1 + y2) / 2)

    def normalised_centroid(self, frame_w: int, frame_h: int) -> tuple[float, float]:
        """Returns centroid as normalised (0-1) coordinates."""
        cx, cy = self.centroid
        return (cx / frame_w, cy / frame_h)


class RetailEvent(BaseModel):
    """A structured retail intelligence event ready for Kafka."""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    store_id: str
    camera_id: str
    visitor_id: str  # stable cross-session identifier derived from track_id
    event_type: EventType
    timestamp: datetime
    zone_id: str | None = None
    dwell_ms: int = 0
    is_staff: bool = False
    confidence: float = 1.0
    metadata: dict[str, Any] = Field(default_factory=dict)

    class Config:
        use_enum_values = True

    def to_kafka_dict(self) -> dict[str, Any]:
        """Serialise to dict suitable for Kafka JSON message."""
        d = self.model_dump()
        d["timestamp"] = self.timestamp.isoformat()
        return d

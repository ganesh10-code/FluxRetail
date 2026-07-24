"""
Pipeline service configuration.

All values are read from environment variables (or .env file).
Pydantic-settings handles coercion and validation automatically.
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineMode(str, Enum):
    """Operating mode for the pipeline service."""

    LIVE = "live"      # Process frames from a real video file in real-time
    REPLAY = "replay"  # Replay a pre-recorded events.jsonl at original cadence


class Settings(BaseSettings):
    """
    Central configuration object for the FluxRetail pipeline service.

    Fields map 1-to-1 with environment variables.
    See .env.example for documentation of each variable.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Identity ────────────────────────────────────────────────────────────
    store_id: str = Field(default="store_1", description="Logical store identifier")
    camera_id: str = Field(default="cam_01", description="Camera identifier")

    # ── Pipeline mode ────────────────────────────────────────────────────────
    pipeline_mode: PipelineMode = Field(
        default=PipelineMode.REPLAY,
        description="Operating mode: 'live' or 'replay'",
    )

    # ── Video / replay paths ─────────────────────────────────────────────────
    video_path: Path = Field(
        default=Path("data/sample.mp4"),
        description="Path to the CCTV video file (LIVE mode only)",
    )
    events_jsonl_path: Path = Field(
        default=Path("data/events.jsonl"),
        description="Path to pre-recorded events file (REPLAY mode only)",
    )

    # ── Frame sampling ────────────────────────────────────────────────────────
    frame_skip: int = Field(
        default=5,
        ge=1,
        description="Process every Nth frame (1 = every frame, 5 = every 5th frame)",
    )

    # ── YOLOv8 detection ─────────────────────────────────────────────────────
    yolo_model: str = Field(
        default="yolov8n.pt",
        description="YOLOv8 model weights file or HuggingFace identifier",
    )
    detection_confidence: float = Field(
        default=0.4,
        ge=0.1,
        le=1.0,
        description="Minimum detection confidence threshold",
    )
    device: str = Field(
        default="cpu",
        description="Inference device: 'cpu' or 'cuda'",
    )

    # ── Zone configuration ────────────────────────────────────────────────────
    zones_config_path: Path = Field(
        default=Path("config/zones.yaml"),
        description="Path to zones YAML configuration file",
    )

    # ── Virtual crossing line ─────────────────────────────────────────────────
    entry_line_y: float = Field(
        default=0.4,
        ge=0.0,
        le=1.0,
        description=(
            "Normalised Y-position (0–1) of the virtual entry/exit crossing line. "
            "Centroids crossing this line trigger ENTRY or EXIT events."
        ),
    )

    # ── Session management ────────────────────────────────────────────────────
    session_reentry_window_seconds: int = Field(
        default=1800,
        description="Seconds within which a re-appearing track is treated as a re-entry (not a new visitor)",
    )
    dwell_event_interval_seconds: int = Field(
        default=30,
        description="Emit a ZONE_DWELL event for active zone occupants every N seconds",
    )
    track_lost_timeout_seconds: int = Field(
        default=5,
        description="Seconds before a lost track is considered exited",
    )

    # ── Redis (for pipeline heartbeat) ───────────────────────────────────────
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL \u2014 used to publish pipeline heartbeat for /health",
    )

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka broker address(es), comma-separated",
    )
    kafka_events_topic: str = Field(
        default="fluxretail.events",
        description="Topic for raw retail events",
    )
    kafka_metrics_topic: str = Field(
        default="fluxretail.metrics",
        description="Topic for aggregated metrics",
    )
    kafka_alerts_topic: str = Field(
        default="fluxretail.alerts",
        description="Topic for operational alerts",
    )
    kafka_producer_timeout_seconds: int = Field(
        default=30,
        description="Seconds to wait for Kafka to become available on startup",
    )

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = Field(
        default="INFO",
        description="Logging level: DEBUG | INFO | WARNING | ERROR",
    )
    log_format: str = Field(
        default="json",
        description="Log format: 'json' for structured logging, 'console' for development",
    )

    # ── Replay mode ───────────────────────────────────────────────────────────
    replay_speed_multiplier: float = Field(
        default=1.0,
        ge=0.1,
        description="Speed multiplier for replay mode (2.0 = replay at 2x speed)",
    )

    @field_validator("video_path", "events_jsonl_path", "zones_config_path", mode="before")
    @classmethod
    def resolve_path(cls, v: str | Path) -> Path:
        """Resolve paths relative to the project root (where docker runs from)."""
        return Path(v)


# Module-level singleton — import this everywhere
settings = Settings()

"""
API service configuration via environment variables.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Identity ─────────────────────────────────────────────────────────────
    store_id: str = Field(default="store_1")

    # ── Database ──────────────────────────────────────────────────────────────
    database_url: str = Field(
        default="postgresql+asyncpg://fluxretail:fluxretail_secret@localhost:5433/fluxretail"
    )

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_url: str = Field(default="redis://localhost:6379/0")

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_events_topic: str = Field(default="fluxretail.events")
    kafka_consumer_group: str = Field(default="fluxretail-api-consumer")

    # ── API ───────────────────────────────────────────────────────────────────
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    cors_origins: str = Field(default="http://localhost:5173")

    # ── Health monitoring ─────────────────────────────────────────────────────
    health_stale_feed_timeout_seconds: int = Field(
        default=60,
        description="Seconds without a new event before /health reports STALE_FEED",
    )
    health_pipeline_heartbeat_timeout_seconds: int = Field(
        default=30,
        description="Seconds without a pipeline heartbeat before /health reports WARNING",
    )

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",")]


settings = Settings()

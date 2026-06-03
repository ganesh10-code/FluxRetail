"""Initial schema: events, sessions, metrics tables

Revision ID: 0001_initial_schema
Revises:
Create Date: 2024-01-01 00:00:00.000000

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── events table ──────────────────────────────────────────────────────────
    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("store_id", sa.String(length=64), nullable=False),
        sa.Column("camera_id", sa.String(length=64), nullable=False),
        sa.Column("visitor_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("zone_id", sa.String(length=64), nullable=True),
        sa.Column("dwell_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_staff", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("event_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default="{}"),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id"),
    )
    op.create_index("ix_events_event_id", "events", ["event_id"])
    op.create_index("ix_events_store_id", "events", ["store_id"])
    op.create_index("ix_events_visitor_id", "events", ["visitor_id"])
    op.create_index("ix_events_event_type", "events", ["event_type"])
    op.create_index("ix_events_event_timestamp", "events", ["event_timestamp"])
    op.create_index("ix_events_store_timestamp", "events", ["store_id", "event_timestamp"])
    op.create_index("ix_events_visitor_type", "events", ["visitor_id", "event_type"])

    # ── sessions table ────────────────────────────────────────────────────────
    op.create_table(
        "sessions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("visitor_id", sa.String(length=36), nullable=False),
        sa.Column("store_id", sa.String(length=64), nullable=False),
        sa.Column("session_state", sa.String(length=32), nullable=False, server_default="ACTIVE"),
        sa.Column("entry_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exit_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("zones_visited", postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default="[]"),
        sa.Column("billing_zone_seen", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("conversion_status", sa.String(length=32), nullable=False, server_default="NOT_CONVERTED"),
        sa.Column("is_staff", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_sessions_visitor_id", "sessions", ["visitor_id"])
    op.create_index("ix_sessions_store_id", "sessions", ["store_id"])

    # ── metrics table ─────────────────────────────────────────────────────────
    op.create_table(
        "metrics",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("store_id", sa.String(length=64), nullable=False),
        sa.Column("bucket_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metric_type", sa.String(length=64), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("metric_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default="{}"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_metrics_store_id", "metrics", ["store_id"])
    op.create_index("ix_metrics_store_bucket", "metrics", ["store_id", "bucket_time"])


def downgrade() -> None:
    op.drop_index("ix_metrics_store_bucket", table_name="metrics")
    op.drop_index("ix_metrics_store_id", table_name="metrics")
    op.drop_table("metrics")

    op.drop_index("ix_sessions_store_id", table_name="sessions")
    op.drop_index("ix_sessions_visitor_id", table_name="sessions")
    op.drop_table("sessions")

    op.drop_index("ix_events_visitor_type", table_name="events")
    op.drop_index("ix_events_store_timestamp", table_name="events")
    op.drop_index("ix_events_event_timestamp", table_name="events")
    op.drop_index("ix_events_event_type", table_name="events")
    op.drop_index("ix_events_visitor_id", table_name="events")
    op.drop_index("ix_events_store_id", table_name="events")
    op.drop_index("ix_events_event_id", table_name="events")
    op.drop_table("events")

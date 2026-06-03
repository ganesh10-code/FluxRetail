"""
Zone mapping and virtual crossing line detection.

Responsibilities:
  - Load zone polygon definitions from zones.yaml
  - Map each TrackedPerson to the zone they currently occupy
  - Detect zone entry and exit transitions between frames
  - Detect crossing of the single virtual entry/exit line using centroid direction

Virtual Line Logic:
  - A single horizontal line at normalised Y = entry_line_y
  - A centroid is 'inside' if its normalised_y < entry_line_y  (top of frame)
  - ENTRY event: centroid transitions from below line to above line
  - EXIT event:  centroid transitions from above line to below line
  - Note: y increases downward in image space
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterator

import cv2
import numpy as np
import structlog
import yaml

from config import Settings
from models import TrackedPerson

logger = structlog.get_logger(__name__)


class LineSide(str, Enum):
    """Which side of the virtual crossing line the centroid is on."""

    INSIDE = "inside"    # above the line (y < line_y) — inside the store
    OUTSIDE = "outside"  # below the line (y >= line_y) — outside / entrance
    UNKNOWN = "unknown"  # not yet determined


@dataclass
class ZonePolygon:
    zone_id: str
    display_name: str
    # Pixel-space polygon (computed after frame dimensions are known)
    polygon_norm: list[tuple[float, float]]  # normalised coords
    color: tuple[int, int, int] = (0, 255, 0)

    def to_pixel_polygon(self, frame_w: int, frame_h: int) -> np.ndarray:
        """Convert normalised polygon to pixel coordinate numpy array."""
        pts = [(int(x * frame_w), int(y * frame_h)) for x, y in self.polygon_norm]
        return np.array(pts, dtype=np.int32)

    def contains_point(self, nx: float, ny: float, frame_w: int, frame_h: int) -> bool:
        """Test if a normalised point is inside this zone polygon."""
        px, py = int(nx * frame_w), int(ny * frame_h)
        poly = self.to_pixel_polygon(frame_w, frame_h)
        result = cv2.pointPolygonTest(poly, (float(px), float(py)), measureDist=False)
        return result >= 0  # 0 = on boundary, 1 = inside


@dataclass
class TrackZoneState:
    """Per-track zone tracking state."""

    track_id: int
    current_zone: str | None = None
    line_side: LineSide = LineSide.UNKNOWN
    # Accumulate dwell time per zone (zone_id -> milliseconds)
    zone_dwell_ms: dict[str, int] = field(default_factory=dict)
    last_update_ts: float = 0.0  # unix timestamp


@dataclass
class ZoneEvent:
    """Intermediate zone crossing event produced by ZoneMapper."""

    track_id: int
    event_type: str  # ENTRY, EXIT, ZONE_ENTER, ZONE_EXIT, ZONE_DWELL
    zone_id: str | None
    dwell_ms: int = 0


class ZoneMapper:
    """
    Maps tracked persons to store zones and detects crossing events.

    Maintains per-track state across frames to emit transitions:
      - ENTRY / EXIT via virtual crossing line
      - ZONE_ENTER / ZONE_EXIT via polygon containment change
      - ZONE_DWELL via accumulated dwell time threshold

    Usage:
        mapper = ZoneMapper(settings)
        mapper.load()
        events = mapper.update(tracked_persons, frame_w, frame_h, frame_timestamp_ms)
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._zones: list[ZonePolygon] = []
        self._line_y: float = settings.entry_line_y  # normalised
        self._track_states: dict[int, TrackZoneState] = {}
        self._dwell_interval_ms: int = settings.dwell_event_interval_seconds * 1000

    def load(self) -> None:
        """Load zone definitions from YAML config."""
        config_path = self._settings.zones_config_path
        if not config_path.exists():
            logger.warning(
                "zones_config_not_found",
                path=str(config_path),
                fallback="using_default_zones",
            )
            self._zones = self._default_zones()
            return

        with open(config_path) as f:
            data = yaml.safe_load(f)

        # Override line_y from YAML if present
        if "virtual_line" in data:
            self._line_y = float(data["virtual_line"].get("y", self._line_y))

        self._zones = []
        for zone_id, cfg in data.get("zones", {}).items():
            self._zones.append(
                ZonePolygon(
                    zone_id=zone_id,
                    display_name=cfg.get("display_name", zone_id),
                    polygon_norm=[tuple(p) for p in cfg["polygon"]],
                    color=tuple(cfg.get("color", [0, 255, 0])),
                )
            )

        logger.info(
            "zones_loaded",
            count=len(self._zones),
            zone_ids=[z.zone_id for z in self._zones],
            entry_line_y=self._line_y,
        )

    def update(
        self,
        tracked_persons: list[TrackedPerson],
        frame_w: int,
        frame_h: int,
        frame_timestamp_ms: float,
    ) -> list[ZoneEvent]:
        """
        Process a frame's tracked persons and return zone events.

        Args:
            tracked_persons: list of TrackedPerson from tracker
            frame_w, frame_h: frame dimensions in pixels
            frame_timestamp_ms: frame timestamp in milliseconds (for dwell)

        Returns:
            List of ZoneEvent for this frame.
        """
        events: list[ZoneEvent] = []
        active_track_ids: set[int] = {p.track_id for p in tracked_persons}

        for person in tracked_persons:
            nx, ny = person.normalised_centroid(frame_w, frame_h)
            track_id = person.track_id

            if track_id not in self._track_states:
                self._track_states[track_id] = TrackZoneState(
                    track_id=track_id,
                    last_update_ts=frame_timestamp_ms,
                )

            state = self._track_states[track_id]

            # ── Virtual line crossing detection ──────────────────────────────
            new_side = LineSide.INSIDE if ny < self._line_y else LineSide.OUTSIDE

            if state.line_side == LineSide.UNKNOWN:
                # First time we see this track — set initial side, no event
                state.line_side = new_side
            elif state.line_side == LineSide.OUTSIDE and new_side == LineSide.INSIDE:
                # Crossed from outside → inside: ENTRY
                events.append(ZoneEvent(track_id=track_id, event_type="ENTRY", zone_id=None))
                state.line_side = new_side
                logger.debug("entry_event", track_id=track_id, ny=round(ny, 3))
            elif state.line_side == LineSide.INSIDE and new_side == LineSide.OUTSIDE:
                # Crossed from inside → outside: EXIT
                events.append(ZoneEvent(track_id=track_id, event_type="EXIT", zone_id=None))
                state.line_side = new_side
                logger.debug("exit_event", track_id=track_id, ny=round(ny, 3))

            # ── Zone polygon membership ──────────────────────────────────────
            new_zone: str | None = self._find_zone(nx, ny, frame_w, frame_h)

            if new_zone != state.current_zone:
                if state.current_zone is not None:
                    # Emit dwell total on zone exit
                    dwell = int(state.zone_dwell_ms.get(state.current_zone, 0))
                    events.append(
                        ZoneEvent(
                            track_id=track_id,
                            event_type="ZONE_EXIT",
                            zone_id=state.current_zone,
                            dwell_ms=dwell,
                        )
                    )
                if new_zone is not None:
                    events.append(
                        ZoneEvent(
                            track_id=track_id,
                            event_type="ZONE_ENTER",
                            zone_id=new_zone,
                        )
                    )
                state.current_zone = new_zone

            # ── Dwell accumulation ───────────────────────────────────────────
            if state.current_zone is not None and state.last_update_ts > 0:
                elapsed_ms = frame_timestamp_ms - state.last_update_ts
                if elapsed_ms > 0:
                    zone_dwell = state.zone_dwell_ms.get(state.current_zone, 0)
                    zone_dwell += int(elapsed_ms)
                    state.zone_dwell_ms[state.current_zone] = zone_dwell

                    # Emit periodic dwell event
                    if zone_dwell > 0 and zone_dwell % self._dwell_interval_ms < int(elapsed_ms):
                        events.append(
                            ZoneEvent(
                                track_id=track_id,
                                event_type="ZONE_DWELL",
                                zone_id=state.current_zone,
                                dwell_ms=zone_dwell,
                            )
                        )

            state.last_update_ts = frame_timestamp_ms

        # Clean up state for tracks no longer visible
        # (session manager handles actual EXIT events for lost tracks)
        lost_ids = set(self._track_states.keys()) - active_track_ids
        for lost_id in lost_ids:
            state = self._track_states[lost_id]
            if state.current_zone is not None:
                dwell = int(state.zone_dwell_ms.get(state.current_zone, 0))
                events.append(
                    ZoneEvent(
                        track_id=lost_id,
                        event_type="ZONE_EXIT",
                        zone_id=state.current_zone,
                        dwell_ms=dwell,
                    )
                )
                state.current_zone = None

        return events

    def _find_zone(self, nx: float, ny: float, frame_w: int, frame_h: int) -> str | None:
        """Return the first zone containing this normalised point, or None."""
        for zone in self._zones:
            if zone.contains_point(nx, ny, frame_w, frame_h):
                return zone.zone_id
        return None

    def get_zone_display_name(self, zone_id: str) -> str:
        for z in self._zones:
            if z.zone_id == zone_id:
                return z.display_name
        return zone_id

    def _default_zones(self) -> list[ZonePolygon]:
        """Fallback hardcoded zones if zones.yaml is missing."""
        return [
            ZonePolygon("ENTRY_ZONE", "Entry / Foyer", [(0.0, 0.0), (1.0, 0.0), (1.0, 0.25), (0.0, 0.25)]),
            ZonePolygon("BILLING_ZONE", "Billing Counter", [(0.70, 0.65), (1.00, 0.65), (1.00, 1.00), (0.70, 1.00)]),
            ZonePolygon("MAKEUP_ZONE", "Makeup Section", [(0.00, 0.30), (0.30, 0.30), (0.30, 0.75), (0.00, 0.75)]),
            ZonePolygon("SKINCARE_ZONE", "Skincare Section", [(0.35, 0.30), (0.65, 0.30), (0.65, 0.75), (0.35, 0.75)]),
            ZonePolygon("CENTER_ZONE", "Center Aisle", [(0.30, 0.25), (0.70, 0.25), (0.70, 0.70), (0.30, 0.70)]),
        ]

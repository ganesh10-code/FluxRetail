"""
Visitor session state machine.

Each unique track_id corresponds to a visitor. The session manager
maintains the lifecycle state of each visitor's in-store session:

  ACTIVE     → visitor is present and tracked
  EXITED     → EXIT event received; session is closed but stored
  REENTERED  → visitor reappears within the re-entry window
  CONVERTED  → visitor was seen in BILLING_ZONE

State transitions:
  ACTIVE    --EXIT event-->         EXITED
  EXITED    --track reappears-->    REENTERED  (within reentry_window)
  ACTIVE    --billing zone seen-->  CONVERTED
  REENTERED --billing zone seen-->  CONVERTED

The session manager also:
  - generates ENTRY RetailEvents for new and re-entering sessions
  - generates EXIT RetailEvents for lost tracks (timeout-based)
  - maintains visitor_id as a stable UUID that persists across re-entries
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Iterator

import structlog

from config import Settings
from models import EventType, RetailEvent, TrackedPerson
from zone_mapper import ZoneEvent

logger = structlog.get_logger(__name__)


class SessionState(str, Enum):
    ACTIVE = "ACTIVE"
    EXITED = "EXITED"
    REENTERED = "REENTERED"
    CONVERTED = "CONVERTED"


@dataclass
class VisitorSession:
    visitor_id: str
    track_id: int
    store_id: str
    state: SessionState
    entry_time: datetime
    exit_time: datetime | None = None
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    zones_visited: set[str] = field(default_factory=set)
    billing_zone_seen: bool = False
    conversion_status: str = "NOT_CONVERTED"
    # Unix timestamp (ms) of last tracker update — for timeout detection
    last_frame_ts_ms: float = 0.0

    def transition(self, new_state: SessionState) -> None:
        logger.info(
            "session_state_transition",
            visitor_id=self.visitor_id,
            from_state=self.state,
            to_state=new_state,
        )
        self.state = new_state


BILLING_ZONE_ID = "BILLING_ZONE"


class SessionManager:
    """
    Manages visitor sessions and drives session state transitions.

    Usage:
        mgr = SessionManager(settings)
        retail_events = mgr.process(
            tracked_persons,
            zone_events,
            frame_timestamp,
            store_id,
            camera_id,
        )
    """

    def __init__(self, settings: Settings, billing_zone_id: str = "BILLING_ZONE") -> None:
        self._settings = settings
        self._sessions: dict[int, VisitorSession] = {}  # track_id → session
        # Map visitor_id → track_id for re-entry matching
        self._visitor_history: list[VisitorSession] = []
        self._billing_zone_id = billing_zone_id

    def process(
        self,
        tracked_persons: list[TrackedPerson],
        zone_events: list[ZoneEvent],
        frame_timestamp: datetime,
        store_id: str,
        camera_id: str,
    ) -> list[RetailEvent]:
        """
        Core method: given tracked persons and zone events for a frame,
        update sessions and return the RetailEvents to publish to Kafka.
        """
        retail_events: list[RetailEvent] = []
        active_track_ids = {p.track_id for p in tracked_persons}
        frame_ts_ms = frame_timestamp.timestamp() * 1000

        # ── Handle active tracks ─────────────────────────────────────────────
        for person in tracked_persons:
            tid = person.track_id
            session = self._sessions.get(tid)

            if session is None:
                # New track — check if it's a re-entry of a recent visitor
                reentry = self._find_recent_exit(frame_timestamp)
                if reentry:
                    # Re-entry: restore session with same visitor_id
                    reentry.track_id = tid
                    reentry.last_seen = frame_timestamp
                    reentry.last_frame_ts_ms = frame_ts_ms
                    reentry.transition(SessionState.REENTERED)
                    self._sessions[tid] = reentry
                    retail_events.append(
                        self._make_event(
                            reentry, EventType.ENTRY, frame_timestamp, camera_id,
                            metadata={"reentry": True},
                        )
                    )
                    logger.info("visitor_reentry", visitor_id=reentry.visitor_id, track_id=tid)
                else:
                    # Brand new visitor
                    session = VisitorSession(
                        visitor_id=str(uuid.uuid4()),
                        track_id=tid,
                        store_id=store_id,
                        state=SessionState.ACTIVE,
                        entry_time=frame_timestamp,
                        last_seen=frame_timestamp,
                        last_frame_ts_ms=frame_ts_ms,
                    )
                    self._sessions[tid] = session
                    logger.info("new_visitor_session", visitor_id=session.visitor_id, track_id=tid)
                    # ENTRY event will be emitted from zone_events (ENTRY type)
            else:
                session.last_seen = frame_timestamp
                session.last_frame_ts_ms = frame_ts_ms

        # ── Process zone events ──────────────────────────────────────────────
        for ze in zone_events:
            session = self._sessions.get(ze.track_id)
            if session is None:
                continue

            event_type = EventType(ze.event_type)

            # Update zone history
            if ze.zone_id:
                session.zones_visited.add(ze.zone_id)
                if ze.zone_id == self._billing_zone_id and event_type in (
                    EventType.ZONE_ENTER,
                    EventType.ZONE_DWELL,
                ):
                    session.billing_zone_seen = True
                    if session.state in (SessionState.ACTIVE, SessionState.REENTERED):
                        session.conversion_status = "CONVERTED"
                        session.transition(SessionState.CONVERTED)

            # Handle EXIT event from virtual line crossing
            if event_type == EventType.EXIT:
                session.exit_time = frame_timestamp
                if session.state != SessionState.EXITED:
                    session.transition(SessionState.EXITED)
                    self._visitor_history.append(session)

            retail_events.append(
                self._make_event(
                    session, event_type, frame_timestamp, camera_id,
                    zone_id=ze.zone_id,
                    dwell_ms=ze.dwell_ms,
                )
            )

        # ── Timeout lost tracks ──────────────────────────────────────────────
        timeout_ms = self._settings.track_lost_timeout_seconds * 1000
        for tid in list(self._sessions.keys()):
            if tid in active_track_ids:
                continue
            session = self._sessions[tid]
            if session.state == SessionState.EXITED:
                # Already exited — clean up from active sessions
                del self._sessions[tid]
                continue
            elapsed = frame_ts_ms - session.last_frame_ts_ms
            if elapsed > timeout_ms:
                # Generate an EXIT event for timed-out track
                session.exit_time = frame_timestamp
                session.transition(SessionState.EXITED)
                self._visitor_history.append(session)
                retail_events.append(
                    self._make_event(
                        session, EventType.EXIT, frame_timestamp, camera_id,
                        metadata={"reason": "track_timeout"},
                    )
                )
                del self._sessions[tid]
                logger.info(
                    "track_timeout_exit",
                    track_id=tid,
                    visitor_id=session.visitor_id,
                    elapsed_ms=int(elapsed),
                )

        return retail_events

    def _find_recent_exit(
        self, now: datetime
    ) -> VisitorSession | None:
        """
        Look for a recently exited session within the re-entry window.
        Returns the most recent match, or None.
        """
        window = self._settings.session_reentry_window_seconds
        candidates = [
            s for s in self._visitor_history
            if s.state == SessionState.EXITED
            and s.exit_time is not None
            and (now - s.exit_time).total_seconds() < window
        ]
        if not candidates:
            return None
        # Return the most recently exited session
        return max(candidates, key=lambda s: s.exit_time)  # type: ignore

    def _make_event(
        self,
        session: VisitorSession,
        event_type: EventType,
        timestamp: datetime,
        camera_id: str,
        zone_id: str | None = None,
        dwell_ms: int = 0,
        metadata: dict | None = None,
    ) -> RetailEvent:
        return RetailEvent(
            store_id=session.store_id,
            camera_id=camera_id,
            visitor_id=session.visitor_id,
            event_type=event_type,
            timestamp=timestamp,
            zone_id=zone_id,
            dwell_ms=dwell_ms,
            is_staff=False,
            confidence=1.0,
            metadata={
                "track_id": session.track_id,
                "session_state": session.state,
                "zones_visited": list(session.zones_visited),
                "billing_zone_seen": session.billing_zone_seen,
                "conversion_status": session.conversion_status,
                **(metadata or {}),
            },
        )

    @property
    def active_session_count(self) -> int:
        return sum(
            1 for s in self._sessions.values()
            if s.state in (SessionState.ACTIVE, SessionState.REENTERED, SessionState.CONVERTED)
        )

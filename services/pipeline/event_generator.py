"""
Event generator — assembles RetailEvents from tracker + zone_mapper outputs.

This is a thin coordination layer: it takes TrackedPersons and ZoneEvents,
passes them through SessionManager, and returns the final list of RetailEvents
ready to be serialised to Kafka.

Kept separate from SessionManager to allow unit-testing event generation
without a full session lifecycle.
"""

from __future__ import annotations

from datetime import datetime

import structlog

from config import Settings
from models import RetailEvent, TrackedPerson
from session_manager import SessionManager
from zone_mapper import ZoneEvent

logger = structlog.get_logger(__name__)


class EventGenerator:
    """
    Thin adapter that delegates to SessionManager.

    Usage:
        gen = EventGenerator(settings)
        events = gen.generate(tracked, zone_events, timestamp, store_id, camera_id)
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session_manager = SessionManager(settings)

    def generate(
        self,
        tracked_persons: list[TrackedPerson],
        zone_events: list[ZoneEvent],
        frame_timestamp: datetime,
        store_id: str,
        camera_id: str,
    ) -> list[RetailEvent]:
        """
        Generate RetailEvents from a frame's tracking + zone analysis.

        Returns an empty list if there are no new events this frame.
        """
        events = self._session_manager.process(
            tracked_persons=tracked_persons,
            zone_events=zone_events,
            frame_timestamp=frame_timestamp,
            store_id=store_id,
            camera_id=camera_id,
        )

        if events:
            logger.debug(
                "events_generated",
                count=len(events),
                types=[e.event_type for e in events],
            )

        return events

    @property
    def active_visitors(self) -> int:
        return self._session_manager.active_session_count

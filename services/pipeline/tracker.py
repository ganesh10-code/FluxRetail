"""
ByteTrack person tracker wrapper using boxmot.

Responsibilities:
  - Maintain a ByteTrack tracker instance
  - Accept per-frame detections and return tracked persons with stable IDs
  - Convert boxmot output format to TrackedPerson objects

boxmot's ByteTrack expects detections as numpy array:
  [[x1, y1, x2, y2, confidence, class_id], ...]
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import structlog

from models import TrackedPerson

logger = structlog.get_logger(__name__)


class PersonTracker:
    """
    Stateful ByteTrack tracker for person re-identification across frames.

    Wraps boxmot.ByteTrack to maintain a consistent API.

    Usage:
        tracker = PersonTracker()
        tracker.load()
        tracked = tracker.update(detections, frame)
    """

    def __init__(self) -> None:
        self._tracker = None

    def load(self) -> None:
        """Initialise the ByteTrack tracker. Call once at startup."""
        try:
            from boxmot import BYTETracker

            self._tracker = BYTETracker(
                track_thresh=0.5,
                track_buffer=30,
                match_thresh=0.8,
            )
            logger.info("bytetrack_tracker_ready")
        except Exception as exc:
            logger.error("bytetrack_load_failed", error=str(exc))
            raise

    def update(
        self,
        detections_xyxy_conf_cls: np.ndarray,
        frame: np.ndarray,
        frame_idx: int,
        timestamp: datetime,
    ) -> list[TrackedPerson]:
        """
        Update the tracker with new detections for this frame.

        Args:
            detections_xyxy_conf_cls: shape (N, 6) — [x1,y1,x2,y2,conf,cls]
            frame: the raw BGR frame (required by some boxmot trackers for ReID)
            frame_idx: current frame index (for logging)
            timestamp: wall-clock timestamp for this frame

        Returns:
            List of TrackedPerson with stable track_ids.
        """
        if self._tracker is None:
            raise RuntimeError("PersonTracker.load() must be called before update()")

        if detections_xyxy_conf_cls.shape[0] == 0:
            # No detections — update tracker with empty array to age out lost tracks
            empty = np.empty((0, 6), dtype=np.float32)
            self._tracker.update(empty, frame)
            return []

        tracked_raw = self._tracker.update(detections_xyxy_conf_cls, frame)
        # boxmot returns: [x1, y1, x2, y2, track_id, confidence, class_id, ...]

        results: list[TrackedPerson] = []
        for row in tracked_raw:
            if len(row) < 7:
                continue
            x1, y1, x2, y2 = float(row[0]), float(row[1]), float(row[2]), float(row[3])
            track_id = int(row[4])
            confidence = float(row[5])

            results.append(
                TrackedPerson(
                    track_id=track_id,
                    bbox=[x1, y1, x2, y2],
                    confidence=confidence,
                    frame_idx=frame_idx,
                    timestamp=timestamp,
                )
            )

        return results

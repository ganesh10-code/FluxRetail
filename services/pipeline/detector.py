"""
YOLOv8n person detector wrapper.

Responsibilities:
  - Load and cache the YOLOv8n model
  - Run inference on a single frame
  - Filter to class_id=0 (person) only
  - Apply confidence threshold
  - Return a typed list of Detection objects

Frame skipping is handled by the Orchestrator, not here.
"""

from __future__ import annotations

import structlog
import numpy as np
from ultralytics import YOLO

from config import Settings
from models import Detection

logger = structlog.get_logger(__name__)

# COCO class ID for 'person'
_PERSON_CLASS_ID = 0


class PersonDetector:
    """
    Thin wrapper around YOLOv8 that returns only person detections.

    Usage:
        detector = PersonDetector(settings)
        detections = detector.detect(frame)  # numpy HWC BGR frame
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._model: YOLO | None = None

    def load(self) -> None:
        """Load the YOLO model. Call once at startup."""
        logger.info(
            "loading_yolo_model",
            model=self._settings.yolo_model,
            device=self._settings.device,
        )
        self._model = YOLO(self._settings.yolo_model)
        # Warm up the model with a dummy inference
        dummy = np.zeros((640, 640, 3), dtype=np.uint8)
        self._model(dummy, verbose=False)
        logger.info("yolo_model_ready")

    def detect(self, frame: np.ndarray) -> list[Detection]:
        """
        Run inference on a single BGR frame.

        Args:
            frame: HxWxC numpy array (BGR, uint8)

        Returns:
            List of Detection objects for class_id=0 above confidence threshold.
        """
        if self._model is None:
            raise RuntimeError("PersonDetector.load() must be called before detect()")

        results = self._model(
            frame,
            classes=[_PERSON_CLASS_ID],
            conf=self._settings.detection_confidence,
            device=self._settings.device,
            verbose=False,
        )

        detections: list[Detection] = []
        for result in results:
            if result.boxes is None:
                continue
            for box in result.boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                conf = float(box.conf[0])
                detections.append(
                    Detection(
                        bbox=[x1, y1, x2, y2],
                        confidence=conf,
                        class_id=_PERSON_CLASS_ID,
                    )
                )

        return detections

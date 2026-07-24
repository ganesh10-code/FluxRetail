"""
YOLOv8n person detector wrapper.

Responsibilities:
  - Load and cache the YOLOv8n model
  - Run inference on a single frame
  - Filter to class_id=0 (person) only
  - Apply confidence threshold
  - Return a typed list of Detection objects

Frame skipping is handled by the Orchestrator, not here.

SINGLETON:
  Use get_singleton_detector(settings) to obtain the shared model instance.
  This ensures YOLO is loaded exactly once globally, preventing OMP thread
  exhaustion when multiple camera threads are active.
"""

from __future__ import annotations

import threading

import structlog
import numpy as np
from ultralytics import YOLO

from config import Settings
from models import Detection

logger = structlog.get_logger(__name__)

# COCO class ID for 'person'
_PERSON_CLASS_ID = 0

# Module-level singleton — guarded by a lock for thread-safe initialisation
_singleton_lock = threading.Lock()
_singleton_instance: PersonDetector | None = None


def get_singleton_detector(settings: Settings) -> PersonDetector:
    """
    Return the shared PersonDetector instance, creating and loading it on
    first call.  Subsequent calls return the cached instance immediately.

    Thread-safe: only one thread will ever call .load() regardless of how
    many camera threads race here at startup.
    """
    global _singleton_instance
    if _singleton_instance is not None:
        return _singleton_instance
    with _singleton_lock:
        # Double-checked locking pattern
        if _singleton_instance is None:
            detector = PersonDetector(settings)
            detector.load()
            _singleton_instance = detector
            logger.info("yolo_singleton_created")
    return _singleton_instance


class PersonDetector:
    """
    Thin wrapper around YOLOv8 that returns only person detections.

    Usage:
        detector = get_singleton_detector(settings)  # preferred
        detections = detector.detect(frame)  # numpy HWC BGR frame
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._model: YOLO | None = None

    def load(self) -> None:
        """Load the YOLO model. Call once at startup."""
        import torch
        import functools
        try:
            # Override weights_only default in PyTorch 2.6+ to allow loading YOLO model safely
            torch.load = functools.partial(torch.load, weights_only=False)
        except Exception:
            pass

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

"""
Pipeline Orchestrator — the core coordinator for FluxRetail's CV pipeline.

The PipelineOrchestrator class is responsible for:
  1. Selecting and running the appropriate pipeline mode (LIVE or REPLAY)
  2. Coordinating all pipeline stages in the correct order:
       video ingestion → frame sampling → detection → tracking
       → zone mapping → event generation → Kafka publishing
  3. Maintaining a pipeline heartbeat timestamp (written to a file
     shared with the API service via a Docker volume)
  4. Writing generated events to data/events.jsonl in LIVE mode
     (so they can be used for future REPLAY mode runs)
  5. Graceful shutdown on SIGTERM / SIGINT

Design:
  - main.py is a thin 10-line entrypoint that calls orchestrator.run()
  - All orchestration logic lives here, not in main.py
  - LIVE and REPLAY modes are implemented as separate private methods
  - The orchestrator owns all component lifetimes
"""

from __future__ import annotations

import json
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import redis
import structlog

from config import PipelineMode, Settings
from detector import PersonDetector
from event_generator import EventGenerator
from kafka_producer import RetailEventProducer
from models import Detection, RetailEvent, TrackedPerson
from tracker import PersonTracker
from zone_mapper import ZoneMapper

logger = structlog.get_logger(__name__)

# Redis key for pipeline heartbeat — must match the key read by the API /health endpoint
REDIS_PIPELINE_HEARTBEAT_KEY = "fluxretail:pipeline_heartbeat"
# Heartbeat TTL: if pipeline crashes, key expires so /health reports PIPELINE_STALE
REDIS_HEARTBEAT_TTL_SECONDS = 120


class PipelineOrchestrator:
    """
    Top-level coordinator for the FluxRetail CV pipeline.

    Instantiate once in main.py and call run().

    Attributes:
        settings: Loaded configuration object.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._running = False

        # Pipeline components — initialised in _setup()
        self._detector: PersonDetector | None = None
        self._tracker: PersonTracker | None = None
        self._zone_mapper: ZoneMapper | None = None
        self._event_generator: EventGenerator | None = None
        self._producer: RetailEventProducer | None = None

        # Redis client for heartbeat publishing (synchronous — pipeline is sync)
        self._redis: redis.Redis | None = None

        # JSONL output for LIVE mode (for future replay)
        self._events_file: Path = settings.events_jsonl_path

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        Main entry point. Blocks until the pipeline finishes or is interrupted.

        Registers signal handlers for SIGTERM and SIGINT so Docker stop
        results in a clean shutdown with Kafka flush.
        """
        self._register_signal_handlers()

        logger.info(
            "pipeline_starting",
            mode=self._settings.pipeline_mode,
            store_id=self._settings.store_id,
            camera_id=self._settings.camera_id,
        )

        try:
            self._setup()
            self._running = True

            if self._settings.pipeline_mode == PipelineMode.LIVE:
                self._run_live()
            else:
                self._run_replay()

        except KeyboardInterrupt:
            logger.info("pipeline_interrupted_by_user")
        except Exception as exc:
            logger.exception("pipeline_fatal_error", error=str(exc))
            sys.exit(1)
        finally:
            self._teardown()
            logger.info("pipeline_stopped")

    # ──────────────────────────────────────────────────────────────────────────
    # Setup and teardown
    # ──────────────────────────────────────────────────────────────────────────

    def _setup(self) -> None:
        """Initialise all pipeline components."""
        logger.info("pipeline_setup_start")

        # Kafka producer (needed in both modes)
        self._producer = RetailEventProducer(self._settings)
        self._producer.connect()

        # Redis client for heartbeat (best-effort — failure is non-fatal)
        try:
            self._redis = redis.Redis.from_url(
                self._settings.redis_url,
                socket_connect_timeout=5,
                socket_timeout=2,
                decode_responses=True,
            )
            self._redis.ping()
            logger.info("pipeline_redis_connected", url=self._settings.redis_url)
        except Exception as exc:
            logger.warning(
                "pipeline_redis_unavailable",
                error=str(exc),
                detail="heartbeat will be skipped; /health will show PIPELINE_STALE",
            )
            self._redis = None

        if self._settings.pipeline_mode == PipelineMode.LIVE:
            self._setup_live_components()

        logger.info("pipeline_setup_complete")

    def _setup_live_components(self) -> None:
        """Load CV components for LIVE mode."""
        self._detector = PersonDetector(self._settings)
        self._detector.load()

        self._tracker = PersonTracker()
        self._tracker.load()

        self._zone_mapper = ZoneMapper(self._settings)
        self._zone_mapper.load()

        self._event_generator = EventGenerator(self._settings)

        # Ensure output directory for events.jsonl exists
        self._events_file.parent.mkdir(parents=True, exist_ok=True)

    def _teardown(self) -> None:
        """Flush Kafka and clean up resources."""
        self._running = False
        if self._producer:
            logger.info("flushing_kafka_producer")
            self._producer.flush(timeout=10.0)
        # Remove heartbeat key so /health immediately sees the pipeline as stopped
        if self._redis:
            try:
                self._redis.delete(REDIS_PIPELINE_HEARTBEAT_KEY)
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────────────────
    # LIVE mode
    # ──────────────────────────────────────────────────────────────────────────

    def _run_live(self) -> None:
        """
        LIVE pipeline mode.

        Opens the video file with OpenCV, processes every Nth frame through
        the full detection → tracking → zone → event → Kafka pipeline.
        Also writes events to events.jsonl for future replay.
        """
        import cv2

        video_path = self._settings.video_path
        if not video_path.exists():
            raise FileNotFoundError(
                f"Video file not found: {video_path}. "
                "Place your CCTV video at data/sample.mp4 or set VIDEO_PATH in .env"
            )

        cap = cv2.VideoCapture(str(video_path))
        if not cap.isOpened():
            raise RuntimeError(f"Failed to open video file: {video_path}")

        fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        frame_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        frame_skip = self._settings.frame_skip

        logger.info(
            "live_pipeline_start",
            video=str(video_path),
            fps=fps,
            resolution=f"{frame_w}x{frame_h}",
            total_frames=total_frames,
            frame_skip=frame_skip,
        )

        frame_idx = 0
        processed_count = 0
        event_count = 0
        video_start_time = datetime.now(timezone.utc)

        try:
            with open(self._events_file, "w") as jsonl_out:
                while self._running:
                    ret, frame = cap.read()
                    if not ret:
                        logger.info("video_ended", total_processed=processed_count)
                        break

                    # ── Frame sampling ──────────────────────────────────────
                    if frame_idx % frame_skip != 0:
                        frame_idx += 1
                        continue

                    frame_idx += 1
                    processed_count += 1

                    # Derive wall-clock timestamp from video position.
                    # cap.get(POS_MSEC) returns the position of the frame just read.
                    video_pos_ms = cap.get(cv2.CAP_PROP_POS_MSEC)
                    frame_ts = video_start_time + timedelta(milliseconds=video_pos_ms)

                    # ── Detection ───────────────────────────────────────────
                    detections: list[Detection] = self._detector.detect(frame)

                    # ── Prepare numpy array for tracker ─────────────────────
                    if detections:
                        det_array = np.array(
                            [
                                [
                                    d.bbox[0], d.bbox[1], d.bbox[2], d.bbox[3],
                                    d.confidence, float(d.class_id),
                                ]
                                for d in detections
                            ],
                            dtype=np.float32,
                        )
                    else:
                        det_array = np.empty((0, 6), dtype=np.float32)

                    # ── Tracking ────────────────────────────────────────────
                    tracked_persons: list[TrackedPerson] = self._tracker.update(
                        det_array, frame, frame_idx, frame_ts
                    )

                    # ── Zone mapping ────────────────────────────────────────
                    frame_ts_ms = video_pos_ms
                    zone_events = self._zone_mapper.update(
                        tracked_persons, frame_w, frame_h, frame_ts_ms
                    )

                    # ── Event generation ────────────────────────────────────
                    events: list[RetailEvent] = self._event_generator.generate(
                        tracked_persons=tracked_persons,
                        zone_events=zone_events,
                        frame_timestamp=frame_ts,
                        store_id=self._settings.store_id,
                        camera_id=self._settings.camera_id,
                    )

                    # ── Publish to Kafka & write to JSONL ───────────────────
                    for event in events:
                        self._producer.publish(event)
                        event_dict = event.to_kafka_dict()
                        jsonl_out.write(json.dumps(event_dict) + "\n")
                        event_count += 1

                    # Write heartbeat to Redis on every processed frame
                    self._write_heartbeat(frame_ts)

                    # Flush every 100 processed frames to avoid buffer buildup
                    if processed_count % 100 == 0:
                        self._producer.flush(timeout=2.0)
                        jsonl_out.flush()
                        logger.info(
                            "live_progress",
                            frame_idx=frame_idx,
                            processed=processed_count,
                            events_published=event_count,
                            active_visitors=self._event_generator.active_visitors,
                        )

        finally:
            cap.release()
            logger.info(
                "live_pipeline_done",
                total_frames=frame_idx,
                processed_frames=processed_count,
                events_published=event_count,
            )

    # ──────────────────────────────────────────────────────────────────────────
    # REPLAY mode
    # ──────────────────────────────────────────────────────────────────────────

    def _run_replay(self) -> None:
        """
        REPLAY pipeline mode.

        Reads data/events.jsonl line by line and re-publishes each event to
        Kafka, preserving the original inter-event timestamp deltas scaled by
        replay_speed_multiplier.

        This allows deterministic load testing and development without a live
        camera feed.
        """
        events_path = self._settings.events_jsonl_path
        if not events_path.exists():
            raise FileNotFoundError(
                f"Replay events file not found: {events_path}. "
                "Run in LIVE mode first to generate events.jsonl, "
                "or place a pre-recorded file at data/events.jsonl."
            )

        speed = self._settings.replay_speed_multiplier
        logger.info(
            "replay_pipeline_start",
            events_file=str(events_path),
            speed_multiplier=speed,
        )

        event_count = 0
        prev_event_ts: datetime | None = None

        with open(events_path) as f:
            for line in f:
                if not self._running:
                    break

                line = line.strip()
                if not line:
                    continue

                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.warning("replay_bad_line", error=str(exc), line=line[:80])
                    continue

                # Parse the original event timestamp
                try:
                    event_ts = datetime.fromisoformat(raw["timestamp"])
                except (KeyError, ValueError) as exc:
                    logger.warning("replay_missing_timestamp", error=str(exc))
                    continue

                # ── Timing: sleep to replay at original cadence ─────────────
                if prev_event_ts is not None:
                    original_delta_s = (event_ts - prev_event_ts).total_seconds()
                    if original_delta_s > 0:
                        sleep_s = original_delta_s / speed
                        # Don't sleep for more than 30 seconds to avoid stalls
                        sleep_s = min(sleep_s, 30.0)
                        time.sleep(sleep_s)

                prev_event_ts = event_ts

                # ── Reconstruct and re-publish the event ────────────────────
                try:
                    event = RetailEvent(**raw)
                except Exception as exc:
                    logger.warning("replay_invalid_event", error=str(exc))
                    continue

                self._producer.publish(event)
                event_count += 1

                # Write heartbeat on every event to keep /health fresh
                self._write_heartbeat(event_ts)

                # Flush every 50 events to avoid buffer buildup
                if event_count % 50 == 0:
                    self._producer.flush(timeout=2.0)
                    logger.info(
                        "replay_progress",
                        events_replayed=event_count,
                        event_ts=event_ts.isoformat(),
                    )

        self._producer.flush(timeout=5.0)
        logger.info("replay_pipeline_done", total_events=event_count)

    # ──────────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _write_heartbeat(self, ts: datetime) -> None:
        """
        Write current timestamp to Redis so the API /health endpoint
        can verify the pipeline is alive.

        Key:  fluxretail:pipeline_heartbeat
        TTL:  REDIS_HEARTBEAT_TTL_SECONDS  (auto-expires if pipeline crashes)
        Read: services/api/routers/health.py :: _check_pipeline_heartbeat()
        """
        if self._redis is None:
            return
        try:
            self._redis.setex(
                REDIS_PIPELINE_HEARTBEAT_KEY,
                REDIS_HEARTBEAT_TTL_SECONDS,
                datetime.now(timezone.utc).isoformat(),
            )
        except Exception as exc:
            # Non-critical — don't let heartbeat failure crash the pipeline
            logger.warning("heartbeat_write_failed", error=str(exc))

    def _register_signal_handlers(self) -> None:
        """Register SIGTERM and SIGINT handlers for graceful shutdown."""

        def _handle_signal(signum, frame):
            logger.info("shutdown_signal_received", signal=signum)
            self._running = False

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

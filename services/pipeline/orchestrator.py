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

Stability notes:
  - OMP_NUM_THREADS / MKL_NUM_THREADS are clamped to 1 here, before any
    CV/torch imports, to prevent OpenMP thread explosion across camera threads.
  - YOLO is loaded exactly ONCE via detector.get_singleton_detector(); all
    camera threads share the same model instance.
  - OpenCV decode threads are set to 1 per thread to reduce CPU contention.
  - FFmpeg decode failures are caught and retried with VideoCapture reconnect.
"""

from __future__ import annotations

# ── Thread-count constraints — must be set before importing CV/torch ─────────
import os
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

import json
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import redis
import structlog

from config import PipelineMode, Settings
from detector import PersonDetector, get_singleton_detector
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

# Max consecutive decode failures before attempting VideoCapture reconnect
_MAX_DECODE_FAILURES = 10


import threading

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
        self._events_file_lock = threading.Lock()

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
        """
        Prime the YOLO singleton so the model is loaded before any camera
        threads start. All threads will then share this single instance.
        """
        # Loading once here prevents each camera thread from independently
        # loading YOLO, which would exhaust OMP threads and cause crashes.
        self._detector = get_singleton_detector(self._settings)
        logger.info("yolo_singleton_primed")

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
        Loads the store configuration, spawns parallel threads for enabled cameras,
        and starts replay analytics for inactive cameras to ensure CPU stability.
        """
        from config_loader import load_store_config
        store_config = load_store_config(self._settings.store_id)

        enabled_cameras = [
            cam_cfg for cam_cfg in store_config.cameras.values()
            if cam_cfg.get("enabled", True)
        ]

        logger.info(
            "live_pipeline_multicamera_start",
            store_id=store_config.store_id,
            store_name=store_config.store_name,
            total_cameras=len(enabled_cameras),
        )

        threads: list[threading.Thread] = []
        inactive_camera_ids: list[str] = []

        # 1. Spawn camera feed threads
        for cam_cfg in enabled_cameras:
            camera_id = cam_cfg["camera_id"]
            # Active cameras for store_1 are cam_entry_01 and cam_zone_01.
            # All other cameras (and all cameras of store_2) are inactive.
            is_active = (
                store_config.store_id == "store_1"
                and camera_id in ("cam_entry_01", "cam_zone_01")
            )
            if not is_active:
                inactive_camera_ids.append(camera_id)

            t = threading.Thread(
                target=self._run_camera_thread,
                args=(cam_cfg, store_config),
                name=f"cam-{camera_id}",
                daemon=True,
            )
            threads.append(t)
            t.start()

        # 2. Spawn replay analytics thread for inactive cameras
        if inactive_camera_ids:
            replay_t = threading.Thread(
                target=self._run_replay_analytics_thread,
                args=(store_config, inactive_camera_ids),
                name="replay-analytics",
                daemon=True,
            )
            threads.append(replay_t)
            replay_t.start()

        # Keep orchestrator thread active
        while self._running:
            alive = any(t.is_alive() for t in threads)
            if not alive:
                logger.info("all_threads_terminated")
                break
            time.sleep(1.0)

    def _run_camera_thread(self, cam_cfg: dict, store_config) -> None:
        import cv2
        from config_loader import get_project_root

        # Clamp OpenCV decode threads to 1 to avoid CPU contention
        cv2.setNumThreads(1)

        camera_id = cam_cfg["camera_id"]
        video_path = get_project_root() / cam_cfg["video_path"]

        is_active = (
            store_config.store_id == "store_1"
            and camera_id in ("cam_entry_01", "cam_zone_01")
        )

        logger.info(
            "camera_thread_start",
            store_id=store_config.store_id,
            camera_id=camera_id,
            is_active=is_active,
            video_path=str(video_path),
        )

        if not video_path.exists():
            logger.error("camera_video_not_found", camera_id=camera_id, path=str(video_path))
            return

        def open_capture(path: str) -> cv2.VideoCapture:
            cap = cv2.VideoCapture(path)
            return cap

        cap = open_capture(str(video_path))
        if not cap.isOpened():
            logger.error("camera_video_open_failed", camera_id=camera_id, path=str(video_path))
            return

        frame_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        frame_skip = cam_cfg.get("frame_skip", 5)

        # ── Active-camera components — use shared singleton detector ─────────
        tracker = None
        zone_mapper = None
        event_generator = None

        if is_active:
            # Re-use the singleton detector loaded once in _setup_live_components
            detector = get_singleton_detector(self._settings)

            tracker = PersonTracker()
            tracker.load()

            zone_mapper = ZoneMapper(self._settings, store_config, camera_id)
            zone_mapper.load()

            billing_zone_id = store_config.get_billing_zone_id()
            event_generator = EventGenerator(self._settings, billing_zone_id)

        frame_idx = 0
        last_snapshot_time = 0.0
        video_start_time = datetime.now(timezone.utc)
        consecutive_decode_failures = 0

        try:
            snapshot_dir = get_project_root() / "data" / "frames" / store_config.store_id / camera_id
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = snapshot_dir / "latest.jpg"

            while self._running:
                try:
                    ret, frame = cap.read()
                except Exception as exc:
                    logger.warning("frame_read_exception", camera_id=camera_id, error=str(exc))
                    ret = False
                    frame = None

                if not ret or frame is None:
                    consecutive_decode_failures += 1
                    if consecutive_decode_failures == 1:
                        # Try looping the video first
                        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        continue
                    if consecutive_decode_failures >= _MAX_DECODE_FAILURES:
                        logger.warning(
                            "camera_reconnecting",
                            camera_id=camera_id,
                            failures=consecutive_decode_failures,
                        )
                        cap.release()
                        time.sleep(0.5)
                        cap = open_capture(str(video_path))
                        consecutive_decode_failures = 0
                        video_start_time = datetime.now(timezone.utc)
                    else:
                        time.sleep(0.1)
                    continue

                consecutive_decode_failures = 0

                if frame_idx % frame_skip != 0:
                    frame_idx += 1
                    continue

                frame_idx += 1

                # ── Snapshot export — every 1.5 s, regardless of active/inactive ──
                current_time = time.time()
                if current_time - last_snapshot_time >= 1.5:
                    try:
                        ok = cv2.imwrite(str(snapshot_path), frame, [int(cv2.IMWRITE_JPEG_QUALITY), 60])
                        if not ok:
                            logger.warning("snapshot_write_failed", camera_id=camera_id, path=str(snapshot_path))
                        else:
                            logger.debug("snapshot_written", camera_id=camera_id, ts=current_time)
                    except Exception as exc:
                        logger.warning("snapshot_exception", camera_id=camera_id, error=str(exc))
                    last_snapshot_time = current_time

                if is_active and tracker and zone_mapper and event_generator:
                    video_pos_ms = cap.get(cv2.CAP_PROP_POS_MSEC)
                    frame_ts = video_start_time + timedelta(milliseconds=video_pos_ms)

                    try:
                        detections = detector.detect(frame)
                    except Exception as exc:
                        logger.warning("detection_failed", camera_id=camera_id, error=str(exc))
                        time.sleep(0.01)
                        continue

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

                    tracked_persons = tracker.update(det_array, frame, frame_idx, frame_ts)

                    zone_events = zone_mapper.update(tracked_persons, frame_w, frame_h, video_pos_ms)

                    events = event_generator.generate(
                        tracked_persons=tracked_persons,
                        zone_events=zone_events,
                        frame_timestamp=frame_ts,
                        store_id=store_config.store_id,
                        camera_id=camera_id,
                    )

                    for event in events:
                        self._producer.publish(event)
                        event_dict = event.to_kafka_dict()
                        with self._events_file_lock:
                            with open(self._events_file, "a") as jsonl_out:
                                jsonl_out.write(json.dumps(event_dict) + "\n")

                    self._write_heartbeat(frame_ts)

                time.sleep(0.01)

        except Exception as exc:
            logger.exception("camera_thread_crashed", camera_id=camera_id, error=str(exc))
        finally:
            cap.release()
            logger.info("camera_thread_stopped", camera_id=camera_id)

    def _run_replay_analytics_thread(self, store_config, inactive_cameras: list[str]) -> None:
        logger.info("replay_analytics_thread_start", inactive_cameras=inactive_cameras)

        from config_loader import get_project_root
        events_path = self._settings.events_jsonl_path
        if not events_path.exists():
            events_path = get_project_root() / "services" / "pipeline" / "data" / "events.jsonl"
            if not events_path.exists():
                logger.warning("replay_analytics_events_file_missing", path=str(events_path))
                return

        speed = self._settings.replay_speed_multiplier

        while self._running:
            prev_event_ts = None

            with open(events_path, "r") as f:
                for line in f:
                    if not self._running:
                        break
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        raw = json.loads(line)
                        mapped = self._map_replay_event(raw, store_config)
                    except Exception:
                        continue

                    if mapped.get("camera_id") not in inactive_cameras:
                        continue

                    try:
                        event_ts = datetime.fromisoformat(mapped["timestamp"])
                    except Exception:
                        continue

                    if prev_event_ts is not None:
                        original_delta_s = (event_ts - prev_event_ts).total_seconds()
                        if original_delta_s > 0:
                            sleep_s = original_delta_s / speed
                            sleep_s = min(sleep_s, 30.0)
                            elapsed = 0.0
                            step = 0.1
                            while elapsed < sleep_s and self._running:
                                time.sleep(min(step, sleep_s - elapsed))
                                elapsed += step

                    if not self._running:
                        break

                    prev_event_ts = event_ts

                    mapped["timestamp"] = datetime.now(timezone.utc).isoformat()
                    # Regenerate event_id so each replay loop iteration produces unique IDs,
                    # preventing stale KPI deduplication that would suppress real events.
                    mapped["event_id"] = str(uuid.uuid4())
                    try:
                        event = RetailEvent(**mapped)
                        self._producer.publish(event)
                        self._write_heartbeat(event.timestamp)
                    except Exception as exc:
                        logger.warning("replay_analytics_invalid_event", error=str(exc))

            time.sleep(1.0)

    def _map_replay_event(self, raw: dict, store_config) -> dict:
        raw["store_id"] = store_config.store_id
        event_type = raw.get("event_type", "")
        orig_zone = raw.get("zone_id")

        mapped_zone = None
        if orig_zone:
            orig_zone_lower = orig_zone.lower()
            if "entry" in orig_zone_lower:
                for target_zone_id in store_config.zones:
                    if "entry" in target_zone_id.lower():
                        mapped_zone = target_zone_id
                        break
            elif "makeup" in orig_zone_lower or "cosmetics" in orig_zone_lower:
                for target_zone_id in store_config.zones:
                    if "makeup" in target_zone_id.lower() or "cosmetics" in target_zone_id.lower():
                        mapped_zone = target_zone_id
                        break
                if not mapped_zone:
                    if "central_retail" in store_config.zones:
                        mapped_zone = "central_retail"
                    elif store_config.zones:
                        mapped_zone = list(store_config.zones.keys())[0]
            elif "skincare" in orig_zone_lower:
                for target_zone_id in store_config.zones:
                    if "skincare" in target_zone_id.lower():
                        mapped_zone = target_zone_id
                        break
                if not mapped_zone:
                    if "central_retail" in store_config.zones:
                        mapped_zone = "central_retail"
                    elif store_config.zones:
                        mapped_zone = list(store_config.zones.keys())[0]
            elif "billing" in orig_zone_lower or "checkout" in orig_zone_lower:
                mapped_zone = store_config.get_billing_zone_id()
            else:
                if orig_zone in store_config.zones:
                    mapped_zone = orig_zone
                else:
                    if "central_retail" in store_config.zones:
                        mapped_zone = "central_retail"
                    elif store_config.zones:
                        mapped_zone = list(store_config.zones.keys())[0]

        raw["zone_id"] = mapped_zone

        cameras_by_type = {}
        for cam_cfg in store_config.cameras.values():
            cam_type = cam_cfg.get("type")
            cameras_by_type.setdefault(cam_type, []).append(cam_cfg.get("camera_id"))

        if event_type in ("ENTRY", "EXIT") and not mapped_zone:
            entries = cameras_by_type.get("ENTRY", [])
            if entries:
                visitor_hash = hash(raw.get("visitor_id", ""))
                raw["camera_id"] = entries[visitor_hash % len(entries)]
            else:
                raw["camera_id"] = "cam_entry_01"
        elif mapped_zone == store_config.get_billing_zone_id() or event_type == "ZONE_ENTER" and mapped_zone == "checkout":
            billings = cameras_by_type.get("BILLING", [])
            raw["camera_id"] = billings[0] if billings else "cam_billing_01"
        else:
            assigned = False
            for cam_cfg in store_config.cameras.values():
                if cam_cfg.get("monitored_zone") == mapped_zone:
                    raw["camera_id"] = cam_cfg.get("camera_id")
                    assigned = True
                    break
            if not assigned:
                zones_cams = cameras_by_type.get("ZONE", [])
                if zones_cams:
                    raw["camera_id"] = zones_cams[0]
                else:
                    raw["camera_id"] = "cam_zone_01"

        if "metadata" in raw:
            if mapped_zone and mapped_zone not in raw["metadata"].get("zones_visited", []):
                raw["metadata"]["zones_visited"] = [mapped_zone]
            if mapped_zone == store_config.get_billing_zone_id():
                raw["metadata"]["billing_zone_seen"] = True
                raw["metadata"]["conversion_status"] = "CONVERTED"
                raw["metadata"]["session_state"] = "CONVERTED"

        return raw

    # ──────────────────────────────────────────────────────────────────────────
    # REPLAY mode
    # ──────────────────────────────────────────────────────────────────────────

    def _run_replay(self) -> None:
        """
        REPLAY pipeline mode.
        Reads data/events.jsonl line by line, maps event attributes dynamically,
        and republishes to Kafka.
        """
        events_path = self._settings.events_jsonl_path
        if not events_path.exists():
            from config_loader import get_project_root
            events_path = get_project_root() / "services" / "pipeline" / "data" / "events.jsonl"
            if not events_path.exists():
                raise FileNotFoundError(f"Replay events file not found: {events_path}")

        from config_loader import load_store_config
        store_config = load_store_config(self._settings.store_id)

        speed = self._settings.replay_speed_multiplier
        logger.info(
            "replay_pipeline_start",
            events_file=str(events_path),
            speed_multiplier=speed,
            store_id=store_config.store_id,
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
                    # Dynamically map the event to the current store configuration
                    raw = self._map_replay_event(raw, store_config)
                except json.JSONDecodeError as exc:
                    logger.warning("replay_bad_line", error=str(exc), line=line[:80])
                    continue

                try:
                    event_ts = datetime.fromisoformat(raw["timestamp"])
                except (KeyError, ValueError) as exc:
                    logger.warning("replay_missing_timestamp", error=str(exc))
                    continue

                if prev_event_ts is not None:
                    original_delta_s = (event_ts - prev_event_ts).total_seconds()
                    if original_delta_s > 0:
                        sleep_s = original_delta_s / speed
                        sleep_s = min(sleep_s, 30.0)
                        elapsed = 0.0
                        step = 0.1
                        while elapsed < sleep_s and self._running:
                            time.sleep(min(step, sleep_s - elapsed))
                            elapsed += step

                if not self._running:
                    break

                prev_event_ts = event_ts

                # Update timestamp to present to keep metrics and dashboards live
                raw["timestamp"] = datetime.now(timezone.utc).isoformat()
                # Regenerate event_id to ensure unique dedup keys per replay pass
                raw["event_id"] = str(uuid.uuid4())
                try:
                    event = RetailEvent(**raw)
                except Exception as exc:
                    logger.warning("replay_invalid_event", error=str(exc))
                    continue

                self._producer.publish(event)
                event_count += 1
                self._write_heartbeat(event.timestamp)

                if event_count % 50 == 0:
                    self._producer.flush(timeout=2.0)
                    logger.info(
                        "replay_progress",
                        events_replayed=event_count,
                        event_ts=event.timestamp.isoformat(),
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
            logger.warning("heartbeat_write_failed", error=str(exc))

    def _register_signal_handlers(self) -> None:
        """Register SIGTERM and SIGINT handlers for graceful shutdown."""

        def _handle_signal(signum, frame):
            logger.info("shutdown_signal_received", signal=signum)
            self._running = False

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

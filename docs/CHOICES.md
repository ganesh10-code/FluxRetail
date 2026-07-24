# FluxRetail Architecture Choices & Engineering Tradeoffs

This document outlines key technical decisions and engineering tradeoffs made during the development of FluxRetail to guarantee resource efficiency, stability, and production-style operation on standard CPU environments.

---

## 1. YOLOv8n Model Selection

### Decision
Use **YOLOv8n** (nano variant, ~6 MB weights) from Ultralytics as the object detection backbone, running on CPU only.

### Reasoning & Tradeoff
| Factor | YOLOv8n | Alternative: YOLOv8s/m | Alternative: YOLOv5 |
|---|---|---|---|
| Inference speed (CPU) | ~25–35ms/frame | ~80–200ms/frame | ~35–60ms/frame |
| Model size | 6 MB | 22–52 MB | 14 MB |
| mAP (COCO) | 37.3 | 44.9–50.2 | 37.4 |
| Memory per thread | ~80 MB | ~250–600 MB | ~150 MB |

- **Constraint**: The system must run live YOLOv8 inference on at least 2 concurrent camera threads on a standard consumer CPU (4–8 cores) without exceeding 80% CPU utilization.
- **Decision**: YOLOv8n achieves ≥20 fps per thread on modern CPU hardware. The 7-point mAP gap vs. YOLOv8s is acceptable because the detection target (people in a retail environment) is large, well-lit, and non-occluded in most frames.
- **Singleton Pattern**: Model weights are loaded once globally via a thread-safe double-checked lock factory (`detector.py`). All camera threads share the same model memory mapping, reducing total memory from O(N×80MB) to O(1×80MB) for N camera threads.
- **Benefit**: Enables 2 live inference threads + 4 inactive replay threads on a single CPU without OOM or thread pool starvation.

---

## 2. Kafka Event Schema Design

### Decision
All computer vision events are published to a single Kafka topic (`fluxretail.events`) using a **self-describing, flat JSON schema** containing every attribute required for stateless consumer processing.

### Schema Structure
```json
{
  "event_id":    "uuid-v4",
  "event_type":  "ENTRY | EXIT | ZONE_ENTER | ZONE_EXIT | ZONE_DWELL",
  "store_id":    "store_1",
  "camera_id":   "cam_entry_01",
  "visitor_id":  "uuid-v4",
  "zone_id":     "cosmetics | null",
  "timestamp":   "ISO-8601 UTC",
  "dwell_seconds": 45
}
```

### Reasoning & Tradeoff
- **Constraint**: The FastAPI consumer needs to aggregate per-store KPIs, per-zone dwell times, and anomaly detection from the event stream without maintaining complex join state or querying PostgreSQL on every message.
- **Self-Describing Payloads**: Including `store_id` and `camera_id` in every event allows the consumer to filter, shard, and aggregate purely from the message payload. This eliminates consumer-side database lookups for hot-path aggregation.
- **Single Topic vs. Multi-Topic**: We chose a single `fluxretail.events` topic rather than per-store topics (`fluxretail.store_1.events`, etc.) because the current scale (2 stores, 6 cameras) does not justify the operational overhead of dynamic topic provisioning. The `store_id` field in the payload enables consumer-side filtering.
- **Explicit Enum for `event_type`**: Using a bounded `event_type` enum instead of free-form strings ensures all consumers and the PostgreSQL schema remain in sync without a schema registry.
- **Benefit**: The Kafka consumer loop in `kafka_consumer.py` is fully stateless per message — each event is persisted and used to update in-memory KPI accumulators without any external lookups, achieving sub-10ms processing latency per event.

---

## 3. WebSocket API Design Decision

### Decision
The FastAPI backend delivers real-time data to the React dashboard via **a persistent WebSocket connection** (`/ws/live`) with a **two-tier broadcast strategy**, rather than REST polling or Server-Sent Events (SSE).

### Design Detail
- **Tier 1 — Immediate event push**: Every time the Kafka consumer processes a new retail event (ENTRY, EXIT, ZONE_ENTER, etc.), it is immediately broadcast to all connected WebSocket clients within the same Python process. Latency: <50ms from Kafka receipt to client.
- **Tier 2 — Scheduled KPI snapshot**: An `asyncio` background task broadcasts a full KPI aggregate snapshot every 2 seconds. This prevents the frontend from receiving 30 KPI updates/second from high-frequency event ingestion and triggering excessive React diffing.

### Reasoning & Tradeoff
| Approach | Pros | Cons |
|---|---|---|
| REST polling (2s interval) | Simple | Stale data between polls; no event-level granularity |
| Server-Sent Events (SSE) | Simple server | Unidirectional, no standard WS protocol support in all clients |
| WebSocket (selected) | Bidirectional, push-based, low-latency | Connection management complexity |

- **Constraint**: The dashboard event log must show individual events in near-real-time (<1s), while KPI cards must update smoothly without causing layout jank.
- **Two-Tier Solution**: Separating event-level push (immediate) from aggregate KPI push (2s throttled) allows both requirements to be met simultaneously with a single WebSocket connection per client.
- **WebSocket Manager**: A connection registry (`websocket/manager.py`) maintains the set of connected clients and handles broadcast fan-out, disconnect cleanup, and error isolation so one slow client cannot stall others.
- **Benefit**: Dashboard receives individual events for the event log within 50ms of Kafka receipt, while KPI cards refresh at a stable 2-second cadence with no client-side polling timer needed.

---

## 4. Hybrid Live / Replay Pipeline

### Decision
Implement a hybrid stream pipeline where Store 1's main entrance and cosmetics zone run live YOLOv8 + ByteTrack object tracking, while other cameras and secondary stores run in an **Inactive Mode** using a high-fidelity replay engine mapped to the layout configuration.

### Tradeoff & Rationale
- **Constraint**: Concurrent YOLOv8 inference across 8+ high-resolution camera feeds easily saturates consumer CPU cores, causing frame drops and thread pool starvation.
- **Solution**: Live detection is reserved for high-impact zones (entrances and active zones). Inactive feeds read frames to write standard JPEG snapshots every 1.5 seconds, while their events are dynamically remapped from pre-recorded logs to match the selected store layout.
- **Benefit**: Achieves high-fidelity dashboard activity across multiple stores on a single CPU without sacrificing real-time detection on core camera streams.

---

## 5. Thread-Safe Singleton YOLO Model

### Decision
Wrap the `PersonDetector` class in a thread-safe double-checked lock factory to load the YOLO model exactly once globally.

### Tradeoff & Rationale
- **Constraint**: Standard implementations load a fresh YOLO network inside each camera thread. Concurrent loading of deep neural networks raises memory overhead exponentially and leads to OpenMP runtime errors.
- **Solution**: A shared global singleton is used. All camera threads run sequential inference requests on the same model memory mapping, avoiding redundant weight allocation.
- **Benefit**: Memory utilization remains low, and OMP thread crashes are entirely resolved.

---

## 6. CPU Core and Thread Constraints

### Decision
Enforce strict environment variables (`OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`) and OpenCV limits (`cv2.setNumThreads(1)`) at the entry points of the computer vision pipeline.

### Tradeoff & Rationale
- **Constraint**: By default, libraries like OpenMP, MKL, and OpenCV spawn as many threads as there are CPU cores for every matrix operation. With multiple cameras running, this leads to heavy thread contention, high context-switching latency, and overall degraded pipeline throughput.
- **Solution**: Pinning internal library worker threads to 1 per camera prevents resource thrashing.
- **Benefit**: Ensures predictable frame-processing latency and frees up CPU cycles for the FastAPI API server and Kafka consumers.

---

## 7. Decoupled Event Bus via Apache Kafka

### Decision
Use Apache Kafka (`fluxretail.events` topic) to decouple the computer vision pipeline (producer) from the FastAPI backend (consumer).

### Tradeoff & Rationale
- **Constraint**: Synchronous REST API requests for event ingestion would block the detector loop, reducing CV frame rates when API latencies spike.
- **Solution**: Kafka provides a persistent, message-queue buffer between processes.
- **Benefit**: Even if the PostgreSQL database or FastAPI server is temporarily loaded or restarting, the pipeline continues publishing events without interruption, achieving resilient backpressure management.

---

## 8. Configuration-Driven Store Topology (`store_config.yaml`)

### Decision
Implement a dynamic schema where each store is defined by a local `store_config.yaml` specifying cameras, crossing lines, polygon zones, and metadata.

### Tradeoff & Rationale
- **Constraint**: Hardcoding line-crossing pixel values or zone definitions makes scaling to new store layouts or camera swaps difficult and developer-dependent.
- **Solution**: All coordinates are normalized (0.0 to 1.0) and parsed at startup. The API and pipeline resolve zone definitions and active lines dynamically based on this config.
- **Benefit**: Simple configuration edits allow new stores or layouts to be deployed instantly without code modifications.

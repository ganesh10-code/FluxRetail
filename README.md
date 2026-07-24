# FluxRetail — Real-Time Retail Intelligence Platform

FluxRetail is a production-grade, event-driven retail intelligence platform that transforms CCTV camera feeds into real-time operational analytics. Using computer vision, Kafka event streaming, and a reactive enterprise dashboard, FluxRetail lets retailers monitor occupancy, track customer journey funnels, detect queue congestion, and flag anomalies — across multiple stores simultaneously.

> **Dashboard URL (local dev):** [http://localhost:5173](http://localhost:5173)  
> **API URL:** [http://localhost:8000](http://localhost:8000) — Swagger docs at [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 🚀 Quick Start for Beginners

The fastest way to get FluxRetail running is using Docker. If you have Docker Desktop installed, follow these simple steps:

1. **Open this project folder** in your terminal.
2. **Start the entire system** with a single command (this runs it in the background):
   ```bash
   docker compose up --build -d
   ```
3. **Wait about 30 seconds** for all services (Kafka, Postgres, API, CV Pipeline) to become healthy.
4. **Access the Dashboard:** Open your browser and go to [http://localhost:5173](http://localhost:5173).

That's it! You should now see the real-time Operations Console. For advanced development setups (running without full Docker), see the [Setup Instructions](#setup-instructions) section below.

---

## Problem Statement

Modern physical retail stores lack the operational visibility that online shopping provides by default. Retail managers struggle to answer critical real-time questions:

- *What is our current store occupancy and conversion rate?*
- *How long are customers dwelling in cosmetics vs. skincare zones?*
- *Are checkout counters congested, causing queue bottlenecks?*
- *Can we scale monitoring across multiple stores without GPU infrastructure?*

FluxRetail solves these challenges by combining low-latency edge AI tracking with a decoupled event-driven backend and a professional enterprise operations console.

---

## Key Features

- **Real-Time Visitor Analytics** — Instant calculation of store entries, exits, active occupancy, and daily totals
- **Multi-Camera Architecture** — Supports entry, billing, and zone camera feeds running in parallel threads
- **Kafka Event Streaming** — High-throughput, asynchronous message broker handles all CV events with zero backpressure
- **Queue & Bottleneck Monitoring** — Detects checkout counter congestion and alerts the operations console
- **Dynamic Anomaly Detection** — Flags queue congestion, zone overcrowding, customer long dwells, and low conversion rates
- **Multi-Store Scalability** — Dashboard switching between stores with isolated topologies and configs
- **Configuration-Driven Deployment** — Store maps, camera settings, line coordinates, and zone polygons from simple YAML

---

## Architecture

```
+--------------+     +------------------+     +---------------+     +------------------+     +--------------------+
| CCTV Cameras | --> | YOLOv8n + BTrack | --> | Kafka Topic   | --> | FastAPI + asyncpg| --> | React Dashboard    |
| (Video Files)|     | (CV Pipeline)    |     | fluxretail.   |     | (Consumer + WS)  |     | (Ops Console UI)   |
+--------------+     +------------------+     | events        |     +------------------+     +--------------------+
                             |                +---------------+             |
                             v                                              v
                      +------------+                               +--------------+
                      | Snapshots  |                               | PostgreSQL   |
                      | latest.jpg |                               | + Redis      |
                      +------------+                               +--------------+
```

---

## System Flow

1. **Video Ingestion** — Individual threads decode CCTV/video files frame-by-frame
2. **Object Detection & Tracking** — YOLOv8n detects shoppers; ByteTrack assigns persistent visitor IDs
3. **Zone Mapping** — Centroids are evaluated against crossing lines (entry/exit) and polygon zones
4. **Kafka Publishing** — Self-describing JSON events are published to `fluxretail.events` (ENTRY, EXIT, ZONE_ENTER, ZONE_EXIT, ZONE_DWELL)
5. **Consumption & Aggregation** — FastAPI Kafka consumer persists to PostgreSQL, accumulates KPIs, and broadcasts via WebSocket
6. **Dashboard Rendering** — React ops console renders live KPI cards, event log, funnel chart, and anomaly panel

---

## Tech Stack

| Layer | Technology |
|---|---|
| Computer Vision | YOLOv8n (Ultralytics), ByteTrack (`boxmot`), OpenCV |
| Event Bus | Apache Kafka 3.7 (KRaft — no Zookeeper) |
| Backend API | FastAPI (Python 3.11), SQLAlchemy Asyncio, asyncpg, Uvicorn |
| Databases | PostgreSQL 15, Redis 7 (heartbeats & KPI cache) |
| Frontend | React 18, Vite, TailwindCSS, Recharts, Lucide Icons |
| Container | Docker, Docker Compose |

---

## Multi-Store Support

FluxRetail uses a configuration-driven architecture. Each store's physical layout, camera specs, and zone polygons live in `data/{store_id}/metadata/store_config.yaml`.

```yaml
store_id: store_1
store_name: "Purplle Flagship Store"
cameras:
  entry_cam:
    camera_id: cam_entry_01
    type: ENTRY
    video_path: data/store_1/entry/main_entry.mp4
    entry_line:
      start: [0.05, 0.42]
      end:   [0.95, 0.42]
zones:
  cosmetics:
    zone_id: cosmetics
    display_name: "Cosmetics"
    polygon: [[0.10, 0.10], [0.45, 0.10], [0.45, 0.45], [0.10, 0.45]]
```

Use the **Store Selector** dropdown in the dashboard to switch between Store 1 (live CV) and Store 2 (replay analytics) at runtime.

---

## Kafka Architecture

- **Topic**: `fluxretail.events` (single topic, `store_id` in payload for consumer-side filtering)
- **Producer**: CV Pipeline — publishes one JSON message per detected event (<500 bytes)
- **Consumer**: FastAPI background task — stateless per-message processing, sub-10ms latency
- **Schema**: Self-describing flat JSON with `event_id`, `event_type`, `store_id`, `camera_id`, `visitor_id`, `zone_id`, `timestamp`, `dwell_seconds`
- **Mode**: KRaft (no Zookeeper) — single-broker, auto topic creation enabled

---

## CPU Optimization Strategy

Running deep learning inference on multiple video streams is computationally expensive. FluxRetail uses:

1. **YOLO Singleton Pattern** — YOLOv8n weights loaded once globally via thread-safe double-checked locking; all camera threads share one model in memory
2. **Hybrid Live/Replay Pipeline** — High-priority feeds (entry + primary zone) run live YOLOv8 tracking; remaining cameras operate in **Inactive Mode** — they write JPEG snapshots every 1.5s while analytics are simulated via a dynamic event replay engine
3. **OMP Thread Constraints** — `OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`, `cv2.setNumThreads(1)` prevent thread contention under concurrent camera loads

---

## Replay / Live Hybrid Explanation

| Camera | Mode | Behavior |
|---|---|---|
| Store 1 — Entrance | **Live** | Full YOLOv8n + ByteTrack inference, real crossing detection |
| Store 1 — Zone 1 | **Live** | Full YOLOv8n + ByteTrack inference, zone polygon mapping |
| Store 1 — Billing | **Inactive** | JPEG snapshot every 1.5s + dynamic event replay |
| Store 1 — Zone 2 | **Inactive** | JPEG snapshot every 1.5s + dynamic event replay |
| Store 2 — All cameras | **Inactive** | Full snapshot + replay engine mapped to Store 2 layout |

Inactive mode cameras produce identical UX (live snapshots + event log entries) without GPU/CPU overhead.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Full system health (Postgres, Redis, Kafka, Pipeline heartbeat) |
| `GET` | `/docs` | Swagger UI — interactive API documentation |
| `GET` | `/api/v1/events` | Paginated retail event log with filters |
| `GET` | `/api/v1/metrics/snapshot` | Current KPI snapshot (occupancy, entries, conversion rate) |
| `GET` | `/api/v1/metrics/funnel` | Visitor zone funnel analytics |
| `GET` | `/api/v1/metrics/anomalies` | Detected anomalies (queue, dwell, occupancy) |
| `GET` | `/api/v1/stores` | List of configured stores |
| `GET` | `/api/v1/stores/{store_id}` | Store config + camera list |
| `WS`  | `/ws/live` | WebSocket: real-time event + KPI stream |

---

## Setup Instructions

### Prerequisites
- Docker Desktop (for Kafka, PostgreSQL, Redis)
- Python 3.11 with `venv`
- Node.js 20+

---

## Mode A — Local Development Workflow

This is the recommended workflow for active development. Services run locally with hot-reload.

### Step 1 — Start Infrastructure (Docker)

```powershell
docker compose -f docker-compose.dev.yml up -d
docker compose -f docker-compose.dev.yml ps
```
*Wait ~30 seconds for Kafka to show `healthy`.*

### Step 2 — Initialize Database (First-Time Only)

```powershell
docker exec fluxretail-postgres psql -U fluxretail -d fluxretail -c "ALTER USER fluxretail WITH PASSWORD 'fluxretail_secret';"
```

### Step 3 — Start API Server

```powershell
cd services\api
py -3.11 -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
$env:DATABASE_URL="postgresql+asyncpg://fluxretail:fluxretail_secret@localhost:5433/fluxretail"
alembic upgrade head
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

### Step 4 — Start CV Pipeline

```powershell
cd services\pipeline
py -3.11 -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

### Step 5 — Launch Dashboard

```powershell
cd services\dashboard
npm install
npm run dev
```

**Dashboard:** [http://localhost:5173](http://localhost:5173)  
**API Health:** [http://localhost:8000/health](http://localhost:8000/health)

---

## Mode B — Full Docker Evaluator Workflow

This mode launches all 6 services in containers with a single command. Recommended for evaluators and clean-machine testing.

### Prerequisites
- Docker Desktop running
- `docker compose` v2+ installed

### Run Everything

```bash
docker compose up --build
```

Or detached (background):

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f api
```

### What starts automatically

| Container | Service | Port |
|---|---|---|
| `fluxretail-kafka` | Apache Kafka (KRaft) | 9092 |
| `fluxretail-postgres` | PostgreSQL 15 | 5433 |
| `fluxretail-redis` | Redis 7 | 6379 |
| `fluxretail-api` | FastAPI + Kafka Consumer | **8000** |
| `fluxretail-pipeline` | YOLOv8 CV Pipeline | — |
| `fluxretail-dashboard` | React Dashboard (Vite) | **5173** |

### Startup Order (auto-managed via healthchecks)

```
kafka (healthy) ─┐
postgres (healthy)─┤─→ api (healthy) ─┬─→ pipeline (starts)
redis (healthy) ─┘                    └─→ dashboard (starts)
```

**Dashboard:** [http://localhost:5173](http://localhost:5173)  
**API Health:** [http://localhost:8000/health](http://localhost:8000/health)  
**API Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)

### Stop & Clean Up
#### only to stop the services:
```bash
docker compose stop
```

#### to restart the services:
```bash
docker compose restart
``` 

#### to stop and remove the container:
```bash
docker compose down
docker compose down -v   # also removes volumes (full reset)
```

### Dual-Mode File Reference

| File | Purpose |
|---|---|
| `docker-compose.dev.yml` | Infrastructure only (Kafka + Postgres + Redis) for local dev |
| `docker-compose.yml` | Full production stack — all 6 services |

---

## Demo Flow

To fully evaluate the platform after startup:

1. **Check Infrastructure** — Run `docker compose ps` (or verify local processes running)
2. **API Health Check** — Visit [http://localhost:8000/health](http://localhost:8000/health) — confirm all components show `"ok"`
3. **Open Dashboard** — Navigate to [http://localhost:5173](http://localhost:5173)
4. **Verify System Status Row** — Below the KPI cards, confirm PostgreSQL, Redis, Kafka, and Pipeline Heartbeat show green "Online"
5. **Switch Stores** — Use the Store dropdown to toggle between **Store 1** (live CV) and **Store 2** (replay mode)
6. **Event Log** — Observe the stable, prepend-only event log — newest events appear at top with no page scroll/jump
7. **Visitor Funnel Tab** — View the conversion funnel from entry → zone dwell → checkout
8. **Anomaly Detection Panel** — Check colored severity badges: 🔴 Queue Congestion · 🟠 Overcrowding · 🔵 Long Dwell
9. **Toggle Layout Map** — Switch from "Snapshot" to "Layout Map" to view zone boundary overlays

---

## Folder Structure

```
FluxRetail/
├── config/                         # Global zone crossing line definitions
│   └── zones.yaml
├── data/                           # Per-store config, assets, and runtime data
│   ├── frames/                     # [GIT-IGNORED] Generated camera frame snapshots
│   ├── replays/                    # [GIT-IGNORED] Generated mock event/transaction replay data
│   ├── store_1/
│   │   ├── metadata/store_config.yaml # Store metadata and zones configurations
│   │   ├── layouts/layout.png      # Store blueprint layouts
│   │   ├── entry/main_entry.mp4    # [GIT-IGNORED] Raw entry camera video feed
│   │   └── zones/                  # [GIT-IGNORED] Raw zone camera video feeds
│   └── store_2/
│       ├── metadata/store_config.yaml
│       ├── layouts/layout.png
│       ├── entry/north_entry.mp4   # [GIT-IGNORED] Raw entry camera video feed
│       └── zones/                  # [GIT-IGNORED] Raw zone camera video feeds
├── docs/
│   ├── CHOICES.md                  # Engineering decision document (model, schema, API)
│   ├── DESIGN.md                   # Architecture design + AI-Assisted Decisions
│   └── screenshots/
├── services/
│   ├── api/                        # FastAPI + SQLAlchemy + Kafka consumer + WebSocket
│   │   ├── tests/test_api.py       # API test suite with prompt block
│   │   ├── venv/                   # [GIT-IGNORED] Python virtual environment
│   │   └── Dockerfile
│   ├── pipeline/                   # YOLOv8n + ByteTrack + zone mapper + orchestrator
│   │   ├── data/                   # [GIT-IGNORED] Generated pipeline events (events.jsonl)
│   │   ├── tests/test_pipeline.py  # Pipeline test suite with prompt block
│   │   ├── venv/                   # [GIT-IGNORED] Python virtual environment
│   │   ├── yolov8n.pt              # [GIT-IGNORED] YOLOv8 model weights
│   │   └── Dockerfile
│   └── dashboard/                  # React 18 + Vite + TailwindCSS operations console
│       ├── node_modules/           # [GIT-IGNORED] Node package dependencies
│       ├── dist/                   # [GIT-IGNORED] Production build output
│       └── Dockerfile
├── docker-compose.yml              # Full production stack (all 6 services)
└── docker-compose.dev.yml          # Infrastructure only (local dev mode)
```

## Git Ignore Policy

To keep the repository lightweight and prevent secret leakage or large file check-ins, the following items are excluded from version control via `.gitignore`:
- **Environment & Secrets**: `.env` and any other local `.env.*.local` files to protect credentials.
- **Python Virtual Environments**: Local dependencies and caches (`venv/`, `.venv/`, `__pycache__/`, `*.pyc`).
- **Frontend Dependencies & Builds**: `node_modules/` and production build outputs (`dist/`, `build/`).
- **Heavy Media & ML Models**: YOLOv8n model weights (`*.pt`), raw CCTV footage (`*.mp4`, `*.mov`), and run logs (`*.log`).
- **Runtime Generated Data**: Live camera snapshot frames (`data/frames/`), transaction replay logs (`data/replays/`), and pipeline-generated events (`services/pipeline/data/`).


---

## Dashboard Features

- **Live KPI Cards** — Active shoppers, conversion rate, completed checkouts, peak zone
- **Enterprise Status Row** — Global health for Kafka, PostgreSQL, Redis, Pipeline heartbeat, active cameras
- **Stable Event Stream** — Prepend-only event log with stable React keys; zero layout shifting under live load
- **Visitor Zone Funnel** — Conversion chart from entry → zone dwells → completed purchases
- **Anomaly Detection Panel** — High-contrast severity badges with zone names and timestamps
- **Layout Map Overlay** — Toggle between camera snapshots and physical store blueprints
- **Store Switcher** — Runtime switching between Store 1 (live) and Store 2 (replay)

---

## Screenshots

### 1. System Architecture
![Architecture](docs/screenshots/architecture_diagram.png)

### 2. Live Dashboard Overview
![Dashboard Overview](docs/screenshots/dashboard_overview.png)

### 3. Live Snapshots Feed
![Live Snapshots](docs/screenshots/live_snapshots.png)

### 4. Visitor Funnel Analytics
![Funnel View](docs/screenshots/funnel_view.png)

### 5. Anomaly Detection Console
![Anomaly Panel](docs/screenshots/anomaly_panel.png)

### 6. Store Layout Blueprints
![Layout Overlay](docs/screenshots/layout_overlay.png)

---

## Future Improvements

- **GPU Acceleration** — Scale live YOLO inference to 16+ feeds via Nvidia TensorRT + CUDA containerization
- **Cross-Camera Re-ID** — Re-identify visitors across cameras using feature embedding vectors
- **Cloud Deployment** — AWS MSK (Managed Kafka), RDS PostgreSQL, ECS Fargate
- **Predictive Analytics** — Transformer models to predict traffic jams and stock replenishment 30 minutes in advance
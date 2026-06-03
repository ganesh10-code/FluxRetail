# FluxRetail

> AI-powered event-driven retail intelligence platform that transforms CCTV streams into real-time operational analytics.

## Architecture

```
CCTV Video
  → YOLOv8n Detection
  → ByteTrack Tracking (boxmot)
  → Zone Mapping + Virtual Line Crossing
  → Event Generator
  → Kafka (fluxretail.events)
  → FastAPI Consumer Service
  → PostgreSQL + Redis
  → WebSocket → React Dashboard
```

---

## Prerequisites

| Tool | Required Version | Notes |
|------|-----------------|-------|
| **Python** | **3.11.x** | Python 3.12+ is **not supported** — `aiokafka` and `pydantic-core` wheels are only available for 3.11. Use `py -3.11` on Windows. |
| **Node.js** | 18+ | For the React dashboard |
| **Docker Desktop** | Any recent | Must be running before `docker compose up` |

---

## Quick Start (Local-First Development)

Infrastructure (PostgreSQL, Redis, Kafka) runs in Docker. The three application services run directly on your host machine for fast hot-reload development.

### Step 1 — Spin Up Docker Infrastructure

```powershell
# From the project root
docker compose up -d

# Verify all three containers are healthy
docker compose ps
```

Expected output — all three should show `healthy`:
```
NAME                  IMAGE                STATUS
fluxretail-kafka      apache/kafka:3.7.0   Up (health: starting → healthy after ~30s)
fluxretail-postgres   postgres:15-alpine   Up (healthy)
fluxretail-redis      redis:7-alpine       Up (healthy)
```

> **Note:** Kafka takes ~30 seconds to become healthy after first start. Wait until its status changes from `health: starting` to `healthy` before proceeding.

---

### Step 2 — Reset Postgres Password (First-Time Only)

The Docker volume may initialize with a different password. Run this once after the first `docker compose up`:

```powershell
docker exec fluxretail-postgres psql -U fluxretail -d fluxretail -c "ALTER USER fluxretail WITH PASSWORD 'fluxretail_secret';"
```

Expected output: `ALTER ROLE`

---

### Step 3 — Set Up & Run the API Backend

Open a **new terminal** and run:

```powershell
cd services\api

# Create virtual environment using Python 3.11 (REQUIRED — use py launcher)
py -3.11 -m venv venv

# Activate the virtual environment
.\venv\Scripts\activate

# Install dependencies (all pre-built wheels for Python 3.11 — fast)
pip install -r requirements.txt

# Run database migrations (must pass DATABASE_URL explicitly)
$env:DATABASE_URL="postgresql+asyncpg://fluxretail:fluxretail_secret@localhost:5433/fluxretail"
alembic upgrade head

# Start the API dev server with hot reload
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

- **Swagger Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Health Check**: [http://localhost:8000/health](http://localhost:8000/health)

> **Expected `/health` response** when pipeline is not yet running:
> `"status": "degraded"` with `STALE_FEED` and `PIPELINE_STALE` warnings — this is **normal**.
> Once the pipeline starts (Step 4), status will upgrade to `"healthy"`.

---

### Step 4 — Set Up & Run the CV Pipeline

Open a **new terminal** and run:

```powershell
cd services\pipeline

# Create virtual environment using Python 3.11
py -3.11 -m venv venv

# Activate
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start the pipeline in default REPLAY mode
# (reads data/events.jsonl and streams mock events to Kafka)
python main.py
```

The pipeline will:
- Connect to Kafka at `localhost:9092`
- Read `data/events.jsonl` and replay events at original cadence
- Write a heartbeat to Redis key `fluxretail:pipeline_heartbeat` on every event

> **To run in LIVE mode** (requires `data/sample.mp4`):
> ```powershell
> $env:PIPELINE_MODE="live"; python main.py
> ```

---

### Step 5 — Set Up & Launch the React Dashboard

Open a **new terminal** and run:

```powershell
cd services\dashboard

# Install npm packages
npm install

# Start Vite dev server
npm run dev
```

- **Dashboard UI**: [http://localhost:5173](http://localhost:5173)

The status indicator in the top-right corner will show **"Live"** (green) once the WebSocket connects to the API. Events and KPIs will populate dynamically as the pipeline replays events.

---

## Teardown

```powershell
# Stop Docker infrastructure (from project root)
docker compose down

# To also remove all data volumes (full reset):
docker compose down -v
```

---

## Pipeline Modes

| Mode | Description | Default |
|------|-------------|---------|
| `replay` | Replays pre-recorded `data/events.jsonl` at original cadence | ✅ |
| `live` | Processes `data/sample.mp4` in real-time with YOLOv8n | — |

Set via `PIPELINE_MODE=live` or `PIPELINE_MODE=replay` in `.env` or as a PowerShell env var.

---

## Zone Configuration

Zones and the virtual entry/exit crossing line are configured in `config/zones.yaml`.
All coordinates are normalised (0.0–1.0) so they adapt to any video resolution.

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Full system health (DB, Redis, Kafka, feed freshness) |
| POST | `/api/v1/events/ingest` | Manually inject an event |
| GET | `/api/v1/events/recent` | Last N events |
| GET | `/api/v1/stores/{id}/metrics` | Hourly store KPIs |
| GET | `/api/v1/stores/{id}/funnel` | Zone conversion funnel |
| GET | `/api/v1/stores/{id}/heatmap` | Zone dwell heatmap data |
| GET | `/api/v1/stores/{id}/anomalies` | Operational anomaly flags |
| WS  | `/ws/live` | Live event + KPI stream |

---

## Tech Stack

- **Python 3.11** — FastAPI, SQLAlchemy, aiokafka, boxmot, ultralytics
- **PostgreSQL 15** — events, sessions, metrics tables
- **Redis 7** — pipeline heartbeat + session cache
- **Kafka 3.7** — KRaft mode (no Zookeeper), `apache/kafka:3.7.0`
- **React 18** — Vite + Tailwind CSS + Recharts
- **Docker Compose** — infrastructure-only (Kafka, Postgres, Redis)

---

## Known Issues & Troubleshooting

### `InvalidPasswordError` on Alembic / API startup
The Docker Postgres volume may have been initialised with a different password. Fix:
```powershell
docker exec fluxretail-postgres psql -U fluxretail -d fluxretail -c "ALTER USER fluxretail WITH PASSWORD 'fluxretail_secret';"
```

### Alembic `getaddrinfo failed` / DNS error
Always pass `DATABASE_URL` explicitly when running Alembic — it does not read `.env` automatically:
```powershell
$env:DATABASE_URL="postgresql+asyncpg://fluxretail:fluxretail_secret@localhost:5433/fluxretail"
alembic upgrade head
```

### `ModuleNotFoundError` / pip build failures
Ensure you are using **Python 3.11**, not the system Python (3.14 is pre-release and incompatible):
```powershell
py -3.11 -m venv venv   # correct
python -m venv venv      # wrong — may pick up Python 3.14
```

### WebSocket shows "Offline"
- Confirm the API server is running on port 8000
- The dashboard WebSocket connects to `/ws/live` via the Vite proxy

### Kafka health check slow
`apache/kafka:3.7.0` needs ~30 seconds to elect its KRaft controller on first start. Wait for `docker compose ps` to show `healthy` before starting the API.
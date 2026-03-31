<h1><img src="https://github.com/user-attachments/assets/158de3d0-8bd5-41a5-a34d-a3a92471cf96" width="50" align="absmiddle" /> F1 Replay Timing</h1>




https://github.com/user-attachments/assets/952b8634-2470-46d9-96e2-67a820459a49



> **Disclaimer:** This project is intended for **personal, non-commercial use only**. This website is unofficial and is not associated in any way with the Formula 1 companies. F1, FORMULA ONE, FORMULA 1, FIA FORMULA ONE WORLD CHAMPIONSHIP, GRAND PRIX and related marks are trade marks of Formula One Licensing B.V.

A web app for watching Formula 1 sessions with real timing data, car positions on track, driver telemetry, and more - both live during race weekends and as replays of past sessions. Built with React (Vite) and a Go backend.

## Features

- **Live timing** (Beta) - connect to live F1 sessions during race weekends with real-time data from the F1 SignalR stream, including a broadcast delay slider and automatic detection of post-session replays
- **Track map** with real-time car positions from GPS telemetry, updating every 0.5 seconds with smooth interpolation, marshal sector flags, and toggleable corner numbers
- **Driver leaderboard** showing position, gap to leader, interval, last lap time, tyre compound and age, tyre history, pit stop count and live pit timer, grid position changes, fastest lap indicator, investigation/penalty status, and sub-1-second interval highlighting
- **Race control messages** - steward decisions, investigations, penalties, track limits, and flag changes displayed in a draggable overlay on the track map with optional sound notifications
- **Pit position prediction** estimates where a driver would rejoin if they pitted now, with predicted gap ahead and behind, using precomputed pit loss times per circuit with Safety Car and Virtual Safety Car adjustments
- **Telemetry** for unlimited drivers showing speed, throttle, brake, gear, and DRS (2025 and earlier) plotted against track distance, with a moveable side panel for 3+ driver comparisons
- **Lap Analysis** (Beta) compare lap times for up to two drivers with a line chart and lap-by-lap history, with pit stop and safety car periods highlighted. Race replay only
- **Picture-in-Picture** mode for a compact floating window with track map, race control, leaderboard, and telemetry
- **Weather data** including air and track temperature, humidity, wind, and rainfall status
- **Track status flags** for green, yellow, Safety Car, Virtual Safety Car, and red flag conditions
- **Playback controls** with 0.5x to 20x speed, skip buttons (5s, 30s, 1m, 5m), lap jumping, a progress bar, and red flag countdown with skip-to-restart
- **Session support** for races, qualifying, sprint qualifying, and practice sessions from 2024 onwards
- **Full screen mode** hides the session banner and enters browser fullscreen for a distraction-free view
- **Imperial units** toggle for °F and mph in settings
- **Passphrase authentication** to optionally restrict access when publicly hosted

## Architecture

- **Frontend**: React + Vite with Tailwind CSS, served by Nginx
- **Backend**: Go web service - serves pre-computed data from SQLite (`/data/f1.db`)
- **Data Source**: Official F1 timing endpoints (`livetiming.formula1.com`) ingested directly by the Go backend

Session data is processed once and stored in SQLite (`/data/f1.db`). You can queue downloads in the UI, or let the app process sessions on demand when you select them.

## Self-Hosting Guide

### Option A: Docker with pre-built images (easiest)

Requires [Docker](https://docs.docker.com/get-docker/) and Docker Compose.

Create a `docker-compose.yml` file:

```yaml
services:
  backend:
    image: ghcr.io/adn8naiagent/f1replaytiming-backend:latest
    environment:
      - DATA_DIR=/data
    volumes:
      - f1data:/data

  frontend:
    image: ghcr.io/adn8naiagent/f1replaytiming-frontend:latest
    ports:
      - "3000:3000"
    depends_on:
      - backend

volumes:
  f1data:
```

Then run:

```bash
docker compose up -d
```

Open http://localhost:3000. Select any past session and it will be processed on demand.

### Option B: Docker from source

If you prefer to build the images yourself, or want to make changes to the code:

```bash
git clone <repo-url>
cd F1timing
docker compose up -d
```

Open http://localhost:3000. Select any past session and it will be processed on demand.

### Docker configuration

#### Network & URL configuration

By default, the frontend proxies `/api` and `/ws` to the backend container internally. This means you usually do **not** need any frontend/backend URL variables for Docker Compose.
In this default proxy mode, backend CORS settings are not needed.

This default works for:
- localhost access
- access from other devices on your LAN (for example `http://192.168.1.50:3000`)
- single-domain reverse proxy setups where the same host serves frontend and forwards `/api` + `/ws`

For advanced setups, choose one of these modes:

| Variable | Set on | Purpose |
|---|---|---|
| `BACKEND_INTERNAL_URL` | frontend | Optional runtime proxy upstream for `/api` and `/ws` (keeps same-origin, no browser CORS needed) |
| `VITE_API_URL` | frontend | Optional absolute backend URL used by the browser (split-domain direct mode) |
| `FRONTEND_URL` | backend | Optional allowed frontend origin for CORS (required with `VITE_API_URL`) |
| `EXTRA_ORIGINS` | backend | Optional comma-separated additional CORS origins |

**Example A — same-origin UI with external backend via frontend proxy (no browser CORS):**
```yaml
frontend:
  environment:
    - BACKEND_INTERNAL_URL=https://api.f1.example.com
```

**Example B — split domains (`f1.example.com` + `api.f1.example.com`) with direct browser API calls:**
```yaml
backend:
  environment:
    - FRONTEND_URL=https://f1.example.com

frontend:
  environment:
    - VITE_API_URL=https://api.f1.example.com
```

#### Optional features

- `AUTH_ENABLED` / `AUTH_PASSPHRASE` - restricts access with a passphrase
- `REPLAY_CACHE_MAX_MB` - maximum RAM budget for cached replay sessions in the Go backend (default `256`)
- `REPLAY_CACHE_TTL_SECONDS` - how long an inactive replay session stays cached after last client disconnect (default `300`)
- `REPLAY_SAMPLE_INTERVAL_SECONDS` - replay frame sampling interval during precompute (default `0.5`; higher values reduce CPU/RAM during downloads)
- `SQLITE_PATH` - SQLite database path (default `/data/f1.db`)
- `SQLITE_BUSY_TIMEOUT_MS` - SQLite lock wait timeout in milliseconds (default `5000`)
- `REPLAY_CHUNK_FRAMES` - replay protobuf chunk frame count (default `256`)
- `TELEMETRY_CHUNK_SAMPLES` - telemetry sampling target per lap (default `512`)
- `PROCESS_CHUNK_CODEC` - chunk codec for replay/telemetry (`protobuf` or `protobuf+zstd`, default `protobuf`)
- `PROCESS_RAW_MIN_DT_SECONDS` - minimum accepted delta between raw stream samples per driver during ingest (default `0.10`)
- `GOMEMLIMIT` / `GOGC` - Go runtime GC tuning knobs for tighter memory limits (for example `GOMEMLIMIT=256MiB`, `GOGC=50`)

#### Data

Session data is persisted in a Docker volume, so it survives restarts.
Use the Downloads page in the UI to enqueue single sessions, weekends, or full-season ranges for background processing.

### Option C: Manual setup

#### Prerequisites

- Go 1.26+
- Node.js 18+

#### 1. Clone the repository

```bash
git clone <repo-url>
cd F1timing
```

#### 2. Configure environment variables

**Backend** (environment variables):
```
PORT=8000
DATA_DIR=./data
SQLITE_PATH=./data/f1.db

# Optional - restrict access with a passphrase
AUTH_ENABLED=false
AUTH_PASSPHRASE=

# Optional - only needed for direct browser calls to another backend origin
# FRONTEND_URL=http://localhost:3000
# EXTRA_ORIGINS=
```

**Frontend** (`frontend/.env`):
```
# Optional runtime proxy target for same-origin mode (/api and /ws).
# BACKEND_INTERNAL_URL=http://localhost:8000

# Optional split-domain mode: browser calls backend directly.
# VITE_API_URL=https://api.example.com
```


#### 3. Install dependencies and start

```bash
# Backend (Go API)
cd backend
go run .

# Frontend (in a separate terminal)
cd frontend
npm install
npm run dev
```

Open http://localhost:3000.

#### 4. Getting session data

There are two ways to get session data into the app:

#### Option A: On-demand processing (recommended for getting started)

Select any past session from the homepage. If the data has not been processed yet, the backend will fetch and process it directly in Go, then store it in SQLite. The first load typically takes **1-3 minutes**.

#### Option B: Queue background downloads (recommended for preparing a full season)

Use the Downloads page to enqueue sessions ahead of time (single session, weekend, or season scopes). The queue runs automatically in the backend.

**Timing estimates:**
- A single session (e.g. one race) takes **1-3 minutes**
- A full race weekend (FP1, FP2, FP3, Qualifying, Race) takes **3-5 minutes**
- A complete season (~24 rounds, all sessions) takes **2-3 hours**

The app also includes a background task that automatically checks for and processes new session data on race weekends (Friday–Monday).

## Acknowledgements

Thanks to the broader motorsport telemetry/open-source community and tools that informed earlier iterations of this project.

## License

MIT

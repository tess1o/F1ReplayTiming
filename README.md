<h1><img src="https://github.com/user-attachments/assets/158de3d0-8bd5-41a5-a34d-a3a92471cf96" width="50" align="absmiddle" /> F1 Replay Timing</h1>




https://github.com/user-attachments/assets/952b8634-2470-46d9-96e2-67a820459a49



> **Disclaimer:** This project is intended for **personal, non-commercial use only**. This website is unofficial and is not associated in any way with the Formula 1 companies. F1, FORMULA ONE, FORMULA 1, FIA FORMULA ONE WORLD CHAMPIONSHIP, GRAND PRIX and related marks are trade marks of Formula One Licensing B.V.

A web app for watching Formula 1 sessions with real timing data, car positions on track, driver telemetry, and more - both live during race weekends and as replays of past sessions. Built with Next.js and a Go backend.

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

- **Frontend**: Next.js (React) with Tailwind CSS
- **Backend**: Go web service - serves pre-computed data from local storage
- **Data Source**: [FastF1](https://github.com/theOehrly/Fast-F1) (used during data processing only)

Session data is processed once and stored locally (or in R2 for remote access). You can either pre-compute data in bulk ahead of time, or let the app process sessions on demand when you select them.

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
      - f1cache:/data/fastf1-cache

  frontend:
    image: ghcr.io/adn8naiagent/f1replaytiming-frontend:latest
    ports:
      - "3000:3000"
    depends_on:
      - backend

volumes:
  f1data:
  f1cache:
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
| `NEXT_PUBLIC_API_URL` | frontend | Optional absolute backend URL used by the browser (split-domain direct mode) |
| `FRONTEND_URL` | backend | Optional allowed frontend origin for CORS (required with `NEXT_PUBLIC_API_URL`) |
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
    - NEXT_PUBLIC_API_URL=https://api.f1.example.com
```

#### Optional features

- `AUTH_ENABLED` / `AUTH_PASSPHRASE` - restricts access with a passphrase
- `REPLAY_CACHE_MAX_MB` - maximum RAM budget for cached replay sessions in the Go backend (default `256`)
- `REPLAY_CACHE_TTL_SECONDS` - how long an inactive replay session stays cached after last client disconnect (default `300`)
- `REPLAY_SAMPLE_INTERVAL_SECONDS` - replay frame sampling interval during precompute (default `0.5`; higher values reduce CPU/RAM during downloads)
- `GOMEMLIMIT` / `GOGC` - Go runtime GC tuning knobs for tighter memory limits (for example `GOMEMLIMIT=256MiB`, `GOGC=50`)

#### Data

Session data is persisted in a Docker volume, so it survives restarts.

To pre-process session data in bulk (instead of on demand), use the precompute script:

```bash
# Process a specific race weekend
docker compose exec backend python data-fetcher/precompute.py 2026 --round 1

# Process only the race session (skip practice/qualifying)
docker compose exec backend python data-fetcher/precompute.py 2026 --round 1 --session R

# Process an entire season (will take several hours)
docker compose exec backend python data-fetcher/precompute.py 2025 --skip-existing

# Process multiple years
docker compose exec backend python data-fetcher/precompute.py 2024 2025 --skip-existing
```

### Option C: Manual setup

#### Prerequisites

- Go 1.26+
- Python 3.10+
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
PY_WORKER_PATH=../data-fetcher/worker_bridge.py
# PYTHON_BIN=python3

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
# NEXT_PUBLIC_API_URL=https://api.example.com
```


#### 3. Install dependencies and start

```bash
# Python data-fetcher dependencies (required for on-demand session processing)
python3 -m pip install -r data-fetcher/requirements-worker.txt

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

Simply select any past session from the homepage. If the data hasn't been processed yet, the app will automatically fetch and process it using FastF1 and start the replay. The first load of a session takes **1-3 minutes**. After that, it's instant.

#### Option B: Bulk pre-compute (recommended for preparing a full season)

Use the CLI script to process sessions ahead of time. This is useful if you want all data ready before you start using the app.

```bash
# Data fetcher (Python)
cd data-fetcher
python -m venv venv
source venv/bin/activate
pip install -r requirements-worker.txt

# Process a specific race weekend
python precompute.py 2026 --round 1

# Process only the race session (skip practice/qualifying)
python precompute.py 2026 --round 1 --session R

# Process an entire season (will take several hours)
python precompute.py 2025 --skip-existing

# Process multiple years
python precompute.py 2024 2025 --skip-existing
```

**Timing estimates:**
- A single session (e.g. one race) takes **1-3 minutes**
- A full race weekend (FP1, FP2, FP3, Qualifying, Race) takes **3-5 minutes**
- A complete season (~24 rounds, all sessions) takes **2-3 hours**

The app also includes a background task that automatically checks for and processes new session data on race weekends (Friday–Monday).

## Acknowledgements

This project is powered by [FastF1](https://github.com/theOehrly/Fast-F1), an open-source Python library for accessing Formula 1 timing and telemetry data. FastF1 is the original inspiration and data source for this project - without it, none of this would be possible.

## License

MIT

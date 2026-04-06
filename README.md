# F1 Replay Timing

Web app for Formula 1 timing data with replay and live modes.

- **Frontend**: React + Vite + Tailwind, served by Nginx
- **Backend**: Go service with SQLite persistence
- **Data source**: official `livetiming.formula1.com` feeds, processed by backend

## What's New

Major improvements comparing to the original repository:

1. **Backend fully reimplemented in Go + SQLite**
   Reduced runtime memory usage from roughly **1.0-1.5 GB** to about **50-100 MB**.
2. **Much smaller Docker images**
    - Backend image reduced from ~**870 MB** to ~**24 MB**
    - Frontend image reduced from ~**213 MB** to ~**63 MB**
3. **Python backend code replaced with Go**
   The previous Python backend implementation is fully removed from runtime.
4. **Faster and cleaner session downloading**
   Added a dedicated Downloads page and significantly improved processing speed (often **under 10 seconds for one race**
   instead of minutes).

Also included:

- Tag-driven multi-arch GHCR releases (`linux/amd64`, `linux/arm64`)
- Automatic static data refresh (circuit metadata and pit loss) with startup/scheduled/manual triggers
- Safer circuit metadata fallback behavior so new sessions do not force immediate app upgrades

## Disclaimer

This project is for personal, non-commercial use only. It is unofficial and not affiliated with Formula 1 companies. F1,
FORMULA ONE, FORMULA 1, FIA FORMULA ONE WORLD CHAMPIONSHIP, GRAND PRIX and related marks are trademarks of Formula One
Licensing B.V.


## Features

- **Live timing** (Beta) - connect to live F1 sessions during race weekends with real-time data from the F1 SignalR stream, including a broadcast delay slider and automatic detection of post-session replays
- **Track map** with real-time car positions from GPS telemetry, updating every 0.5 seconds with smooth interpolation, marshal sector flags, and toggleable corner numbers
- **Driver leaderboard** showing position, gap to leader, interval, last lap time, sector indicators (qualifying/practice), tyre compound and age, tyre history, pit stop count and live pit timer, grid position changes, fastest lap indicator, investigation/penalty status, and sub-1-second interval highlighting
- **Race control messages** - steward decisions, investigations, penalties, track limits, and flag changes displayed in a draggable overlay on the track map with optional sound notifications
- **Pit position prediction** estimates where a driver would rejoin if they pitted now, with predicted gap ahead and behind, using precomputed pit loss times per circuit with Safety Car and Virtual Safety Car adjustments
- **Telemetry** for unlimited drivers showing speed, throttle, brake, gear, and DRS (2025 and earlier) plotted against track distance, with a moveable side panel for 3+ driver comparisons
- **Lap Analysis** (Beta) compare lap times for up to two drivers with a line chart and lap-by-lap history, with pit stop and safety car periods highlighted. Race replay only
- **Qualifying Lap Compare** compare two best Q3 laps (`Q` / `SQ`) with progressive racing lines, directional car markers, live signed delta, and per-sector timing/colors
- **Picture-in-Picture** mode for a compact floating window with track map, race control, leaderboard, and telemetry
- **Weather data** including air and track temperature, humidity, wind, and rainfall status
- **Track status flags** for green, yellow, Safety Car, Virtual Safety Car, and red flag conditions
- **Playback controls** with 0.5x to 20x speed, skip buttons (5s, 30s, 1m, 5m), lap jumping, a progress bar, and red flag countdown with skip-to-restart
- **Session support** for races, qualifying, sprint qualifying, and practice sessions from 2024 onwards
- **Full screen mode** hides the session banner and enters browser fullscreen for a distraction-free view
- **Imperial units** toggle for °F and mph in settings
- **Passphrase authentication** to optionally restrict access when publicly hosted


## Quick Start (Prebuilt Images)

This repository includes a production-oriented `docker-compose.yml` that pulls prebuilt GHCR images:

- `ghcr.io/tess1o/f1replaytiming-backend`
- `ghcr.io/tess1o/f1replaytiming-frontend`

1. Start services:
   Create `docker-compose.yml`

```yaml
services:
  f1-backend:
    image: ghcr.io/tess1o/f1replaytiming-backend:${F1RT_TAG:-latest}
    container_name: f1-backend
    environment:
      - REPLAY_CACHE_MAX_MB=64
      - REPLAY_CACHE_TTL_SECONDS=45
      - GOMEMLIMIT=192MiB
      - GOGC=30
    volumes:
      - f1data:/data

  f1-frontend:
    image: ghcr.io/tess1o/f1replaytiming-frontend:${F1RT_TAG:-latest}
    container_name: f1-frontend
    ports:
      - "3000:3000"
    depends_on:
      - f1-backend

volumes:
  f1data:
```

```bash
docker compose up -d
```

2. Open:

`http://localhost:3000`

3. Optional: pin a specific release tag instead of `latest`:

```bash
F1RT_TAG=v1.2.3 docker compose up -d
```

By default, `F1RT_TAG` falls back to `latest`.

## Development Compose (Build From Source)

Use `docker-compose-dev.yaml` when you want local source builds:

```bash
docker compose -f docker-compose-dev.yaml up -d --build
```

This file builds:

- backend from `backend/Dockerfile`
- frontend from `frontend/Dockerfile`

## GHCR Release Flow (Tag-Driven)

GitHub Actions workflow: `.github/workflows/release-images.yml`

### Trigger

- pushes to tags matching `v*` (for example `v1.0.0`)
- optional manual `workflow_dispatch` with a `tag` input

### What it publishes

Multi-arch images for `linux/amd64` and `linux/arm64`:

- `ghcr.io/<owner>/<repo>-backend`
- `ghcr.io/<owner>/<repo>-frontend`

### Tags policy

For every release tag `vX.Y.Z`, it publishes:

- `ghcr.io/<owner>/<repo>-backend:vX.Y.Z`
- `ghcr.io/<owner>/<repo>-backend:latest`
- `ghcr.io/<owner>/<repo>-frontend:vX.Y.Z`
- `ghcr.io/<owner>/<repo>-frontend:latest`

### Create a release

```bash
git tag v1.0.0
git push origin v1.0.0
```

After the workflow completes, users can pull `:latest` or pin `:v1.0.0`.

### Tag Helper Script

Use the helper script for normal tag push and optional forced tag override:

```bash
# create + push a new annotated tag from HEAD
scripts/release-tag.sh v1.0.0

# create + push tag from a specific commit
scripts/release-tag.sh v1.0.0 <commit-sha>

# override existing tag (local + remote) after a hotfix
scripts/release-tag.sh --force -m "Re-release v1.0.0 with hotfix" v1.0.0
```

## Runtime Configuration

Common backend variables:

- `DATA_DIR` (default `/data`)
- `SQLITE_PATH` (default `/data/f1.db`)
- `AUTH_ENABLED`, `AUTH_PASSPHRASE` (optional auth)
- `FRONTEND_URL`, `EXTRA_ORIGINS` (CORS for split-domain setups)

Common frontend variables:

- `BACKEND_INTERNAL_URL` (proxy upstream for `/api` and `/ws`)
- `VITE_API_URL` (direct browser API mode)

In Docker Compose default mode, frontend proxies traffic to backend internally, so browser CORS is usually not needed.

## Static Data Refresh

The backend supports startup/scheduled refresh for static artifacts:

- circuit metadata
- pit loss data

Useful variables:

- `STATIC_DATA_REFRESH_ENABLED`
- `STATIC_DATA_REFRESH_ON_START`
- `STATIC_DATA_REFRESH_INTERVAL_HOURS`
- `PIT_LOSS_REFRESH_URL`
- `PIT_LOSS_AUTO_REFRESH_ENABLED`

Manual API trigger:

- `POST /api/static-data/refresh`
- `GET /api/static-data/status`

## Migration Note (Legacy Images)

Old image names such as:

- `ghcr.io/adn8naiagent/f1replaytiming-backend`
- `ghcr.io/adn8naiagent/f1replaytiming-frontend`

are no longer part of this repository release process. Use images published from this repo namespace instead.

## Acknowledgements

It's a fork of repository https://github.com/adn8naiagent/F1ReplayTiming . The backend was fully re-implemented due to
performance reasons, UI is almost unchanged.

## License

MIT

> **Disclaimer:** This project is intended for **personal, non-commercial use only**. This website is unofficial and is
> not associated in any way with the Formula 1 companies. F1, FORMULA ONE, FORMULA 1, FIA FORMULA ONE WORLD CHAMPIONSHIP,
> GRAND PRIX and related marks are trade marks of Formula One Licensing B.V.

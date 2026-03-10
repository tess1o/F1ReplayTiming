# Changelog

All notable changes to F1 Timing Replay will be documented in this file.

## Unreleased

### New Features
- Docker Compose support for self-hosting — run the full app with `docker compose up`
- Picture-in-Picture popup window with collapsible track map, telemetry, and leaderboard sections (contributed by [@Clav3rbot](https://github.com/Clav3rbot))
- Clipboard paste support for leaderboard sync — users can now paste a screenshot of the F1 TV broadcast leaderboard directly from clipboard (Ctrl+V) instead of uploading a file, with a visual Ctrl+V hint in the UI (contributed by [@Clav3rbot](https://github.com/Clav3rbot))
- Pit prediction now shows both gap ahead (↑) and gap behind (↓) after pitting
- Live sector indicators for qualifying and sprint qualifying. Colour-coded bars (purple/green/yellow) show sector times as they complete during flying laps. Also shown in the telemetry panel. Togglable via settings.

### Improvements
- Pit predictiction now colour coded based on gap to re-enter the field (risk of losing additional place/s) 
- Season schedule data is now fetched on demand from FastF1 when not already in storage, removing the need to run precompute before using the app
- Leaderboard interval/leader toggle replaced with a clickable pill on the P1 row
- Leaderboard no longer wastes horizontal space when scaled down to fit shorter viewports

### Note
For live sector indicators to take effect, you'll need to re-run precompute for any Qualifying sessions.

## 1.0.1 - 2026-03-07

### Improvements
- Improved mobile layout, including track map rendering and playback controls
- Starting grid positions now fall back to qualifying result data when grid position data is unavailable
- Retired drivers now remain on the leaderboard in their final position, marked as "Out"
- Overall improvements to interval timing, including handling of lapped drivers
- Minor UI consistency fixes

### Bug Fixes
- Drivers with unavailable position data are now temporarily hidden from the track map and restored automatically when data resumes

### Security
- Upgraded Next.js 14 to 15 and React 18 to 19

### Note
For the position data, starting grid position, retired driver, and interval timing fixes to take effect, you'll need to re-run precompute for any Race sessions.

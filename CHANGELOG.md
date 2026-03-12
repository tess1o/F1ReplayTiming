# Changelog

All notable changes to F1 Timing Replay will be documented in this file.

## Unreleased

### New Features
- **Live Timing** — real-time timing data via F1 SignalR stream, with broadcast delay slider, post-session replay check, and PiP window support
- **Race Control Messages** — live feed of steward decisions, investigations, penalties, track limits, and flag changes accessible via the RC button on the track map (available in both live and replay modes)
- **Driver Indicators** — investigation (warning triangle) and penalty (circled exclamation) icons on the leaderboard, with automatic clearing when stewards resolve incidents

### Improvements
- Pit prediction now appears from lap 5 onwards (previously lap 15)
- Session picker shows a LIVE banner and session badges when a session is active or starting soon
- Penalty indicator on leaderboard now clears when the penalty is served

### Note
Race control messages in replay mode require a re-run of precompute for each session to take effect.

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

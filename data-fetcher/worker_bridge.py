#!/usr/bin/env python3
"""Bridge worker for Go backend.

Commands:
- ensure-schedule: generate and store season schedule JSON
- process-session: run on-demand FastF1 processing for a session
- live-stream: stream live frames as JSON lines
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Any

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional for local runs
    def load_dotenv() -> None:
        return

load_dotenv()

ROOT = os.path.dirname(__file__)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker_bridge")


def emit(obj: dict[str, Any]) -> None:
    print(json.dumps(obj, separators=(",", ":")), flush=True)


def cmd_ensure_schedule(year: int) -> int:
    from services.f1_data import _get_season_events_sync
    from services import storage

    try:
        emit({"type": "status", "message": f"Generating schedule for {year}..."})
        events = _get_season_events_sync(year)
        storage.put_json(f"seasons/{year}/schedule.json", {"year": year, "events": events})
        emit({"type": "done", "ok": True, "events": len(events)})
        return 0
    except Exception as e:
        emit({"type": "error", "message": f"Failed to generate schedule: {e}"})
        return 1


def cmd_process_session(year: int, round_num: int, session_type: str) -> int:
    from services.process import process_session_sync

    def on_status(msg: str):
        emit({"type": "status", "message": msg})

    try:
        ok = process_session_sync(year, round_num, session_type, on_status=on_status)
        if ok:
            emit({"type": "done", "ok": True})
            return 0
        emit({"type": "error", "message": "Session processing failed"})
        return 1
    except Exception as e:
        emit({"type": "error", "message": f"Session processing error: {e}"})
        return 1


async def _run_live_stream(year: int, round_num: int, session_type: str, source: str, speed: float) -> int:
    from services.live_state import LiveStateManager
    from services.storage import get_json
    from services.track_data import find_track_data, get_test_data_dir

    pit_loss_green = 0.0
    pit_loss_sc = 0.0
    pit_loss_vsc = 0.0

    if session_type in ("R", "S"):
        pit_data = get_json("pit_loss.json")
        if pit_data:
            ga = pit_data.get("global_averages", {})
            pit_loss_green = ga.get("green", 22.0)
            pit_loss_sc = ga.get("sc", 10.0)
            pit_loss_vsc = ga.get("vsc", 14.5)

    track_norm = None
    track_points = None
    track_data = find_track_data(year, round_num, session_type)
    if track_data:
        track_norm = track_data.get("norm")
        track_points = track_data.get("track_points")

    state = LiveStateManager(
        session_type=session_type,
        pit_loss_green=pit_loss_green,
        pit_loss_sc=pit_loss_sc,
        pit_loss_vsc=pit_loss_vsc,
        track_norm=track_norm,
        track_points=track_points,
    )

    data_dir = get_test_data_dir(year, round_num, session_type)
    selected_mode = source
    if source == "auto":
        selected_mode = "test" if data_dir else "signalr"

    emit({
        "type": "ready",
        "mode": "live",
        "total_frames": 0,
        "total_time": 0,
        "total_laps": 0,
        "quali_phases": None,
    })

    async def on_message(topic: str, data: dict, timestamp: float):
        state.process_message(topic, data, timestamp)

    async def publisher_loop() -> None:
        while True:
            frame = state.get_frame()
            if frame:
                emit({"type": "frame", **frame})
                if state._session_was_started and state.session_status in ("Finalised", "Finished"):
                    emit({
                        "type": "finished",
                        "message": "Session ended. Full replay with track positions and telemetry will be available shortly.",
                    })
                    return
            await asyncio.sleep(0.5)

    if selected_mode == "test":
        if not data_dir:
            emit({"type": "error", "message": "No test data found for requested live session"})
            return 1
        from services.live_test_replayer import LiveTestReplayer

        replayer = LiveTestReplayer(data_dir, speed_multiplier=speed)
        replayer.load()

        replay_task = asyncio.create_task(replayer.replay(on_message))
        publish_task = asyncio.create_task(publisher_loop())
        try:
            done, pending = await asyncio.wait(
                {replay_task, publish_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            for t in done:
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    emit({"type": "error", "message": f"Live replayer error: {e}"})
                    return 1
            return 0
        finally:
            replayer.stop()

    from services.live_signalr import LiveSignalRClient

    client = LiveSignalRClient()
    connect_task = asyncio.create_task(client.connect(on_message))
    publish_task = asyncio.create_task(publisher_loop())
    try:
        done, pending = await asyncio.wait(
            {connect_task, publish_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        for t in done:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception as e:
                emit({"type": "error", "message": f"Live stream error: {e}"})
                return 1
        return 0
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def cmd_live_stream(year: int, round_num: int, session_type: str, source: str, speed: float) -> int:
    try:
        return asyncio.run(_run_live_stream(year, round_num, session_type, source, speed))
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        emit({"type": "error", "message": f"Live stream fatal error: {e}"})
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Go bridge worker")
    sub = parser.add_subparsers(dest="command", required=True)

    p_sched = sub.add_parser("ensure-schedule")
    p_sched.add_argument("--year", type=int, required=True)

    p_proc = sub.add_parser("process-session")
    p_proc.add_argument("--year", type=int, required=True)
    p_proc.add_argument("--round", type=int, required=True)
    p_proc.add_argument("--type", type=str, required=True)

    p_live = sub.add_parser("live-stream")
    p_live.add_argument("--year", type=int, required=True)
    p_live.add_argument("--round", type=int, required=True)
    p_live.add_argument("--type", type=str, required=True)
    p_live.add_argument("--source", type=str, default="auto")
    p_live.add_argument("--speed", type=float, default=10.0)

    args = parser.parse_args()

    if args.command == "ensure-schedule":
        return cmd_ensure_schedule(args.year)
    if args.command == "process-session":
        return cmd_process_session(args.year, args.round, args.type)
    if args.command == "live-stream":
        return cmd_live_stream(args.year, args.round, args.type, args.source, args.speed)

    emit({"type": "error", "message": "Unknown command"})
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

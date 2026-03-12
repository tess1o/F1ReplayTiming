"""
Live State Manager for F1 live timing.

Accumulates incremental SignalR updates and maintains a complete session state
that matches the ReplayFrame shape used by the existing replay system.
"""

from __future__ import annotations

import logging
import math
import re
import time
from typing import Any

logger = logging.getLogger(__name__)

# Track status code → normalised status string
_TRACK_STATUS_MAP: dict[str, str] = {
    "1": "green",
    "2": "yellow",
    "4": "sc",
    "5": "red",
    "6": "vsc",
    "7": "vsc",  # VSC ending
}


def _parse_gap_seconds(gap: str | None) -> float | None:
    """Parse a gap string like '+1.234' into seconds.  Returns None for
    non-numeric gaps (leader, lapped, etc.)."""
    if not gap:
        return None
    if gap.startswith("LAP "):
        return None
    m = re.match(r"^\+?([\d.]+)$", gap)
    if m:
        return float(m.group(1))
    m = re.match(r"^(\d+)\s*L(?:ap)?", gap)
    if m:
        return None
    return None


def _parse_remaining(remaining: str) -> float:
    """Parse a time string like '00:15:32.000' into total seconds."""
    try:
        parts = remaining.split(":")
        if len(parts) == 3:
            h, m, rest = parts
            s = float(rest)
            return int(h) * 3600 + int(m) * 60 + s
        if len(parts) == 2:
            m, rest = parts
            s = float(rest)
            return int(m) * 60 + s
        return float(remaining)
    except (ValueError, IndexError):
        return 0.0


def _sanitize_value(val: Any) -> Any:
    """Replace NaN / Infinity floats with None."""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


class _DriverState:
    """Mutable per-driver state."""

    __slots__ = (
        "racing_number",
        "abbr",
        "team",
        "color",
        "position",
        "gap",
        "interval",
        "compound",
        "tyre_life",
        "tyre_history",
        "pit_stops",
        "in_pit",
        "has_fastest_lap",
        "flag",
        "retired",
        "no_timing",
        "grid_position",
        "sectors",
        "pit_prediction",
        "pit_prediction_margin",
        "pit_prediction_free_air",
        "pit_start",
        "_sector_best_personal",
        "_sector_best_overall",
        "_stint_count",
    )

    def __init__(self, racing_number: str) -> None:
        self.racing_number: str = racing_number
        self.abbr: str = ""
        self.team: str = ""
        self.color: str = "#FFFFFF"
        self.position: int | None = None
        self.gap: str | None = None
        self.interval: str | None = None
        self.compound: str | None = None
        self.tyre_life: int | None = None
        self.tyre_history: list[str] = []
        self.pit_stops: int = 0
        self.in_pit: bool = False
        self.has_fastest_lap: bool = False
        self.flag: str | None = None  # "investigation" | "penalty" | None
        self.retired: bool = False
        self.no_timing: bool = False
        self.grid_position: int | None = None
        self.sectors: list[dict[str, Any]] | None = None
        self.pit_prediction: int | None = None
        self.pit_prediction_margin: float | None = None
        self.pit_prediction_free_air: float | None = None
        self.pit_start: bool = False
        # Internal tracking for sector colours
        self._sector_best_personal: dict[int, float] = {}  # sector_num → best time
        self._sector_best_overall: dict[int, bool] = {}  # sector_num → ever overall fastest
        self._stint_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "abbr": self.abbr,
            "position": self.position,
            "gap": self.gap,
            "interval": self.interval,
            "color": self.color,
            "team": self.team,
            "compound": self.compound,
            "tyre_life": self.tyre_life,
            "tyre_history": list(self.tyre_history[-2:]) if self.tyre_history else [],
            "pit_stops": self.pit_stops,
            "in_pit": self.in_pit,
            "has_fastest_lap": self.has_fastest_lap,
            "flag": self.flag,
            "retired": self.retired,
            "no_timing": self.no_timing,
            "grid_position": self.grid_position,
            "sectors": self.sectors,
            "pit_prediction": self.pit_prediction,
            "pit_prediction_margin": self.pit_prediction_margin,
            "pit_prediction_free_air": self.pit_prediction_free_air,
            "pit_start": self.pit_start,
            # No position data without auth
            "x": 0,
            "y": 0,
            "relative_distance": 0,
            # No telemetry data in live
            "speed": None,
            "throttle": None,
            "brake": False,
            "gear": None,
            "rpm": None,
            "drs": None,
        }


class LiveStateManager:
    """Accumulates incremental SignalR messages and produces complete
    ReplayFrame-shaped snapshots on demand.

    Parameters
    ----------
    session_type:
        One of "R", "Q", "S", "SQ", "FP1", "FP2", "FP3", etc.
    pit_loss_green:
        Estimated pit stop time loss under green flag conditions (seconds).
    pit_loss_sc:
        Estimated pit stop time loss under safety car (seconds).
    pit_loss_vsc:
        Estimated pit stop time loss under virtual safety car (seconds).
    """

    def __init__(
        self,
        session_type: str,
        pit_loss_green: float = 0.0,
        pit_loss_sc: float = 0.0,
        pit_loss_vsc: float = 0.0,
    ) -> None:
        self._session_type: str = session_type
        self._pit_loss_green: float = pit_loss_green
        self._pit_loss_sc: float = pit_loss_sc
        self._pit_loss_vsc: float = pit_loss_vsc

        # Per-driver state keyed by racing number string
        self._drivers: dict[str, _DriverState] = {}

        # Session-level state
        self._status: str = "green"
        self._weather: dict[str, Any] | None = None
        self._current_lap: int = 0
        self._total_laps: int = 0
        self._session_status: str = "Inactive"  # Inactive, Started, Finished, Finalised, Ends
        self._quali_phase: int = 0  # 0 = unknown, 1/2/3
        self._clock_remaining: float = 0.0
        self._clock_extrapolating: bool = False
        self._clock_utc: str = ""
        self._clock_update_time: float = 0.0  # monotonic time when clock was last set
        self._last_timestamp: float = 0.0

        # Race control messages (most recent first, capped at 50)
        self._rc_messages: list[dict[str, Any]] = []

        # Overall sector bests (sector index 0-2 → best time)
        self._overall_sector_bests: dict[int, float] = {}

    # ------------------------------------------------------------------
    # Driver helpers
    # ------------------------------------------------------------------

    def _get_driver(self, number: str) -> _DriverState:
        """Get or create a driver state entry."""
        if number not in self._drivers:
            self._drivers[number] = _DriverState(number)
        return self._drivers[number]

    @property
    def _is_race(self) -> bool:
        return self._session_type in ("R", "S")

    @property
    def _is_quali(self) -> bool:
        return self._session_type in ("Q", "SQ")

    @property
    def session_status(self) -> str:
        """Current session status (Inactive, Started, Finished, Finalised, Ends)."""
        return self._session_status

    # ------------------------------------------------------------------
    # Message processing
    # ------------------------------------------------------------------

    def process_message(self, topic: str, data: dict, timestamp: float) -> None:
        """Process a single SignalR message.

        Parameters
        ----------
        topic:
            The SignalR hub topic name (e.g. "TimingData", "WeatherData").
        data:
            The message payload dict.
        timestamp:
            Unix epoch timestamp of when the message was received.
        """
        self._last_timestamp = timestamp

        handler = self._HANDLERS.get(topic)
        if handler is not None:
            try:
                handler(self, data, timestamp)
            except Exception:
                logger.exception("Error processing %s message", topic)

    # --- DriverList ---------------------------------------------------

    def _handle_driver_list(self, data: dict, _ts: float) -> None:
        for number, info in data.items():
            if not isinstance(info, dict):
                continue
            drv = self._get_driver(str(number))
            if "Tla" in info:
                drv.abbr = info["Tla"]
            if "TeamName" in info:
                drv.team = info["TeamName"]
            if "TeamColour" in info:
                colour = info["TeamColour"]
                if not colour.startswith("#"):
                    colour = "#" + colour
                drv.color = colour

    # --- TimingData ---------------------------------------------------

    def _handle_timing_data(self, data: dict, _ts: float) -> None:
        lines = data.get("Lines")
        if not lines:
            return
        for number, updates in lines.items():
            if not isinstance(updates, dict):
                continue
            drv = self._get_driver(str(number))

            if "Position" in updates:
                try:
                    drv.position = int(updates["Position"])
                except (ValueError, TypeError):
                    pass

            if "GapToLeader" in updates:
                val = updates["GapToLeader"]
                if isinstance(val, dict):
                    val = val.get("Value", "")
                drv.gap = val if val else drv.gap

            if "IntervalToPositionAhead" in updates:
                ival = updates["IntervalToPositionAhead"]
                if isinstance(ival, dict):
                    ival = ival.get("Value", "")
                drv.interval = ival if ival else drv.interval

            if "InPit" in updates:
                drv.in_pit = bool(updates["InPit"])

            if "Retired" in updates:
                if updates["Retired"]:
                    drv.retired = True

            if "KnockedOut" in updates:
                if updates["KnockedOut"]:
                    drv.retired = True

            # Sectors (qualifying sector indicators)
            if "Sectors" in updates:
                sectors_raw = updates["Sectors"]
                if isinstance(sectors_raw, list):
                    sectors_raw = {str(i): v for i, v in enumerate(sectors_raw) if isinstance(v, dict)}
                if isinstance(sectors_raw, dict):
                    self._process_sectors(drv, sectors_raw)

            # Status / no_timing detection
            if "Status" in updates:
                status_val = updates["Status"]
                if not status_val or (isinstance(status_val, dict) and not status_val):
                    drv.no_timing = True
            # If a driver has no position and no gap, mark no_timing
            if drv.position is None and not drv.gap:
                drv.no_timing = True
            else:
                drv.no_timing = False

    def _process_sectors(self, drv: _DriverState, sectors: dict) -> None:
        """Update sector colour indicators for a driver."""
        sector_list: list[dict[str, Any]] = []
        for idx_str, sector_data in sorted(sectors.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
            if not isinstance(sector_data, dict):
                continue
            try:
                sector_idx = int(idx_str)
            except ValueError:
                continue
            sector_num = sector_idx + 1  # 0-based → 1-based

            overall_fastest = bool(sector_data.get("OverallFastest", False))
            personal_fastest = bool(sector_data.get("PersonalFastest", False))

            # Parse time value for tracking bests
            val_str = sector_data.get("Value", "")
            if val_str:
                try:
                    sec_time = float(val_str)
                    # Track personal bests
                    current_pb = drv._sector_best_personal.get(sector_idx)
                    if current_pb is None or sec_time < current_pb:
                        drv._sector_best_personal[sector_idx] = sec_time
                    # Track overall bests
                    current_ob = self._overall_sector_bests.get(sector_idx)
                    if current_ob is None or sec_time < current_ob:
                        self._overall_sector_bests[sector_idx] = sec_time
                except ValueError:
                    pass

            # Determine colour
            if overall_fastest:
                color = "purple"
            elif personal_fastest:
                color = "green"
            else:
                color = "yellow"

            sector_list.append({"num": sector_num, "color": color})

        if sector_list:
            drv.sectors = sector_list

    # --- TimingAppData ------------------------------------------------

    def _handle_timing_app_data(self, data: dict, _ts: float) -> None:
        lines = data.get("Lines")
        if not lines:
            return
        for number, updates in lines.items():
            if not isinstance(updates, dict):
                continue
            drv = self._get_driver(str(number))

            if "GridPos" in updates:
                try:
                    drv.grid_position = int(updates["GridPos"])
                except (ValueError, TypeError):
                    pass

            if "Stints" in updates:
                stints_raw = updates["Stints"]
                if isinstance(stints_raw, list):
                    # Initial dump sends array — convert to dict
                    stints_raw = {str(i): v for i, v in enumerate(stints_raw) if isinstance(v, dict)}
                if isinstance(stints_raw, dict):
                    self._process_stints(drv, stints_raw)

    def _process_stints(self, drv: _DriverState, stints: dict) -> None:
        """Process stint data — update compound, tyre_life, pit_stops, tyre_history."""
        # Find the highest-index stint (the current one)
        max_idx = -1
        latest_stint: dict | None = None
        for idx_str, stint_data in stints.items():
            if not isinstance(stint_data, dict):
                continue
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            if idx > max_idx:
                max_idx = idx
                latest_stint = stint_data

        if latest_stint is None:
            return

        # Update compound
        if "Compound" in latest_stint:
            new_compound = latest_stint["Compound"].upper()
            old_compound = drv.compound
            drv.compound = new_compound
            # Track stint changes for tyre_history
            if old_compound and new_compound != old_compound:
                if not drv.tyre_history or drv.tyre_history[-1] != old_compound:
                    drv.tyre_history.append(old_compound)

        # Update tyre life
        if "TotalLaps" in latest_stint:
            try:
                drv.tyre_life = int(latest_stint["TotalLaps"])
            except (ValueError, TypeError):
                pass

        # Pit stops = number of stints - 1 (based on highest stint index)
        new_stint_count = max_idx + 1
        if new_stint_count > drv._stint_count:
            drv._stint_count = new_stint_count
            drv.pit_stops = max(0, new_stint_count - 1)

    # --- TimingStats --------------------------------------------------

    def _handle_timing_stats(self, data: dict, _ts: float) -> None:
        lines = data.get("Lines")
        if not lines:
            return

        # Check for fastest lap holder change
        new_fastest_number: str | None = None
        for number, stats in lines.items():
            if not isinstance(stats, dict):
                continue
            pb = stats.get("PersonalBestLapTime")
            if isinstance(pb, dict):
                pos = pb.get("Position")
                if pos == 1 or pos == "1":
                    new_fastest_number = str(number)

        if new_fastest_number is not None:
            for num, drv in self._drivers.items():
                drv.has_fastest_lap = (num == new_fastest_number)

    # --- RaceControlMessages ------------------------------------------

    def _handle_race_control(self, data: dict, ts: float) -> None:
        messages = data.get("Messages")
        if not messages:
            return
        # Messages can be a list (initial dump) or dict (incremental updates)
        if isinstance(messages, list):
            items = enumerate(messages)
        elif isinstance(messages, dict):
            items = messages.items()
        else:
            return
        for _, msg_data in items:
            if not isinstance(msg_data, dict):
                continue
            message = msg_data.get("Message", "")
            category = msg_data.get("Category", "")
            racing_number = msg_data.get("RacingNumber")
            lap = msg_data.get("Lap")
            upper_msg = message.upper()

            # Store the message for the RC feed
            if message:
                rc_entry: dict[str, Any] = {
                    "message": message,
                    "category": category,
                    "timestamp": ts,
                    "lap": lap,
                }
                if racing_number:
                    rc_entry["racing_number"] = str(racing_number)
                self._rc_messages.append(rc_entry)
                # Cap at 50 most recent
                if len(self._rc_messages) > 50:
                    self._rc_messages = self._rc_messages[-50:]

            if not racing_number:
                # Try to extract car number from message
                car_match = re.search(r"CAR\s+(\d+)", message)
                if car_match:
                    racing_number = car_match.group(1)

            if not racing_number:
                continue

            drv = self._get_driver(str(racing_number))

            if "NO FURTHER ACTION" in upper_msg or "NO INVESTIGATION" in upper_msg:
                drv.flag = None
            elif "PENALTY SERVED" in upper_msg:
                drv.flag = None
            elif "DECISION" in upper_msg and "PENALTY" not in upper_msg:
                drv.flag = None
            elif "UNDER INVESTIGATION" in upper_msg or "IS NOTED" in upper_msg:
                drv.flag = "investigation"
            elif ("TIME PENALTY" in upper_msg or "PENALTY" in upper_msg) and "NO FURTHER" not in upper_msg:
                drv.flag = "penalty"

    # --- TrackStatus --------------------------------------------------

    def _handle_track_status(self, data: dict, _ts: float) -> None:
        status_code = data.get("Status", "")
        mapped = _TRACK_STATUS_MAP.get(str(status_code))
        if mapped:
            self._status = mapped

    # --- WeatherData --------------------------------------------------

    def _handle_weather(self, data: dict, _ts: float) -> None:
        try:
            self._weather = {
                "air_temp": float(data["AirTemp"]) if "AirTemp" in data else (self._weather or {}).get("air_temp"),
                "track_temp": float(data["TrackTemp"]) if "TrackTemp" in data else (self._weather or {}).get("track_temp"),
                "humidity": float(data["Humidity"]) if "Humidity" in data else (self._weather or {}).get("humidity"),
                "rainfall": str(data.get("Rainfall", "0")) != "0" if "Rainfall" in data else (self._weather or {}).get("rainfall", False),
                "wind_speed": float(data["WindSpeed"]) if "WindSpeed" in data else (self._weather or {}).get("wind_speed"),
                "wind_direction": float(data["WindDirection"]) if "WindDirection" in data else (self._weather or {}).get("wind_direction"),
            }
        except (ValueError, TypeError):
            logger.warning("Failed to parse weather data: %s", data)

    # --- LapCount -----------------------------------------------------

    def _handle_lap_count(self, data: dict, _ts: float) -> None:
        if "CurrentLap" in data:
            try:
                self._current_lap = int(data["CurrentLap"])
            except (ValueError, TypeError):
                pass
        if "TotalLaps" in data:
            try:
                self._total_laps = int(data["TotalLaps"])
            except (ValueError, TypeError):
                pass

    # --- ExtrapolatedClock --------------------------------------------

    def _handle_extrapolated_clock(self, data: dict, ts: float) -> None:
        if "Remaining" in data:
            self._clock_remaining = _parse_remaining(data["Remaining"])
            self._clock_update_time = time.monotonic()
        if "Extrapolating" in data:
            self._clock_extrapolating = bool(data["Extrapolating"])
        if "Utc" in data:
            self._clock_utc = data["Utc"]

    # --- SessionStatus ------------------------------------------------

    def _handle_session_status(self, data: dict, _ts: float) -> None:
        if "Status" in data:
            self._session_status = data["Status"]

    # --- SessionData --------------------------------------------------

    def _handle_session_data(self, data: dict, _ts: float) -> None:
        series = data.get("Series")
        if not series or not isinstance(series, dict):
            return
        # Find the highest-index entry to get current qualifying part
        max_idx = -1
        latest: dict | None = None
        for idx_str, entry in series.items():
            if not isinstance(entry, dict):
                continue
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            if idx > max_idx:
                max_idx = idx
                latest = entry
        if latest and "QualifyingPart" in latest:
            try:
                self._quali_phase = int(latest["QualifyingPart"])
            except (ValueError, TypeError):
                pass

    # ------------------------------------------------------------------
    # Handler dispatch table
    # ------------------------------------------------------------------

    _HANDLERS: dict[str, Any] = {
        "DriverList": _handle_driver_list,
        "TimingData": _handle_timing_data,
        "TimingAppData": _handle_timing_app_data,
        "TimingStats": _handle_timing_stats,
        "RaceControlMessages": _handle_race_control,
        "TrackStatus": _handle_track_status,
        "WeatherData": _handle_weather,
        "LapCount": _handle_lap_count,
        "ExtrapolatedClock": _handle_extrapolated_clock,
        "SessionStatus": _handle_session_status,
        "SessionData": _handle_session_data,
    }

    # ------------------------------------------------------------------
    # Frame construction
    # ------------------------------------------------------------------

    def get_frame(self) -> dict:
        """Build and return a complete frame dict matching the ReplayFrame shape.

        This is intended to be called at ~2 Hz by the broadcaster.
        """
        drivers_list: list[dict[str, Any]] = []
        for drv in self._drivers.values():
            d = drv.to_dict()
            # Sanitize all values
            for key in list(d.keys()):
                d[key] = _sanitize_value(d[key])
            drivers_list.append(d)

        # Sort by position (None positions go to the end)
        drivers_list.sort(key=lambda d: d["position"] if d["position"] is not None else 9999)

        # Set leader gap for races
        if self._is_race and drivers_list:
            for d in drivers_list:
                if d["position"] == 1:
                    d["gap"] = f"LAP {self._current_lap}" if self._current_lap > 0 else d["gap"]
                    break

        # Build quali_phase
        quali_phase: dict[str, Any] | None = None
        if self._is_quali and self._quali_phase > 0:
            # Compute remaining time with extrapolation
            remaining = self._clock_remaining
            if self._clock_extrapolating and self._clock_update_time > 0:
                elapsed_since = time.monotonic() - self._clock_update_time
                remaining = max(0.0, remaining - elapsed_since)
            quali_phase = {
                "phase": f"Q{self._quali_phase}",
                "elapsed": 0,  # Not reliably available from live data
                "remaining": round(remaining, 1),
            }

        frame: dict[str, Any] = {
            "timestamp": self._last_timestamp,
            "lap": self._current_lap,
            "total_laps": self._total_laps,
            "session_type": self._session_type,
            "status": self._status,
            "weather": self._weather,
            "quali_phase": quali_phase,
            "drivers": drivers_list,
            "rc_messages": list(reversed(self._rc_messages)),
        }

        # Add pit predictions for race sessions
        if self._is_race and self._pit_loss_green > 0:
            self._add_pit_predictions(frame)

        # Final sanitization pass on top-level numeric fields
        for key in ("timestamp", "lap", "total_laps"):
            frame[key] = _sanitize_value(frame[key])

        return frame

    # ------------------------------------------------------------------
    # Pit prediction (mirrors replay.py logic)
    # ------------------------------------------------------------------

    def _add_pit_predictions(self, frame: dict) -> None:
        """Add pit_prediction, pit_prediction_margin, and pit_prediction_free_air
        to each driver in the frame."""
        drivers = frame.get("drivers", [])
        status = frame.get("status", "green")
        lap = frame.get("lap", 0)

        # Don't show before lap 5
        if lap < 5:
            return

        # Select pit loss based on track status
        if status == "sc":
            pit_loss = self._pit_loss_sc
        elif status == "vsc":
            pit_loss = self._pit_loss_vsc
        else:
            pit_loss = self._pit_loss_green

        # Build list of (driver_abbr, gap_seconds) for drivers on track
        driver_gaps: list[tuple[str, float]] = []
        for d in drivers:
            if d.get("retired") or d.get("in_pit"):
                continue
            if d.get("position") == 1:
                driver_gaps.append((d["abbr"], 0.0))
            else:
                gap_sec = _parse_gap_seconds(d.get("gap"))
                if gap_sec is not None:
                    driver_gaps.append((d["abbr"], gap_sec))

        if not driver_gaps:
            return

        # Sort by gap (ascending = leader first)
        driver_gaps.sort(key=lambda x: x[1])

        for d in drivers:
            if d.get("retired") or d.get("in_pit"):
                d["pit_prediction"] = None
                d["pit_prediction_margin"] = None
                d["pit_prediction_free_air"] = None
                continue

            current_gap: float | None = None
            if d.get("position") == 1:
                current_gap = 0.0
            else:
                current_gap = _parse_gap_seconds(d.get("gap"))

            if current_gap is None:
                d["pit_prediction"] = None
                d["pit_prediction_margin"] = None
                d["pit_prediction_free_air"] = None
                continue

            projected_gap = current_gap + pit_loss

            # Build gap list excluding this driver
            other_gaps = [g for abbr, g in driver_gaps if abbr != d["abbr"]]

            # Find what position this projected gap would be
            predicted_pos = 1
            for g in other_gaps:
                if projected_gap > g:
                    predicted_pos += 1
                else:
                    break

            # Cap at field size
            predicted_pos = min(predicted_pos, len(other_gaps) + 1)

            # Only show if they'd lose at least 1 position
            if predicted_pos > (d.get("position") or 0):
                d["pit_prediction"] = predicted_pos
                # Margin to the driver one position behind
                behind_idx = predicted_pos - 1
                if behind_idx < len(other_gaps):
                    margin = other_gaps[behind_idx] - projected_gap
                    d["pit_prediction_margin"] = round(max(0.0, margin), 3)
                else:
                    d["pit_prediction_margin"] = None
                # Free air — gap to the car one position ahead
                ahead_idx = predicted_pos - 2
                if ahead_idx >= 0:
                    free_air = projected_gap - other_gaps[ahead_idx]
                    d["pit_prediction_free_air"] = round(max(0.0, free_air), 1)
                else:
                    d["pit_prediction_free_air"] = None
            else:
                d["pit_prediction"] = None
                d["pit_prediction_margin"] = None
                d["pit_prediction_free_air"] = None

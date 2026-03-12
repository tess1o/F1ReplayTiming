"""Replay .jsonStream files as if they were a live SignalR stream.

Reads files downloaded from the F1 static API (livetiming.formula1.com)
and replays them with original timing, useful for testing the live timing
feature without waiting for an actual session.

Expected directory layout:
    backend/data/live_test/{year}_{round}_{session}/
        TimingData.jsonStream
        TimingAppData.jsonStream
        ...
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)

# Matches HH:MM:SS.mmm at the start of a line (with optional BOM)
_TIMESTAMP_RE = re.compile(
    r"^\ufeff?(\d{2}):(\d{2}):(\d{2})\.(\d{3})(.*)",
    re.DOTALL,
)


def _parse_timestamp(hours: str, minutes: str, seconds: str, millis: str) -> float:
    """Convert timestamp components to total seconds."""
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds) + int(millis) / 1000


class _Message:
    """A single parsed message from a .jsonStream file."""

    __slots__ = ("timestamp", "topic", "data")

    def __init__(self, timestamp: float, topic: str, data: dict) -> None:
        self.timestamp = timestamp
        self.topic = topic
        self.data = data

    def __lt__(self, other: _Message) -> bool:
        return self.timestamp < other.timestamp


class LiveTestReplayer:
    """Replays .jsonStream files with original timing.

    Parameters
    ----------
    data_dir:
        Path to the directory containing .jsonStream files.
    speed_multiplier:
        Factor by which to speed up (>1) or slow down (<1) playback.
        A value of 1.0 means real-time replay.
    """

    def __init__(self, data_dir: str, speed_multiplier: float = 1.0) -> None:
        self._data_dir = Path(data_dir)
        self._speed_multiplier = max(speed_multiplier, 0.01)  # prevent zero/negative
        self._messages: list[_Message] = []
        self._running = False
        self._current_index = 0

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Parse all .jsonStream files and merge into a single sorted timeline."""
        self._messages.clear()

        if not self._data_dir.is_dir():
            raise FileNotFoundError(f"Data directory not found: {self._data_dir}")

        stream_files = sorted(self._data_dir.glob("*.jsonStream"))
        if not stream_files:
            logger.warning("No .jsonStream files found in %s", self._data_dir)
            return

        for filepath in stream_files:
            topic = filepath.stem  # e.g. TimingData.jsonStream -> TimingData
            self._parse_file(filepath, topic)

        self._messages.sort()
        logger.info(
            "Loaded %d messages from %d files in %s",
            len(self._messages),
            len(stream_files),
            self._data_dir,
        )

    def _parse_file(self, filepath: Path, topic: str) -> None:
        """Parse a single .jsonStream file, appending messages to the timeline."""
        count = 0
        # Read with utf-8-sig to automatically strip BOM if present
        with open(filepath, "r", encoding="utf-8-sig") as f:
            for line_num, raw_line in enumerate(f, start=1):
                line = raw_line.strip()
                if not line:
                    continue

                match = _TIMESTAMP_RE.match(line)
                if not match:
                    logger.warning(
                        "Skipping malformed line %d in %s: %s",
                        line_num,
                        filepath.name,
                        line[:80],
                    )
                    continue

                hours, minutes, seconds, millis, json_str = match.groups()
                timestamp = _parse_timestamp(hours, minutes, seconds, millis)
                json_str = json_str.strip()

                if not json_str:
                    logger.warning(
                        "Empty JSON payload at line %d in %s",
                        line_num,
                        filepath.name,
                    )
                    continue

                try:
                    data = json.loads(json_str)
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "Invalid JSON at line %d in %s: %s",
                        line_num,
                        filepath.name,
                        exc,
                    )
                    continue

                self._messages.append(_Message(timestamp, topic, data))
                count += 1

        logger.debug("Parsed %d messages from %s", count, filepath.name)

    # ------------------------------------------------------------------
    # Replay
    # ------------------------------------------------------------------

    async def replay(
        self,
        callback: Callable[[str, dict, float], Awaitable[None]],
    ) -> None:
        """Replay messages with original timing, adjusted by speed_multiplier.

        Parameters
        ----------
        callback:
            Async function called for each message as
            ``callback(topic, data, timestamp_seconds)``.
        """
        if not self._messages:
            logger.warning("No messages loaded — nothing to replay")
            return

        self._running = True

        logger.info(
            "Starting replay from index %d (%.3fs) at %.1fx speed",
            self._current_index,
            self._messages[self._current_index].timestamp if self._current_index < len(self._messages) else 0,
            self._speed_multiplier,
        )

        prev_ts: float | None = None

        while self._running and self._current_index < len(self._messages):
            idx = self._current_index
            msg = self._messages[idx]

            # Sleep for the time delta between consecutive messages
            if prev_ts is not None:
                delta = msg.timestamp - prev_ts
                if delta > 0:
                    await asyncio.sleep(delta / self._speed_multiplier)

                # Check again after sleep in case stop() or skip was called
                if not self._running:
                    break
                # If index was moved externally (skip), reset prev_ts
                if self._current_index != idx:
                    prev_ts = None
                    continue

            try:
                await callback(msg.topic, msg.data, msg.timestamp)
            except Exception:
                logger.exception(
                    "Callback error for %s at %.3fs", msg.topic, msg.timestamp
                )

            prev_ts = msg.timestamp
            self._current_index = idx + 1

        if self._current_index >= len(self._messages):
            logger.info("Replay complete — all %d messages sent", len(self._messages))

        self._running = False

    def stop(self) -> None:
        """Stop an in-progress replay."""
        self._running = False

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def jump_to(self, timestamp_seconds: float) -> None:
        """Skip ahead to the first message at or after *timestamp_seconds*.

        If a replay is running it will continue from the new position.
        If called before ``replay()``, the next replay starts from this point.
        """
        if not self._messages:
            return

        # Binary search for the target timestamp
        lo, hi = 0, len(self._messages)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._messages[mid].timestamp < timestamp_seconds:
                lo = mid + 1
            else:
                hi = mid

        self._current_index = lo
        logger.info(
            "Jumped to index %d (%.3fs) — target was %.3fs",
            lo,
            self._messages[lo].timestamp if lo < len(self._messages) else 0,
            timestamp_seconds,
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def message_count(self) -> int:
        """Total number of loaded messages."""
        return len(self._messages)

    @property
    def duration(self) -> float:
        """Duration of the session in seconds (last timestamp minus first)."""
        if len(self._messages) < 2:
            return 0.0
        return self._messages[-1].timestamp - self._messages[0].timestamp

    @property
    def topics(self) -> list[str]:
        """Sorted list of unique topics present in the loaded data."""
        return sorted({m.topic for m in self._messages})

    @property
    def is_running(self) -> bool:
        """Whether a replay is currently in progress."""
        return self._running

    @property
    def current_timestamp(self) -> float:
        """Timestamp of the next message to be sent."""
        if not self._messages or self._current_index >= len(self._messages):
            return 0.0
        return self._messages[self._current_index].timestamp

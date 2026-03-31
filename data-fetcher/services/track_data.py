"""Track data helper functions shared by worker-mode tooling."""

from __future__ import annotations

import logging
import os

from services.storage import get_json

logger = logging.getLogger(__name__)


def find_track_data(year: int, round_num: int, session_type: str) -> dict | None:
    """Find track data for a session, with fallback to other sessions/years."""
    data = get_json(f"sessions/{year}/{round_num}/{session_type}/track.json")
    if data:
        return data

    for alt_type in ("R", "Q", "S", "SQ", "FP1", "FP2", "FP3"):
        if alt_type == session_type:
            continue
        data = get_json(f"sessions/{year}/{round_num}/{alt_type}/track.json")
        if data:
            logger.info(
                "Track data fallback: using %s/%s/%s for %s",
                year,
                round_num,
                alt_type,
                session_type,
            )
            return data

    for prev_year in range(year - 1, year - 4, -1):
        for alt_type in ("R", "Q"):
            data = get_json(f"sessions/{prev_year}/{round_num}/{alt_type}/track.json")
            if data:
                logger.info(
                    "Track data fallback: using %s/%s/%s for %s/%s/%s",
                    prev_year,
                    round_num,
                    alt_type,
                    year,
                    round_num,
                    session_type,
                )
                return data

    return None


def get_test_data_dir(year: int, round_num: int, session_type: str) -> str | None:
    """Find test data directory for a given session."""
    base = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "live_test")
    path = os.path.join(base, f"{year}_{round_num}_{session_type}")
    if os.path.isdir(path):
        return path
    return None

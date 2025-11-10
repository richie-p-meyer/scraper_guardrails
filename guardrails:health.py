"""
Health reporting utilities for scraper guardrails.

Writes a simple JSON status file after each pipeline run.
Useful for monitoring, alerting, and debugging.
"""

import json
import time
from pathlib import Path
from typing import Any, Dict


def write_status(path: Path, **kv: Any):
    """
    Write a JSON file containing run health info.

    Example:
        write_status(
            Path("out/health.json"),
            ok=23,
            total=25,
            pipeline="nba",
            struct_changes=2
        )
    """

    payload: Dict[str, Any] = {
        "ts": int(time.time()),
        **kv
    }

    # Ensure output directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write JSON with indentation for readability
    path.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

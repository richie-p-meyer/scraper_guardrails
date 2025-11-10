"""
Structured JSON logging for scraper guardrails.
Designed for observability, debugging, and incident triage.
"""

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Format logs as structured JSON lines."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }

        # Enrich logs with metadata if provided
        for field in ("run_id", "pipeline", "step", "url", "error_code"):
            if hasattr(record, field):
                payload[field] = getattr(record, field)

        # Capture exceptions cleanly
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def setup(level=logging.INFO, **defaults) -> logging.LoggerAdapter:
    """
    Set up structured logging with default metadata.
    Example:
        log = setup_logs(pipeline="nba", step="fetch")
        log.info("Fetched page", extra={"url": url})
    """

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)

    return logging.LoggerAdapter(root, defaults or {})

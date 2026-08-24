"""Structured JSON logging.

Databricks ships driver stdout to Log Analytics / cluster logs. Emitting one JSON
object per line makes every log searchable with a KQL `parse_json` query instead of
regexing free text.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from contextlib import contextmanager
from typing import Any

_RUN_ID = os.environ.get("DATABRICKS_RUN_ID") or uuid.uuid4().hex[:12]

_RESERVED = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


def safe_extra(context: dict[str, Any]) -> dict[str, Any]:
    """Rename keys that would collide with LogRecord attributes.

    `logging` raises KeyError if `extra` contains e.g. "message" or "name", which
    would turn an alert into a crash. Prefixing is preferable to dropping the field:
    the value still reaches the log.
    """
    return {(f"ctx_{k}" if k in _RESERVED else k): v for k, v in context.items()}


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single-line JSON document."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "run_id": _RUN_ID,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str = "lakehouse") -> logging.Logger:
    """Return a process-wide singleton logger writing JSON lines to stdout."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return logger


@contextmanager
def timed(stage: str, logger: logging.Logger | None = None, **context: Any):
    """Time a pipeline stage and log start/finish (or failure) with duration."""
    log = logger or get_logger()
    start = time.perf_counter()
    log.info("stage.start", extra={"stage": stage, **context})
    try:
        yield
    except Exception as exc:
        log.error(
            "stage.failed",
            extra={
                "stage": stage,
                "duration_s": round(time.perf_counter() - start, 3),
                "error": str(exc),
                **context,
            },
            exc_info=True,
        )
        raise
    log.info(
        "stage.finished",
        extra={"stage": stage, "duration_s": round(time.perf_counter() - start, 3), **context},
    )


def run_id() -> str:
    """Correlation id shared by every log line and metric of this run."""
    return _RUN_ID

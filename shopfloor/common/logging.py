"""One JSON object per line on stdout (CONTRACTS 2.5). Absent fields are omitted."""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import uuid
from datetime import UTC, datetime
from typing import IO

TRACE_HEADER = "X-Trace-Id"
_trace_id: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="")

FIELDS = ("route", "status", "latency_ms", "trace_id", "error")


def new_trace_id() -> str:
    return uuid.uuid4().hex


def set_trace_id(value: str) -> contextvars.Token[str]:
    return _trace_id.set(value)


def reset_trace_id(token: contextvars.Token[str]) -> None:
    _trace_id.reset(token)


def current_trace_id() -> str:
    return _trace_id.get()


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname.lower(),
            "service": self.service,
            "msg": record.getMessage(),
        }
        for name in FIELDS:
            value = getattr(record, name, None)
            if value is not None and value != "":
                out[name] = value
        if "trace_id" not in out and (tid := current_trace_id()):
            out["trace_id"] = tid
        return json.dumps(out, separators=(",", ":"))


def get_logger(service: str, stream: IO[str] | None = None) -> logging.Logger:
    """Logger for a service, writing JSON lines to stdout (or `stream`)."""
    logger = logging.getLogger(f"shopfloor.{service}")
    if not any(isinstance(h.formatter, JsonFormatter) for h in logger.handlers):
        handler = logging.StreamHandler(stream or sys.stdout)
        handler.setFormatter(JsonFormatter(service))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger

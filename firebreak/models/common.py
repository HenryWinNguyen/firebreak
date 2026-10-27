"""Shared primitives for every contract module."""

from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4


def new_id(prefix: str) -> str:
    """Prefixed random id, e.g. ``ev_3f9c...``. Prefix says what kind of object it is."""
    return f"{prefix}_{uuid4().hex}"


def utcnow() -> datetime:
    return datetime.now(UTC)


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

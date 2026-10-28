"""Shared primitives for every contract module."""

import hashlib
import itertools
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

# Replay (evals, ARCH §9) needs the same ids and timestamps on every run, so LLM request
# hashes match the recording. Both default to the real thing; `deterministic()` overrides them
# for the current context only.
_id_factory: ContextVar[Callable[[str], str] | None] = ContextVar("_id_factory", default=None)
_clock: ContextVar[Callable[[], datetime] | None] = ContextVar("_clock", default=None)


def new_id(prefix: str) -> str:
    """Prefixed random id, e.g. ``ev_3f9c...``. Prefix says what kind of object it is."""
    factory = _id_factory.get()
    if factory is not None:
        return factory(prefix)
    return f"{prefix}_{uuid4().hex}"


def utcnow() -> datetime:
    clock = _clock.get()
    return clock() if clock is not None else datetime.now(UTC)


@contextmanager
def deterministic(seed: str, *, at: datetime) -> Iterator[None]:
    """Seeded ids and a frozen clock for the current context (and tasks it creates).

    Ids are ``<prefix>_<sha256(seed:prefix:n)[:32]>`` with a per-prefix counter, so the
    sequence depends only on the seed and the order of calls. ``at`` must be timezone-aware.
    """
    if at.tzinfo is None:
        raise ValueError("deterministic(): `at` must be timezone-aware")
    counters: dict[str, itertools.count[int]] = {}

    def factory(prefix: str) -> str:
        n = next(counters.setdefault(prefix, itertools.count()))
        return f"{prefix}_{hashlib.sha256(f'{seed}:{prefix}:{n}'.encode()).hexdigest()[:32]}"

    frozen = at.astimezone(UTC)
    id_token = _id_factory.set(factory)
    clock_token = _clock.set(lambda: frozen)
    try:
        yield
    finally:
        _clock.reset(clock_token)
        _id_factory.reset(id_token)


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

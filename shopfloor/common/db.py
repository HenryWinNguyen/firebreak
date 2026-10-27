"""asyncpg connection helper with startup retry."""

from __future__ import annotations

import asyncio
import logging
import os

import asyncpg

log = logging.getLogger("shopfloor.db")


def database_url() -> str:
    return os.environ.get(
        "SHOPFLOOR_DATABASE_URL", "postgresql://firebreak:firebreak@localhost:5432/shopfloor"
    )


async def create_raw_pool(max_size: int, attempts: int = 30) -> asyncpg.Pool:
    """Connect to Postgres, retrying while it (or the init step) is still coming up."""
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await asyncpg.create_pool(database_url(), min_size=1, max_size=max_size)
        except (OSError, asyncpg.PostgresError) as exc:
            last = exc
            log.warning("postgres not ready (attempt %d/%d): %r", attempt, attempts, exc)
            await asyncio.sleep(1)
    raise RuntimeError(f"postgres unavailable after {attempts} attempts") from last

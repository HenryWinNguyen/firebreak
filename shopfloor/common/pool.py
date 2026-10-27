"""A DB pool whose effective size can change at runtime and that reports usage."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, Protocol


class RawPool(Protocol):
    async def acquire(self) -> Any: ...
    async def release(self, conn: Any) -> None: ...


class PoolExhaustedError(Exception):
    """No connection became available within the acquire timeout."""


class BoundedPool:
    def __init__(
        self, raw: RawPool, size_fn: Callable[[], int], acquire_timeout: float = 2.0
    ) -> None:
        self._raw = raw
        self._size_fn = size_fn
        self._timeout = acquire_timeout
        self._cond = asyncio.Condition()
        self.in_use = 0

    @property
    def size(self) -> int:
        return int(self._size_fn())

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Any]:
        deadline = time.monotonic() + self._timeout
        async with self._cond:
            while self.in_use >= self.size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PoolExhaustedError(f"db pool exhausted: {self.in_use}/{self.size} in use")
                try:
                    # Wake periodically so a runtime size increase is noticed.
                    await asyncio.wait_for(self._cond.wait(), min(remaining, 0.05))
                except TimeoutError:
                    continue
            self.in_use += 1
        try:
            conn = await self._raw.acquire()
        except BaseException:
            await self._release_slot()
            raise
        try:
            yield conn
        finally:
            try:
                await self._raw.release(conn)
            finally:
                await self._release_slot()

    async def _release_slot(self) -> None:
        async with self._cond:
            self.in_use -= 1
            self._cond.notify_all()

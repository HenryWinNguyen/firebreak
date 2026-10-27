"""In-process async token-bucket limiter (requests per minute). Not cross-process."""

import asyncio
import time
from collections.abc import Awaitable, Callable


class TokenBucket:
    def __init__(
        self,
        requests_per_minute: float,
        *,
        capacity: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be > 0")
        self._rate = requests_per_minute / 60.0  # tokens per second
        self._capacity = capacity if capacity is not None else max(1.0, requests_per_minute)
        self._tokens = self._capacity
        self._clock = clock
        self._sleep = sleep
        self._updated = clock()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        # Holding the lock while sleeping keeps waiters FIFO and the accounting simple.
        async with self._lock:
            self._refill()
            if self._tokens < 1.0:
                await self._sleep((1.0 - self._tokens) / self._rate)
                self._refill()
            self._tokens -= 1.0

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self._capacity, self._tokens + (now - self._updated) * self._rate)
        self._updated = now

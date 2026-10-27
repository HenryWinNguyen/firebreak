"""Orders service: validates via inventory, stores orders in Postgres, caches products in Redis."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, Protocol

import httpx
import redis.asyncio as aioredis
from common.app import create_base_app
from common.db import create_raw_pool
from common.errors import DownstreamError, call_service
from common.limits import MAX_QUANTITY
from common.pool import BoundedPool
from common.runtime import RuntimeSettings
from fastapi import FastAPI
from pydantic import BaseModel, Field

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""
CACHE_KEY = "products"
CACHE_TTL_SECONDS = 30
RAW_POOL_MAX = 50  # hard ceiling; orders.db_pool_size is clamped to it
ORDER_WORK_SECONDS = float(os.environ.get("SHOPFLOOR_ORDER_WORK_SECONDS", "0.02"))
ITEM_QUERY_SECONDS = float(os.environ.get("SHOPFLOOR_ITEM_QUERY_SECONDS", "0.25"))
BAD_DEPLOY_ERROR_RATE = 0.1


def pool_size_fn(runtime: RuntimeSettings) -> Callable[[], int]:
    """Effective pool size: the flag, clamped to the raw pool so a large value can't hang."""
    return lambda: min(int(runtime.get("orders.db_pool_size")), RAW_POOL_MAX)


class Cache(Protocol):
    async def get(self, key: str) -> str | None: ...
    async def set(self, key: str, value: str, ex: int | None = None) -> Any: ...
    async def delete(self, key: str) -> Any: ...


class OrderRequest(BaseModel):
    product_id: int
    quantity: int = Field(ge=1, le=MAX_QUANTITY)


class OrderStore:
    def __init__(
        self,
        pool: BoundedPool,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> None:
        self.pool = pool
        self._sleep = sleep  # injectable so tests can assert simulated query time

    async def init_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA)

    async def insert(self, product_id: int, quantity: int, n_plus_one: bool = False) -> int:
        async with self.pool.acquire() as conn:
            if n_plus_one:
                # S3: one lookup round trip per item instead of one for the whole cart.
                for _ in range(quantity):
                    await conn.fetchval("SELECT 1")
                    await self._sleep(ITEM_QUERY_SECONDS)
            else:
                await conn.fetchval("SELECT 1")
                await self._sleep(ORDER_WORK_SECONDS)
            return int(
                await conn.fetchval(
                    "INSERT INTO orders (product_id, quantity) VALUES ($1, $2) RETURNING id",
                    product_id,
                    quantity,
                )
            )


class PricingBugError(RuntimeError):
    """The S1 regression: raised by the broken build of the order path."""


def apply_promotion(order: OrderRequest) -> None:
    """Stands in for the code the bad deploy broke; raises like a real null-deref."""
    promo: dict[str, Any] | None = None
    try:
        promo["discount_pct"]  # type: ignore[index]
    except TypeError as exc:
        raise PricingBugError(
            f"promotion lookup failed for product {order.product_id}: {exc}"
        ) from exc


class SafeCache:
    """Cache facade for S6: a Redis failure is logged and treated as a miss, never a 5xx."""

    def __init__(self, get_cache: Any, logger: logging.Logger) -> None:
        self._get_cache = get_cache
        self._log = logger

    def _failed(self, op: str, exc: Exception) -> None:
        self._log.warning(
            f"cache {op} failed, falling back to db: {type(exc).__name__}",
            extra={"error": f"redis {op}: {type(exc).__name__}: {exc}"},
        )

    async def get(self, key: str) -> str | None:
        try:
            return await (await self._get_cache()).get(key)  # type: ignore[no-any-return]
        except CACHE_ERRORS as exc:
            self._failed("get", exc)
            return None

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        try:
            await (await self._get_cache()).set(key, value, ex=ex)
        except CACHE_ERRORS as exc:
            self._failed("set", exc)

    async def delete(self, key: str) -> None:
        try:
            await (await self._get_cache()).delete(key)
        except CACHE_ERRORS as exc:
            self._failed("delete", exc)


CACHE_ERRORS = (aioredis.RedisError, OSError, TimeoutError)


class RedisProvider:
    """Redis client that is rebuilt when the runtime `orders.redis_host` changes."""

    def __init__(self, runtime: RuntimeSettings) -> None:
        self._runtime = runtime
        self._host: str | None = None
        self._client: aioredis.Redis | None = None

    async def get(self) -> Cache:
        host = str(self._runtime.get("orders.redis_host"))
        if self._client is None or host != self._host:
            old = self._client
            self._client = aioredis.Redis(
                host=host,
                port=6379,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=1,
            )
            self._host = host
            if old is not None:
                await old.aclose()
        return self._client  # type: ignore[return-value]

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()


def create_app(
    store: OrderStore | None = None,
    cache: Cache | None = None,
    inventory: httpx.AsyncClient | None = None,
) -> FastAPI:
    runtime = RuntimeSettings(
        {
            "orders.error_rate": 0.0,
            "orders.db_pool_size": 20,
            "orders.n_plus_one": False,
            "orders.leak_kb_per_request": 0,
            "orders.redis_host": os.environ.get("SHOPFLOOR_REDIS_HOST", "redis"),
        },
        bounds={
            "orders.error_rate": (0.0, 1.0),
            "orders.db_pool_size": (1, None),
            "orders.leak_kb_per_request": (0, None),
        },
    )
    leaked: list[bytes] = []  # S5: never freed until the process restarts

    def on_deploy(version: str, previous: str) -> None:
        if version.endswith("-bad"):
            runtime.set("orders.error_rate", BAD_DEPLOY_ERROR_RATE)  # S1
        else:
            # Any non-bad deploy (rollback or fix-forward) resets error_rate to 0.0,
            # overriding a manually set rate.
            runtime.set("orders.error_rate", 0.0)

    provider = RedisProvider(runtime)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        raw = None
        client = inventory
        if client is None:
            client = httpx.AsyncClient(
                base_url=os.environ.get("INVENTORY_URL", "http://inventory:8102"), timeout=5.0
            )
        app.state.inventory = client
        if store is None:
            raw = await create_raw_pool(RAW_POOL_MAX)
            app.state.store = OrderStore(BoundedPool(raw, pool_size_fn(runtime)))
            await app.state.store.init_schema()
        else:
            app.state.store = store
        app.state.metrics.bind_pool(app.state.store.pool)
        try:
            yield
        finally:
            await client.aclose()
            await provider.close()
            if raw is not None:
                await raw.close()

    async def get_cache() -> Cache:
        return cache if cache is not None else await provider.get()

    app = create_base_app("orders", runtime, lifespan, on_deploy=on_deploy)
    app.state.leaked = leaked
    safe_cache = SafeCache(get_cache, app.state.logger)

    @app.get("/products")
    async def products() -> list[dict[str, Any]]:
        cached = await safe_cache.get(CACHE_KEY)
        if cached is not None:
            result: list[dict[str, Any]] = json.loads(cached)
            return result
        fresh: list[dict[str, Any]] = await call_service(
            app.state.inventory, "inventory", "GET", "/products"
        )
        await safe_cache.set(CACHE_KEY, json.dumps(fresh), ex=CACHE_TTL_SECONDS)
        return fresh

    async def release_stock(req: OrderRequest) -> None:
        """Compensate a failed order: hand the reserved stock back to inventory."""
        try:
            await call_service(
                app.state.inventory,
                "inventory",
                "POST",
                "/release",
                json={"product_id": req.product_id, "quantity": req.quantity},
            )
        except DownstreamError as exc:
            app.state.logger.error(
                f"could not release reserved stock for product {req.product_id} "
                f"x{req.quantity}; stock is leaked",
                extra={"error": str(exc)},
            )

    @app.post("/orders", status_code=201)
    async def create_order(req: OrderRequest) -> dict[str, Any]:
        reservation = await call_service(
            app.state.inventory,
            "inventory",
            "POST",
            "/reserve",
            json={"product_id": req.product_id, "quantity": req.quantity},
        )
        leak_kb = int(runtime.get("orders.leak_kb_per_request"))
        if leak_kb > 0:
            leaked.append(b"\xa5" * (leak_kb * 1024))  # filled, so it counts toward RSS
        try:
            if random.random() < float(runtime.get("orders.error_rate")):
                apply_promotion(req)
            order_id = await app.state.store.insert(
                req.product_id, req.quantity, bool(runtime.get("orders.n_plus_one"))
            )
        except (Exception, asyncio.CancelledError):
            # Shielded so a cancelled request still returns the reserved stock.
            await asyncio.shield(release_stock(req))
            raise
        await safe_cache.delete(CACHE_KEY)  # stock changed
        return {
            "order_id": order_id,
            "product_id": req.product_id,
            "quantity": req.quantity,
            "remaining_stock": reservation["remaining"],
        }

    return app


app = create_app()

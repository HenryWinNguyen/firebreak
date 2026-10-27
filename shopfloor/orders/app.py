"""Orders service: validates via inventory, stores orders in Postgres, caches products in Redis."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Protocol

import httpx
import redis.asyncio as aioredis
from common.app import create_base_app
from common.db import create_raw_pool
from common.errors import call_service
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
RAW_POOL_MAX = 50  # hard ceiling; orders.db_pool_size limits within it


class Cache(Protocol):
    async def get(self, key: str) -> str | None: ...
    async def set(self, key: str, value: str, ex: int | None = None) -> Any: ...
    async def delete(self, key: str) -> Any: ...


class OrderRequest(BaseModel):
    product_id: int
    quantity: int = Field(ge=1)


class OrderStore:
    def __init__(self, pool: BoundedPool) -> None:
        self.pool = pool

    async def init_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA)

    async def insert(self, product_id: int, quantity: int) -> int:
        async with self.pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    "INSERT INTO orders (product_id, quantity) VALUES ($1, $2) RETURNING id",
                    product_id,
                    quantity,
                )
            )


class RedisProvider:
    """Redis client that is rebuilt when the runtime `orders.redis_host` changes."""

    def __init__(self, runtime: RuntimeSettings) -> None:
        self._runtime = runtime
        self._host: str | None = None
        self._client: aioredis.Redis | None = None

    def get(self) -> Cache:
        host = str(self._runtime.get("orders.redis_host"))
        if self._client is None or host != self._host:
            self._client = aioredis.Redis(
                host=host, port=6379, decode_responses=True, socket_connect_timeout=2
            )
            self._host = host
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
            "orders.db_pool_size": 20,
            "orders.redis_host": os.environ.get("SHOPFLOOR_REDIS_HOST", "redis"),
        }
    )
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
            app.state.store = OrderStore(
                BoundedPool(raw, lambda: int(runtime.get("orders.db_pool_size")))
            )
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

    app = create_base_app("orders", runtime, lifespan)

    def get_cache() -> Cache:
        return cache if cache is not None else provider.get()

    @app.get("/products")
    async def products() -> list[dict[str, Any]]:
        c = get_cache()
        cached = await c.get(CACHE_KEY)
        if cached is not None:
            result: list[dict[str, Any]] = json.loads(cached)
            return result
        fresh: list[dict[str, Any]] = await call_service(
            app.state.inventory, "inventory", "GET", "/products"
        )
        await c.set(CACHE_KEY, json.dumps(fresh), ex=CACHE_TTL_SECONDS)
        return fresh

    @app.post("/orders", status_code=201)
    async def create_order(req: OrderRequest) -> dict[str, Any]:
        reservation = await call_service(
            app.state.inventory,
            "inventory",
            "POST",
            "/reserve",
            json={"product_id": req.product_id, "quantity": req.quantity},
        )
        order_id = await app.state.store.insert(req.product_id, req.quantity)
        await get_cache().delete(CACHE_KEY)  # stock changed
        return {
            "order_id": order_id,
            "product_id": req.product_id,
            "quantity": req.quantity,
            "remaining_stock": reservation["remaining"],
        }

    return app


app = create_app()

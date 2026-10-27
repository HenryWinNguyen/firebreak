"""Inventory service: product stock in Postgres."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from common.app import create_base_app
from common.db import create_raw_pool
from common.pool import BoundedPool
from common.runtime import RuntimeSettings
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    stock INTEGER NOT NULL
)
"""
SEED = [
    ("Widget", 999),
    ("Gadget", 1999),
    ("Gizmo", 2999),
    ("Doohickey", 499),
    ("Thingamajig", 1499),
]
POOL_SIZE = 10


class ReserveRequest(BaseModel):
    product_id: int
    quantity: int = Field(ge=1)


class InventoryStore:
    def __init__(self, pool: BoundedPool) -> None:
        self.pool = pool

    async def init_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA)
            if await conn.fetchval("SELECT count(*) FROM products") == 0:
                for name, price in SEED:
                    await conn.execute(
                        "INSERT INTO products (name, price_cents, stock) VALUES ($1, $2, 1000000)",
                        name,
                        price,
                    )

    async def list_products(self) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, name, price_cents, stock FROM products ORDER BY id")
        return [dict(r) for r in rows]

    async def reserve(self, product_id: int, quantity: int) -> int | None:
        """Decrement stock. Returns remaining stock, None if insufficient. KeyError if unknown."""
        async with self.pool.acquire() as conn:
            remaining = await conn.fetchval(
                "UPDATE products SET stock = stock - $2 WHERE id = $1 AND stock >= $2 "
                "RETURNING stock",
                product_id,
                quantity,
            )
            if remaining is not None:
                return int(remaining)
            if await conn.fetchval("SELECT 1 FROM products WHERE id = $1", product_id) is None:
                raise KeyError(product_id)
            return None


def create_app(store: InventoryStore | None = None) -> FastAPI:
    runtime = RuntimeSettings({"inventory.unavailable": False})

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        raw = None
        if store is None:
            raw = await create_raw_pool(POOL_SIZE)
            app.state.store = InventoryStore(BoundedPool(raw, lambda: POOL_SIZE))
            await app.state.store.init_schema()
        else:
            app.state.store = store
        app.state.metrics.bind_pool(app.state.store.pool)
        try:
            yield
        finally:
            if raw is not None:
                await raw.close()

    app = create_base_app("inventory", runtime, lifespan)

    @app.get("/products")
    async def products() -> list[dict[str, Any]]:
        return await app.state.store.list_products()

    @app.post("/reserve")
    async def reserve(req: ReserveRequest) -> dict[str, int]:
        try:
            remaining = await app.state.store.reserve(req.product_id, req.quantity)
        except KeyError:
            raise HTTPException(404, f"unknown product {req.product_id}") from None
        if remaining is None:
            raise HTTPException(409, "insufficient stock")
        return {"product_id": req.product_id, "reserved": req.quantity, "remaining": remaining}

    return app


app = create_app()

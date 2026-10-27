"""Fakes and a wired-up gateway -> orders -> inventory stack with no network."""

from __future__ import annotations

import io
import json
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
import redis
from common.logging import JsonFormatter
from common.pool import BoundedPool
from gateway.app import create_app as create_gateway
from inventory.app import create_app as create_inventory
from orders.app import create_app as create_orders
from orders.app import pool_size_fn


class FakeRaw:
    async def acquire(self) -> object:
        return object()

    async def release(self, conn: object) -> None:
        return None


class FakeInventoryStore:
    def __init__(self) -> None:
        self.pool = BoundedPool(FakeRaw(), lambda: 10)
        self.stock = {1: 5, 2: 1}

    async def list_products(self) -> list[dict[str, Any]]:
        return [
            {"id": i, "name": f"p{i}", "price_cents": 100, "stock": s}
            for i, s in self.stock.items()
        ]

    async def reserve(self, product_id: int, quantity: int) -> int | None:
        async with self.pool.acquire():
            if product_id not in self.stock:
                raise KeyError(product_id)
            if self.stock[product_id] < quantity:
                return None
            self.stock[product_id] -= quantity
            return self.stock[product_id]

    async def release(self, product_id: int, quantity: int) -> int:
        async with self.pool.acquire():
            if product_id not in self.stock:
                raise KeyError(product_id)
            self.stock[product_id] += quantity
            return self.stock[product_id]


class FakeOrderStore:
    def __init__(self, size: int = 20) -> None:
        self.size = size
        self.pool = BoundedPool(FakeRaw(), lambda: self.size)
        self.rows: list[tuple[int, int]] = []
        self.queries = 0
        self.fail: Exception | None = None

    async def insert(self, product_id: int, quantity: int, n_plus_one: bool = False) -> int:
        async with self.pool.acquire():
            if self.fail is not None:
                raise self.fail
            self.queries += quantity if n_plus_one else 1
            self.rows.append((product_id, quantity))
            return len(self.rows)


class FakeCache:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.gets = 0

    async def get(self, key: str) -> str | None:
        self.gets += 1
        return self.data.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.data[key] = value

    async def delete(self, key: str) -> None:
        self.data.pop(key, None)


class BrokenCache:
    """Every call fails the way a bad redis host does."""

    async def get(self, key: str) -> str | None:
        raise redis.ConnectionError("Error -2 connecting to redis-typo:6379. Name not known.")

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        await self.get(key)

    async def delete(self, key: str) -> None:
        await self.get(key)


@dataclass
class Stack:
    gateway: httpx.AsyncClient
    orders: httpx.AsyncClient
    inventory: httpx.AsyncClient
    order_store: FakeOrderStore
    inv_store: FakeInventoryStore
    cache: FakeCache
    apps: dict[str, Any]
    logs: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


class _Capture(logging.Handler):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.buf = io.StringIO()
        self.setFormatter(JsonFormatter(service))
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))


@asynccontextmanager
async def build_stack(inventory_up: bool = True, cache: Any = None) -> AsyncIterator[Stack]:
    inv_store, order_store = FakeInventoryStore(), FakeOrderStore()
    cache = cache if cache is not None else FakeCache()
    inv_app = create_inventory(inv_store)  # type: ignore[arg-type]
    inv_http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=inv_app), base_url="http://inventory"
    )
    if not inventory_up:
        inv_http = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(503, json={"detail": "down"})),
            base_url="http://inventory",
        )
    ord_app = create_orders(order_store, cache, inv_http)  # type: ignore[arg-type]
    ord_http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ord_app), base_url="http://orders"
    )
    order_store.pool = BoundedPool(FakeRaw(), pool_size_fn(ord_app.state.runtime))
    gw_app = create_gateway(ord_http)
    gw_http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gw_app), base_url="http://gateway"
    )
    captures = {}
    for name, app in (("gateway", gw_app), ("orders", ord_app), ("inventory", inv_app)):
        cap = _Capture(name)
        app.state.logger.addHandler(cap)
        captures[name] = cap
    async with AsyncExitStack() as stack:
        for app in (inv_app, ord_app, gw_app):
            await stack.enter_async_context(app.router.lifespan_context(app))
        s = Stack(
            gw_http,
            ord_http,
            inv_http,
            order_store,
            inv_store,
            cache,
            {"gateway": gw_app, "orders": ord_app, "inventory": inv_app},
        )
        s.logs = captures  # type: ignore[assignment]
        yield s
        for app, cap in zip((gw_app, ord_app, inv_app), captures.values(), strict=True):
            app.state.logger.removeHandler(cap)


@pytest.fixture
async def stack() -> AsyncIterator[Stack]:
    async with build_stack() as s:
        yield s


def parse_lines(cap: Any) -> list[dict[str, Any]]:
    return [json.loads(line) for line in cap.lines]

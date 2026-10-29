from __future__ import annotations

import asyncio

import pytest
from common.metrics import Metrics
from common.pool import BoundedPool
from common.runtime import RuntimeSettings
from conftest import Stack
from orders import app as orders_app
from orders.app import OrderStore
from prometheus_client import generate_latest

AUTH = {"Authorization": "Bearer secret"}


@pytest.fixture(autouse=True)
def admin_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHOPFLOOR_ADMIN_TOKEN", "secret")


class _Conn:
    async def fetchval(self, *args: object) -> int:
        return 1


class _Raw:
    async def acquire(self) -> _Conn:
        return _Conn()

    async def release(self, conn: object) -> None:
        return None


async def _simulated_insert_seconds(quantity: int, n_plus_one: bool) -> float:
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    store = OrderStore(BoundedPool(_Raw(), lambda: 5), sleep=fake_sleep)
    await store.insert(1, quantity, n_plus_one)
    return sum(slept)


async def test_s3_five_item_cart_exceeds_one_second_only_with_flag() -> None:
    assert await _simulated_insert_seconds(5, True) > 1.0
    assert await _simulated_insert_seconds(5, False) < 0.05
    assert await _simulated_insert_seconds(1, True) < await _simulated_insert_seconds(5, True)


async def _queries_for(quantity: int, n_plus_one: bool) -> float:
    async def no_sleep(seconds: float) -> None:
        return None

    metrics = Metrics("orders", "1.0.0")
    assert 'db_queries_total{service="orders"} 0.0' in generate_latest(metrics.registry).decode()
    counter = metrics.db_queries.labels("orders")
    store = OrderStore(BoundedPool(_Raw(), lambda: 5), sleep=no_sleep, queries=counter)
    await store.insert(1, quantity, n_plus_one)
    text = generate_latest(metrics.registry).decode()
    line = next(x for x in text.splitlines() if x.startswith("db_queries_total{"))
    return float(line.rsplit(" ", 1)[1])


async def test_db_queries_total_normal_order_is_two() -> None:
    assert await _queries_for(5, False) == 2.0


async def test_db_queries_total_n_plus_one_is_cart_size_plus_one() -> None:
    assert await _queries_for(3, True) == 4.0
    assert await _queries_for(5, True) == 6.0


async def test_quantity_above_max_is_422(stack: Stack) -> None:
    ok = await stack.orders.post("/orders", json={"product_id": 1, "quantity": 20})
    assert ok.status_code in (201, 409)
    for client, path in ((stack.orders, "/orders"), (stack.gateway, "/api/orders")):
        resp = await client.post(path, json={"product_id": 1, "quantity": 21})
        assert resp.status_code == 422


async def test_non_bad_deploy_resets_manual_error_rate(stack: Stack) -> None:
    flag = {"flag": "orders.error_rate", "value": 0.5}
    await stack.orders.post("/admin/chaos", json=flag, headers=AUTH)
    await stack.orders.post("/admin/deploy", json={"version": "1.0.1"}, headers=AUTH)
    assert stack.apps["orders"].state.runtime.get("orders.error_rate") == 0.0


async def test_cancelled_request_releases_reserved_stock(stack: Stack) -> None:
    entered = asyncio.Event()

    async def hang(product_id: int, quantity: int, n_plus_one: bool = False) -> int:
        entered.set()
        await asyncio.sleep(60)
        return 1

    stack.order_store.insert = hang  # type: ignore[method-assign]
    task = asyncio.create_task(stack.orders.post("/orders", json={"product_id": 1, "quantity": 2}))
    await asyncio.wait_for(entered.wait(), 2)
    assert stack.inv_store.stock[1] == 3  # reserved
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    for _ in range(50):
        if stack.inv_store.stock[1] == 5:
            break
        await asyncio.sleep(0.01)
    assert stack.inv_store.stock[1] == 5


async def test_redis_client_has_socket_timeout_and_old_client_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[FakeRedis] = []

    class FakeRedis:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.closed = False
            created.append(self)

        async def aclose(self) -> None:
            self.closed = True

    monkeypatch.setattr(orders_app.aioredis, "Redis", FakeRedis)
    runtime = RuntimeSettings({"orders.redis_host": "redis"})
    provider = orders_app.RedisProvider(runtime)
    await provider.get()
    await provider.get()
    assert len(created) == 1
    assert created[0].kwargs["socket_timeout"] == 1
    runtime.set("orders.redis_host", "other")
    await provider.get()
    assert len(created) == 2 and created[0].closed and not created[1].closed

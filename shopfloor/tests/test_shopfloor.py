from __future__ import annotations

import re

import pytest
from common.pool import BoundedPool, PoolExhaustedError
from conftest import Stack, build_stack, parse_lines


def sample_lines(text: str, name: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith(name)]


async def test_order_happy_path(stack: Stack) -> None:
    products = await stack.gateway.get("/api/products")
    assert products.status_code == 200
    assert len(products.json()) == 2
    assert "products" in stack.cache.data  # Redis product cache populated

    resp = await stack.gateway.post("/api/orders", json={"product_id": 1, "quantity": 2})
    assert resp.status_code == 201
    body = resp.json()
    assert body["order_id"] == 1
    assert body["remaining_stock"] == 3
    assert stack.order_store.rows == [(1, 2)]
    assert "products" not in stack.cache.data  # invalidated by the order


async def test_cache_hit_skips_inventory(stack: Stack) -> None:
    await stack.gateway.get("/api/products")
    before = len(parse_lines(stack.logs["inventory"]))
    await stack.gateway.get("/api/products")
    assert len(parse_lines(stack.logs["inventory"])) == before


async def test_client_errors_pass_through(stack: Stack) -> None:
    assert (await stack.gateway.post("/api/orders", json={"product_id": 99})).status_code == 404
    assert (
        await stack.gateway.post("/api/orders", json={"product_id": 2, "quantity": 5})
    ).status_code == 409


async def test_metrics_names_and_labels(stack: Stack) -> None:
    await stack.gateway.post("/api/orders", json={"product_id": 1, "quantity": 1})
    text = (await stack.orders.get("/metrics")).text
    assert (
        'http_requests_total{method="POST",route="/orders",service="orders",status="201"} 1.0'
        in text
    )
    assert 'http_request_duration_seconds_count{route="/orders",service="orders"}' in text
    assert 'app_info{service="orders",version="1.0.0"} 1.0' in text
    assert 'db_pool_size{service="orders"} 20.0' in text
    assert 'db_pool_in_use{service="orders"} 0.0' in text
    # route is the template, never the raw path
    await stack.inventory.get("/healthz")
    inv = (await stack.inventory.get("/metrics")).text
    assert 'route="/healthz"' in inv
    assert not re.search(r'route="/api/orders/\d', inv)


async def test_every_service_exposes_metric_families(stack: Stack) -> None:
    for client in (stack.gateway, stack.orders, stack.inventory):
        text = (await client.get("/metrics")).text
        for name in (
            "http_requests_total",
            "http_request_duration_seconds",
            "db_pool_in_use",
            "db_pool_size",
            "app_info",
        ):
            assert f"# TYPE {name}" in text
        assert (await client.get("/healthz")).json() == {"status": "ok"}


async def test_log_format_and_trace_propagation(stack: Stack) -> None:
    resp = await stack.gateway.post(
        "/api/orders", json={"product_id": 1, "quantity": 1}, headers={"X-Trace-Id": "abc123"}
    )
    assert resp.headers["X-Trace-Id"] == "abc123"
    for service, route in (
        ("gateway", "/api/orders"),
        ("orders", "/orders"),
        ("inventory", "/reserve"),
    ):
        (line,) = parse_lines(stack.logs[service])
        assert line["service"] == service
        assert line["level"] == "info"
        assert line["route"] == route
        assert line["status"] in (200, 201)
        assert line["trace_id"] == "abc123"
        assert isinstance(line["latency_ms"], float)
        assert "ts" in line and "msg" in line
        assert "error" not in line  # absent fields are omitted, not null


async def test_downstream_failure_is_5xx_with_error_log() -> None:
    async with build_stack(inventory_up=False) as s:
        resp = await s.gateway.post("/api/orders", json={"product_id": 1, "quantity": 1})
        assert resp.status_code == 502
        for service in ("gateway", "orders"):
            (line,) = parse_lines(s.logs[service])
            assert line["level"] == "error"
            assert line["status"] == 502
            assert line["error"]
            if service == "orders":
                assert "inventory" in line["error"]
        assert s.order_store.rows == []


async def test_pool_size_is_runtime_mutable_and_exported(stack: Stack) -> None:
    runtime = stack.apps["orders"].state.runtime
    runtime.set("orders.db_pool_size", 1)
    text = (await stack.orders.get("/metrics")).text
    assert 'db_pool_size{service="orders"} 1.0' in text

    pool: BoundedPool = stack.order_store.pool
    pool._timeout = 0.2
    async with pool.acquire():
        assert pool.in_use == 1
        assert 'db_pool_in_use{service="orders"} 1.0' in (await stack.orders.get("/metrics")).text
        with pytest.raises(PoolExhaustedError):
            async with pool.acquire():
                pass
        runtime.set("orders.db_pool_size", 2)
        async with pool.acquire():
            assert pool.in_use == 2
    assert pool.in_use == 0


async def test_admin_requires_token(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHOPFLOOR_ADMIN_TOKEN", "secret")
    assert (await stack.orders.get("/admin/chaos")).status_code == 401
    ok = await stack.orders.get("/admin/chaos", headers={"Authorization": "Bearer secret"})
    assert ok.status_code == 200
    assert ok.json()["orders.db_pool_size"] == 20

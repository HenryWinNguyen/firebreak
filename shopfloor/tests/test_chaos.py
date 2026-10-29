from __future__ import annotations

import httpx
import pytest
from common.pool import BoundedPool
from conftest import BrokenCache, FakeRaw, Stack, build_stack, parse_lines

AUTH = {"Authorization": "Bearer secret"}
ORDER = {"product_id": 1, "quantity": 1}


@pytest.fixture(autouse=True)
def admin_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHOPFLOOR_ADMIN_TOKEN", "secret")


async def set_flag(client: httpx.AsyncClient, flag: str, value: object) -> httpx.Response:
    return await client.post("/admin/chaos", json={"flag": flag, "value": value}, headers=AUTH)


def orders_request_line(stack: Stack) -> dict[str, object]:
    return [x for x in parse_lines(stack.logs["orders"]) if x.get("route") == "/orders"][-1]


# ---- admin API ----------------------------------------------------------------------------


async def test_all_admin_routes_require_auth(stack: Stack) -> None:
    calls = [
        ("GET", "/admin/chaos", None),
        ("POST", "/admin/chaos", {"flag": "orders.n_plus_one", "value": True}),
        ("POST", "/admin/chaos/reset", None),
        ("GET", "/admin/deploys", None),
        ("POST", "/admin/deploy", {"version": "2.0.0", "message": "x"}),
    ]
    for method, path, body in calls:
        no_auth = await stack.orders.request(method, path, json=body)
        bad = await stack.orders.request(
            method, path, json=body, headers={"Authorization": "Bearer nope"}
        )
        assert no_auth.status_code == 401, path
        assert bad.status_code == 401, path
    assert stack.apps["orders"].state.runtime.get("orders.n_plus_one") is False
    assert len(stack.apps["orders"].state.deploys) == 1


async def test_chaos_set_get_reset(stack: Stack) -> None:
    resp = await set_flag(stack.orders, "orders.db_pool_size", 2)
    assert resp.status_code == 200
    assert resp.json()["orders.db_pool_size"] == 2
    got = (await stack.orders.get("/admin/chaos", headers=AUTH)).json()
    assert got == resp.json()
    assert set(got) == {
        "orders.error_rate",
        "orders.db_pool_size",
        "orders.n_plus_one",
        "orders.leak_kb_per_request",
        "orders.redis_host",
    }
    reset = await stack.orders.post("/admin/chaos/reset", headers=AUTH)
    assert reset.status_code == 200
    assert reset.json()["orders.db_pool_size"] == 20
    assert reset.json()["orders.redis_host"] == "redis"


async def test_unknown_flag_and_bad_values_are_422(stack: Stack) -> None:
    assert (await set_flag(stack.orders, "orders.nope", 1)).status_code == 422
    assert (await set_flag(stack.orders, "inventory.unavailable", True)).status_code == 422
    assert (await set_flag(stack.orders, "orders.db_pool_size", "big")).status_code == 422
    assert (await set_flag(stack.orders, "orders.db_pool_size", 0)).status_code == 422
    assert (await set_flag(stack.orders, "orders.n_plus_one", 1)).status_code == 422
    assert (await set_flag(stack.orders, "orders.error_rate", 1.5)).status_code == 422
    assert (await set_flag(stack.orders, "orders.leak_kb_per_request", -1)).status_code == 422
    assert (await set_flag(stack.gateway, "anything", 1)).status_code == 422
    missing = await stack.orders.post(
        "/admin/chaos", json={"flag": "orders.n_plus_one"}, headers=AUTH
    )
    assert missing.status_code == 422
    assert stack.apps["orders"].state.runtime.get("orders.db_pool_size") == 20


async def test_error_rate_accepts_int_and_is_stored_as_float(stack: Stack) -> None:
    body = (await set_flag(stack.orders, "orders.error_rate", 1)).json()
    assert body["orders.error_rate"] == 1.0
    assert isinstance(stack.apps["orders"].state.runtime.get("orders.error_rate"), float)


async def test_deploy_history_newest_first_and_app_info(stack: Stack) -> None:
    initial = (await stack.inventory.get("/admin/deploys", headers=AUTH)).json()
    assert [d["version"] for d in initial] == ["1.0.0"]
    resp = await stack.inventory.post(
        "/admin/deploy", json={"version": "1.1.0", "message": "tune"}, headers=AUTH
    )
    assert resp.status_code == 200
    history = (await stack.inventory.get("/admin/deploys", headers=AUTH)).json()
    assert [d["version"] for d in history] == ["1.1.0", "1.0.0"]
    assert history[0]["service"] == "inventory"
    assert history[0]["message"] == "tune"
    assert set(history[0]) == {"service", "version", "deployed_at", "message"}
    text = (await stack.inventory.get("/metrics")).text
    assert 'app_info{service="inventory",version="1.1.0"} 1.0' in text
    assert 'version="1.0.0"' not in text
    # a -bad version on a service with no bug path is only recorded
    bad = await stack.inventory.post("/admin/deploy", json={"version": "1.2.0-bad"}, headers=AUTH)
    assert bad.status_code == 200


async def test_deploy_requires_version(stack: Stack) -> None:
    resp = await stack.orders.post("/admin/deploy", json={"message": "x"}, headers=AUTH)
    assert resp.status_code == 422


# ---- S1 -----------------------------------------------------------------------------------


async def test_s1_bad_deploy_sets_error_rate_and_rollback_clears_it(stack: Stack) -> None:
    resp = await stack.orders.post(
        "/admin/deploy", json={"version": "1.1.0-bad", "message": "new promos"}, headers=AUTH
    )
    assert resp.status_code == 200
    runtime = stack.apps["orders"].state.runtime
    assert runtime.get("orders.error_rate") == 0.1
    chaos = (await stack.orders.get("/admin/chaos", headers=AUTH)).json()
    assert chaos["orders.error_rate"] == 0.1
    metrics = (await stack.orders.get("/metrics")).text
    assert 'app_info{service="orders",version="1.1.0-bad"} 1.0' in metrics
    deploys = (await stack.orders.get("/admin/deploys", headers=AUTH)).json()
    assert [d["version"] for d in deploys] == ["1.1.0-bad", "1.0.0"]

    await stack.orders.post("/admin/deploy", json={"version": "1.0.0"}, headers=AUTH)
    assert runtime.get("orders.error_rate") == 0.0


async def test_s1_errors_are_500_with_stack_trace_and_stock_released(stack: Stack) -> None:
    await set_flag(stack.orders, "orders.error_rate", 1.0)
    resp = await stack.gateway.post("/api/orders", json={"product_id": 1, "quantity": 2})
    assert resp.status_code == 500 or resp.status_code == 502
    line = orders_request_line(stack)
    assert line["status"] == 500
    assert line["level"] == "error"
    error = str(line["error"])
    assert "Traceback" in error
    assert "PricingBugError" in error
    assert "apply_promotion" in error
    assert stack.order_store.rows == []
    assert stack.inv_store.stock[1] == 5  # reservation was compensated


async def test_s1_error_rate_zero_never_fails(stack: Stack) -> None:
    for _ in range(20):
        assert (await stack.gateway.post("/api/orders", json=ORDER)).status_code == 201
        stack.inv_store.stock[1] = 5


# ---- S2 / pool ----------------------------------------------------------------------------


async def test_s2_small_pool_causes_503_when_saturated(stack: Stack) -> None:
    await set_flag(stack.orders, "orders.db_pool_size", 1)
    pool: BoundedPool = stack.order_store.pool
    pool._timeout = 0.1
    async with pool.acquire():
        resp = await stack.orders.post("/orders", json=ORDER)
        assert resp.status_code == 503  # PoolExhaustedError over HTTP
        assert "db pool exhausted" in resp.json()["detail"]
        assert 'db_pool_in_use{service="orders"} 1.0' in (await stack.orders.get("/metrics")).text
    line = orders_request_line(stack)
    assert line["status"] == 503 and line["level"] == "error"
    assert stack.inv_store.stock[1] == 5  # reserved stock released
    assert (await stack.orders.post("/orders", json=ORDER)).status_code == 201


async def test_pool_size_above_raw_max_is_clamped(stack: Stack) -> None:
    await set_flag(stack.orders, "orders.db_pool_size", 500)
    assert 'db_pool_size{service="orders"} 50.0' in (await stack.orders.get("/metrics")).text


async def test_pool_in_use_returns_to_zero_when_body_raises() -> None:
    pool = BoundedPool(FakeRaw(), lambda: 2)
    with pytest.raises(RuntimeError):
        async with pool.acquire():
            assert pool.in_use == 1
            raise RuntimeError("boom")
    assert pool.in_use == 0


# ---- S3 -----------------------------------------------------------------------------------


async def test_s3_n_plus_one_issues_a_query_per_item(stack: Stack) -> None:
    stack.inv_store.stock[1] = 1000
    await stack.orders.post("/orders", json={"product_id": 1, "quantity": 3})
    assert stack.order_store.queries == 1
    await set_flag(stack.orders, "orders.n_plus_one", True)
    stack.order_store.queries = 0
    await stack.orders.post("/orders", json={"product_id": 1, "quantity": 4})
    assert stack.order_store.queries == 4
    await stack.orders.post("/admin/chaos/reset", headers=AUTH)
    stack.order_store.queries = 0
    await stack.orders.post("/orders", json={"product_id": 1, "quantity": 4})
    assert stack.order_store.queries == 1


# ---- S4 -----------------------------------------------------------------------------------


async def test_s4_inventory_unavailable(stack: Stack) -> None:
    await set_flag(stack.inventory, "inventory.unavailable", True)
    assert (await stack.inventory.get("/products")).status_code == 503
    assert (await stack.inventory.post("/reserve", json=ORDER)).status_code == 503
    assert (await stack.inventory.get("/healthz")).status_code == 200
    assert (await stack.inventory.get("/metrics")).status_code == 200

    resp = await stack.gateway.post("/api/orders", json=ORDER)
    assert resp.status_code == 502
    line = orders_request_line(stack)
    assert line["level"] == "error"
    assert "inventory returned 503" in str(line["error"])

    await stack.inventory.post("/admin/chaos/reset", headers=AUTH)
    assert (await stack.gateway.post("/api/orders", json=ORDER)).status_code == 201


# ---- S5 -----------------------------------------------------------------------------------


async def test_s5_leak_retains_memory_per_request(stack: Stack) -> None:
    stack.inv_store.stock[1] = 1000
    leaked = stack.apps["orders"].state.leaked
    await stack.orders.post("/orders", json=ORDER)
    assert leaked == []
    await set_flag(stack.orders, "orders.leak_kb_per_request", 64)
    for _ in range(3):
        await stack.orders.post("/orders", json=ORDER)
    assert sum(len(b) for b in leaked) == 3 * 64 * 1024


async def test_chaos_reset_releases_the_s5_leak(stack: Stack) -> None:
    stack.inv_store.stock[1] = 1000
    leaked = stack.apps["orders"].state.leaked
    await set_flag(stack.orders, "orders.leak_kb_per_request", 64)
    for _ in range(3):
        await stack.orders.post("/orders", json=ORDER)
    assert len(leaked) == 3
    assert (await stack.orders.post("/admin/chaos/reset", headers=AUTH)).status_code == 200
    assert leaked == []
    assert stack.apps["orders"].state.runtime.get("orders.leak_kb_per_request") == 0


async def test_leak_release_is_admin_only(stack: Stack) -> None:
    stack.inv_store.stock[1] = 1000
    leaked = stack.apps["orders"].state.leaked
    await set_flag(stack.orders, "orders.leak_kb_per_request", 64)
    await stack.orders.post("/orders", json=ORDER)
    assert (await stack.orders.post("/admin/chaos/reset")).status_code == 401
    assert len(leaked) == 1


# ---- S6 / cache degradation ---------------------------------------------------------------


async def test_s6_redis_failure_degrades_to_db_and_logs_connection_errors() -> None:
    async with build_stack(cache=BrokenCache()) as s:
        resp = await s.gateway.get("/api/products")  # Redis failure on /products
        assert resp.status_code == 200
        assert len(resp.json()) == 2  # served from inventory
        order = await s.gateway.post("/api/orders", json=ORDER)
        assert order.status_code == 201
        assert s.order_store.rows == [(1, 1)]
        warnings = [x for x in parse_lines(s.logs["orders"]) if x["level"] == "warning"]
        ops = {x["error"].split(":")[0] for x in warnings}
        assert ops == {"redis get", "redis set", "redis delete"}
        assert all("ConnectionError" in x["error"] for x in warnings)


async def test_s6_redis_host_flag_is_a_string(stack: Stack) -> None:
    assert (await set_flag(stack.orders, "orders.redis_host", "redis-typo")).status_code == 200
    assert (await set_flag(stack.orders, "orders.redis_host", 5)).status_code == 422


# ---- T-011 follow-ups ---------------------------------------------------------------------


async def test_insert_failure_releases_reserved_stock(stack: Stack) -> None:
    stack.order_store.fail = RuntimeError("insert failed")
    resp = await stack.orders.post("/orders", json={"product_id": 1, "quantity": 2})
    assert resp.status_code == 500
    assert stack.inv_store.stock[1] == 5
    error = str(orders_request_line(stack)["error"])
    assert "Traceback" in error and "insert failed" in error


async def test_failed_release_is_logged_and_original_error_kept(stack: Stack) -> None:
    stack.order_store.fail = RuntimeError("insert failed")
    reserve = stack.inv_store.reserve

    async def reserve_then_break(product_id: int, quantity: int) -> int | None:
        result = await reserve(product_id, quantity)
        await set_flag(stack.inventory, "inventory.unavailable", True)
        return result

    stack.inv_store.reserve = reserve_then_break  # type: ignore[method-assign]
    resp = await stack.orders.post("/orders", json={"product_id": 1, "quantity": 2})
    assert resp.status_code == 500
    msgs = [x["msg"] for x in parse_lines(stack.logs["orders"]) if x["level"] == "error"]
    assert any("could not release reserved stock" in m for m in msgs)
    assert "insert failed" in str(orders_request_line(stack)["error"])


async def test_cache_delete_failure_after_insert_is_not_a_5xx() -> None:
    async with build_stack() as s:

        async def boom(key: str) -> None:
            raise ConnectionError("redis gone")

        s.cache.delete = boom  # type: ignore[method-assign]
        resp = await s.gateway.post("/api/orders", json=ORDER)
        assert resp.status_code == 201
        assert s.order_store.rows == [(1, 1)]
        warn = [x for x in parse_lines(s.logs["orders"]) if x["level"] == "warning"]
        assert warn and "redis delete" in warn[0]["error"]


async def test_trace_id_generated_when_header_missing(stack: Stack) -> None:
    resp = await stack.gateway.post("/api/orders", json=ORDER)
    trace = resp.headers["X-Trace-Id"]
    assert len(trace) == 32
    for service in ("gateway", "orders", "inventory"):
        (line,) = parse_lines(stack.logs[service])
        assert line["trace_id"] == trace


async def test_quantity_below_one_is_422(stack: Stack) -> None:
    for qty in (0, -3):
        resp = await stack.orders.post("/orders", json={"product_id": 1, "quantity": qty})
        assert resp.status_code == 422
        gw = await stack.gateway.post("/api/orders", json={"product_id": 1, "quantity": qty})
        assert gw.status_code == 422
    assert stack.inv_store.stock[1] == 5
    zero = await stack.inventory.post("/reserve", json={"product_id": 1, "quantity": 0})
    assert zero.status_code == 422


async def test_inventory_release_endpoint(stack: Stack) -> None:
    resp = await stack.inventory.post("/release", json={"product_id": 1, "quantity": 2})
    assert resp.status_code == 200
    assert resp.json()["remaining"] == 7
    missing = await stack.inventory.post("/release", json={"product_id": 99, "quantity": 1})
    assert missing.status_code == 404

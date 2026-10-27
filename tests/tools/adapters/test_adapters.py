from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from firebreak.tools.adapters.base import (
    BackendError,
    BackendTimeout,
    BackendUnavailable,
    ForbiddenRequest,
    ReadOnlyClient,
)
from firebreak.tools.adapters.loki import LokiAdapter
from firebreak.tools.adapters.prometheus import PrometheusAdapter
from firebreak.tools.adapters.shopfloor import ShopfloorAdapter

T0 = datetime(2026, 10, 27, 12, 0, tzinfo=UTC)
Handler = Callable[[httpx.Request], httpx.Response]


def transport(handler: Handler) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


async def test_prometheus_instant_parses_vector_and_sends_get() -> None:
    seen: list[httpx.Request] = []

    def h(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"metric": {"service": "orders"}, "value": [1.0, "0.5"]}],
                },
            },
        )

    a = PrometheusAdapter("http://prom", timeout_s=1, transport=transport(h))
    res = await a.instant("up", T0)
    assert res.series[0].labels == {"service": "orders"}
    assert res.series[0].points == [(1.0, "0.5")]
    assert seen[0].method == "GET"
    assert seen[0].url.params["query"] == "up"
    await a.aclose()


async def test_prometheus_status_error_body_is_typed() -> None:
    def h(req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400, json={"status": "error", "errorType": "bad_data", "error": "parse error"}
        )

    a = PrometheusAdapter("http://prom", timeout_s=1, transport=transport(h))
    with pytest.raises(BackendError):
        await a.instant("sum(", T0)


async def test_status_error_with_200_is_typed() -> None:
    def h(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "error", "error": "boom"})

    a = LokiAdapter("http://loki", timeout_s=1, transport=transport(h))
    with pytest.raises(BackendError) as ei:
        await a.query_range('{a="b"}', T0, T0, 10)
    assert "boom" in (ei.value.untrusted_detail or "")


async def test_non_json_body_is_typed() -> None:
    def h(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>")

    a = PrometheusAdapter("http://prom", timeout_s=1, transport=transport(h))
    with pytest.raises(BackendError):
        await a.metric_names()


async def test_timeout_and_connect_errors_are_typed() -> None:
    def slow(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=req)

    def down(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=req)

    with pytest.raises(BackendTimeout):
        await PrometheusAdapter("http://p", timeout_s=1, transport=transport(slow)).metric_names()
    with pytest.raises(BackendUnavailable):
        await PrometheusAdapter("http://p", timeout_s=1, transport=transport(down)).metric_names()


async def test_loki_parses_streams() -> None:
    def h(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/loki/api/v1/query_range"
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {"stream": {"service": "orders"}, "values": [["1761566400000000000", "hi"]]}
                    ],
                },
            },
        )

    res = await LokiAdapter("http://loki", timeout_s=1, transport=transport(h)).query_range(
        '{service="orders"}', T0, T0, 5
    )
    assert res.streams[0].entries == [(1761566400000000000, "hi")]


async def test_shopfloor_only_reaches_admin_deploys() -> None:
    paths: list[tuple[str, str]] = []

    def h(req: httpx.Request) -> httpx.Response:
        paths.append((req.method, req.url.path))
        assert req.headers["authorization"] == "Bearer tok"
        return httpx.Response(
            200,
            json=[
                {"service": "orders", "version": "v1", "deployed_at": "2026-10-27T10:00:00Z"},
                {
                    "service": "orders",
                    "version": "v2-bad",
                    "deployed_at": "2026-10-27T11:00:00Z",
                    "message": "m",
                },
            ],
        )

    a = ShopfloorAdapter(
        {"orders": "http://orders:8101"}, admin_token="tok", timeout_s=1, transport=transport(h)
    )
    deploys = await a.recent_deploys("orders", 1)
    assert [d.version for d in deploys] == ["v2-bad"]
    assert paths == [("GET", "/admin/deploys")]


async def test_shopfloor_malformed_record_is_typed() -> None:
    def h(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"nope": 1}])

    a = ShopfloorAdapter(
        {"orders": "http://o"}, admin_token="t", timeout_s=1, transport=transport(h)
    )
    with pytest.raises(BackendError):
        await a.recent_deploys("orders", 5)


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/admin/chaos"),
        ("GET", "/admin/chaos/reset"),
        ("POST", "/admin/chaos"),
        ("POST", "/admin/deploy"),
        ("POST", "/admin/deploys"),
        ("DELETE", "/admin/deploys"),
        ("GET", "/admin/deploys/../chaos"),
    ],
)
async def test_shopfloor_client_cannot_reach_anything_else(method: str, path: str) -> None:
    reached: list[str] = []

    def h(req: httpx.Request) -> httpx.Response:
        reached.append(f"{req.method} {req.url.path}")
        return httpx.Response(200, json=[])

    a = ShopfloorAdapter(
        {"orders": "http://o"}, admin_token="t", timeout_s=1, transport=transport(h)
    )
    client = a._http._client  # the raw httpx client is the worst case an adapter bug could use
    with pytest.raises(ForbiddenRequest):
        await client.request(method, f"http://o{path}")
    assert reached == []


async def test_guard_refuses_non_get_on_every_backend() -> None:
    def h(req: httpx.Request) -> httpx.Response:
        raise AssertionError("must not be reached")

    for adapter in (
        PrometheusAdapter("http://p", timeout_s=1, transport=transport(h)),
        LokiAdapter("http://l", timeout_s=1, transport=transport(h)),
    ):
        with pytest.raises(ForbiddenRequest):
            await adapter._http._client.post("http://x/api/v1/query")  # type: ignore[attr-defined]


async def test_redirects_are_not_followed() -> None:
    hits: list[str] = []

    def h(req: httpx.Request) -> httpx.Response:
        hits.append(req.url.path)
        return httpx.Response(302, headers={"location": "/admin/chaos"})

    c = ReadOnlyClient(
        backend="x", allowed_paths=frozenset({"/a"}), timeout_s=1, transport=transport(h)
    )
    with pytest.raises(BackendError):
        await c.get_json("http://h/a")
    assert hits == ["/a"]

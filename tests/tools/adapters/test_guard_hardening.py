"""T-035: guard origin/path pinning, response size cap, strict JSON."""

from collections.abc import AsyncIterator

import httpx
import pytest

from firebreak.tools.adapters.base import MAX_RESPONSE_BYTES, BackendError, ForbiddenRequest
from firebreak.tools.adapters.prometheus import PrometheusAdapter
from firebreak.tools.adapters.shopfloor import ShopfloorAdapter
from tests.tools.adapters.test_adapters import T0, transport


def _shop(h: httpx.MockTransport | None = None, token: str = "t") -> ShopfloorAdapter:
    def ok(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    return ShopfloorAdapter(
        {"orders": "http://o"}, admin_token=token, timeout_s=1, transport=h or transport(ok)
    )


async def test_guard_refuses_encoded_slash_path() -> None:
    reached: list[str] = []

    def h(req: httpx.Request) -> httpx.Response:
        reached.append(str(req.url))
        return httpx.Response(200, json=[])

    a = _shop(transport(h))
    with pytest.raises(ForbiddenRequest):
        await a._http._client.get("http://o/admin%2Fdeploys")
    assert reached == []


async def test_guard_ignores_query_when_matching_path() -> None:
    resp = await _shop()._http._client.get("http://o/admin/deploys?x=/admin/chaos")
    assert resp.status_code == 200


@pytest.mark.parametrize(
    "url",
    [
        "http://evil/admin/deploys",
        "http://o:9999/admin/deploys",
        "https://o/admin/deploys",
        "http://o.evil.com/admin/deploys",
    ],
)
async def test_guard_pins_origin_and_never_sends_token_elsewhere(url: str) -> None:
    reached: list[str] = []

    def h(req: httpx.Request) -> httpx.Response:
        reached.append(str(req.url))
        return httpx.Response(200, json=[])

    a = _shop(transport(h), token="secret")
    with pytest.raises(ForbiddenRequest):
        await a._http._client.get(url)
    assert reached == []


async def test_response_too_large_by_content_length() -> None:
    def h(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-length": str(MAX_RESPONSE_BYTES + 1)})

    with pytest.raises(BackendError, match="response too large; narrow the query"):
        await PrometheusAdapter("http://p", timeout_s=1, transport=transport(h)).metric_names()


class _BigStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        for _ in range(5):
            yield b"x" * (1024 * 1024)


async def test_response_too_large_when_streamed_without_length() -> None:
    def h(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_BigStream())

    with pytest.raises(BackendError, match="response too large; narrow the query"):
        await PrometheusAdapter("http://p", timeout_s=1, transport=transport(h)).metric_names()


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
async def test_bare_nonfinite_json_literals_are_rejected(literal: str) -> None:
    def h(req: httpx.Request) -> httpx.Response:
        body = f'{{"status":"success","data":{{"resultType":"scalar","result":[1,{literal}]}}}}'
        return httpx.Response(200, content=body.encode())

    with pytest.raises(BackendError):
        await PrometheusAdapter("http://p", timeout_s=1, transport=transport(h)).instant("x", T0)


async def test_float_overflow_literal_is_rejected() -> None:
    def h(req: httpx.Request) -> httpx.Response:
        body = b'{"status":"success","data":{"resultType":"scalar","result":[1,1e999]}}'
        return httpx.Response(200, content=body)

    with pytest.raises(BackendError):
        await PrometheusAdapter("http://p", timeout_s=1, transport=transport(h)).instant("x", T0)


async def test_highly_compressible_body_hits_the_cap_in_bounded_pieces() -> None:
    import gzip

    payload = gzip.compress(b"0" * (MAX_RESPONSE_BYTES * 8))  # tiny on the wire, 32 MB decoded
    assert len(payload) < 1024 * 1024

    def h(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-encoding": "gzip"}, content=payload)

    with pytest.raises(BackendError, match="response too large; narrow the query"):
        await PrometheusAdapter("http://p", timeout_s=1, transport=transport(h)).metric_names()

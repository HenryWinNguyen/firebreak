"""M1 acceptance: turning on scenarios S1-S4 makes the expected alert fire in Alertmanager.

Needs the full compose stack (`docker compose up -d --build`). Skipped unless
FIREBREAK_INTEGRATION=1. Each scenario: reset, wait for a clean slate, apply the
fault, generate traffic, poll Alertmanager until the expected alert fires, reset.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("FIREBREAK_INTEGRATION") != "1",
        reason="set FIREBREAK_INTEGRATION=1 and start the compose stack",
    ),
]

GATEWAY = "http://localhost:8100"
ORDERS = "http://localhost:8101"
INVENTORY = "http://localhost:8102"
ALERTMANAGER = "http://localhost:9093"
ADMIN_HEADERS = {
    "Authorization": f"Bearer {os.environ.get('SHOPFLOOR_ADMIN_TOKEN', 'dev-admin-token')}"
}

FIRE_TIMEOUT_S = 300
CLEAN_TIMEOUT_S = 600  # alert rules look back 5m, so residue from the previous scenario lingers
POLL_S = 5


@dataclass(frozen=True)
class Scenario:
    sid: str
    name: str
    # Any one of these (alertname, service) pairs firing counts as success (CONTRACTS 3.1 mapping).
    expected: tuple[tuple[str, str], ...]
    apply: Callable[[httpx.AsyncClient], Awaitable[None]]
    concurrency: int
    quantity: int


@dataclass
class Result:
    sid: str
    expected: str
    fired: str = "NO"
    seconds: float | None = None
    notes: list[str] = field(default_factory=list)


RESULTS: list[Result] = []


async def _post(client: httpx.AsyncClient, url: str, body: dict[str, Any]) -> None:
    resp = await client.post(url, json=body, headers=ADMIN_HEADERS)
    resp.raise_for_status()


async def _chaos(client: httpx.AsyncClient, base: str, flag: str, value: Any) -> None:
    await _post(client, f"{base}/admin/chaos", {"flag": flag, "value": value})


async def apply_s1(client: httpx.AsyncClient) -> None:
    await _post(client, f"{ORDERS}/admin/deploy", {"version": "v2.0.0-bad", "message": "S1"})


async def apply_s2(client: httpx.AsyncClient) -> None:
    await _chaos(client, ORDERS, "orders.db_pool_size", 2)


async def apply_s3(client: httpx.AsyncClient) -> None:
    await _chaos(client, ORDERS, "orders.n_plus_one", True)


async def apply_s4(client: httpx.AsyncClient) -> None:
    await _chaos(client, INVENTORY, "inventory.unavailable", True)


SCENARIOS = [
    Scenario("S1", "bad deploy", (("HighErrorRate", "orders"),), apply_s1, 5, 1),
    Scenario(
        "S2",
        "pool exhaustion",
        (("DBPoolSaturated", "orders"), ("HighLatencyP95", "orders")),
        apply_s2,
        30,
        1,
    ),
    Scenario("S3", "N+1 query", (("HighLatencyP95", "orders"),), apply_s3, 10, 5),
    Scenario(
        "S4",
        "dependency down",
        (("HighErrorRate", "orders"), ("ServiceDown", "inventory")),
        apply_s4,
        5,
        1,
    ),
]


async def reset_all(client: httpx.AsyncClient) -> None:
    for base in (ORDERS, INVENTORY):
        resp = await client.post(f"{base}/admin/chaos/reset", headers=ADMIN_HEADERS)
        resp.raise_for_status()
    # S1 leaves a "-bad" version recorded; a clean deploy mirrors the real rollback.
    await _post(client, f"{ORDERS}/admin/deploy", {"version": "v1.0.0", "message": "reset"})


async def firing(client: httpx.AsyncClient) -> set[tuple[str, str]]:
    resp = await client.get(f"{ALERTMANAGER}/api/v2/alerts", params={"active": "true"})
    resp.raise_for_status()
    return {
        (a["labels"].get("alertname", ""), a["labels"].get("service", ""))
        for a in resp.json()
        if a.get("status", {}).get("state") == "active"
    }


class LoadGen:
    """Async load loop against the gateway. Counts outcomes; 5xx are expected during faults."""

    def __init__(self, concurrency: int, quantity: int, pause_s: float) -> None:
        self.concurrency = concurrency
        self.quantity = quantity
        self.pause_s = pause_s
        self.ok = 0
        self.http_errors = 0
        self.transport_errors = 0
        self._tasks: list[asyncio.Task[None]] = []
        self._client = httpx.AsyncClient(base_url=GATEWAY, timeout=15.0)
        self._product_ids: list[int] = [1]

    async def _worker(self, n: int) -> None:
        i = n
        while True:
            try:
                if i % 5 == 0:
                    resp = await self._client.get("/api/products")
                else:
                    pid = self._product_ids[i % len(self._product_ids)]
                    resp = await self._client.post(
                        "/api/orders", json={"product_id": pid, "quantity": self.quantity}
                    )
                if resp.status_code < 400:
                    self.ok += 1
                else:
                    self.http_errors += 1
            except httpx.TransportError:
                self.transport_errors += 1
            i += 1
            await asyncio.sleep(self.pause_s)

    async def start(self) -> None:
        resp = await self._client.get("/api/products")
        resp.raise_for_status()
        self._product_ids = [int(p["id"]) for p in resp.json()] or [1]
        self._tasks = [asyncio.create_task(self._worker(n)) for n in range(self.concurrency)]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._client.aclose()

    def summary(self) -> str:
        return (
            f"ok={self.ok} http_errors={self.http_errors} transport_errors={self.transport_errors}"
        )


@contextlib.asynccontextmanager
async def load(concurrency: int, quantity: int, pause_s: float) -> AsyncIterator[LoadGen]:
    gen = LoadGen(concurrency, quantity, pause_s)
    await gen.start()
    try:
        yield gen
    finally:
        await gen.stop()


async def wait_until_clean(
    client: httpx.AsyncClient, expected: tuple[tuple[str, str], ...]
) -> None:
    """Keep light healthy traffic flowing (so 5m rate windows refill) until none of the
    expected alerts is active, so a leftover from the previous scenario can't pass this one."""
    deadline = time.monotonic() + CLEAN_TIMEOUT_S
    async with load(concurrency=3, quantity=1, pause_s=0.2):
        while True:
            still = (await firing(client)) & set(expected)
            if not still:
                return
            if time.monotonic() > deadline:
                pytest.fail(f"alerts still firing {CLEAN_TIMEOUT_S}s after reset: {still}")
            await asyncio.sleep(POLL_S)


async def run_scenario(sc: Scenario) -> Result:
    result = Result(sc.sid, " | ".join(f"{a}/{s}" for a, s in sc.expected))
    RESULTS.append(result)
    async with httpx.AsyncClient(timeout=15.0) as client:
        await reset_all(client)
        await wait_until_clean(client, sc.expected)
        await sc.apply(client)
        start = time.monotonic()
        try:
            async with load(sc.concurrency, sc.quantity, pause_s=0.1) as gen:
                while time.monotonic() - start < FIRE_TIMEOUT_S:
                    hit = (await firing(client)) & set(sc.expected)
                    if hit:
                        result.seconds = time.monotonic() - start
                        result.fired = "yes: " + ", ".join(f"{a}/{s}" for a, s in sorted(hit))
                        break
                    await asyncio.sleep(POLL_S)
                result.notes.append(gen.summary())
        finally:
            await reset_all(client)
    return result


@pytest.fixture(scope="module", autouse=True)
def print_table() -> Iterator[None]:
    yield
    print("\n\nM1 acceptance results")
    print(f"{'scenario':<9}{'expected alert':<52}{'fired?':<40}{'time to fire'}")
    for r in RESULTS:
        t = f"{r.seconds:.0f}s" if r.seconds is not None else "-"
        print(f"{r.sid:<9}{r.expected:<52}{r.fired:<40}{t}   ({'; '.join(r.notes)})")


@pytest.fixture(scope="module")
def stack_ready() -> None:
    try:
        for base in (GATEWAY, ORDERS, INVENTORY):
            httpx.get(f"{base}/healthz", timeout=5.0).raise_for_status()
        httpx.get(f"{ALERTMANAGER}/api/v2/status", timeout=5.0).raise_for_status()
    except httpx.HTTPError as exc:
        pytest.fail(f"compose stack is not up: {exc!r}")


@pytest.mark.parametrize("sc", SCENARIOS, ids=[s.sid for s in SCENARIOS])
def test_scenario_fires_expected_alert(sc: Scenario, stack_ready: None) -> None:
    result = asyncio.run(run_scenario(sc))
    assert result.seconds is not None, (
        f"{sc.sid} ({sc.name}): none of {sc.expected} fired within {FIRE_TIMEOUT_S}s"
    )

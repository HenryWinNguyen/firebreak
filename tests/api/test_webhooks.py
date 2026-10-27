from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from firebreak.api.app import app
from firebreak.api.deps import get_alert_repo, get_grouper
from firebreak.db.repositories import AlertRepository
from firebreak.models.alert import Alert
from firebreak.models.incident import GroupingDecision

AUTH = {"Authorization": "Bearer dev-webhook-token"}
STARTS = "2026-01-01T00:00:00Z"


class FakeGrouper:
    def __init__(self) -> None:
        self.seen: list[Alert] = []
        self.linked: set[str] = set()

    async def assign(self, alert: Alert) -> GroupingDecision:
        self.seen.append(alert)
        dup = alert.idempotency_key in self.linked
        self.linked.add(alert.idempotency_key)
        return GroupingDecision(incident_id="inc_fake", created=not dup, duplicate=dup)


class MemoryRepo:
    def __init__(self) -> None:
        self.keys: set[str] = set()

    async def insert_if_new(self, alert: Alert) -> bool:
        new = alert.idempotency_key not in self.keys
        self.keys.add(alert.idempotency_key)
        return new


def am_alert(
    fp: str = "fp1", status: str = "firing", labels: dict[str, str] | None = None
) -> dict[str, Any]:
    return {
        "status": status,
        "labels": labels
        if labels is not None
        else {"alertname": "HighErrorRate", "service": "orders", "severity": "critical"},
        "annotations": {"summary": "5xx high"},
        "startsAt": STARTS,
        "fingerprint": fp,
    }


def am_hook(*alerts: dict[str, Any]) -> dict[str, Any]:
    return {"version": "4", "status": "firing", "receiver": "firebreak", "alerts": list(alerts)}


@pytest.fixture
def grouper() -> FakeGrouper:
    g = FakeGrouper()
    app.dependency_overrides[get_grouper] = lambda: g
    yield g  # type: ignore[misc]
    app.dependency_overrides.clear()


@pytest.fixture
def repo() -> MemoryRepo:
    return MemoryRepo()


@pytest.fixture
async def client(grouper: FakeGrouper, repo: MemoryRepo) -> AsyncIterator[httpx.AsyncClient]:
    app.dependency_overrides[get_alert_repo] = lambda: repo
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c


async def test_missing_token_401(client: httpx.AsyncClient) -> None:
    r = await client.post("/webhooks/alertmanager", json=am_hook(am_alert()))
    assert r.status_code == 401


async def test_bad_token_401(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/webhooks/alertmanager",
        json=am_hook(am_alert()),
        headers={"Authorization": "Bearer nope"},
    )
    assert r.status_code == 401


async def test_generic_requires_auth(client: httpx.AsyncClient) -> None:
    r = await client.post("/webhooks/generic", json={})
    assert r.status_code == 401


async def test_alertmanager_ok_and_replay_is_noop(
    client: httpx.AsyncClient, grouper: FakeGrouper, repo: MemoryRepo
) -> None:
    body = am_hook(am_alert("a"), am_alert("b"))
    r1 = await client.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert r1.status_code == 200
    assert [x["decision"]["duplicate"] for x in r1.json()["results"]] == [False, False]
    assert len(grouper.seen) == 2

    r2 = await client.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert [x["decision"]["duplicate"] for x in r2.json()["results"]] == [True, True]
    assert len(repo.keys) == 2


async def test_missing_service_422_and_nothing_stored(
    client: httpx.AsyncClient, grouper: FakeGrouper, repo: MemoryRepo
) -> None:
    body = am_hook(am_alert("a"), am_alert("b", labels={"alertname": "X"}))
    r = await client.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert r.status_code == 422
    assert "service" in str(r.json()["detail"])
    assert grouper.seen == []
    assert repo.keys == set()


async def test_resolved_not_grouped(client: httpx.AsyncClient, grouper: FakeGrouper) -> None:
    r = await client.post(
        "/webhooks/alertmanager", json=am_hook(am_alert("a", status="resolved")), headers=AUTH
    )
    assert r.status_code == 200
    assert r.json()["results"][0]["decision"]["incident_id"] is None
    assert grouper.seen == []


async def test_generic_webhook(client: httpx.AsyncClient, grouper: FakeGrouper) -> None:
    body = {"alertname": "Custom", "service": "orders", "started_at": STARTS}
    r = await client.post("/webhooks/generic", json=body, headers=AUTH)
    assert r.status_code == 200
    assert r.json()["results"][0]["decision"]["incident_id"] == "inc_fake"


@pytest.mark.parametrize(
    "header",
    [
        "bearer dev-webhook-token",
        "dev-webhook-token",
        "Bearer ",
        "Bearer",
        "Basic dev-webhook-token",
    ],
)
async def test_auth_variants(client: httpx.AsyncClient, header: str) -> None:
    r = await client.post(
        "/webhooks/alertmanager", json=am_hook(am_alert()), headers={"Authorization": header}
    )
    # Lowercase scheme is valid per RFC 7235; everything else is rejected.
    assert r.status_code == (
        200 if header.startswith("bearer ") and header.endswith("token") else 401
    )


async def test_generic_invalid_body_422(client: httpx.AsyncClient) -> None:
    r = await client.post("/webhooks/generic", json={"alertname": "A"}, headers=AUTH)
    assert r.status_code == 422


async def test_generic_naive_timestamp_422(client: httpx.AsyncClient) -> None:
    body = {"alertname": "A", "service": "s", "started_at": "2026-01-01T00:00:00"}
    r = await client.post("/webhooks/generic", json=body, headers=AUTH)
    assert r.status_code == 422


async def test_generic_explicit_fingerprint_and_replay(client: httpx.AsyncClient) -> None:
    body = {"alertname": "A", "service": "s", "fingerprint": "mine", "started_at": STARTS}
    r1 = await client.post("/webhooks/generic", json=body, headers=AUTH)
    r2 = await client.post("/webhooks/generic", json=body, headers=AUTH)
    assert r1.json()["results"][0]["fingerprint"] == "mine"
    assert r2.json()["results"][0]["decision"]["duplicate"] is True


async def test_default_grouper_is_503(repo: MemoryRepo) -> None:
    app.dependency_overrides[get_alert_repo] = lambda: repo
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            r = await c.post("/webhooks/alertmanager", json=am_hook(am_alert()), headers=AUTH)
    finally:
        app.dependency_overrides.clear()
    assert r.status_code == 503


@pytest.mark.db
async def test_replay_against_postgres(alert_repo: AlertRepository, grouper: FakeGrouper) -> None:
    app.dependency_overrides[get_alert_repo] = lambda: alert_repo
    body = am_hook(am_alert("pg1"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        r1 = await c.post("/webhooks/alertmanager", json=body, headers=AUTH)
        r2 = await c.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert r1.json()["results"][0]["decision"]["duplicate"] is False
    assert r2.json()["results"][0]["decision"]["duplicate"] is True
    assert len(grouper.linked) == 1

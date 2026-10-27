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

    async def assign(self, alert: Alert) -> GroupingDecision:
        self.seen.append(alert)
        return GroupingDecision(incident_id="inc_fake", created=True, duplicate=False)


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
async def client(grouper: FakeGrouper) -> AsyncIterator[httpx.AsyncClient]:
    repo = MemoryRepo()
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
    client: httpx.AsyncClient, grouper: FakeGrouper
) -> None:
    body = am_hook(am_alert("a"), am_alert("b"))
    r1 = await client.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert r1.status_code == 200
    assert [x["decision"]["duplicate"] for x in r1.json()["results"]] == [False, False]
    assert len(grouper.seen) == 2

    r2 = await client.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert [x["decision"]["duplicate"] for x in r2.json()["results"]] == [True, True]
    assert len(grouper.seen) == 2


async def test_missing_service_422_and_nothing_stored(
    client: httpx.AsyncClient, grouper: FakeGrouper
) -> None:
    body = am_hook(am_alert("a"), am_alert("b", labels={"alertname": "X"}))
    r = await client.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert r.status_code == 422
    assert "service" in str(r.json()["detail"])
    assert grouper.seen == []


async def test_resolved_not_grouped(client: httpx.AsyncClient, grouper: FakeGrouper) -> None:
    r = await client.post(
        "/webhooks/alertmanager", json=am_hook(am_alert("a", status="resolved")), headers=AUTH
    )
    assert r.status_code == 200
    assert grouper.seen == []


async def test_generic_webhook(client: httpx.AsyncClient, grouper: FakeGrouper) -> None:
    body = {"alertname": "Custom", "service": "orders", "started_at": STARTS}
    r = await client.post("/webhooks/generic", json=body, headers=AUTH)
    assert r.status_code == 200
    assert r.json()["results"][0]["decision"]["incident_id"] == "inc_fake"


@pytest.mark.db
async def test_replay_against_postgres(alert_repo: AlertRepository, grouper: FakeGrouper) -> None:
    app.dependency_overrides[get_alert_repo] = lambda: alert_repo
    body = am_hook(am_alert("pg1"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        r1 = await c.post("/webhooks/alertmanager", json=body, headers=AUTH)
        r2 = await c.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert r1.json()["results"][0]["decision"]["duplicate"] is False
    assert r2.json()["results"][0]["decision"]["duplicate"] is True
    assert len(grouper.seen) == 1

import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from firebreak.api.app import app
from firebreak.api.deps import IngestAlertRepository, get_alert_repo, get_engine, get_grouper
from firebreak.ingest.grouping import PostgresIncidentGrouper
from firebreak.ingest.service_graph import ServiceGraph
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
        self.ids: dict[str, str] = {}
        self.resolved: list[str] = []

    @property
    def keys(self) -> set[str]:
        return set(self.ids)

    async def insert_or_get(self, alert: Alert) -> tuple[str, bool]:
        if alert.idempotency_key in self.ids:
            return self.ids[alert.idempotency_key], False
        self.ids[alert.idempotency_key] = alert.id
        return alert.id, True

    async def mark_resolved(self, idempotency_key: str, ended_at: datetime) -> bool:
        self.resolved.append(idempotency_key)
        return idempotency_key in self.ids


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


async def test_duplicate_response_carries_stored_alert_id(client: httpx.AsyncClient) -> None:
    body = am_hook(am_alert("a"))
    r1 = await client.post("/webhooks/alertmanager", json=body, headers=AUTH)
    r2 = await client.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert r1.json()["results"][0]["alert_id"] == r2.json()["results"][0]["alert_id"]


async def test_empty_fingerprint_422(client: httpx.AsyncClient) -> None:
    r = await client.post("/webhooks/alertmanager", json=am_hook(am_alert("")), headers=AUTH)
    assert r.status_code == 422
    assert "fingerprint" in str(r.json()["detail"])


async def test_naive_alertmanager_timestamp_422(client: httpx.AsyncClient) -> None:
    alert = am_alert("a")
    alert["startsAt"] = "2026-01-01T00:00:00"
    r = await client.post("/webhooks/alertmanager", json=am_hook(alert), headers=AUTH)
    assert r.status_code == 422
    assert "timezone" in str(r.json()["detail"])


async def test_generic_oversized_fields_422(client: httpx.AsyncClient) -> None:
    base = {"alertname": "A", "service": "s", "started_at": STARTS}
    for patch in ({"alertname": "x" * 257}, {"fingerprint": "x" * 257}, {"summary": "y" * 2001}):
        r = await client.post("/webhooks/generic", json={**base, **patch}, headers=AUTH)
        assert r.status_code == 422, patch


async def test_oversized_annotations_422(client: httpx.AsyncClient) -> None:
    alert = am_alert("a")
    alert["annotations"] = {"summary": "y" * 2001}
    r = await client.post("/webhooks/alertmanager", json=am_hook(alert), headers=AUTH)
    assert r.status_code == 422
    assert "annotations" in str(r.json()["detail"])


async def test_resolved_notification_marks_stored_alert_resolved(
    client: httpx.AsyncClient, repo: MemoryRepo
) -> None:
    await client.post("/webhooks/alertmanager", json=am_hook(am_alert("a")), headers=AUTH)
    r = await client.post(
        "/webhooks/alertmanager", json=am_hook(am_alert("a", status="resolved")), headers=AUTH
    )
    d = r.json()["results"][0]["decision"]
    assert d == {"incident_id": None, "created": False, "duplicate": False}
    assert len(repo.resolved) == 1


class BrokenRepo(MemoryRepo):
    def __init__(self, exc: Exception) -> None:
        super().__init__()
        self.exc = exc

    async def insert_or_get(self, alert: Alert) -> tuple[str, bool]:
        raise self.exc


@pytest.mark.parametrize(
    "exc",
    [
        OSError("connection refused"),
        OperationalError("SELECT 1", {}, Exception("server closed")),
        InterfaceError("SELECT 1", {}, Exception("connection is closed")),
    ],
)
async def test_unreachable_database_is_503(
    grouper: FakeGrouper, exc: Exception, caplog: pytest.LogCaptureFixture
) -> None:
    app.dependency_overrides[get_alert_repo] = lambda: BrokenRepo(exc)
    with caplog.at_level(logging.ERROR, logger="firebreak.api.webhooks"):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            r = await c.post("/webhooks/alertmanager", json=am_hook(am_alert()), headers=AUTH)
    assert r.status_code == 503
    assert "database unavailable" in r.json()["detail"]
    assert "database unreachable during ingest of 1 alert" in caplog.text


async def test_other_exceptions_still_propagate(grouper: FakeGrouper) -> None:
    app.dependency_overrides[get_alert_repo] = lambda: BrokenRepo(RuntimeError("bug"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        with pytest.raises(RuntimeError, match="bug"):
            await c.post("/webhooks/alertmanager", json=am_hook(am_alert()), headers=AUTH)


async def test_lifespan_disposes_cached_engines(monkeypatch: pytest.MonkeyPatch) -> None:
    get_engine.cache_clear()
    engine = get_engine("postgresql+asyncpg://u:p@localhost:1/x_test")
    disposed: list[AsyncEngine] = []

    async def fake_dispose(self: AsyncEngine, close: bool = True) -> None:
        disposed.append(self)

    monkeypatch.setattr(AsyncEngine, "dispose", fake_dispose)
    async with app.router.lifespan_context(app):
        assert disposed == []
    assert disposed == [engine]
    assert get_engine.cache_info().currsize == 0


async def _stored(sessions: async_sessionmaker[AsyncSession]) -> list[tuple[str, str, bool]]:
    async with sessions() as s:
        rows = await s.execute(
            text("SELECT id, status, ended_at IS NOT NULL FROM alerts ORDER BY id")
        )
        return [(r[0], r[1], r[2]) for r in rows]


@pytest.mark.db
async def test_replay_against_postgres(
    alert_repo: IngestAlertRepository, grouper: FakeGrouper
) -> None:
    app.dependency_overrides[get_alert_repo] = lambda: alert_repo
    body = am_hook(am_alert("pg1"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        r1 = await c.post("/webhooks/alertmanager", json=body, headers=AUTH)
        r2 = await c.post("/webhooks/alertmanager", json=body, headers=AUTH)
    assert r1.json()["results"][0]["decision"]["duplicate"] is False
    assert r2.json()["results"][0]["decision"]["duplicate"] is True
    assert r1.json()["results"][0]["alert_id"] == r2.json()["results"][0]["alert_id"]
    assert len(grouper.linked) == 1


@pytest.mark.db
async def test_resolved_updates_stored_alert_in_postgres(
    alert_repo: IngestAlertRepository,
    sessions: async_sessionmaker[AsyncSession],
    grouper: FakeGrouper,
) -> None:
    app.dependency_overrides[get_alert_repo] = lambda: alert_repo
    resolved = am_alert("pg2", status="resolved")
    resolved["endsAt"] = "2026-01-01T00:05:00Z"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        r1 = await c.post("/webhooks/alertmanager", json=am_hook(am_alert("pg2")), headers=AUTH)
        assert (await _stored(sessions))[0][1:] == ("firing", False)
        r2 = await c.post("/webhooks/alertmanager", json=am_hook(resolved), headers=AUTH)
        r3 = await c.post("/webhooks/alertmanager", json=am_hook(resolved), headers=AUTH)
    assert r2.status_code == r3.status_code == 200
    res = r2.json()["results"][0]
    assert res["alert_id"] == r1.json()["results"][0]["alert_id"]
    assert res["decision"] == {"incident_id": None, "created": False, "duplicate": False}
    assert await _stored(sessions) == [(res["alert_id"], "resolved", True)]
    assert len(grouper.seen) == 1


@pytest.mark.db
async def test_replay_through_real_grouper(
    alert_repo: IngestAlertRepository, sessions: async_sessionmaker[AsyncSession]
) -> None:
    grouper = PostgresIncidentGrouper(sessions, ServiceGraph.load())
    app.dependency_overrides[get_alert_repo] = lambda: alert_repo
    app.dependency_overrides[get_grouper] = lambda: grouper
    warn = {"alertname": "HighErrorRate", "service": "orders", "severity": "warning"}
    body = am_hook(am_alert("g1", labels=warn), am_alert("g2", labels=warn))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        r1 = await c.post("/webhooks/alertmanager", json=body, headers=AUTH)
        r2 = await c.post("/webhooks/alertmanager", json=body, headers=AUTH)
    d1 = [x["decision"] for x in r1.json()["results"]]
    d2 = [x["decision"] for x in r2.json()["results"]]
    assert [d["duplicate"] for d in d1] == [False, False]
    assert [d["duplicate"] for d in d2] == [True, True]
    assert {d["incident_id"] for d in d1 + d2} == {d1[0]["incident_id"]}
    assert [x["alert_id"] for x in r1.json()["results"]] == [
        x["alert_id"] for x in r2.json()["results"]
    ]
    async with sessions() as s:
        assert (await s.execute(text("SELECT count(*) FROM incident_alerts"))).scalar_one() == 2

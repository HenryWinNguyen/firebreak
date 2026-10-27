import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from firebreak.db.repositories import (
    AlertNotFoundError,
    AlertRepository,
    IncidentNotFoundError,
    IncidentRepository,
    PostgresEvidenceSink,
)
from firebreak.models.alert import Alert, AlertSource
from firebreak.models.common import Severity, utcnow
from firebreak.models.evidence import Evidence, EvidenceKind, EvidenceSink
from firebreak.models.incident import Incident, IncidentState

pytestmark = pytest.mark.db

Sessions = async_sessionmaker[AsyncSession]


def make_alert(fingerprint: str = "fp1", service: str = "orders", **kw: object) -> Alert:
    base: dict[str, object] = {
        "source": AlertSource.ALERTMANAGER,
        "fingerprint": fingerprint,
        "alertname": "HighErrorRate",
        "service": service,
        "severity": Severity.CRITICAL,
        "summary": "boom",
        "labels": {"service": service},
        "started_at": utcnow().replace(microsecond=0),
    }
    base.update(kw)
    return Alert(**base)  # type: ignore[arg-type]


def make_incident(**kw: object) -> Incident:
    now = utcnow()
    base: dict[str, object] = {
        "severity": Severity.CRITICAL,
        "cluster": "checkout",
        "services": ["orders"],
        "alert_ids": [],
        "opened_at": now,
        "window_closes_at": now + timedelta(seconds=60),
    }
    base.update(kw)
    return Incident(**base)  # type: ignore[arg-type]


async def count(engine: AsyncEngine, table: str) -> int:
    async with engine.connect() as conn:
        return (await conn.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one()


async def test_insert_if_new_then_duplicate(sessions: Sessions, engine: AsyncEngine) -> None:
    repo = AlertRepository(sessions)
    a = make_alert()
    assert await repo.insert_if_new(a) is True
    # Same idempotency key, different generated id: still a duplicate.
    dup = make_alert(started_at=a.started_at)
    assert dup.id != a.id
    assert await repo.insert_if_new(dup) is False
    assert await count(engine, "alerts") == 1


async def test_different_started_at_is_new(sessions: Sessions) -> None:
    repo = AlertRepository(sessions)
    a = make_alert()
    assert await repo.insert_if_new(a)
    assert await repo.insert_if_new(make_alert(started_at=a.started_at + timedelta(seconds=1)))


async def test_concurrent_duplicate_insert(sessions: Sessions, engine: AsyncEngine) -> None:
    repo = AlertRepository(sessions)
    started = utcnow()
    alerts = [make_alert(started_at=started) for _ in range(10)]
    results = await asyncio.gather(*(repo.insert_if_new(a) for a in alerts))
    assert sum(results) == 1
    assert await count(engine, "alerts") == 1


async def test_alert_roundtrip_fields(sessions: Sessions, engine: AsyncEngine) -> None:
    a = make_alert(ended_at=utcnow())
    await AlertRepository(sessions).insert_if_new(a)
    async with engine.connect() as conn:
        row = (await conn.execute(text("SELECT * FROM alerts"))).mappings().one()
    assert row["id"] == a.id
    assert row["idempotency_key"] == a.idempotency_key
    assert row["labels"] == a.labels
    assert row["started_at"] == a.started_at


async def test_incident_create_get_attach(sessions: Sessions) -> None:
    alerts = AlertRepository(sessions)
    incidents = IncidentRepository(sessions)
    a1, a2 = make_alert("f1", "orders"), make_alert("f2", "inventory")
    await alerts.insert_if_new(a1)
    await alerts.insert_if_new(a2)

    inc = await incidents.create(make_incident(alert_ids=[a1.id]))
    got = await incidents.get(inc.id)
    assert got is not None
    assert got.alert_ids == [a1.id]
    assert got.state == IncidentState.GROUPING

    await incidents.attach_alert(inc.id, a2.id, "inventory")
    await incidents.attach_alert(inc.id, a2.id, "inventory")  # idempotent
    got = await incidents.get(inc.id)
    assert got is not None
    assert got.alert_ids == [a1.id, a2.id]
    assert got.services == ["orders", "inventory"]
    assert got.updated_at >= inc.updated_at
    assert await incidents.get("inc_missing") is None


async def test_create_with_unknown_alert_rolls_back(
    sessions: Sessions, engine: AsyncEngine
) -> None:
    with pytest.raises(AlertNotFoundError):
        await IncidentRepository(sessions).create(make_incident(alert_ids=["al_nope"]))
    assert await count(engine, "incidents") == 0


async def test_attach_to_missing_incident(sessions: Sessions) -> None:
    a = make_alert()
    await AlertRepository(sessions).insert_if_new(a)
    with pytest.raises(IncidentNotFoundError):
        await IncidentRepository(sessions).attach_alert("inc_missing", a.id, "orders")


async def test_concurrent_attach_keeps_services_unique(sessions: Sessions) -> None:
    alerts = AlertRepository(sessions)
    incidents = IncidentRepository(sessions)
    inc = await incidents.create(make_incident(services=["orders"]))
    batch = [make_alert(f"f{i}", "inventory") for i in range(8)]
    for a in batch:
        await alerts.insert_if_new(a)
    await asyncio.gather(*(incidents.attach_alert(inc.id, a.id, "inventory") for a in batch))
    got = await incidents.get(inc.id)
    assert got is not None
    assert got.services == ["orders", "inventory"]
    assert len(got.alert_ids) == 8


async def test_find_open_window_boundary(sessions: Sessions) -> None:
    incidents = IncidentRepository(sessions)
    closes = utcnow()
    inc = await incidents.create(
        make_incident(opened_at=closes - timedelta(seconds=60), window_closes_at=closes)
    )
    before = await incidents.find_open_for_cluster("checkout", closes - timedelta(microseconds=1))
    assert before is not None and before.id == inc.id
    # Window just closed: exactly at, and after, window_closes_at.
    assert await incidents.find_open_for_cluster("checkout", closes) is None
    assert await incidents.find_open_for_cluster("checkout", closes + timedelta(seconds=1)) is None


async def test_find_open_ignores_other_cluster_and_terminal(sessions: Sessions) -> None:
    incidents = IncidentRepository(sessions)
    now = utcnow()
    await incidents.create(make_incident(cluster="other"))
    await incidents.create(make_incident(state=IncidentState.RESOLVED))
    assert await incidents.find_open_for_cluster("checkout", now) is None
    open_inc = await incidents.create(make_incident())
    found = await incidents.find_open_for_cluster("checkout", now)
    assert found is not None and found.id == open_inc.id


def make_evidence(incident_id: str, **kw: object) -> Evidence:
    base: dict[str, object] = {
        "incident_id": incident_id,
        "tool_call_id": "tc_1",
        "kind": EvidenceKind.LOG_LINES,
        "query": "{service='orders'}",
        "summary": "5xx spike",
        "raw_ref": "blob/1",
    }
    base.update(kw)
    return Evidence(**base)  # type: ignore[arg-type]


async def test_evidence_sink_record_and_exists(sessions: Sessions) -> None:
    incidents = IncidentRepository(sessions)
    inc = await incidents.create(make_incident())
    other = await incidents.create(make_incident())
    sink = PostgresEvidenceSink(sessions)
    _: EvidenceSink = sink  # structural conformance, checked by mypy

    ev = make_evidence(inc.id)
    assert await sink.record(ev, "RAW PAYLOAD") == ev
    assert await sink.record(ev, "RAW PAYLOAD") == ev  # retry is a no-op

    assert await sink.exists(inc.id, [ev.id, "ev_missing"]) == {ev.id}
    assert await sink.exists(other.id, [ev.id]) == set()  # scoped to the incident
    assert await sink.exists(inc.id, []) == set()
    assert await sink.get_raw(ev.raw_ref) == "RAW PAYLOAD"
    assert await sink.get(ev.id) == ev

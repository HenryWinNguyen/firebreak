import asyncio
import logging
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import firebreak.ingest.grouping as grouping_mod
from firebreak.db.repositories import AlertRepository, IncidentRepository
from firebreak.ingest.grouping import PostgresIncidentGrouper
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.models.alert import Alert, AlertSource, AlertStatus
from firebreak.models.common import Severity, utcnow
from firebreak.models.incident import Incident, IncidentState

pytestmark = pytest.mark.db


def make_alert(service: str, severity: Severity = Severity.WARNING, n: int = 0) -> Alert:
    return Alert(
        source=AlertSource.GENERIC,
        fingerprint=f"{service}-{n}",
        alertname=f"{service}_down",
        service=service,
        severity=severity,
        summary="x",
        started_at=utcnow() + timedelta(microseconds=n),
    )


def grouper(
    sessions: async_sessionmaker[AsyncSession], window: float = 60.0
) -> PostgresIncidentGrouper:
    return PostgresIncidentGrouper(sessions, ServiceGraph.load(), window_seconds=window)


async def test_second_alert_joins_open_incident(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    d1 = await g.assign(make_alert("gateway"))
    d2 = await g.assign(make_alert("orders"))
    assert d1.created and not d2.created
    assert d1.incident_id == d2.incident_id
    inc = await IncidentRepository(sessions).get(d1.incident_id)
    assert inc is not None
    assert set(inc.services) == {"gateway", "orders"}
    assert len(inc.alert_ids) == 2
    assert inc.window_closes_at - inc.opened_at == timedelta(seconds=60)


async def test_duplicate_alert_is_noop(sessions: async_sessionmaker[AsyncSession]) -> None:
    g = grouper(sessions)
    a = make_alert("gateway")
    d1 = await g.assign(a)
    replay = a.model_copy(update={"id": "al_replay"})
    d2 = await g.assign(replay)
    assert d2.duplicate and not d2.created and d2.incident_id == d1.incident_id
    inc = await IncidentRepository(sessions).get(d1.incident_id)
    assert inc is not None and len(inc.alert_ids) == 1


async def test_closed_window_opens_new_incident(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions, window=0.2)
    d1 = await g.assign(make_alert("gateway"))
    await asyncio.sleep(0.3)
    d2 = await g.assign(make_alert("orders"))
    assert d2.created and d2.incident_id != d1.incident_id


async def test_unknown_service_gets_own_cluster(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    d1 = await g.assign(make_alert("gateway"))
    d2 = await g.assign(make_alert("mystery"))
    d3 = await g.assign(make_alert("mystery", n=1))
    assert d2.created and d2.incident_id != d1.incident_id
    assert d3.incident_id == d2.incident_id


async def test_severity_is_max(sessions: async_sessionmaker[AsyncSession]) -> None:
    g = grouper(sessions)
    d = await g.assign(make_alert("gateway", Severity.WARNING))
    await g.assign(make_alert("orders", Severity.INFO))
    inc = await IncidentRepository(sessions).get(d.incident_id)
    assert inc is not None and inc.severity is Severity.WARNING
    await g.assign(make_alert("redis", Severity.CRITICAL))
    inc = await IncidentRepository(sessions).get(d.incident_id)
    assert inc is not None and inc.severity is Severity.CRITICAL


async def test_critical_new_incident_closes_window_now(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    d = await g.assign(make_alert("gateway", Severity.CRITICAL))
    inc = await IncidentRepository(sessions).get(d.incident_id)
    assert inc is not None and inc.window_closes_at == inc.opened_at
    d2 = await g.assign(make_alert("orders"))
    assert d2.created


async def test_critical_joins_open_incident_and_closes_window(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    d1 = await g.assign(make_alert("gateway"))
    d2 = await g.assign(make_alert("orders", Severity.CRITICAL))
    assert d2.incident_id == d1.incident_id and not d2.created
    inc = await IncidentRepository(sessions).get(d1.incident_id)
    assert inc is not None
    assert inc.window_closes_at <= utcnow()
    assert inc.window_closes_at < inc.opened_at + timedelta(seconds=60)
    assert await IncidentRepository(sessions).find_open_for_cluster("shopfloor", utcnow()) is None


async def test_concurrent_alerts_one_incident(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    services = ["gateway", "orders", "inventory", "postgres", "redis"]
    alerts = [make_alert(services[i % 5], n=i) for i in range(20)]
    decisions = await asyncio.gather(*(g.assign(a) for a in alerts))
    assert len({d.incident_id for d in decisions}) == 1
    assert sum(d.created for d in decisions) == 1
    async with sessions() as s:
        count = (await s.execute(text("SELECT count(*) FROM incidents"))).scalar_one()
    assert count == 1
    inc = await IncidentRepository(sessions).get(decisions[0].incident_id or "")
    assert inc is not None and set(inc.services) == set(services)


async def test_s10_storm_15_alerts_one_incident(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    services = ["gateway", "orders", "inventory"]
    decisions = [await g.assign(make_alert(services[i % 3], n=i)) for i in range(15)]
    assert len({d.incident_id for d in decisions}) == 1
    inc = await IncidentRepository(sessions).get(decisions[0].incident_id or "")
    assert inc is not None
    assert len(inc.alert_ids) == 15
    assert set(inc.services) == set(services)


@pytest.mark.parametrize(
    ("offset", "joins"), [(timedelta(0), False), (timedelta(microseconds=-1), True)]
)
async def test_window_boundary_is_exclusive(
    sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    offset: timedelta,
    joins: bool,
) -> None:
    g = grouper(sessions)
    d1 = await g.assign(make_alert("gateway"))
    inc = await IncidentRepository(sessions).get(d1.incident_id or "")
    assert inc is not None
    at = inc.window_closes_at + offset
    monkeypatch.setattr(grouping_mod, "utcnow", lambda: at)
    d2 = await g.assign(make_alert("orders"))
    assert (d2.incident_id == d1.incident_id) is joins
    assert d2.created is (not joins)


async def test_terminal_incident_never_joined(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    now = utcnow()
    terminal = await IncidentRepository(sessions).create(
        Incident(
            state=IncidentState.RESOLVED,
            severity=Severity.WARNING,
            cluster="shopfloor",
            services=["gateway"],
            alert_ids=[],
            opened_at=now,
            window_closes_at=now + timedelta(minutes=5),
        )
    )
    d = await grouper(sessions).assign(make_alert("gateway"))
    assert d.created and d.incident_id != terminal.id


async def test_same_alert_twice_concurrently(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    a = make_alert("gateway")
    replay = a.model_copy(update={"id": "al_replay"})
    d1, d2 = await asyncio.gather(g.assign(a), g.assign(replay))
    assert d1.incident_id == d2.incident_id
    assert sorted([d1.duplicate, d2.duplicate]) == [False, True]
    assert sum(d.created for d in (d1, d2)) == 1
    inc = await IncidentRepository(sessions).get(d1.incident_id or "")
    assert inc is not None and len(inc.alert_ids) == 1
    async with sessions() as s:
        assert (await s.execute(text("SELECT count(*) FROM alerts"))).scalar_one() == 1
        assert (await s.execute(text("SELECT count(*) FROM incident_alerts"))).scalar_one() == 1


async def test_pre_inserted_unlinked_alert_gets_grouped(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    a = make_alert("gateway")
    assert await AlertRepository(sessions).insert_if_new(a)
    g = grouper(sessions)
    d = await g.assign(a)
    assert d.created and not d.duplicate
    inc = await IncidentRepository(sessions).get(d.incident_id or "")
    assert inc is not None and inc.alert_ids == [a.id]
    # A replay under a fresh id resolves to the stored row.
    d2 = await g.assign(a.model_copy(update={"id": "al_other"}))
    assert d2.duplicate and d2.incident_id == d.incident_id


async def test_resolved_alert_without_open_incident_opens_nothing(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    a = make_alert("gateway").model_copy(
        update={"status": AlertStatus.RESOLVED, "ended_at": utcnow()}
    )
    g = grouper(sessions)
    d = await g.assign(a)
    assert d.incident_id is None and not d.created and not d.duplicate
    async with sessions() as s:
        assert (await s.execute(text("SELECT count(*) FROM incidents"))).scalar_one() == 0
        assert (await s.execute(text("SELECT count(*) FROM alerts"))).scalar_one() == 1
    d2 = await g.assign(a.model_copy(update={"id": "al_replay"}))
    assert d2.duplicate and d2.incident_id is None


async def test_resolved_alert_attaches_without_severity_or_window_effects(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    d1 = await g.assign(make_alert("gateway", Severity.WARNING))
    repo = IncidentRepository(sessions)
    before = await repo.get(d1.incident_id or "")
    assert before is not None
    resolved = make_alert("orders", Severity.CRITICAL, n=1).model_copy(
        update={"status": AlertStatus.RESOLVED, "ended_at": utcnow()}
    )
    d2 = await g.assign(resolved)
    assert d2.incident_id == d1.incident_id and not d2.created and not d2.duplicate
    after = await repo.get(d1.incident_id or "")
    assert after is not None
    assert after.severity is Severity.WARNING
    assert after.window_closes_at == before.window_closes_at
    assert resolved.id in after.alert_ids


async def test_concurrent_services_not_lost(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Another transaction holds the incident row lock and has attached a service but not
    committed. The grouper must wait on the row (FOR UPDATE) and then see that service."""
    g = grouper(sessions)
    d0 = await g.assign(make_alert("gateway"))
    incident_id = d0.incident_id or ""
    extra = make_alert("redis", n=99)
    assert await AlertRepository(sessions).insert_if_new(extra)
    now = utcnow()
    async with sessions() as holder, holder.begin():
        locked = await IncidentRepository.find_open_for_cluster_in(
            holder, "shopfloor", now, for_update=True
        )
        assert locked is not None and locked.id == incident_id
        await IncidentRepository.attach_alert_in(holder, incident_id, extra.id, "redis")
        task = asyncio.create_task(g.assign(make_alert("orders", n=1)))
        await asyncio.sleep(0.5)
        assert not task.done()  # blocked on the row lock
    d = await task
    assert d.incident_id == incident_id and not d.created
    inc = await IncidentRepository(sessions).get(incident_id)
    assert inc is not None
    assert set(inc.services) == {"gateway", "redis", "orders"}


async def test_firing_replay_after_resolved_is_not_grouped(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    firing = make_alert("gateway")
    await AlertRepository(sessions).insert_if_new(firing)
    await AlertRepository(sessions).mark_resolved(firing.idempotency_key, utcnow())
    d = await g.assign(firing.model_copy(update={"id": "al_replay"}))
    assert d.incident_id is None and not d.created and d.duplicate
    async with sessions() as s:
        assert (await s.execute(text("SELECT count(*) FROM incidents"))).scalar_one() == 0


async def test_unknown_service_warns_once_across_alerts(
    sessions: async_sessionmaker[AsyncSession], caplog: pytest.LogCaptureFixture
) -> None:
    g = grouper(sessions)
    with caplog.at_level(logging.WARNING, logger="firebreak.ingest.service_graph"):
        for n in range(3):
            await g.assign(make_alert("mystery", n=n))
    assert caplog.text.count("mystery") == 1

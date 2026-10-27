import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from firebreak.db.engine import make_session_factory
from firebreak.db.repositories import IncidentRepository
from firebreak.ingest.grouping import PostgresIncidentGrouper
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.models.alert import Alert, AlertSource
from firebreak.models.common import Severity, utcnow

pytestmark = pytest.mark.db

DB_URL = os.environ.get("FIREBREAK_TEST_DATABASE_URL")
ALEMBIC_INI = Path(__file__).resolve().parents[2] / "firebreak" / "db" / "alembic.ini"
MIGRATIONS = ALEMBIC_INI.parent / "migrations"


def _upgrade(sync_conn: Connection) -> None:
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("script_location", str(MIGRATIONS))
    cfg.attributes["connection"] = sync_conn
    command.upgrade(cfg, "head")


async def _migrate(url: str) -> None:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
            await conn.run_sync(_upgrade)
    finally:
        await engine.dispose()


@pytest.fixture(scope="module")
def migrated_url() -> Iterator[str]:
    if not DB_URL:
        pytest.skip("FIREBREAK_TEST_DATABASE_URL is not set")
    asyncio.run(_migrate(DB_URL))
    yield DB_URL


@pytest.fixture
async def engine(migrated_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(migrated_url, poolclass=NullPool)
    async with eng.begin() as conn:
        await conn.execute(text("TRUNCATE alerts, incidents, incident_alerts CASCADE"))
    yield eng
    await eng.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return make_session_factory(engine)


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


async def test_s10_storm_15_alerts_one_incident(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    g = grouper(sessions)
    services = ["gateway", "orders", "inventory"]
    decisions = [await g.assign(make_alert(services[i % 3], n=i)) for i in range(15)]
    assert len({d.incident_id for d in decisions}) == 1
    inc = await IncidentRepository(sessions).get(decisions[0].incident_id)
    assert inc is not None
    assert len(inc.alert_ids) == 15
    assert set(inc.services) == set(services)

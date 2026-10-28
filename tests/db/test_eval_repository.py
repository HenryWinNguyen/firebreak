"""Migration 0002 and the eval repositories."""

from datetime import UTC, datetime, timedelta

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from firebreak.db.repositories import (
    AlertRepository,
    EvalRepository,
    EvalResult,
    EvalRun,
    IncidentRepository,
    PostgresEvidenceSink,
)
from firebreak.db.tables import Base
from firebreak.models.evidence import Evidence, EvidenceKind
from tests.conftest import ALEMBIC_INI, MIGRATIONS
from tests.db.test_repositories import make_alert, make_incident

pytestmark = pytest.mark.db

Sessions = async_sessionmaker[AsyncSession]
T0 = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)


def _cfg(conn: Connection) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("script_location", str(MIGRATIONS))
    cfg.attributes["connection"] = conn
    return cfg


async def test_0002_is_reversible_and_leaves_no_drift(migrated_url: str) -> None:
    eng = create_async_engine(migrated_url, poolclass=NullPool)

    def tables(c: Connection) -> set[str]:
        return set(inspect(c).get_table_names())

    def drift(c: Connection) -> list[object]:
        return list(compare_metadata(MigrationContext.configure(c), Base.metadata))

    try:
        async with eng.begin() as conn:
            assert {"eval_runs", "eval_results"} <= await conn.run_sync(tables)
            await conn.run_sync(lambda c: command.downgrade(_cfg(c), "0001"))
            names = await conn.run_sync(tables)
            assert not {"eval_runs", "eval_results"} & names and "incidents" in names
            await conn.run_sync(lambda c: command.upgrade(_cfg(c), "head"))
            assert await conn.run_sync(drift) == []
    finally:
        await eng.dispose()


def _run(rid: str, agent: str, minutes: int) -> EvalRun:
    return EvalRun(
        id=rid,
        started_at=T0 + timedelta(minutes=minutes),
        mode="replay",
        agent=agent,
        git_sha="abc",
        prompt_sha256="def",
    )


def _res(sid: str, top1: bool = True) -> EvalResult:
    return EvalResult(
        scenario_id=sid,
        top1=top1,
        top3=True,
        cited_kinds_ok=True,
        report_valid=True,
        cost_usd=0.01,
        latency_s=1.5,
        steps=2,
        report={"findings": [], "n": 1},
    )


async def test_save_get_and_latest_run_per_agent(sessions: Sessions, engine: AsyncEngine) -> None:
    repo = EvalRepository(sessions)
    await repo.save_run(_run("r1", "baseline", 0), [_res("S2"), _res("S1")])
    await repo.save_run(_run("r2", "baseline", 5), [_res("S1", top1=False)])
    await repo.save_run(_run("r3", "multi", 1), [_res("S1")])

    got = await repo.get_run("r1")
    assert got is not None
    run, results = got
    assert run == _run("r1", "baseline", 0)
    assert [r.scenario_id for r in results] == ["S1", "S2"]
    assert results[0].report == {"findings": [], "n": 1} and results[0].error is None
    assert await repo.get_run("nope") is None

    latest = await repo.latest_run_per_agent()
    assert [(r.id, [x.scenario_id for x in rs]) for r, rs in latest] == [
        ("r2", ["S1"]),
        ("r3", ["S1"]),
    ]
    assert latest[0][1][0].top1 is False


async def test_results_cascade_with_run_and_duplicate_scenario_rejected(
    sessions: Sessions, engine: AsyncEngine
) -> None:
    repo = EvalRepository(sessions)
    with pytest.raises(Exception, match=r"pk_eval_results|duplicate"):
        await repo.save_run(_run("r1", "baseline", 0), [_res("S1"), _res("S1")])
    async with engine.connect() as conn:  # the failed save rolled back entirely
        assert (await conn.execute(text("SELECT count(*) FROM eval_runs"))).scalar_one() == 0
    await repo.save_run(_run("r1", "baseline", 0), [_res("S1")])
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM eval_runs"))
        assert (await conn.execute(text("SELECT count(*) FROM eval_results"))).scalar_one() == 0


async def test_naive_started_at_is_rejected(sessions: Sessions) -> None:
    run = _run("r1", "baseline", 0).model_copy(update={"started_at": datetime(2026, 1, 1)})
    with pytest.raises(ValueError, match="timezone-aware"):
        await EvalRepository(sessions).save_run(run, [])


async def test_evidence_kinds_and_purge_incident(sessions: Sessions, engine: AsyncEngine) -> None:
    alert = make_alert()
    await AlertRepository(sessions).insert_if_new(alert)
    inc = await IncidentRepository(sessions).create(make_incident(alert_ids=[alert.id]))
    await PostgresEvidenceSink(sessions).record(
        Evidence(
            id="ev_1",
            incident_id=inc.id,
            tool_call_id="tc_1",
            kind=EvidenceKind.DEPLOY,
            query="q",
            summary="s",
            raw_ref="raw/1",
            untrusted=True,
        ),
        "raw",
    )
    repo = EvalRepository(sessions)
    assert await repo.evidence_kinds(inc.id) == {"ev_1": EvidenceKind.DEPLOY}
    assert await repo.evidence_kinds("inc_other") == {}

    assert await repo.purge_incident(inc.id) is True
    assert await repo.purge_incident(inc.id) is False
    async with engine.connect() as conn:
        for table in ("incidents", "alerts", "evidence", "evidence_blobs", "incident_alerts"):
            assert (await conn.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one() == 0

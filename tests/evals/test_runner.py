"""T-045 eval runner, results tables and REPORT.md. Synthetic recording; no network, no API key."""

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import anthropic
import httpx
import httpx2
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from firebreak.db.engine import make_session_factory
from firebreak.db.repositories import (
    AlertRepository,
    EvalRepository,
    EvalResult,
    EvalRun,
    IncidentRepository,
    SessionFactory,
)
from firebreak.evals import runner
from firebreak.evals.__main__ import main
from firebreak.evals.cassette import recording_dir
from firebreak.evals.report import NoRunsError, render_report
from firebreak.evals.runner import (
    RUN_FILE,
    EvalRunError,
    EvalSettings,
    LiveModeRefusedError,
    RunOutcome,
    ScenarioOutcome,
    ensure_eval_database,
    record_scenario,
    replay_all,
    require_eval_session,
    require_eval_url,
    require_live_env,
    scenario_key,
    select_scenarios,
    store_run,
)
from firebreak.evals.scenario import load_scenarios
from firebreak.llm.client import AnthropicLLMClient
from firebreak.models.alert import Alert, AlertSource
from firebreak.models.common import Severity, utcnow
from firebreak.models.incident import Incident
from firebreak.tools.settings import ToolSettings
from tests.evals.test_cassette import Live, live_adapters, script

SCENARIOS = load_scenarios(Path("evals/scenarios"))
S1 = SCENARIOS["S1"]
LIVE_ENV = {"FIREBREAK_EVAL_LIVE": "1", "ANTHROPIC_API_KEY": "sk-test-not-a-real-key"}


@pytest.fixture
def no_http(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Any HTTP transport, live LLM client or real-adapter build is a hard failure.

    Sockets stay open on purpose: the runner talks to Postgres.
    """
    attempts: list[str] = []

    def refuse(what: str) -> Any:
        def _raise(*a: Any, **kw: Any) -> Any:
            attempts.append(what)
            raise AssertionError(f"replay reached out via {what}")

        return _raise

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", refuse("httpx"))
    monkeypatch.setattr(httpx2.AsyncHTTPTransport, "handle_async_request", refuse("httpx2"))
    monkeypatch.setattr(AnthropicLLMClient, "__init__", refuse("AnthropicLLMClient"))
    monkeypatch.setattr(anthropic.AsyncAnthropic, "__init__", refuse("AsyncAnthropic"))
    for mod in ("firebreak.agents.baseline", "firebreak.tools.read_tools"):
        monkeypatch.setattr(f"{mod}.build_default_adapters", refuse("build_default_adapters"))
    yield attempts
    assert attempts == []


async def seed_live_incident(sessions: SessionFactory) -> str:
    now = utcnow()
    alert = Alert(
        source=AlertSource.ALERTMANAGER,
        fingerprint="fp-orders-5xx",
        alertname="HighErrorRate",
        service="orders",
        severity=Severity.CRITICAL,
        summary="orders 5xx above 5%",
        labels={"team": "shop"},
        started_at=now - timedelta(minutes=2),
    )
    assert await AlertRepository(sessions).insert_if_new(alert)
    inc = await IncidentRepository(sessions).create(
        Incident(
            severity=Severity.CRITICAL,
            cluster="checkout",
            services=["orders"],
            alert_ids=[alert.id],
            opened_at=now,
            window_closes_at=now + timedelta(seconds=60),
        )
    )
    return inc.id


LIVE_DB = "t045_live_test"


@pytest.fixture
async def live_sessions(migrated_url: str) -> AsyncIterator[SessionFactory]:
    """A second database standing in for the live dev DB (record mode reads it, read-only)."""
    base = make_url(migrated_url)
    live_url = base.set(database=LIVE_DB).render_as_string(hide_password=False)
    admin = create_async_engine(
        base.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    async with admin.connect() as conn:
        await conn.execute(text(f"DROP DATABASE IF EXISTS {LIVE_DB} WITH (FORCE)"))
    await ensure_eval_database(live_url)  # create + migrate (the name ends _test)
    eng = create_async_engine(live_url, poolclass=NullPool)
    try:
        yield make_session_factory(eng)
    finally:
        await eng.dispose()
        async with admin.connect() as conn:
            await conn.execute(text(f"DROP DATABASE IF EXISTS {LIVE_DB} WITH (FORCE)"))
        await admin.dispose()


async def make_recording(
    root: Path, sessions: SessionFactory, live_sessions: SessionFactory
) -> str:
    """A synthetic S1 recording through the real record path (scripted LLM, fake adapters)."""
    incident_id = await seed_live_incident(live_sessions)
    outcome = await record_scenario(
        S1,
        incident_id,
        agent="baseline",
        root=root,
        sessions=sessions,
        live_sessions=live_sessions,
        tools_settings=ToolSettings(),
        llm=script(),
        adapters=live_adapters(Live()),
        env=LIVE_ENV,
    )
    assert outcome.result is not None and outcome.result.top1
    return incident_id


# --- pure unit tests ------------------------------------------------------------------------


def test_scenario_key_and_selection() -> None:
    assert [scenario_key(x) for x in ("S1", "s01", "S12", "custom")] == [
        "s01",
        "s01",
        "s12",
        "custom",
    ]
    assert [s.id for s in select_scenarios(SCENARIOS, ["s02", "S1"])] == ["S1", "S2"]
    assert len(select_scenarios(SCENARIOS, [])) == len(SCENARIOS)
    with pytest.raises(EvalRunError, match="s99"):
        select_scenarios(SCENARIOS, ["s99"])


def test_record_mode_refuses_without_opt_in_and_key() -> None:
    with pytest.raises(LiveModeRefusedError, match="FIREBREAK_EVAL_LIVE"):
        require_live_env({"ANTHROPIC_API_KEY": "k"})
    with pytest.raises(LiveModeRefusedError, match="FIREBREAK_EVAL_LIVE"):
        require_live_env({"FIREBREAK_EVAL_LIVE": "0", "ANTHROPIC_API_KEY": "k"})
    with pytest.raises(LiveModeRefusedError, match="ANTHROPIC_API_KEY"):
        require_live_env({"FIREBREAK_EVAL_LIVE": "1"})
    require_live_env(LIVE_ENV)


def test_cli_record_refuses_before_touching_anything(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("FIREBREAK_EVAL_LIVE", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    # An unreachable database proves no connection was needed to refuse.
    code = main(
        ["--database-url", "postgresql+asyncpg://x:x@127.0.0.1:1/x", "run", "--mode", "record"]
    )
    assert code == 1
    assert "FIREBREAK_EVAL_LIVE=1" in capsys.readouterr().err
    monkeypatch.setenv("FIREBREAK_EVAL_LIVE", "1")
    assert main(["run", "--mode", "record"]) == 1
    assert "ANTHROPIC_API_KEY" in capsys.readouterr().err


def _result(
    sid: str, top1: bool, cost: float, latency: float, error: str | None = None
) -> EvalResult:
    return EvalResult(
        scenario_id=sid,
        top1=top1,
        top3=True,
        cited_kinds_ok=top1,
        report_valid=error is None,
        cost_usd=cost,
        latency_s=latency,
        steps=3,
        error=error,
    )


def test_render_report_tables_and_summary() -> None:
    run = EvalRun(
        id="evr_1",
        started_at=datetime(2026, 10, 28, 9, 30, tzinfo=UTC),
        mode="replay",
        agent="baseline",
        git_sha="abcdef0123456789",
        prompt_sha256="f" * 64,
    )
    results = [
        _result("S1", True, 0.10, 10.0),
        _result("S2", False, 0.30, 30.0, error="a | b"),
        _result("S3", True, 0.20, 20.0),
    ]
    md = render_report([(run, results)], SCENARIOS)
    assert md.startswith("# Eval report")
    assert "## Agent: baseline" in md and "`evr_1` (replay)" in md and "abcdef012345" in md
    assert f"| S1 | {SCENARIOS['S1'].expect.root_cause_category.value} | yes | yes |" in md
    assert "| S2 |" in md and "a \\| b" in md  # pipes escaped
    assert "top-1 67%, top-3 100%" in md
    assert "latency (recorded live run) p50 20.0s / p95 30.0s" in md
    assert "cost (recorded live run) p50 $0.2000 / p95 $0.3000, 1 errored" in md
    assert render_report([(run, results)], SCENARIOS) == md  # deterministic
    with pytest.raises(NoRunsError):
        render_report([], SCENARIOS)


# --- end to end (Postgres) --------------------------------------------------------------------


@pytest.mark.db
async def test_replay_end_to_end_offline(
    tmp_path: Path,
    engine: AsyncEngine,
    sessions: SessionFactory,
    live_sessions: SessionFactory,
    no_http: list[str],
) -> None:
    root = tmp_path / "recordings"
    incident_id = await make_recording(root, sessions, live_sessions)
    assert (recording_dir("baseline", "s01", root) / RUN_FILE).exists()
    scenarios = select_scenarios(SCENARIOS, ["s01", "s02"])  # s02 has no recording

    outcome = await replay_all(
        scenarios, agent="baseline", root=root, sessions=sessions, tools_settings=ToolSettings()
    )
    assert outcome.not_recorded == ["S2"]
    (graded,) = outcome.graded
    assert graded.result is not None
    r = graded.result
    assert (r.top1, r.top3, r.report_valid, r.error) == (True, True, True, None)
    assert r.steps == 2 and r.cost_usd == pytest.approx(0.006)
    assert r.report is not None and r.report["hypotheses"][0]["root_cause_category"] == "bad_deploy"
    # replay recreated the recorded incident and re-ran it again on a second pass
    again = await replay_all(
        scenarios, agent="baseline", root=root, sessions=sessions, tools_settings=ToolSettings()
    )
    assert again.graded[0].result == r
    assert await IncidentRepository(sessions).get(incident_id) is not None

    run = await store_run(outcome, mode="replay", agent="baseline", sessions=sessions)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM eval_runs"))).scalar_one() == 1
        row = (
            await conn.execute(text("SELECT scenario_id, top1, steps, report FROM eval_results"))
        ).one()
    assert row.scenario_id == "S1" and row.top1 is True and row.steps == 2
    assert row.report["hypotheses"][0]["rank"] == 1

    ((stored_run, stored),) = await EvalRepository(sessions).latest_run_per_agent()
    assert stored_run.id == run.id and stored == [r]
    md = render_report([(stored_run, stored)], SCENARIOS)
    assert "| S1 | bad_deploy | yes | yes | yes | yes |" in md
    assert "top-1 100%, top-3 100%" in md
    assert no_http == []


@pytest.mark.db
async def test_stale_recording_fails_naming_the_scenario_and_stores_nothing(
    tmp_path: Path,
    engine: AsyncEngine,
    sessions: SessionFactory,
    live_sessions: SessionFactory,
    no_http: list[str],
) -> None:
    root = tmp_path / "recordings"
    await make_recording(root, sessions, live_sessions)
    path = recording_dir("baseline", "s01", root) / RUN_FILE
    doc = json.loads(path.read_text())
    doc["alerts"][0]["summary"] = "a different alert text"  # the agent's prompt changes
    path.write_text(json.dumps(doc))

    with pytest.raises(EvalRunError, match="s01"):
        await replay_all(
            select_scenarios(SCENARIOS, ["s01"]),
            agent="baseline",
            root=root,
            sessions=sessions,
            tools_settings=ToolSettings(),
        )
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM eval_runs"))).scalar_one() == 0


@pytest.mark.db
async def test_recording_without_run_file_is_an_error_not_a_skip(
    tmp_path: Path, sessions: SessionFactory, live_sessions: SessionFactory, no_http: list[str]
) -> None:
    root = tmp_path / "recordings"
    await make_recording(root, sessions, live_sessions)
    (recording_dir("baseline", "s01", root) / RUN_FILE).unlink()
    with pytest.raises(EvalRunError, match=r"S1.*run\.json"):
        await replay_all(
            [S1], agent="baseline", root=root, sessions=sessions, tools_settings=ToolSettings()
        )


@pytest.mark.db
async def test_replay_with_nothing_recorded_stores_nothing(
    tmp_path: Path, sessions: SessionFactory, live_sessions: SessionFactory, no_http: list[str]
) -> None:
    outcome = await replay_all(
        [S1], agent="baseline", root=tmp_path, sessions=sessions, tools_settings=ToolSettings()
    )
    assert outcome.graded == [] and outcome.not_recorded == ["S1"]
    with pytest.raises(EvalRunError, match="nothing to store"):
        await store_run(outcome, mode="replay", agent="baseline", sessions=sessions)


@pytest.mark.db
async def test_cli_run_then_report(
    tmp_path: Path,
    migrated_url: str,
    engine: AsyncEngine,
    sessions: SessionFactory,
    live_sessions: SessionFactory,
    no_http: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "recordings"
    await make_recording(root, sessions, live_sessions)
    out = tmp_path / "REPORT.md"
    base = ["--database-url", migrated_url]
    run_args = [
        *base,
        "run",
        "--mode",
        "replay",
        "--agent",
        "baseline",
        "--recordings-dir",
        str(root),
    ]
    assert await asyncio.to_thread(main, [*run_args, "--scenarios", "s01,s02"]) == 0
    printed = capsys.readouterr().out
    assert "S2: not recorded" in printed and "S1: top1=True" in printed
    assert await asyncio.to_thread(main, [*base, "report", "--out", str(out)]) == 0
    md = out.read_text()
    assert "## Agent: baseline" in md and "| S1 | bad_deploy |" in md
    assert "S2" not in md


def test_cli_refuses_databases_not_named_eval_or_test(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    for sub in (["run", "--mode", "replay"], ["report", "--out", str(tmp_path / "R.md")]):
        code = main(["--database-url", "postgresql+asyncpg://x:x@127.0.0.1:1/firebreak", *sub])
        assert code == 1
        assert "must end with _eval or _test" in capsys.readouterr().err
    assert not (tmp_path / "R.md").exists()


def test_default_eval_url_is_firebreak_eval_on_the_dev_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FIREBREAK_EVAL_DATABASE_URL", raising=False)
    monkeypatch.setenv("FIREBREAK_DATABASE_URL", "postgresql+asyncpg://u:p@h:5/firebreak")
    url = make_url(EvalSettings(_env_file=None).resolved_database_url())
    assert (url.database, url.host, url.username, url.password) == ("firebreak_eval", "h", "u", "p")
    monkeypatch.setenv("FIREBREAK_EVAL_DATABASE_URL", "postgresql+asyncpg://u:p@h:5/other_eval")
    assert make_url(EvalSettings(_env_file=None).resolved_database_url()).database == "other_eval"


def test_url_query_database_override_is_rejected() -> None:
    for key in ("database", "dbname", "DATABASE"):
        url = f"postgresql+asyncpg://x:x@127.0.0.1:1/firebreak_probe_eval?{key}=firebreak_live"
        with pytest.raises(EvalRunError, match="override"):
            require_eval_url(url)
    assert require_eval_url("postgresql+asyncpg://x:x@h/foo_eval?ssl=false") == "foo_eval"


@pytest.mark.db
async def test_query_override_bypass_is_refused_before_any_write(
    migrated_url: str, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    base = make_url(migrated_url)
    sneaky = base.set(database="firebreak_probe_eval", query={"database": "postgres"})
    url = sneaky.render_as_string(hide_password=False)
    with pytest.raises(EvalRunError, match="override"):
        await ensure_eval_database(url)
    assert await asyncio.to_thread(main, ["--database-url", url, "run", "--mode", "replay"]) == 1
    assert "override" in capsys.readouterr().err
    assert await asyncio.to_thread(main, ["--database-url", url, "report"]) == 1
    # nothing was created: the probe database does not exist
    admin = create_async_engine(base.set(database="postgres"), poolclass=NullPool)
    async with admin.connect() as conn:
        q = text("SELECT count(*) FROM pg_database WHERE datname = 'firebreak_probe_eval'")
        assert (await conn.execute(q)).scalar_one() == 0
    await admin.dispose()


@pytest.mark.db
async def test_current_database_mismatch_is_refused_before_any_write(
    tmp_path: Path, migrated_url: str, live_sessions: SessionFactory
) -> None:
    """Sessions that really point at a non-eval database (here `postgres`) are refused."""
    url = make_url(migrated_url).set(database="postgres")
    eng = create_async_engine(url, poolclass=NullPool)
    bad = make_session_factory(eng)
    try:
        with pytest.raises(EvalRunError, match="'postgres'"):
            await require_eval_session(bad)
        with pytest.raises(EvalRunError, match="'postgres'"):
            await replay_all(
                [S1], agent="baseline", root=tmp_path, sessions=bad, tools_settings=ToolSettings()
            )
        with pytest.raises(EvalRunError, match="'postgres'"):
            await store_run(
                RunOutcome(outcomes=[ScenarioOutcome("S1", _result("S1", True, 0.1, 1.0))]),
                mode="replay",
                agent="baseline",
                sessions=bad,
            )
        with pytest.raises(EvalRunError, match="'postgres'"):
            await record_scenario(
                S1,
                "inc_x",
                agent="baseline",
                root=tmp_path,
                sessions=bad,
                live_sessions=live_sessions,
                tools_settings=ToolSettings(),
                llm=script(),
                adapters=live_adapters(Live()),
                env=LIVE_ENV,
            )
    finally:
        await eng.dispose()
    assert await require_eval_session(live_sessions) == LIVE_DB


@pytest.mark.db
async def test_record_refuses_when_eval_and_live_are_the_same_database(
    tmp_path: Path, sessions: SessionFactory
) -> None:
    incident_id = await seed_live_incident(sessions)
    with pytest.raises(EvalRunError, match="same database"):
        await record_scenario(
            S1,
            incident_id,
            agent="baseline",
            root=tmp_path,
            sessions=sessions,
            live_sessions=sessions,
            tools_settings=ToolSettings(),
            llm=script(),
            adapters=live_adapters(Live()),
            env=LIVE_ENV,
        )
    assert not (tmp_path / "baseline").exists()


@pytest.mark.db
async def test_replay_refuses_when_the_eval_db_has_an_extra_alert_on_the_incident(
    tmp_path: Path,
    engine: AsyncEngine,
    sessions: SessionFactory,
    live_sessions: SessionFactory,
    no_http: list[str],
) -> None:
    root = tmp_path / "recordings"
    incident_id = await make_recording(root, sessions, live_sessions)
    extra = Alert(
        source=AlertSource.ALERTMANAGER,
        fingerprint="fp-late",
        alertname="Late",
        service="orders",
        severity=Severity.WARNING,
        summary="attached after recording",
        started_at=utcnow(),
    )
    await AlertRepository(sessions).insert_if_new(extra)
    await IncidentRepository(sessions).attach_alert(incident_id, extra.id, "orders")
    with pytest.raises(EvalRunError, match="not in the recording"):
        await replay_all(
            [S1], agent="baseline", root=root, sessions=sessions, tools_settings=ToolSettings()
        )
    async with engine.connect() as conn:  # nothing was deleted
        assert (await conn.execute(text("SELECT count(*) FROM alerts"))).scalar_one() == 2
        assert (await conn.execute(text("SELECT count(*) FROM incidents"))).scalar_one() == 1


@pytest.mark.db
async def test_replay_refuses_an_alert_shared_with_another_incident(
    tmp_path: Path,
    engine: AsyncEngine,
    sessions: SessionFactory,
    live_sessions: SessionFactory,
    no_http: list[str],
) -> None:
    root = tmp_path / "recordings"
    incident_id = await make_recording(root, sessions, live_sessions)
    (alert_id,) = (await IncidentRepository(sessions).list_alerts(incident_id))[0].id.split()
    other = await IncidentRepository(sessions).create(
        Incident(
            severity=Severity.WARNING,
            cluster="checkout",
            services=["orders"],
            alert_ids=[alert_id],
            window_closes_at=utcnow() + timedelta(seconds=60),
        )
    )
    # Drop the original incident so the recorded one is "absent" and the alert is only shared.
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM incidents WHERE id = :i"), {"i": incident_id})
    with pytest.raises(EvalRunError, match="linked to another incident"):
        await replay_all(
            [S1], agent="baseline", root=root, sessions=sessions, tools_settings=ToolSettings()
        )
    assert await IncidentRepository(sessions).get(other.id) is not None
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM alerts"))).scalar_one() == 1


@pytest.mark.db
async def test_ensure_eval_database_creates_and_migrates_idempotently(migrated_url: str) -> None:
    base = make_url(migrated_url)
    eval_url = base.set(database="t045_runner_eval").render_as_string(hide_password=False)
    admin = create_async_engine(
        base.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    async with admin.connect() as conn:
        await conn.execute(text("DROP DATABASE IF EXISTS t045_runner_eval WITH (FORCE)"))
    try:
        await ensure_eval_database(eval_url)
        await ensure_eval_database(eval_url)
        eng = create_async_engine(eval_url, poolclass=NullPool)
        try:
            async with eng.connect() as conn:
                n = (await conn.execute(text("SELECT count(*) FROM eval_runs"))).scalar_one()
            assert n == 0
        finally:
            await eng.dispose()
    finally:
        async with admin.connect() as conn:
            await conn.execute(text("DROP DATABASE IF EXISTS t045_runner_eval WITH (FORCE)"))
        await admin.dispose()


@pytest.mark.db
async def test_record_reads_live_and_writes_only_recording_and_eval_db(
    tmp_path: Path, engine: AsyncEngine, sessions: SessionFactory, live_sessions: SessionFactory
) -> None:
    incident_id = await make_recording(tmp_path / "rec", sessions, live_sessions)
    n = "SELECT count(*) FROM "
    live_eng = live_sessions.kw["bind"]
    async with live_eng.connect() as conn:  # live DB: the seeded incident, nothing else
        assert (await conn.execute(text(n + "incidents"))).scalar_one() == 1
        assert (await conn.execute(text(n + "evidence"))).scalar_one() == 0
    assert await IncidentRepository(sessions).get(incident_id) is not None
    async with engine.connect() as conn:
        assert (await conn.execute(text(n + "evidence"))).scalar_one() == 6


def test_require_live_env_key_only_for_anthropic_agents() -> None:
    local = {"FIREBREAK_EVAL_LIVE": "1"}
    with pytest.raises(LiveModeRefusedError, match="FIREBREAK_EVAL_LIVE"):
        require_live_env({}, agent="baseline-local")  # the flag is always required
    require_live_env(local, agent="baseline-local")  # all-local: no Anthropic key needed
    require_live_env({**local, "ANTHROPIC_API_KEY": "k"}, agent="baseline-local")
    with pytest.raises(LiveModeRefusedError, match="ANTHROPIC_API_KEY"):
        require_live_env(local, agent="baseline")
    require_live_env(LIVE_ENV, agent="baseline")


def test_require_live_env_checks_the_fallback_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "mixed.yaml").write_text(
        "name: mixed\nmodel: qwen3:8b\nfallback_model: claude-haiku-5-5\n"
        "system_prompt: baseline.md\noutput_schema: InvestigationReport\n"
    )
    monkeypatch.setattr(runner, "CONFIGS_DIR", tmp_path)
    assert runner.agent_providers("mixed") == {"openai_compat", "anthropic"}
    with pytest.raises(LiveModeRefusedError, match="ANTHROPIC_API_KEY"):
        require_live_env({"FIREBREAK_EVAL_LIVE": "1"}, agent="mixed")


def test_unknown_agent_and_unpriced_model_are_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(EvalRunError, match="unknown agent 'nope'"):
        runner.agent_config_path("nope")
    with pytest.raises(EvalRunError, match="unknown agent"):
        runner.agent_config_path("../baseline")
    for wrong_case in ("BASELINE", "Baseline-Local"):  # exact stems only, even on macOS
        with pytest.raises(EvalRunError, match="unknown agent"):
            runner.agent_config_path(wrong_case)
    assert runner.agent_config_path("baseline-local").name == "baseline-local.yaml"
    (tmp_path / "ghost.yaml").write_text(
        "name: ghost\nmodel: not-a-model\nsystem_prompt: baseline.md\n"
        "output_schema: InvestigationReport\n"
    )
    monkeypatch.setattr(runner, "CONFIGS_DIR", tmp_path)
    with pytest.raises(EvalRunError, match="not-a-model"):
        runner.agent_providers("ghost")


def test_cli_baseline_local_needs_the_flag_but_no_anthropic_key(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("FIREBREAK_EVAL_LIVE", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    url = "postgresql+asyncpg://x:x@127.0.0.1:1/x"
    assert main(["--database-url", url, "run", "--mode", "record", "--agent", "nope"]) == 1
    assert "unknown agent" in capsys.readouterr().err
    local = ["--database-url", url, "run", "--mode", "record", "--agent", "baseline-local"]
    assert main(local) == 1
    assert "FIREBREAK_EVAL_LIVE=1" in capsys.readouterr().err
    monkeypatch.setenv("FIREBREAK_EVAL_LIVE", "1")
    assert main(local) == 1  # gets past the live guard (no key needed), then stops later
    assert "ANTHROPIC_API_KEY" not in capsys.readouterr().err
    assert main(["--database-url", url, "run", "--mode", "record"]) == 1  # baseline: key needed
    assert "ANTHROPIC_API_KEY" in capsys.readouterr().err


def test_recordings_are_per_agent(tmp_path: Path) -> None:
    local = recording_dir("baseline-local", "s01", tmp_path)
    assert local == tmp_path / "baseline-local" / "s01"
    assert local != recording_dir("baseline", "s01", tmp_path)

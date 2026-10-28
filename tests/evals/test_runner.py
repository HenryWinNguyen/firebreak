"""T-045 eval runner, results tables and REPORT.md. Synthetic recording; no network, no API key."""

import asyncio
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import anthropic
import httpx
import httpx2
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from firebreak.db.repositories import (
    AlertRepository,
    EvalRepository,
    EvalResult,
    EvalRun,
    IncidentRepository,
    SessionFactory,
)
from firebreak.evals.__main__ import main
from firebreak.evals.cassette import recording_dir
from firebreak.evals.report import NoRunsError, render_report
from firebreak.evals.runner import (
    RUN_FILE,
    EvalRunError,
    LiveModeRefusedError,
    record_scenario,
    replay_all,
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


async def make_recording(root: Path, sessions: SessionFactory) -> str:
    """A synthetic S1 recording through the real record path (scripted LLM, fake adapters)."""
    incident_id = await seed_live_incident(sessions)
    outcome = await record_scenario(
        S1,
        incident_id,
        agent="baseline",
        root=root,
        sessions=sessions,
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
    assert "latency p50 20.0s / p95 30.0s" in md
    assert "cost p50 $0.2000 / p95 $0.3000, 1 errored" in md
    assert render_report([(run, results)], SCENARIOS) == md  # deterministic
    with pytest.raises(NoRunsError):
        render_report([], SCENARIOS)


# --- end to end (Postgres) --------------------------------------------------------------------


@pytest.mark.db
async def test_replay_end_to_end_offline(
    tmp_path: Path, engine: AsyncEngine, sessions: SessionFactory, no_http: list[str]
) -> None:
    root = tmp_path / "recordings"
    incident_id = await make_recording(root, sessions)
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
    tmp_path: Path, engine: AsyncEngine, sessions: SessionFactory, no_http: list[str]
) -> None:
    root = tmp_path / "recordings"
    await make_recording(root, sessions)
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
    tmp_path: Path, sessions: SessionFactory, no_http: list[str]
) -> None:
    root = tmp_path / "recordings"
    await make_recording(root, sessions)
    (recording_dir("baseline", "s01", root) / RUN_FILE).unlink()
    with pytest.raises(EvalRunError, match=r"S1.*run\.json"):
        await replay_all(
            [S1], agent="baseline", root=root, sessions=sessions, tools_settings=ToolSettings()
        )


@pytest.mark.db
async def test_replay_with_nothing_recorded_stores_nothing(
    tmp_path: Path, sessions: SessionFactory, no_http: list[str]
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
    no_http: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "recordings"
    await make_recording(root, sessions)
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

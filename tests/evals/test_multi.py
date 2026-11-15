"""T-067 multi-local record/replay through the real IncidentWorkflow.

Fake LLM, fake adapters, in-process Temporal; no network, no Ollama. The core tests use stand-in
incident activities and an in-memory evidence sink; the ``db`` test drives the runner end to end.
"""

import asyncio
import json
import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio import activity

import firebreak.agents.runtime as runtime
from firebreak.activities.agents import AgentActivities
from firebreak.agents.settings import AgentSettings
from firebreak.db.repositories import SessionFactory
from firebreak.evals import runner
from firebreak.evals.cassette import META_FILE, RecordingMeta, StaleRecordingError, recording_dir
from firebreak.evals.multi import (
    CALLS_DIR,
    FAILED_FILE,
    LOCAL_TIMEOUT,
    FailedCall,
    MultiRecordError,
    MultiReplayError,
    MultiRun,
    UnsetLLM,
    call_key,
    record_multi,
    replay_multi,
    run_incident_workflow,
)
from firebreak.evals.runner import (
    RUN_FILE,
    _outcome_result,
    agent_config_path,
    agent_config_paths,
    agent_models,
    agent_providers,
    is_profile,
    record_scenario,
    replay_all,
    require_live_env,
)
from firebreak.evals.scenario import load_scenarios
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.llm.errors import LLMConnectionRefused
from firebreak.models.common import Severity
from firebreak.models.evidence import Evidence, EvidenceKind, Finding, SpecialistReport
from firebreak.models.incident import Incident, IncidentState
from firebreak.models.llm import LLMRequest, LLMResponse
from firebreak.models.orchestration import (
    ACTIVITY_LOAD_CONTEXT,
    ACTIVITY_SET_INCIDENT_STATE,
    IncidentContext,
    Plan,
    SpecialistTask,
    StateUpdate,
    TriageResult,
    TriageVerdict,
    WorkflowOutcome,
)
from firebreak.models.reasoning import Hypothesis, InvestigationReport, RootCauseCategory
from firebreak.tools.adapters.loki import LogResult, LogStream
from firebreak.tools.adapters.prometheus import MetricResult, Series
from firebreak.tools.adapters.shopfloor import Deploy
from firebreak.tools.read_tools import ReadAdapters
from firebreak.tools.settings import ToolSettings
from tests.evals.test_runner import live_sessions, no_http, seed_live_incident
from tests.tools.fakes import FakeEvidenceSink, evidence_ids_in, text_response, tool_response

__all__ = ["live_sessions", "no_http"]  # fixtures re-used from test_runner

AT = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)
SCENARIO = "s01"
INCIDENT = "inc_multi1"
SUMMARY = "Investigate incident inc_multi1.\nSeverity: critical. Services involved: orders."
S1 = load_scenarios(Path("evals/scenarios"))["S1"]
PLAN = [("metrics", "is orders erroring?"), ("deploys", "did orders deploy recently?")]
EV = re.compile(r"ev_[0-9a-f]{32}")
QUESTION = re.compile(r"Your question: (.*)$", re.S)


# --- fakes ------------------------------------------------------------------------------------


class SkewedLive:
    """Fake live adapters; ``slow`` methods sleep, so one specialist finishes last."""

    def __init__(self, slow: dict[str, float], *, once: bool = False, version: str = "") -> None:
        self.slow = dict(slow)
        self.once = once  # slow only on the first call of each slow method
        self.version = version or "v2.0.0-bad"
        self.finished: list[str] = []

    async def _wait(self, name: str) -> None:
        delay = self.slow.pop(name, 0.0) if self.once else self.slow.get(name, 0.0)
        await asyncio.sleep(delay)
        self.finished.append(name)

    async def instant(self, query: str, at: datetime) -> MetricResult:
        await self._wait("instant")
        raw = {"resultType": "vector", "result": [{"metric": {"service": "orders"}}]}
        return MetricResult(
            "vector", [Series({"service": "orders"}, [(at.timestamp(), "0.12")])], raw
        )

    async def range(self, query: str, start: datetime, end: datetime, step: str) -> MetricResult:
        await self._wait("range")
        pts = [(start.timestamp() + 60 * i, str(i)) for i in range(5)]
        return MetricResult(
            "matrix", [Series({"service": "orders"}, pts)], {"resultType": "matrix"}
        )

    async def metric_names(self) -> list[str]:
        await self._wait("names")
        return ["up", "http_errors_total"]

    async def query_range(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> LogResult:
        await self._wait("logs")
        ns = int(end.timestamp() * 1e9)
        return LogResult(
            "streams", [LogStream({"service_name": "orders"}, [(ns, "ERROR")])], {"r": 1}
        )

    async def recent_deploys(self, service: str, limit: int) -> list[Deploy]:
        await self._wait("deploys")
        return [
            Deploy(service=service, version=self.version, deployed_at=AT - timedelta(minutes=7))
        ]

    async def aclose(self) -> None:
        raise AssertionError("the eval wrapper must not close the shared adapters per call")


def adapters(live: SkewedLive) -> ReadAdapters:
    return ReadAdapters(metrics=live, logs=live, deploys=live, graph=ServiceGraph.load())


class FakeMultiLLM:
    """Answers as each roster agent, picked from the task text and the tools offered.

    Specialists take two tool turns, then report. ``fail`` maps an agent to how many of its
    calls raise ``LLMConnectionRefused`` (a large number: always); ``delays`` maps
    ``(agent, evidence ids seen)`` to seconds slept before answering.
    """

    def __init__(
        self,
        plan: list[tuple[str, str]],
        *,
        fail: dict[str, int] | None = None,
        delays: dict[tuple[str, int], float] | None = None,
        timeout_s: int = 60,
        invalid_plan: bool = False,
        delay_once: bool = False,
    ) -> None:
        self.plan = plan
        self.fail = dict(fail or {})
        self.delays = dict(delays or {})
        self.delay_once = delay_once
        self.timeout_s = timeout_s
        self.invalid_plan = invalid_plan
        self.agents: list[str] = []

    @staticmethod
    def agent_of(req: LLMRequest) -> str:
        task = req.messages[0]["content"]
        tools = {t.name for t in req.tools}
        if task.endswith("Triage this incident."):
            return "triage"
        if task.endswith("Plan the investigation."):
            return "supervisor"
        if task.endswith("Write the InvestigationReport."):
            return "synthesizer"
        if "promql_query" in tools:
            return "metrics"
        if "recent_deploys" in tools:
            return "deploys"
        raise AssertionError(f"unknown agent for task {task[:80]!r}")

    async def complete(self, req: LLMRequest) -> LLMResponse:
        agent = self.agent_of(req)
        self.agents.append(agent)
        if self.fail.get(agent, 0) > 0:
            self.fail[agent] -= 1
            raise LLMConnectionRefused("model server down")
        seen = evidence_ids_in(req)
        await asyncio.sleep(
            self.delays.pop((agent, len(seen)), 0.0)
            if self.delay_once
            else self.delays.get((agent, len(seen)), 0.0)
        )
        if agent == "triage":
            out = TriageResult(
                verdict=TriageVerdict.INVESTIGATE,
                severity=Severity.CRITICAL,
                affected_services=["orders"],
                rationale="5xx",
            )
            return text_response(out.model_dump_json())
        if agent == "supervisor":
            if self.invalid_plan:
                return text_response('{"tasks": [], "rationale": "nothing to do"}')
            plan = Plan(
                tasks=[
                    SpecialistTask(agent=a, question=q, timeout_s=self.timeout_s)
                    for a, q in self.plan
                ],
                rationale="metrics and deploys",
            )
            return text_response(plan.model_dump_json())
        task = req.messages[0]["content"]
        if agent == "synthesizer":
            ids = list(dict.fromkeys(EV.findall(task)))
            report = InvestigationReport(
                findings=[Finding(claim="bad deploy", evidence_ids=ids, confidence=0.9, agent="s")],
                hypotheses=[
                    Hypothesis(
                        root_cause_category=RootCauseCategory.BAD_DEPLOY,
                        explanation="errors began right after the deploy",
                        supporting_findings=["f0"],
                        confidence=0.85,
                        rank=1,
                    )
                ],
            )
            return text_response(report.model_dump_json(), cost=0.002)
        if agent == "metrics" and not seen:
            return tool_response(
                ("m1", "promql_query", {"query": 'rate(http_errors_total{job="orders"}[1m])'}),
                ("m2", "promql_range", {"query": 'up{job="orders"}', "minutes_back": 30}),
            )
        if agent == "metrics" and len(seen) == 2:
            return tool_response(("m3", "list_metrics", {"match": "http_"}))
        if agent == "deploys" and not seen:
            return tool_response(("d1", "recent_deploys", {"service": "orders"}))
        if agent == "deploys" and len(seen) == 1:
            return tool_response(("d2", "service_graph", {}))
        match = QUESTION.search(task)
        assert match is not None
        report = SpecialistReport(
            agent=agent,
            question=match.group(1),
            findings=[
                Finding(claim=f"{agent} saw it", evidence_ids=seen, confidence=0.8, agent=agent)
            ],
        )
        return text_response(report.model_dump_json())


class FakeIncident:
    """Stands in for ``IncidentActivities`` (no database)."""

    def __init__(self) -> None:
        self.states: list[IncidentState] = []

    @activity.defn(name=ACTIVITY_LOAD_CONTEXT)
    async def load_context(self, incident_id: str) -> IncidentContext:
        # window_closes_at None: T-066's GROUPING wait is a no-op.
        return IncidentContext(incident_id=incident_id, summary=SUMMARY, services=["orders"])

    @activity.defn(name=ACTIVITY_SET_INCIDENT_STATE)
    async def set_incident_state(self, update: StateUpdate) -> None:
        self.states.append(update.state)


class SlowDeploySink(FakeEvidenceSink):
    """Replay-side skew: storing deploy evidence is slow, so ``deploys`` starts its second
    tool turn (and finishes) after ``metrics``, the opposite of the recording."""

    async def record(self, evidence: Evidence, raw: str) -> Evidence:
        if evidence.kind is EvidenceKind.DEPLOY:
            await asyncio.sleep(0.6)
        return await super().record(evidence, raw)


def base_activities(sink: FakeEvidenceSink) -> AgentActivities:
    async def no_db() -> Any:
        raise AssertionError("no database in these tests")

    return AgentActivities(
        llm=UnsetLLM(),
        sessions=no_db,  # type: ignore[arg-type]
        tools_settings=ToolSettings(),
        settings=AgentSettings(profile="multi-local"),
        sink_factory=lambda: sink,
    )


# --- helpers ----------------------------------------------------------------------------------


def directory_for(tmp_path: Path) -> Path:
    return recording_dir("multi-local", SCENARIO, tmp_path)


async def record(
    tmp_path: Path,
    *,
    plan: list[tuple[str, str]] = PLAN,
    live: SkewedLive | None = None,
    llm: FakeMultiLLM | None = None,
    time_skipping: bool = True,
) -> tuple[WorkflowOutcome, MultiRun, FakeEvidenceSink]:
    sink = FakeEvidenceSink()
    live = live or SkewedLive({})
    with record_multi(
        directory_for(tmp_path),
        agent="multi-local",
        scenario=SCENARIO,
        at=AT,
        base=base_activities(sink),
        adapters=adapters(live),
        llm=llm or FakeMultiLLM(plan),
        secrets=["sk-not-a-real-secret-0123456789"],
    ) as run:
        outcome, _ = await run_incident_workflow(
            INCIDENT, run=run, incident_activities=FakeIncident(), time_skipping=time_skipping
        )
    return outcome, run, sink


async def replay(
    tmp_path: Path, sink: FakeEvidenceSink | None = None
) -> tuple[WorkflowOutcome, MultiRun, FakeEvidenceSink]:
    sink = sink or FakeEvidenceSink()
    with replay_multi(directory_for(tmp_path), base=base_activities(sink)) as run:
        outcome, _ = await run_incident_workflow(
            INCIDENT, run=run, incident_activities=FakeIncident(), time_skipping=True
        )
    return outcome, run, sink


def no_timing(outcome: WorkflowOutcome) -> dict[str, Any]:
    doc = outcome.model_dump(mode="json")
    for run in doc["agent_runs"]:
        for call in run["tool_calls"]:
            call["duration_ms"] = 0
    return doc


def second_turns(sink: FakeEvidenceSink) -> list[str]:
    """Tool call ids of the specialists' second turns, in the order their evidence was stored."""
    return [e.tool_call_id for e, _ in sink.stored.values() if e.tool_call_id in ("m3", "d2")]


def kinds(sink: FakeEvidenceSink) -> dict[str, EvidenceKind]:
    return {k: e.kind for k, (e, _) in sink.stored.items()}


def meta(directory: Path) -> RecordingMeta:
    return RecordingMeta.model_validate_json((directory / META_FILE).read_text())


def keys_of(agent: str, question: str, *indexes: int) -> list[str]:
    return [call_key(agent, question, i) for i in indexes]


@pytest.fixture
def no_live_llm(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Replay must never build a live client or the real adapters."""

    def refuse(*a: Any, **kw: Any) -> Any:
        raise AssertionError("replay built a live LLM client or real adapters")

    monkeypatch.setattr("firebreak.activities.agents.build_default_adapters", refuse)
    monkeypatch.setattr("firebreak.llm.routing.RoutingLLMClient.__init__", refuse)
    yield


# --- record / replay --------------------------------------------------------------------------


async def test_skewed_parallel_specialists_replay_identically(
    tmp_path: Path, no_live_llm: None
) -> None:
    # Record on the real-time dev server (what record mode uses) with metrics' tools slow and
    # metrics' second LLM turn slow, so deploys mints its second-turn evidence first.
    live = SkewedLive({"instant": 0.5, "range": 0.2})
    llm = FakeMultiLLM(PLAN, delays={("metrics", 2): 0.6})
    recorded, rec, rec_sink = await record(tmp_path, live=live, llm=llm, time_skipping=False)
    metrics_key, deploys_key = (
        call_key("metrics", PLAN[0][1], 0),
        call_key("deploys", PLAN[1][1], 0),
    )
    assert recorded.final_state is IncidentState.REPORT_ONLY and recorded.report is not None
    # deploys finished first while recording; inside metrics, range finished before instant
    assert rec.completed.index(deploys_key) < rec.completed.index(metrics_key)
    assert live.finished.index("range") < live.finished.index("instant")
    assert second_turns(rec_sink) == ["d2", "m3"]

    # Replay with the opposite skew: storing deploy evidence is now the slow part, so the
    # second tool turns *start* in the other order. One shared id stream would swap their ids.
    replayed, rp, rp_sink = await replay(tmp_path, SlowDeploySink())
    assert rp.completed.index(metrics_key) < rp.completed.index(deploys_key)
    assert second_turns(rp_sink) == ["m3", "d2"]

    assert sorted(rp_sink.stored) == sorted(rec_sink.stored)  # identical evidence ids
    assert {k: raw for k, (_, raw) in rp_sink.stored.items()} == {
        k: raw for k, (_, raw) in rec_sink.stored.items()
    }
    assert no_timing(replayed) == no_timing(recorded)
    assert sorted(rp.completed) == sorted(rec.completed) == rp.recorded_calls
    graded = [
        _outcome_result(S1, o, kinds(s), 12.5).model_dump()
        for o, s in ((recorded, rec_sink), (replayed, rp_sink))
    ]
    assert graded[0] == graded[1]
    assert graded[0]["top1"] and graded[0]["cited_kinds_ok"] and graded[0]["error"] is None
    assert graded[0]["steps"] == 1 + 1 + 3 + 3 + 1
    assert graded[0]["cost_usd"] == pytest.approx(0.001 * 8 + 0.002)


async def test_layout_and_meta(tmp_path: Path) -> None:
    _, rec, _ = await record(tmp_path)
    directory = directory_for(tmp_path)
    calls = sorted(p.name for p in (directory / CALLS_DIR).iterdir())
    expected = sorted(
        [
            "triage-0",
            "supervisor-0",
            "synthesizer-0",
            *keys_of("metrics", PLAN[0][1], 0),
            *keys_of("deploys", PLAN[1][1], 0),
        ]
    )
    assert calls == expected and sorted(rec.completed) == expected
    scenario_meta = meta(directory)
    assert scenario_meta.agent == "multi-local" and scenario_meta.scenario == SCENARIO
    assert scenario_meta.at == AT and scenario_meta.seed == f"{SCENARIO}@{AT.isoformat()}"
    per_call = [meta(directory / CALLS_DIR / c) for c in calls]
    # LLM keys are kept per call; the scenario meta concatenates them in call-key order
    assert scenario_meta.llm_steps == [s for m in per_call for s in m.llm_steps]
    assert scenario_meta.tool_calls == sum(m.tool_calls for m in per_call) == 4  # adapter calls
    for c, m in zip(calls, per_call, strict=True):
        assert m.scenario == f"{SCENARIO}/{c}" and m.seed == f"{SCENARIO}/{c}@{AT.isoformat()}"
        assert (directory / CALLS_DIR / c / "tools.jsonl").exists()
        assert len(list((directory / CALLS_DIR / c / "llm").glob("*.json"))) == len(m.llm_steps)
    # no staging leftovers next to the recording
    assert [p.name for p in directory.parent.iterdir()] == [SCENARIO]


async def test_same_agent_and_question_twice_get_distinct_keys(tmp_path: Path) -> None:
    plan = [PLAN[0], PLAN[0], PLAN[1]]
    recorded, rec, rec_sink = await record(tmp_path, plan=plan)
    dup = keys_of("metrics", PLAN[0][1], 0, 1)
    assert set(dup) <= set(rec.completed)
    assert len(rec_sink.stored) == 3 + 3 + 2  # each duplicate minted its own evidence ids
    replayed, rp, rp_sink = await replay(tmp_path)
    assert sorted(rp_sink.stored) == sorted(rec_sink.stored)
    assert no_timing(replayed) == no_timing(recorded)
    assert sorted(rp.completed) == sorted(rec.completed)


async def test_dismissed_is_a_graded_result(tmp_path: Path) -> None:
    class Noise(FakeMultiLLM):
        async def complete(self, req: LLMRequest) -> LLMResponse:
            out = TriageResult(verdict=TriageVerdict.NOISE, severity=Severity.INFO, rationale="r")
            return text_response(out.model_dump_json())

    recorded, rec, _ = await record(tmp_path, llm=Noise(PLAN))
    assert recorded.final_state is IncidentState.DISMISSED and rec.completed == ["triage-0"]
    replayed, _, _ = await replay(tmp_path)
    assert no_timing(replayed) == no_timing(recorded)
    r = _outcome_result(S1, replayed, {}, 3.0)
    assert (r.top1, r.report_valid, r.latency_s) == (False, False, 3.0)
    assert r.error == "workflow ended dismissed without a report"


# --- drift ------------------------------------------------------------------------------------


async def test_changed_prompt_is_stale(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    await record(tmp_path)
    original = runtime.build_system_prompt
    monkeypatch.setattr(
        runtime, "build_system_prompt", lambda p, s: original(p, s) + "\nOne new line."
    )
    with pytest.raises(StaleRecordingError, match=r"s01/triage-0.*llm step 1"):
        await replay(tmp_path)


async def test_changed_specialist_prompt_is_stale_not_degraded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drift inside one parallel specialist must not turn into a graded 'degraded' result."""
    await record(tmp_path)
    original = runtime.build_system_prompt

    def patched(prompt: str, schema: Any) -> str:
        out = original(prompt, schema)
        return (
            out + "\nchanged" if "deploy" in prompt.lower() and "SpecialistReport" in out else out
        )

    monkeypatch.setattr(runtime, "build_system_prompt", patched)
    with pytest.raises(StaleRecordingError, match=r"s01/deploys-[0-9a-f]{8}-0"):
        await replay(tmp_path)


async def test_missing_call_recording_is_stale(tmp_path: Path) -> None:
    await record(tmp_path)
    key = call_key("deploys", PLAN[1][1], 0)
    for p in sorted((directory_for(tmp_path) / CALLS_DIR / key).rglob("*"), reverse=True):
        p.rmdir() if p.is_dir() else p.unlink()
    (directory_for(tmp_path) / CALLS_DIR / key).rmdir()
    with pytest.raises(StaleRecordingError, match=rf"call {key}.*not in the recording"):
        await replay(tmp_path)


async def test_extra_recorded_call_is_stale(tmp_path: Path) -> None:
    await record(tmp_path)
    calls = directory_for(tmp_path) / CALLS_DIR
    extra = calls / "logs-0000aaaa-0"
    extra.mkdir()
    (extra / META_FILE).write_text((calls / "triage-0" / META_FILE).read_text())
    with pytest.raises(StaleRecordingError, match=r"logs-0000aaaa-0.*never made in replay"):
        await replay(tmp_path)


async def test_terminal_specialist_failure_is_recorded_and_replays_degraded(
    tmp_path: Path,
) -> None:
    """Retries exhausted while recording: kept as failed.json, replayed as the same failure."""
    llm = FakeMultiLLM(PLAN, fail={"deploys": 99})
    recorded, _rec, rec_sink = await record(tmp_path, llm=llm)
    deploys = call_key("deploys", PLAN[1][1], 0)
    assert llm.agents.count("deploys") == 3  # Temporal's retry policy ran: 3 attempts
    assert recorded.final_state is IncidentState.REPORT_ONLY
    assert recorded.degraded_sources == ["deploys"]
    failed = FailedCall.model_validate_json(
        (directory_for(tmp_path) / CALLS_DIR / deploys / FAILED_FILE).read_text()
    )
    assert (failed.agent, failed.attempts, failed.error_type) == (
        "deploys",
        3,
        "LLMConnectionRefused",
    )
    assert sorted(p.name for p in (directory_for(tmp_path) / CALLS_DIR / deploys).iterdir()) == [
        FAILED_FILE
    ]

    replayed, rp, rp_sink = await replay(tmp_path)
    assert rp.replayed_failures == [deploys]
    assert no_timing(replayed) == no_timing(recorded)
    assert sorted(rp_sink.stored) == sorted(rec_sink.stored)
    graded = [
        _outcome_result(S1, o, kinds(s_), 1.0).model_dump()
        for o, s_ in ((recorded, rec_sink), (replayed, rp_sink))
    ]
    assert graded[0] == graded[1]
    assert graded[0]["cited_kinds_ok"] is False  # no deploy evidence: degraded, and graded so


async def test_retried_attempt_is_rerecorded_and_replays_identically(tmp_path: Path) -> None:
    """Attempt 1 hits a refused connection, attempt 2 succeeds: only attempt 2 is kept."""
    llm = FakeMultiLLM(PLAN, fail={"deploys": 1})
    recorded, _rec, rec_sink = await record(tmp_path, llm=llm)
    deploys = call_key("deploys", PLAN[1][1], 0)
    assert recorded.final_state is IncidentState.REPORT_ONLY and recorded.degraded_sources == []
    call_meta = meta(directory_for(tmp_path) / CALLS_DIR / deploys)
    assert call_meta.seed == f"{SCENARIO}/{deploys}#a2@{AT.isoformat()}"  # per-attempt seed
    assert not (directory_for(tmp_path) / CALLS_DIR / deploys / FAILED_FILE).exists()
    replayed, _, rp_sink = await replay(tmp_path)
    assert sorted(rp_sink.stored) == sorted(rec_sink.stored)
    assert no_timing(replayed) == no_timing(recorded)


class ShortTimeoutLLM(FakeMultiLLM):
    def __init__(self, **kw: Any) -> None:
        super().__init__(PLAN, timeout_s=10, **kw)  # the smallest SpecialistTask timeout


async def test_specialist_timeout_then_retry_is_rerecorded(tmp_path: Path) -> None:
    """Attempt 1 runs into its start_to_close (real time, dev server) while still waiting on the
    model; it is stopped just before the deadline and attempt 2 re-records. Only attempt 2 is
    kept, and the stopped attempt never got to mint evidence."""
    llm = ShortTimeoutLLM(delays={("metrics", 0): 15.0}, delay_once=True)
    recorded, _rec, rec_sink = await record(tmp_path, llm=llm, time_skipping=False)
    metrics = call_key("metrics", PLAN[0][1], 0)
    assert recorded.final_state is IncidentState.REPORT_ONLY and recorded.degraded_sources == []
    assert meta(directory_for(tmp_path) / CALLS_DIR / metrics).seed.startswith(
        f"{SCENARIO}/{metrics}#a2@"
    )
    replayed, _, rp_sink = await replay(tmp_path)
    assert no_timing(replayed) == no_timing(recorded)
    assert sorted(rp_sink.stored) == sorted(rec_sink.stored)


async def test_specialist_timeout_on_every_attempt_replays_degraded(tmp_path: Path) -> None:
    """The reviewer's case: a specialist that always times out is a degraded, kept recording."""
    live = SkewedLive({"instant": 13.0})
    recorded, _rec, rec_sink = await record(
        tmp_path, live=live, llm=ShortTimeoutLLM(), time_skipping=False
    )
    metrics = call_key("metrics", PLAN[0][1], 0)
    assert recorded.final_state is IncidentState.REPORT_ONLY
    assert recorded.degraded_sources == ["metrics"]
    failed = FailedCall.model_validate_json(
        (directory_for(tmp_path) / CALLS_DIR / metrics / FAILED_FILE).read_text()
    )
    assert failed.error_type == LOCAL_TIMEOUT and failed.attempts == 3
    replayed, _, rp_sink = await replay(tmp_path)
    assert no_timing(replayed) == no_timing(recorded)
    # Stopped metrics attempts may have minted evidence (their gateway timeouts let them go on)
    # before being stopped: orphans while recording, never produced by replay.
    assert set(rp_sink.stored) <= set(rec_sink.stored)
    orphans = set(rec_sink.stored) - set(rp_sink.stored)
    assert all(rec_sink.stored[o][0].tool_call_id in ("m1", "m2", "m3") for o in orphans)
    assert _outcome_result(S1, replayed, kinds(rp_sink), 1.0) == _outcome_result(
        S1, recorded, kinds(rec_sink), 1.0
    )


async def test_non_specialist_failure_escalates_identically(tmp_path: Path) -> None:
    """Triage fails on every attempt: ESCALATED in both modes; the runner keeps the error."""
    recorded, _, _ = await record(tmp_path, llm=FakeMultiLLM(PLAN, fail={"triage": 99}))
    assert recorded.final_state is IncidentState.ESCALATED and recorded.error
    replayed, rp, _ = await replay(tmp_path)
    info = runner.RecordedRunInfo(
        incident=Incident(
            id=INCIDENT,
            severity=Severity.CRITICAL,
            cluster="checkout",
            services=["orders"],
            alert_ids=[],
            window_closes_at=AT,
        ),
        alerts=[],
        latency_s=1.0,
        final_state=recorded.final_state,
        degraded_sources=recorded.degraded_sources,
        outcome_error=recorded.error,
    )
    checked = runner._check_outcome(SCENARIO, replayed, info, rp)
    assert no_timing(checked) == no_timing(recorded)
    r = _outcome_result(S1, checked, {}, 1.0)
    assert r.error == recorded.error and not r.report_valid
    # a degraded set that no longer matches the recording is drift
    drifted = info.model_copy(update={"degraded_sources": ["metrics"]})
    with pytest.raises(StaleRecordingError, match="workflow outcome"):
        runner._check_outcome(SCENARIO, replayed, drifted, rp)


async def test_escalated_invalid_plan_is_a_graded_result(tmp_path: Path) -> None:
    recorded, _, _ = await record(tmp_path, llm=FakeMultiLLM(PLAN, invalid_plan=True))
    assert recorded.final_state is IncidentState.ESCALATED
    assert recorded.error and "supervisor" in recorded.error
    replayed, _, _ = await replay(tmp_path)
    assert no_timing(replayed) == no_timing(recorded)
    r = _outcome_result(S1, replayed, {}, 2.0)
    assert (r.top1, r.report_valid, r.error) == (False, False, recorded.error)


async def test_recording_problem_aborts_the_workflow_and_keeps_nothing(tmp_path: Path) -> None:
    """A secret in a tool result is a recording problem: fail fast, no synthesizer call."""
    llm = FakeMultiLLM(PLAN, delays={("metrics", 0): 1.0})
    live = SkewedLive({}, version="sk-not-a-real-secret-0123456789")
    with pytest.raises(MultiRecordError, match="secret"):
        await record(tmp_path, live=live, llm=llm)
    assert "synthesizer" not in llm.agents
    assert list(directory_for(tmp_path).parent.iterdir()) == []


async def test_replay_failure_that_is_not_drift_is_not_stale(tmp_path: Path) -> None:
    await record(tmp_path)

    class DownSink(FakeEvidenceSink):
        """The citation check (an infrastructure call) fails on every attempt for deploys."""

        async def exists(self, incident_id: str, evidence_ids: list[str]) -> set[str]:
            if any(
                self.stored[i][0].kind is EvidenceKind.DEPLOY
                for i in evidence_ids
                if i in self.stored
            ):
                raise ConnectionError("database down")
            return await super().exists(incident_id, evidence_ids)

    with pytest.raises(MultiReplayError, match="database down"):
        await replay(tmp_path, DownSink())


async def test_record_refuses_to_clobber(tmp_path: Path) -> None:
    await record(tmp_path)
    with pytest.raises(FileExistsError):
        await record(tmp_path)


# --- profiles in the runner -------------------------------------------------------------------


def test_profile_agents_are_known_without_breaking_single_configs() -> None:
    assert is_profile("multi-local") and not is_profile("baseline")
    assert agent_config_path("multi-local").name == "multi-local"
    assert agent_config_path("baseline-local").name == "baseline-local.yaml"
    assert {p.stem for p in agent_config_paths("multi-local")} == {
        "triage",
        "supervisor",
        "metrics",
        "logs",
        "deploys",
        "synthesizer",
    }
    assert agent_models("multi-local") == ["qwen3:8b"]
    assert agent_providers("multi-local") == agent_providers("baseline-local")
    require_live_env({"FIREBREAK_EVAL_LIVE": "1"}, agent="multi-local")  # all local: no key
    with pytest.raises(runner.EvalRunError, match="unknown agent"):
        agent_config_path("nope")


# --- end to end through the runner (Postgres) -------------------------------------------------


@pytest.mark.db
async def test_runner_records_and_replays_multi_local(
    tmp_path: Path,
    engine: AsyncEngine,
    sessions: SessionFactory,
    live_sessions: SessionFactory,
    no_http: list[str],
    no_live_llm: None,
) -> None:
    root = tmp_path / "recordings"
    incident_id = await seed_live_incident(live_sessions)  # window_closes_at in the future
    recorded = await record_scenario(
        S1,
        incident_id,
        agent="multi-local",
        root=root,
        sessions=sessions,
        live_sessions=live_sessions,
        tools_settings=ToolSettings(),
        llm=FakeMultiLLM(PLAN),
        adapters=adapters(SkewedLive({"instant": 0.3})),
        env={"FIREBREAK_EVAL_LIVE": "1"},
    )
    assert recorded.result is not None and recorded.result.top1, recorded.result
    directory = recording_dir("multi-local", "s01", root)
    info = json.loads((directory / RUN_FILE).read_text())
    assert info["incident"]["id"] == incident_id and info["latency_s"] > 0

    outcome = await replay_all(
        [S1], agent="multi-local", root=root, sessions=sessions, tools_settings=ToolSettings()
    )
    (graded,) = outcome.graded
    assert graded.result is not None
    assert graded.result.model_dump(exclude={"latency_s"}) == recorded.result.model_dump(
        exclude={"latency_s"}
    )
    assert graded.result.latency_s == pytest.approx(info["latency_s"])
    assert graded.prompt_sha256 == meta(directory).prompt_sha256
    assert no_http == []


async def test_replay_retry_reuses_the_call_key_and_ids(tmp_path: Path) -> None:
    """A transient failure in replay is retried by Temporal with the same key, seed and ids."""
    recorded, _, rec_sink = await record(tmp_path)

    class FlakyOnce(FakeEvidenceSink):
        failed = False

        async def exists(self, incident_id: str, evidence_ids: list[str]) -> set[str]:
            if not FlakyOnce.failed and any(
                self.stored[i][0].kind is EvidenceKind.DEPLOY
                for i in evidence_ids
                if i in self.stored
            ):
                FlakyOnce.failed = True
                raise ConnectionError("database blip")
            return await super().exists(incident_id, evidence_ids)

    replayed, rp, rp_sink = await replay(tmp_path, FlakyOnce())
    assert FlakyOnce.failed
    deploys = call_key("deploys", PLAN[1][1], 0)
    assert rp.completed.count(deploys) == 1
    assert "database blip" in rp._last_error[deploys]  # attempt 1 really failed
    assert sorted(rp_sink.stored) == sorted(rec_sink.stored)
    assert no_timing(replayed) == no_timing(recorded)

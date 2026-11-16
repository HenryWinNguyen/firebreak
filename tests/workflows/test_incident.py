"""IncidentWorkflow against Temporal's in-process test server with stub activities."""

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from typing import Any

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from firebreak.models.agents import AgentRunResult
from firebreak.models.common import Severity
from firebreak.models.evidence import Finding, SpecialistReport
from firebreak.models.incident import IncidentState
from firebreak.models.llm import Usage
from firebreak.models.orchestration import (
    ACTIVITY_LOAD_CONTEXT,
    ACTIVITY_RUN_AGENT,
    ACTIVITY_SET_INCIDENT_STATE,
    AGENT_TASK_QUEUE,
    ORCHESTRATOR_TASK_QUEUE,
    AgentTask,
    IncidentContext,
    Plan,
    SpecialistTask,
    StateUpdate,
    TriageResult,
    TriageVerdict,
    WorkflowOutcome,
)
from firebreak.models.reasoning import (
    Hypothesis,
    InvestigationReport,
    RootCauseCategory,
)
from firebreak.workflows.client import start_incident_workflow
from firebreak.workflows.incident import (
    MAX_ATTEMPTS,
    SUPERVISOR_TIMEOUT,
    SYNTHESIZER_TIMEOUT,
    TRIAGE_TIMEOUT,
    IncidentWorkflow,
    workflow_id,
)

INCIDENT = "inc_test1"

Behavior = Callable[[AgentTask], AgentRunResult]


def _run(task: AgentTask, output: dict[str, Any], **kw: Any) -> AgentRunResult:
    base: dict[str, Any] = {
        "agent": task.agent,
        "incident_id": task.incident_id,
        "output": output,
        "steps": 1,
        "tool_calls": [],
        "usage": Usage(input_tokens=10, output_tokens=5, cost_usd=0.01),
        "stopped_reason": "final_output",
    }
    base.update(kw)
    return AgentRunResult(**base)


def triage_ok(verdict: TriageVerdict = TriageVerdict.INVESTIGATE) -> Behavior:
    def fn(task: AgentTask) -> AgentRunResult:
        out = TriageResult(verdict=verdict, severity=Severity.CRITICAL, rationale="r")
        return _run(task, out.model_dump(mode="json"))

    return fn


def plan_ok(*agents: str) -> Behavior:
    def fn(task: AgentTask) -> AgentRunResult:
        plan = Plan(
            tasks=[SpecialistTask(agent=a, question=f"q-{a}", timeout_s=10) for a in agents],
            rationale="r",
        )
        return _run(task, plan.model_dump(mode="json"))

    return fn


def specialist_ok(task: AgentTask) -> AgentRunResult:
    report = SpecialistReport(
        agent=task.agent,
        question=task.question or "",
        findings=[Finding(claim="c", evidence_ids=["ev_1"], confidence=0.9, agent=task.agent)],
    )
    return _run(task, report.model_dump(mode="json"))


def synth_ok(task: AgentTask) -> AgentRunResult:
    finding = Finding(claim="c", evidence_ids=["ev_1"], confidence=0.9, agent="metrics")
    report = InvestigationReport(
        findings=[finding],
        hypotheses=[
            Hypothesis(
                root_cause_category=RootCauseCategory.BAD_DEPLOY,
                explanation="e",
                supporting_findings=["f0"],
                confidence=0.8,
                rank=1,
            )
        ],
    )
    return _run(task, report.model_dump(mode="json"))


class StubActivities:
    """Stands in for IncidentActivities and T-065's run_agent."""

    def __init__(self, behaviors: dict[str, Behavior]) -> None:
        self.behaviors = behaviors
        self.states: list[IncidentState] = []
        self.contexts: dict[str, IncidentContext] = {}
        self.calls: list[str] = []

    @activity.defn(name=ACTIVITY_LOAD_CONTEXT)
    async def load_context(self, incident_id: str) -> IncidentContext:
        return IncidentContext(incident_id=incident_id, summary="facts", services=["orders"])

    @activity.defn(name=ACTIVITY_SET_INCIDENT_STATE)
    async def set_incident_state(self, update: StateUpdate) -> None:
        self.states.append(update.state)

    @activity.defn(name=ACTIVITY_RUN_AGENT)
    async def run_agent(self, task: AgentTask) -> AgentRunResult:
        self.calls.append(task.agent)
        self.contexts[task.agent] = task.context
        return self.behaviors[task.agent](task)


HAPPY: dict[str, Behavior] = {
    "triage": triage_ok(),
    "supervisor": plan_ok("metrics", "logs"),
    "metrics": specialist_ok,
    "logs": specialist_ok,
    "synthesizer": synth_ok,
}


@asynccontextmanager
async def env_client(
    *, time_skipping: bool = True
) -> AsyncIterator[tuple[WorkflowEnvironment, Client]]:
    start = (
        WorkflowEnvironment.start_time_skipping
        if time_skipping
        else WorkflowEnvironment.start_local
    )
    async with await start(data_converter=pydantic_data_converter) as env:
        yield env, env.client


def make_workers(client: Client, stub: StubActivities, **kw: Any) -> tuple[Worker, Worker]:
    wf = Worker(client, task_queue=ORCHESTRATOR_TASK_QUEUE, workflows=[IncidentWorkflow])
    act = Worker(
        client,
        task_queue=AGENT_TASK_QUEUE,
        activities=[stub.load_context, stub.set_incident_state, stub.run_agent],
        **kw,
    )
    return wf, act


async def run_incident(behaviors: dict[str, Behavior]) -> tuple[WorkflowOutcome, StubActivities]:
    stub = StubActivities(behaviors)
    async with env_client() as (_, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            return await handle.result(), stub


async def test_happy_path() -> None:
    out, stub = await run_incident(HAPPY)
    assert out.final_state == IncidentState.REPORT_ONLY
    assert out.report is not None and out.error is None
    assert out.degraded_sources == []
    assert {r.agent for r in out.agent_runs} == {
        "triage",
        "supervisor",
        "metrics",
        "logs",
        "synthesizer",
    }
    assert out.usage.cost_usd == pytest.approx(0.05)
    assert out.usage.input_tokens == 50
    # the synthesizer sees the reports, specialists see the plan and triage
    synth_ctx = stub.contexts["synthesizer"]
    assert [r.agent for r in synth_ctx.reports] == ["metrics", "logs"]
    assert stub.contexts["metrics"].plan is not None
    assert stub.contexts["metrics"].triage is not None


async def test_states_recorded_in_order() -> None:
    _, stub = await run_incident(HAPPY)
    assert stub.states == [
        IncidentState.GROUPING,
        IncidentState.TRIAGE,
        IncidentState.PLANNING,
        IncidentState.INVESTIGATING,
        IncidentState.SYNTHESIZING,
        IncidentState.REPORT_ONLY,
    ]


@pytest.mark.parametrize("verdict", [TriageVerdict.NOISE, TriageVerdict.DUPLICATE])
async def test_noise_is_dismissed(verdict: TriageVerdict) -> None:
    out, stub = await run_incident({"triage": triage_ok(verdict)})
    assert out.final_state == IncidentState.DISMISSED
    assert out.report is None
    assert stub.calls == ["triage"]
    assert stub.states == [IncidentState.GROUPING, IncidentState.TRIAGE, IncidentState.DISMISSED]


def _raises(task: AgentTask) -> AgentRunResult:
    raise ApplicationError("boom", non_retryable=True)


async def test_one_specialist_fails_is_degraded() -> None:
    out, stub = await run_incident({**HAPPY, "logs": _raises})
    assert out.final_state == IncidentState.REPORT_ONLY
    assert out.degraded_sources == ["logs"]
    assert [r.agent for r in stub.contexts["synthesizer"].reports] == ["metrics"]
    assert stub.contexts["synthesizer"].degraded_sources == ["logs"]


async def test_specialist_without_valid_report_is_degraded() -> None:
    def no_output(task: AgentTask) -> AgentRunResult:
        return _run(task, {}, stopped_reason="max_steps", error="ran out of steps")

    def wrong_agent(task: AgentTask) -> AgentRunResult:
        out = specialist_ok(task).model_dump()["output"]
        out["agent"] = "someone-else"
        return _run(task, out)

    out, _ = await run_incident(
        {
            **HAPPY,
            "supervisor": plan_ok("metrics", "logs", "deploys"),
            "logs": no_output,
            "deploys": wrong_agent,
        }
    )
    assert out.final_state == IncidentState.REPORT_ONLY
    assert out.degraded_sources == ["logs", "deploys"]


def plan_raw(*tasks: tuple[str, str]) -> Behavior:
    """A supervisor output that the v1.9 ``Plan`` schema rejects (built as a dict, not a model)."""

    def fn(task: AgentTask) -> AgentRunResult:
        out = {"tasks": [{"agent": a, "question": q} for a, q in tasks], "rationale": "r"}
        return _run(task, out)

    return fn


async def test_plan_naming_a_non_specialist_escalates() -> None:
    """v1.9: an unknown agent fails Plan validation (the runtime repair step tried first)."""
    plan = plan_raw(("metrics", "q"), ("service_graph", "q"))
    out, stub = await run_incident({**HAPPY, "supervisor": plan})
    assert out.final_state == IncidentState.ESCALATED
    assert out.error is not None and "supervisor" in out.error
    assert out.dropped_tasks == []
    assert stub.calls == ["triage", "supervisor"]


async def test_plan_with_empty_question_escalates() -> None:
    out, stub = await run_incident({**HAPPY, "supervisor": plan_raw(("metrics", ""))})
    assert out.final_state == IncidentState.ESCALATED
    assert out.error is not None and "supervisor" in out.error
    assert stub.calls == ["triage", "supervisor"]


async def test_agent_activities_scheduled_with_backoff_and_timeouts() -> None:
    stub = StubActivities(HAPPY)
    async with env_client() as (_, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            await handle.result()
            history = await handle.fetch_history()
    timeouts: list[int] = []
    for ev in history.events:
        if not ev.HasField("activity_task_scheduled_event_attributes"):
            continue
        attrs = ev.activity_task_scheduled_event_attributes
        if attrs.activity_type.name != ACTIVITY_RUN_AGENT:
            continue
        rp = attrs.retry_policy
        assert rp.maximum_attempts == MAX_ATTEMPTS
        assert rp.initial_interval.ToTimedelta() >= timedelta(seconds=10)
        assert rp.backoff_coefficient == 2.0
        timeouts.append(int(attrs.start_to_close_timeout.ToSeconds()))
    # triage, supervisor, two specialists (plan_ok sets timeout_s=10), synthesizer
    assert timeouts[0] == int(TRIAGE_TIMEOUT.total_seconds()) == 900
    assert timeouts[1] == int(SUPERVISOR_TIMEOUT.total_seconds()) == 1200
    assert sorted(timeouts[2:4]) == [10, 10]
    assert timeouts[4] == int(SYNTHESIZER_TIMEOUT.total_seconds()) == 1800


async def test_specialist_retried_then_succeeds() -> None:
    attempts = {"n": 0}

    def flaky(task: AgentTask) -> AgentRunResult:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("transient")
        return specialist_ok(task)

    out, _ = await run_incident({**HAPPY, "logs": flaky})
    assert attempts["n"] == 3
    assert out.degraded_sources == []


async def test_retries_capped_at_three_attempts() -> None:
    attempts = {"n": 0}

    def always(task: AgentTask) -> AgentRunResult:
        attempts["n"] += 1
        raise RuntimeError("down")

    out, _ = await run_incident({**HAPPY, "logs": always})
    assert attempts["n"] == 3
    assert out.degraded_sources == ["logs"]


async def test_all_specialists_fail_escalates() -> None:
    out, stub = await run_incident({**HAPPY, "metrics": _raises, "logs": _raises})
    assert out.final_state == IncidentState.ESCALATED
    assert out.report is None
    assert out.error is not None and "all specialists failed" in out.error
    assert out.degraded_sources == ["metrics", "logs"]
    assert "synthesizer" not in stub.calls
    assert stub.states[-1] == IncidentState.ESCALATED


async def test_invalid_supervisor_output_escalates() -> None:
    def bad_plan(task: AgentTask) -> AgentRunResult:
        return _run(task, {"tasks": []})

    out, stub = await run_incident({**HAPPY, "supervisor": bad_plan})
    assert out.final_state == IncidentState.ESCALATED
    assert out.error is not None and "supervisor" in out.error
    assert stub.calls == ["triage", "supervisor"]
    assert stub.states == [
        IncidentState.GROUPING,
        IncidentState.TRIAGE,
        IncidentState.PLANNING,
        IncidentState.ESCALATED,
    ]


async def test_triage_without_output_escalates() -> None:
    def stopped(task: AgentTask) -> AgentRunResult:
        return _run(task, {}, stopped_reason="budget", error="over budget")

    out, _ = await run_incident({**HAPPY, "triage": stopped})
    assert out.final_state == IncidentState.ESCALATED
    assert out.error is not None and "budget" in out.error


async def test_synthesizer_activity_failure_escalates() -> None:
    out, _ = await run_incident({**HAPPY, "synthesizer": _raises})
    assert out.final_state == IncidentState.ESCALATED
    assert out.error is not None and "synthesizer" in out.error
    assert "ApplicationError: boom" in out.error  # root cause, not the ActivityError wrapper
    assert len(out.agent_runs) == 4


async def test_unknown_incident_fails_workflow() -> None:
    class Missing(StubActivities):
        @activity.defn(name=ACTIVITY_LOAD_CONTEXT)
        async def load_context(self, incident_id: str) -> IncidentContext:
            raise ApplicationError("nope", non_retryable=True)

    stub = Missing(HAPPY)
    async with env_client() as (_, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            with pytest.raises(WorkflowFailureError):
                await handle.result()


async def test_duplicate_start_refused() -> None:
    stub = StubActivities(HAPPY)
    async with env_client() as (_, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            assert handle.id == workflow_id(INCIDENT) == f"incident-{INCIDENT}"
            await handle.result()
            with pytest.raises(WorkflowAlreadyStartedError):
                await start_incident_workflow(client, INCIDENT)


async def test_worker_killed_mid_specialist_resumes() -> None:
    """M5 acceptance: stop the worker while a specialist is in flight; a new one finishes it.

    This one runs on the real-time dev server with a 10 s specialist timeout: the time-skipping
    server does not skip while the killed activity attempt is still "in flight" (verified:
    both auto-skip and ``env.sleep`` hang), so the dead attempt's start_to_close really elapses.
    """
    in_flight = asyncio.Event()
    attempts = {"n": 0}

    class Hanging(StubActivities):
        @activity.defn(name=ACTIVITY_RUN_AGENT)
        async def run_agent(self, task: AgentTask) -> AgentRunResult:
            if task.agent == "logs":
                attempts["n"] += 1
                if attempts["n"] == 1:
                    in_flight.set()
                    await asyncio.sleep(3600)  # the "process" dies here
            self.calls.append(task.agent)
            return self.behaviors[task.agent](task)

    stub = Hanging(HAPPY)
    async with env_client(time_skipping=False) as (_, client):
        wf1, act1 = make_workers(client, stub, graceful_shutdown_timeout=timedelta(0))
        async with AsyncExitStack() as first_process:
            await first_process.enter_async_context(wf1)
            await first_process.enter_async_context(act1)
            handle = await start_incident_workflow(client, INCIDENT)
            await asyncio.wait_for(in_flight.wait(), timeout=30)
        # leaving the stack stops both workers: the first "process" is dead

        wf2, act2 = make_workers(client, stub)
        async with wf2, act2:
            # the dead attempt only fails once its 10 s start_to_close elapses (real time)
            out = await asyncio.wait_for(handle.result(), timeout=60)
    assert attempts["n"] >= 2
    assert out.final_state == IncidentState.REPORT_ONLY
    assert out.report is not None
    assert out.degraded_sources == []

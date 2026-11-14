import asyncio
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
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
from firebreak.models.reasoning import Hypothesis, InvestigationReport, RootCauseCategory
from firebreak.workflows.client import start_incident_workflow
from firebreak.workflows.incident import IncidentWorkflow

INC = "inc_1"


@pytest.fixture
async def env() -> AsyncIterator[WorkflowEnvironment]:
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter
    ) as e:
        yield e


def _run(agent: str, output: dict[str, Any], **kw: Any) -> AgentRunResult:
    base: dict[str, Any] = {
        "agent": agent,
        "incident_id": INC,
        "output": output,
        "steps": 1,
        "tool_calls": [],
        "usage": Usage(input_tokens=10, output_tokens=5, cost_usd=0.01),
        "stopped_reason": "final_output",
    }
    base.update(kw)
    return AgentRunResult(**base)


def triage_out(verdict: TriageVerdict = TriageVerdict.INVESTIGATE) -> dict[str, Any]:
    return TriageResult(
        verdict=verdict, severity=Severity.CRITICAL, affected_services=["orders"], rationale="r"
    ).model_dump(mode="json")


def plan_out(*agents: str, timeout_s: int = 60) -> dict[str, Any]:
    return Plan(
        tasks=[SpecialistTask(agent=a, question=f"q-{a}", timeout_s=timeout_s) for a in agents],
        rationale="r",
    ).model_dump(mode="json")


def specialist_out(agent: str) -> dict[str, Any]:
    return SpecialistReport(
        agent=agent,
        question=f"q-{agent}",
        findings=[Finding(claim="c", evidence_ids=["ev_1"], confidence=0.8, agent=agent)],
    ).model_dump(mode="json")


def report_out() -> dict[str, Any]:
    f = Finding(claim="c", evidence_ids=["ev_1"], confidence=0.8, agent="metrics")
    return InvestigationReport(
        findings=[f],
        hypotheses=[
            Hypothesis(
                root_cause_category=RootCauseCategory.BAD_DEPLOY,
                explanation="e",
                supporting_findings=["f0"],
                confidence=0.7,
                rank=1,
            )
        ],
    ).model_dump(mode="json")


Handler = Callable[[AgentTask], AgentRunResult | Any]


class Stubs:
    """Stub activities. ``agents`` maps agent name -> handler (sync or async)."""

    def __init__(self, agents: dict[str, Handler]) -> None:
        self.agents = agents
        self.states: list[IncidentState] = []
        self.tasks: list[AgentTask] = []

    @activity.defn(name=ACTIVITY_LOAD_CONTEXT)
    async def load_context(self, incident_id: str) -> IncidentContext:
        return IncidentContext(incident_id=incident_id, summary="facts", services=["orders"])

    @activity.defn(name=ACTIVITY_SET_INCIDENT_STATE)
    async def set_state(self, update: StateUpdate) -> None:
        self.states.append(update.state)

    @activity.defn(name=ACTIVITY_RUN_AGENT)
    async def run_agent(self, task: AgentTask) -> AgentRunResult:
        self.tasks.append(task)
        result = self.agents[task.agent](task)
        if asyncio.iscoroutine(result):
            result = await result
        assert isinstance(result, AgentRunResult)
        return result


def happy_agents(*specialists: str) -> dict[str, Handler]:
    agents: dict[str, Handler] = {
        "triage": lambda t: _run("triage", triage_out()),
        "supervisor": lambda t: _run("supervisor", plan_out(*specialists)),
        "synthesizer": lambda t: _run("synthesizer", report_out()),
    }
    for s in specialists:
        agents[s] = lambda t, s=s: _run(s, specialist_out(s))
    return agents


async def execute(env: WorkflowEnvironment, stubs: Stubs) -> WorkflowOutcome:
    client: Client = env.client
    async with (
        Worker(client, task_queue=ORCHESTRATOR_TASK_QUEUE, workflows=[IncidentWorkflow]),
        Worker(
            client,
            task_queue=AGENT_TASK_QUEUE,
            activities=[stubs.load_context, stubs.set_state, stubs.run_agent],
        ),
    ):
        handle = await start_incident_workflow(client, f"{INC}-{uuid.uuid4().hex[:6]}")
        return await handle.result()


async def test_happy_path(env: WorkflowEnvironment) -> None:
    stubs = Stubs(happy_agents("metrics", "logs"))
    out = await execute(env, stubs)
    assert out.final_state is IncidentState.REPORT_ONLY
    assert out.report is not None and out.error is None
    assert out.degraded_sources == []
    assert [r.agent for r in out.agent_runs].count("metrics") == 1
    assert len(out.agent_runs) == 5
    assert out.usage.cost_usd == pytest.approx(0.05)
    assert out.usage.input_tokens == 50
    assert stubs.states == [
        IncidentState.TRIAGE,
        IncidentState.PLANNING,
        IncidentState.INVESTIGATING,
        IncidentState.SYNTHESIZING,
        IncidentState.REPORT_ONLY,
    ]
    synth = next(t for t in stubs.tasks if t.agent == "synthesizer")
    assert len(synth.context.reports) == 2
    assert synth.context.triage is not None and synth.context.plan is not None
    specialist = next(t for t in stubs.tasks if t.agent == "metrics")
    assert specialist.question == "q-metrics"


@pytest.mark.parametrize("verdict", [TriageVerdict.NOISE, TriageVerdict.DUPLICATE])
async def test_noise_is_dismissed(env: WorkflowEnvironment, verdict: TriageVerdict) -> None:
    agents = happy_agents("metrics")
    agents["triage"] = lambda t: _run("triage", triage_out(verdict))
    stubs = Stubs(agents)
    out = await execute(env, stubs)
    assert out.final_state is IncidentState.DISMISSED
    assert out.report is None
    assert stubs.states == [IncidentState.TRIAGE, IncidentState.DISMISSED]
    assert [t.agent for t in stubs.tasks] == ["triage"]


async def test_one_specialist_fails_is_degraded(env: WorkflowEnvironment) -> None:
    agents = happy_agents("metrics", "logs", "deploys")

    def boom(t: AgentTask) -> AgentRunResult:
        raise ApplicationError("backend down", non_retryable=True)

    agents["logs"] = boom
    agents["deploys"] = lambda t: _run("deploys", {"nonsense": 1})  # invalid report
    out = await execute(env, Stubs(agents))
    assert out.final_state is IncidentState.REPORT_ONLY
    assert out.degraded_sources == ["logs", "deploys"]
    assert out.report is not None


async def test_specialist_with_wrong_agent_is_degraded(env: WorkflowEnvironment) -> None:
    agents = happy_agents("metrics", "logs")
    agents["logs"] = lambda t: _run("logs", specialist_out("metrics"))
    out = await execute(env, Stubs(agents))
    assert out.degraded_sources == ["logs"]
    assert out.final_state is IncidentState.REPORT_ONLY


async def test_specialist_budget_stop_is_degraded(env: WorkflowEnvironment) -> None:
    agents = happy_agents("metrics", "logs")
    agents["logs"] = lambda t: _run("logs", {}, stopped_reason="budget", error="over budget")
    stubs = Stubs(agents)
    out = await execute(env, stubs)
    assert out.degraded_sources == ["logs"]
    synth = next(t for t in stubs.tasks if t.agent == "synthesizer")
    assert synth.context.degraded_sources == ["logs"]
    assert len(out.agent_runs) == 5  # the failed run is still recorded


async def test_all_specialists_fail_escalates(env: WorkflowEnvironment) -> None:
    agents = happy_agents("metrics", "logs")

    def boom(t: AgentTask) -> AgentRunResult:
        raise ApplicationError("down", non_retryable=True)

    agents["metrics"] = boom
    agents["logs"] = boom
    stubs = Stubs(agents)
    out = await execute(env, stubs)
    assert out.final_state is IncidentState.ESCALATED
    assert out.error is not None and "all specialists failed" in out.error
    assert out.report is None
    assert stubs.states[-1] is IncidentState.ESCALATED
    assert IncidentState.SYNTHESIZING not in stubs.states


async def test_invalid_supervisor_output_escalates(env: WorkflowEnvironment) -> None:
    agents = happy_agents("metrics")
    agents["supervisor"] = lambda t: _run("supervisor", {"tasks": []})
    stubs = Stubs(agents)
    out = await execute(env, stubs)
    assert out.final_state is IncidentState.ESCALATED
    assert out.error is not None and "supervisor" in out.error
    assert stubs.states == [IncidentState.TRIAGE, IncidentState.PLANNING, IncidentState.ESCALATED]


async def test_failed_triage_escalates(env: WorkflowEnvironment) -> None:
    agents = happy_agents("metrics")
    agents["triage"] = lambda t: _run("triage", {}, stopped_reason="error", error="refused")
    out = await execute(env, Stubs(agents))
    assert out.final_state is IncidentState.ESCALATED
    assert out.error is not None and "triage" in out.error


async def test_invalid_synthesizer_output_escalates(env: WorkflowEnvironment) -> None:
    agents = happy_agents("metrics")
    agents["synthesizer"] = lambda t: _run("synthesizer", {"findings": []})
    out = await execute(env, Stubs(agents))
    assert out.final_state is IncidentState.ESCALATED
    assert out.error is not None and "synthesizer" in out.error
    assert out.report is None


async def test_retry_then_success(env: WorkflowEnvironment) -> None:
    agents = happy_agents("metrics")
    calls = {"n": 0}

    def flaky(t: AgentTask) -> AgentRunResult:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return _run("metrics", specialist_out("metrics"))

    agents["metrics"] = flaky
    out = await execute(env, Stubs(agents))
    assert calls["n"] == 3
    assert out.degraded_sources == []


async def test_duplicate_start_is_refused(env: WorkflowEnvironment) -> None:
    stubs = Stubs(happy_agents("metrics"))
    client: Client = env.client
    async with (
        Worker(client, task_queue=ORCHESTRATOR_TASK_QUEUE, workflows=[IncidentWorkflow]),
        Worker(
            client,
            task_queue=AGENT_TASK_QUEUE,
            activities=[stubs.load_context, stubs.set_state, stubs.run_agent],
        ),
    ):
        handle = await start_incident_workflow(client, "inc_dup")
        assert handle.id == "incident-inc_dup"
        await handle.result()
        with pytest.raises(WorkflowAlreadyStartedError):
            await start_incident_workflow(client, "inc_dup")


@pytest.fixture
async def local_env() -> AsyncIterator[WorkflowEnvironment]:
    async with await WorkflowEnvironment.start_local(data_converter=pydantic_data_converter) as e:
        yield e


async def test_kill_worker_mid_specialist_then_resume(local_env: WorkflowEnvironment) -> None:
    """M5 acceptance: stop the worker with a specialist in flight; a new worker finishes it.

    Runs on the real-time dev server rather than the time-skipping one: the time-skipping
    server stalled a workflow task intermittently in CI after a worker shutdown. No timeout
    has to elapse here (the cancelled activity fails fast and is retried), so wall time is
    short either way.
    """
    env = local_env
    in_flight = asyncio.Event()
    release = asyncio.Event()
    attempts = {"metrics": 0}

    async def metrics(t: AgentTask) -> AgentRunResult:
        attempts["metrics"] += 1
        if attempts["metrics"] == 1:
            in_flight.set()
            await release.wait()  # "hangs" until the worker is killed (cancelled)
        return _run("metrics", specialist_out("metrics"))

    agents = happy_agents("metrics", "logs")
    agents["metrics"] = metrics
    stubs = Stubs(agents)
    client: Client = env.client

    def agent_worker() -> Worker:
        return Worker(
            client,
            task_queue=AGENT_TASK_QUEUE,
            activities=[stubs.load_context, stubs.set_state, stubs.run_agent],
        )

    async with Worker(client, task_queue=ORCHESTRATOR_TASK_QUEUE, workflows=[IncidentWorkflow]):
        first = agent_worker()
        async with first:
            handle = await start_incident_workflow(client, "inc_kill")
            await asyncio.wait_for(in_flight.wait(), timeout=30)
        # first worker is shut down here with the metrics activity still running
        release.set()
        async with agent_worker():
            try:
                out = await asyncio.wait_for(handle.result(), timeout=60)
            except TimeoutError:
                history = await handle.fetch_history()
                events = [f"{e.event_id} {EventType.Name(e.event_type)}" for e in history.events]
                pytest.fail(f"workflow did not finish; attempts={attempts}; history: {events}")
    assert out.final_state is IncidentState.REPORT_ONLY
    assert out.report is not None
    assert out.degraded_sources == []
    assert attempts["metrics"] >= 2

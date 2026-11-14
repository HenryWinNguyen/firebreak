"""IncidentWorkflow's GROUPING wait: window timer, critical early close, context reload."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import pytest
from temporalio import activity
from temporalio.exceptions import WorkflowAlreadyStartedError

from firebreak.models.agents import AgentRunResult
from firebreak.models.common import Severity
from firebreak.models.incident import IncidentState
from firebreak.models.orchestration import (
    ACTIVITY_LOAD_CONTEXT,
    ACTIVITY_RUN_AGENT,
    AgentTask,
    AlertAdded,
    IncidentContext,
    TriageVerdict,
)
from firebreak.workflows.client import signal_alert_added, start_incident_workflow
from firebreak.workflows.incident import IncidentWorkflow
from tests.workflows.test_incident import (
    HAPPY,
    INCIDENT,
    Behavior,
    StubActivities,
    env_client,
    make_workers,
    triage_ok,
)

NOISE: dict[str, Behavior] = {"triage": triage_ok(TriageVerdict.NOISE)}


class WindowStub(StubActivities):
    """``load_context`` returns a window; the second load shows an alert that joined late."""

    def __init__(self, window: datetime | None, behaviors: dict[str, Behavior] = NOISE) -> None:
        super().__init__(behaviors)
        self.window = window
        self.loads = 0

    @activity.defn(name=ACTIVITY_LOAD_CONTEXT)
    async def load_context(self, incident_id: str) -> IncidentContext:
        self.loads += 1
        return IncidentContext(
            incident_id=incident_id,
            summary=f"load-{self.loads}",
            services=["orders"] if self.loads == 1 else ["orders", "inventory"],
            window_closes_at=self.window,
        )


def in_(seconds: float) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


async def test_waits_for_window_then_triages() -> None:
    stub = WindowStub(in_(300))
    async with env_client() as (env, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            await env.sleep(timedelta(seconds=100))
            assert stub.calls == []
            assert stub.states == [IncidentState.GROUPING]
            await handle.result()
    assert stub.calls == ["triage"]


async def test_alert_joining_during_window_reaches_triage() -> None:
    stub = WindowStub(in_(300))
    async with env_client() as (_, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            await handle.result()
    assert stub.loads == 2
    assert stub.contexts["triage"].summary == "load-2"
    assert stub.contexts["triage"].services == ["orders", "inventory"]


async def test_critical_signal_closes_window_early() -> None:
    stub = WindowStub(in_(3600))
    async with env_client() as (env, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            await env.sleep(timedelta(seconds=10))
            assert stub.calls == []
            await handle.signal(
                IncidentWorkflow.alert_added, AlertAdded(alert_id="al_1", severity=Severity.WARNING)
            )
            await env.sleep(timedelta(seconds=10))
            assert stub.calls == []  # non-critical does not close the window
            await handle.signal(
                IncidentWorkflow.alert_added,
                AlertAdded(alert_id="al_2", severity=Severity.CRITICAL),
            )
            await env.sleep(timedelta(seconds=10))
            assert stub.calls == ["triage"]  # an hour early
            await handle.result()


async def test_critical_signal_with_start_closes_window() -> None:
    """Signal-with-start delivers the signal before ``run``: state must be set in __init__."""
    stub = WindowStub(in_(3600))
    async with env_client() as (env, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            await signal_alert_added(
                client, INCIDENT, AlertAdded(alert_id="al_1", severity=Severity.CRITICAL)
            )
            await env.sleep(timedelta(seconds=10))
            assert stub.calls == ["triage"]


@pytest.mark.parametrize("window", [None, "past"])
async def test_no_window_or_past_window_starts_at_once(window: str | None) -> None:
    stub = WindowStub(None if window is None else in_(-60))
    async with env_client() as (env, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            await env.sleep(timedelta(seconds=1))
            assert stub.calls == ["triage"]
            await handle.result()
    assert stub.states[0] == IncidentState.GROUPING


async def test_signal_after_window_is_ignored(caplog: pytest.LogCaptureFixture) -> None:
    release = asyncio.Event()
    started = asyncio.Event()

    class Blocking(WindowStub):
        @activity.defn(name=ACTIVITY_RUN_AGENT)
        async def run_agent(self, task: AgentTask) -> AgentRunResult:
            started.set()
            await release.wait()
            self.calls.append(task.agent)
            return self.behaviors[task.agent](task)

    # Real-time server (time skipping stalls while an activity is blocked): 1 s window.
    stub = Blocking(in_(1))
    async with env_client(time_skipping=False) as (_, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            await asyncio.wait_for(started.wait(), timeout=30)  # window closed, in triage
            with caplog.at_level(logging.INFO):
                await handle.signal(
                    IncidentWorkflow.alert_added,
                    AlertAdded(alert_id="al_late", severity=Severity.CRITICAL),
                )
                await asyncio.sleep(0.5)
            release.set()
            out = await handle.result()
    assert out.final_state == IncidentState.DISMISSED
    assert any("al_late" in r.getMessage() and "ignored" in r.getMessage() for r in caplog.records)


async def test_happy_path_after_window() -> None:
    stub = WindowStub(in_(60), HAPPY)
    async with env_client() as (_, client):
        wf, act = make_workers(client, stub)
        async with wf, act:
            handle = await start_incident_workflow(client, INCIDENT)
            out = await handle.result()
    assert out.final_state == IncidentState.REPORT_ONLY
    assert stub.contexts["synthesizer"].summary == "load-2"


async def test_signal_to_finished_workflow_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class Finished:
        async def start_workflow(self, *_a: object, **_k: object) -> None:
            raise WorkflowAlreadyStartedError("incident-inc_1", "IncidentWorkflow")

    with caplog.at_level(logging.INFO):
        await signal_alert_added(
            Finished(),  # type: ignore[arg-type]
            "inc_1",
            AlertAdded(alert_id="al_1", severity=Severity.WARNING),
        )
    rec = [r for r in caplog.records if "already finished" in r.getMessage()]
    assert len(rec) == 1 and rec[0].levelno == logging.INFO

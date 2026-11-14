"""Worker wiring: which activities are served, and shutdown cleanup. No network, DB or LLM."""

import asyncio
import signal
from typing import Any

import pytest
from temporalio.client import Client

from firebreak.activities import agents as agents_module
from firebreak.activities.agents import AgentActivities
from firebreak.activities.incident import IncidentActivities
from firebreak.agents.settings import AgentSettings
from firebreak.tools.settings import ToolSettings
from firebreak.workflows import worker


def _fake_agent_activities() -> AgentActivities:
    return AgentActivities(
        llm=object(),  # type: ignore[arg-type]
        sessions=object(),  # type: ignore[arg-type]
        tools_settings=ToolSettings(),
        settings=AgentSettings(),
    )


def test_collect_activities_includes_real_run_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(agents_module, "build_agent_activities", _fake_agent_activities)
    agent_acts = worker.load_agent_activities()
    assert isinstance(agent_acts, AgentActivities)
    incident = IncidentActivities(object())  # type: ignore[arg-type]
    funcs = worker.collect_activities(incident, agent_acts)
    assert agent_acts.run_agent in funcs
    assert incident.load_context in funcs and incident.set_incident_state in funcs


def test_collect_activities_without_agents() -> None:
    incident = IncidentActivities(object())  # type: ignore[arg-type]
    assert len(worker.collect_activities(incident, None)) == 2


async def test_main_closes_agent_activities_on_sigterm(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[str] = []

    class FakeAgents:
        async def aclose(self) -> None:
            closed.append("agents")

    class FakeEngine:
        async def dispose(self) -> None:
            closed.append("engine")

    async def fake_connect(settings: Any = None) -> Client:
        return object()  # type: ignore[return-value]

    started = asyncio.Event()

    async def fake_run_worker(client: Any, acts: Any, *, stop: asyncio.Event, **kw: Any) -> None:
        assert kw["agent_activities"] is not None
        started.set()
        await stop.wait()

    monkeypatch.setattr(worker, "make_engine", lambda: FakeEngine())
    monkeypatch.setattr(worker, "make_session_factory", lambda e: object())
    monkeypatch.setattr(worker, "load_agent_activities", lambda: FakeAgents())
    monkeypatch.setattr(worker, "connect", fake_connect)
    monkeypatch.setattr(worker, "run_worker", fake_run_worker)

    task = asyncio.create_task(worker.main())
    await asyncio.wait_for(started.wait(), timeout=5)
    signal.raise_signal(signal.SIGTERM)  # the handler installed by main() sets the stop event
    await asyncio.wait_for(task, timeout=5)
    assert closed == ["agents", "engine"]

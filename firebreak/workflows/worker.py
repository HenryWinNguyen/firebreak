"""Temporal worker process: ``uv run python -m firebreak.workflows.worker``.

One process runs a workflow worker (``ORCHESTRATOR_TASK_QUEUE``) and an activity worker
(``AGENT_TASK_QUEUE``) so slow LLM calls never starve workflow tasks. SIGTERM/SIGINT stop
both workers gracefully (in-flight activities get ``worker_shutdown_grace_s``).
"""

import asyncio
import contextlib
import importlib
import logging
import signal
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from temporalio.client import Client
from temporalio.worker import Worker

from firebreak.activities.incident import IncidentActivities
from firebreak.db.engine import make_engine, make_session_factory
from firebreak.models.orchestration import AGENT_TASK_QUEUE, ORCHESTRATOR_TASK_QUEUE
from firebreak.workflows.client import connect
from firebreak.workflows.incident import IncidentWorkflow
from firebreak.workflows.settings import TemporalSettings

logger = logging.getLogger(__name__)

AGENTS_MODULE = "firebreak.activities.agents"


def load_agent_activities() -> Any | None:
    """T-065's ``build_agent_activities()`` result, or None if that module doesn't exist yet."""
    try:
        module = importlib.import_module(AGENTS_MODULE)
    except ModuleNotFoundError as e:
        if e.name != AGENTS_MODULE:
            raise  # a missing dependency inside the module is a real error
        logger.warning("%s not importable: agent activities are NOT registered", AGENTS_MODULE)
        return None
    builder: Callable[[], Any] = module.build_agent_activities
    return builder()


def collect_activities(
    incident_activities: IncidentActivities, agent_activities: Any | None
) -> list[Callable[..., Any]]:
    """Every activity this process serves on ``AGENT_TASK_QUEUE``."""
    activities: list[Callable[..., Any]] = [
        incident_activities.load_context,
        incident_activities.set_incident_state,
    ]
    if agent_activities is not None:
        activities.append(agent_activities.run_agent)
    return activities


async def run_worker(
    client: Client,
    incident_activities: IncidentActivities,
    *,
    stop: asyncio.Event,
    agent_activities: Any | None,
    grace: timedelta,
) -> None:
    """Run both workers until ``stop`` is set, then shut them down gracefully."""
    activities = collect_activities(incident_activities, agent_activities)
    workflow_worker = Worker(
        client, task_queue=ORCHESTRATOR_TASK_QUEUE, workflows=[IncidentWorkflow]
    )
    activity_worker = Worker(
        client,
        task_queue=AGENT_TASK_QUEUE,
        activities=activities,
        graceful_shutdown_timeout=grace,
    )
    async with workflow_worker, activity_worker:
        logger.info("workers running (queues %s, %s)", ORCHESTRATOR_TASK_QUEUE, AGENT_TASK_QUEUE)
        await stop.wait()
        logger.info("shutting down workers")


async def main() -> None:
    settings = TemporalSettings()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    engine = make_engine()
    agent_activities = load_agent_activities()
    try:
        client = await connect(settings)
        await run_worker(
            client,
            IncidentActivities(make_session_factory(engine)),
            stop=stop,
            agent_activities=agent_activities,
            grace=timedelta(seconds=settings.worker_shutdown_grace_s),
        )
    finally:
        try:
            if agent_activities is not None:
                await agent_activities.aclose()
        finally:
            await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())

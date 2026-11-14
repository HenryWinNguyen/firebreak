"""Temporal worker process: ``python -m firebreak.workflows.worker``.

One process runs a workflow worker on ``ORCHESTRATOR_TASK_QUEUE`` and an activity worker on
``AGENT_TASK_QUEUE``. SIGTERM/SIGINT stop both gracefully (in-flight activities get
``graceful_shutdown_s`` to finish; otherwise they are cancelled and Temporal re-runs them on
the next worker).
"""

import asyncio
import contextlib
import importlib
import logging
import signal
from collections.abc import Callable, Sequence
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


def build_workers(
    client: Client,
    activities: Sequence[Callable[..., Any]],
    *,
    graceful_shutdown_s: int = 30,
) -> tuple[Worker, Worker]:
    """The workflow worker and the activity worker for ``activities``."""
    workflow_worker = Worker(
        client, task_queue=ORCHESTRATOR_TASK_QUEUE, workflows=[IncidentWorkflow]
    )
    activity_worker = Worker(
        client,
        task_queue=AGENT_TASK_QUEUE,
        activities=list(activities),
        graceful_shutdown_timeout=timedelta(seconds=graceful_shutdown_s),
    )
    return workflow_worker, activity_worker


async def run_workers(workflow_worker: Worker, activity_worker: Worker) -> None:
    """Run both workers until SIGTERM/SIGINT, then shut both down."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    async with workflow_worker, activity_worker:
        logger.info("workers running; waiting for a stop signal")
        await stop.wait()
        logger.info("stop signal received; shutting down")


async def amain(settings: TemporalSettings | None = None) -> None:
    settings = settings or TemporalSettings()
    engine = make_engine()
    agent_closer: Callable[[], Any] | None = None
    try:
        activities: list[Callable[..., Any]] = []
        incident = IncidentActivities(make_session_factory(engine))
        activities += [incident.load_context, incident.set_incident_state]
        try:
            agents_mod = importlib.import_module("firebreak.activities.agents")
        except ModuleNotFoundError as e:
            if e.name != "firebreak.activities.agents":
                raise
            logger.warning("agent activities are not registered: %s is not importable", e.name)
        else:
            agent_acts = agents_mod.build_agent_activities()
            agent_closer = agent_acts.aclose
            activities.append(agent_acts.run_agent)
        client = await connect(settings)
        workers = build_workers(
            client, activities, graceful_shutdown_s=settings.graceful_shutdown_s
        )
        await run_workers(*workers)
    finally:
        if agent_closer is not None:
            try:
                await agent_closer()
            except Exception:
                logger.exception("closing agent activities failed")
        await engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(amain())


if __name__ == "__main__":
    main()

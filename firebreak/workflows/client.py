"""Temporal client helpers: connecting and starting ``IncidentWorkflow`` (not workflow code)."""

import logging

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import WorkflowAlreadyStartedError

from firebreak.models.orchestration import (
    ORCHESTRATOR_TASK_QUEUE,
    SIGNAL_ALERT_ADDED,
    AlertAdded,
    WorkflowOutcome,
)
from firebreak.workflows.incident import IncidentWorkflow, workflow_id
from firebreak.workflows.settings import TemporalSettings

__all__ = ["connect", "signal_alert_added", "start_incident_workflow", "workflow_id"]

log = logging.getLogger(__name__)


async def connect(settings: TemporalSettings | None = None) -> Client:
    """A client with the pydantic data converter, address and namespace from ``settings``."""
    settings = settings or TemporalSettings()
    return await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
        data_converter=pydantic_data_converter,
    )


async def start_incident_workflow(
    client: Client, incident_id: str
) -> WorkflowHandle[IncidentWorkflow, WorkflowOutcome]:
    """Start ``IncidentWorkflow`` with id ``incident-<incident_id>``.

    ``REJECT_DUPLICATE`` refuses a second start for the same incident whether the first is
    running or already closed (Temporal raises ``WorkflowAlreadyStartedError``): one incident
    gets exactly one investigation. A re-investigation needs an explicit new design.
    """
    return await client.start_workflow(
        IncidentWorkflow.run,
        incident_id,
        id=workflow_id(incident_id),
        task_queue=ORCHESTRATOR_TASK_QUEUE,
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
    )


async def signal_alert_added(client: Client, incident_id: str, alert: AlertAdded) -> None:
    """Signal-with-start: deliver ``AlertAdded`` to ``incident-<id>``, starting it if needed.

    ``USE_EXISTING`` makes concurrent first alerts safe and re-signalling idempotent for a
    running workflow. A workflow that already finished for this id (``REJECT_DUPLICATE``) is
    not an error: the incident was investigated, the signal has nothing left to do.
    """
    try:
        await client.start_workflow(
            IncidentWorkflow.run,
            incident_id,
            id=workflow_id(incident_id),
            task_queue=ORCHESTRATOR_TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            start_signal=SIGNAL_ALERT_ADDED,
            start_signal_args=[alert],
        )
    except WorkflowAlreadyStartedError:
        log.info(
            "workflow for incident %s already finished; alert %s not signalled",
            incident_id,
            alert.alert_id,
        )

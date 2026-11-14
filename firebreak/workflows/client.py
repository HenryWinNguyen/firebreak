"""Temporal client helpers: connecting and starting ``IncidentWorkflow`` (not workflow code)."""

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDReusePolicy
from temporalio.contrib.pydantic import pydantic_data_converter

from firebreak.models.orchestration import ORCHESTRATOR_TASK_QUEUE, WorkflowOutcome
from firebreak.workflows.incident import IncidentWorkflow, workflow_id
from firebreak.workflows.settings import TemporalSettings

__all__ = ["connect", "start_incident_workflow", "workflow_id"]


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

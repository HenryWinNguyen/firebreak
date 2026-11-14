"""Client helpers: connect, and start an incident workflow."""

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDReusePolicy
from temporalio.contrib.pydantic import pydantic_data_converter

from firebreak.models.orchestration import ORCHESTRATOR_TASK_QUEUE, WorkflowOutcome
from firebreak.workflows.settings import TemporalSettings


def workflow_id(incident_id: str) -> str:
    return f"incident-{incident_id}"


async def connect(settings: TemporalSettings | None = None) -> Client:
    s = settings or TemporalSettings()
    return await Client.connect(
        s.address, namespace=s.namespace, data_converter=pydantic_data_converter
    )


async def start_incident_workflow(
    client: Client, incident_id: str
) -> WorkflowHandle[object, WorkflowOutcome]:
    """Start ``IncidentWorkflow`` for one incident under id ``incident-<incident_id>``.

    Id-reuse policy ``REJECT_DUPLICATE``: a second start for the same incident is refused
    (``WorkflowAlreadyStartedError``) whether the first run is still running or already closed,
    so an incident is investigated once.
    """
    return await client.start_workflow(
        "IncidentWorkflow",
        incident_id,
        id=workflow_id(incident_id),
        task_queue=ORCHESTRATOR_TASK_QUEUE,
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        result_type=WorkflowOutcome,
    )

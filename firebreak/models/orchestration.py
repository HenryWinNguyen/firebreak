"""Multi-agent orchestration: what flows between `IncidentWorkflow` and its activities (ARCH §4).

The workflow (core) holds decisions only; every LLM, tool or database call is an activity. The
types here are the activity inputs/outputs and the agents' output schemas, so the workflow and
the activities can be built separately. Temporal clients and workers use
``temporalio.contrib.pydantic.pydantic_data_converter`` so these models cross the wire as-is.
"""

from enum import StrEnum
from typing import Literal

from pydantic import AwareDatetime, BaseModel, Field

from firebreak.models.agents import AgentRunResult
from firebreak.models.common import Severity
from firebreak.models.evidence import SpecialistReport
from firebreak.models.incident import IncidentState
from firebreak.models.llm import Usage
from firebreak.models.reasoning import InvestigationReport

#: Task queue for workflow code (``orchestrator-worker``, ARCH §1).
ORCHESTRATOR_TASK_QUEUE = "firebreak-orchestrator"
#: Task queue for activities (``agent-worker``): slow LLM calls never starve workflow tasks.
AGENT_TASK_QUEUE = "firebreak-agents"

#: Activity names. Signatures:
#:   load_context(incident_id: str) -> IncidentContext
#:   run_agent(task: AgentTask) -> AgentRunResult   (``output`` validates as the agent's schema)
#:   set_incident_state(update: StateUpdate) -> None
ACTIVITY_LOAD_CONTEXT = "load_context"
ACTIVITY_RUN_AGENT = "run_agent"
ACTIVITY_SET_INCIDENT_STATE = "set_incident_state"

#: Signal on ``IncidentWorkflow`` (v1.7): an alert joined the incident. Payload: ``AlertAdded``.
#: Sent with signal-with-start by ingest, so the first alert also starts the workflow.
SIGNAL_ALERT_ADDED = "alert_added"

#: Specialists a ``Plan`` may name (v1.8). The workflow drops tasks naming anything else (a local
#: model once planned the tool ``service_graph`` as an agent) and lists them in the outcome.
SPECIALIST_AGENTS = frozenset({"metrics", "logs", "deploys"})
#: v1.9: ``SpecialistTask.agent`` is this literal, so a plan naming anything else fails validation
#: and the runtime repair step sends it back to the model. Keep in sync with SPECIALIST_AGENTS.
SpecialistName = Literal["metrics", "logs", "deploys"]


class AlertAdded(BaseModel):
    """Payload of ``SIGNAL_ALERT_ADDED``. A ``critical`` alert closes the grouping window now."""

    alert_id: str
    severity: Severity


class TriageVerdict(StrEnum):
    INVESTIGATE = "investigate"
    NOISE = "noise"
    DUPLICATE = "duplicate"


class TriageResult(BaseModel):
    """Output schema of the ``triage`` agent."""

    verdict: TriageVerdict
    severity: Severity
    affected_services: list[str] = Field(default_factory=list)
    rationale: str


class SpecialistTask(BaseModel):
    agent: SpecialistName = Field(description="Specialist agent config name (v1.9: one of three)")
    question: str = Field(min_length=1, description="v1.9: must not be empty")
    timeout_s: int = Field(
        default=1800,
        ge=10,
        le=3600,
        description=(
            "start_to_close for this task (v1.8: local models are slow and a single Ollama "
            "serves parallel specialists one request at a time)"
        ),
    )


class Plan(BaseModel):
    """Output schema of the ``supervisor`` agent: which specialists, which questions."""

    tasks: list[SpecialistTask] = Field(min_length=1, max_length=5)
    rationale: str


class IncidentContext(BaseModel):
    """The blackboard (ARCH §4.4): the only state agents share. Never raw transcripts.

    ``summary`` holds incident facts with alert free text already wrapped as untrusted (ARCH §8).
    """

    incident_id: str
    summary: str
    services: list[str]
    window_closes_at: AwareDatetime | None = Field(
        default=None,
        description="Grouping window end (v1.7); the workflow waits for it. None: no wait",
    )
    triage: TriageResult | None = None
    plan: Plan | None = None
    reports: list[SpecialistReport] = Field(default_factory=list)
    degraded_sources: list[str] = Field(
        default_factory=list, description="Specialists that failed or timed out (§4.3)"
    )


class AgentTask(BaseModel):
    """Input of the ``run_agent`` activity. The activity picks the context slice the agent needs."""

    agent: str = Field(description="Agent config name under firebreak/agents/configs/")
    incident_id: str
    question: str | None = Field(default=None, description="Set for specialist tasks")
    context: IncidentContext


class StateUpdate(BaseModel):
    """Input of the ``set_incident_state`` activity."""

    incident_id: str
    state: IncidentState


class WorkflowOutcome(BaseModel):
    """Return value of ``IncidentWorkflow.run``.

    M5 ends at ``REPORT_ONLY`` (report produced), ``DISMISSED`` (triage: noise/duplicate) or
    ``ESCALATED`` (no usable report: every specialist failed, or synthesis failed; ``error`` says
    why). Critic (M6) and remediation (M8) extend this later.
    """

    incident_id: str
    final_state: IncidentState
    report: InvestigationReport | None = None
    degraded_sources: list[str] = Field(default_factory=list)
    dropped_tasks: list[str] = Field(
        default_factory=list, description="Plan tasks naming no known specialist (v1.8)"
    )
    agent_runs: list[AgentRunResult] = Field(default_factory=list)
    usage: Usage = Field(default_factory=Usage)
    error: str | None = None

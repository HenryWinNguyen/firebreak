"""Incidents and grouping (ARCH §3, §4.1)."""

from datetime import datetime
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, Field

from firebreak.models.alert import Alert
from firebreak.models.common import Severity, new_id, utcnow


class IncidentState(StrEnum):
    GROUPING = "grouping"
    TRIAGE = "triage"
    PLANNING = "planning"
    INVESTIGATING = "investigating"
    SYNTHESIZING = "synthesizing"
    CRITIQUING = "critiquing"
    PROPOSING = "proposing"
    AWAITING_APPROVAL = "awaiting_approval"
    EXECUTING = "executing"
    VERIFYING_RECOVERY = "verifying_recovery"
    POSTMORTEM = "postmortem"
    # terminal
    RESOLVED = "resolved"
    DISMISSED = "dismissed"
    REPORT_ONLY = "report_only"
    ESCALATED = "escalated"
    BUDGET_EXHAUSTED = "budget_exhausted"


TERMINAL_STATES = frozenset(
    {
        IncidentState.RESOLVED,
        IncidentState.DISMISSED,
        IncidentState.REPORT_ONLY,
        IncidentState.ESCALATED,
        IncidentState.BUDGET_EXHAUSTED,
    }
)


class Incident(BaseModel):
    id: str = Field(default_factory=lambda: new_id("inc"))
    state: IncidentState = IncidentState.GROUPING
    severity: Severity
    cluster: str = Field(description="Dependency cluster from config/service_graph.yaml")
    services: list[str]
    alert_ids: list[str]
    opened_at: datetime = Field(default_factory=utcnow)
    window_closes_at: datetime
    updated_at: datetime = Field(default_factory=utcnow)


class GroupingDecision(BaseModel):
    incident_id: str
    created: bool = Field(description="True if this alert opened a new incident")
    duplicate: bool = Field(description="True if the alert was already ingested (no-op)")


class IncidentGrouper(Protocol):
    """Assigns an alert to an open incident in its cluster, or opens a new one.

    Implemented by core (T-023); called by ingest (T-021).
    """

    async def assign(self, alert: Alert) -> GroupingDecision: ...

"""Evidence and findings: the only currency shared between agents (ARCH §2, ADR-003)."""

from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, Field

from firebreak.models.common import new_id


class EvidenceKind(StrEnum):
    METRIC_SERIES = "metric_series"
    LOG_LINES = "log_lines"
    TRACE = "trace"
    DEPLOY = "deploy"
    RUNBOOK = "runbook"
    CONFIG = "config"


class Evidence(BaseModel):
    id: str = Field(default_factory=lambda: new_id("ev"))
    incident_id: str
    tool_call_id: str
    kind: EvidenceKind
    query: str = Field(description="Exact query that produced it; must be reproducible")
    summary: str = Field(max_length=4000, description="Size-capped digest shown to agents")
    raw_ref: str = Field(description="Key of the full payload in the blob store")
    untrusted: bool = Field(default=True, description="Contains target-produced text (ARCH §8)")


class Finding(BaseModel):
    claim: str
    evidence_ids: list[str] = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    agent: str


class SpecialistReport(BaseModel):
    agent: str
    question: str
    findings: list[Finding]
    unavailable_sources: list[str] = Field(default_factory=list)


class EvidenceSink(Protocol):
    """Persists a tool result as Evidence. Implemented by core, used by the tool gateway."""

    async def record(self, evidence: Evidence, raw: str) -> Evidence: ...

    async def exists(self, incident_id: str, evidence_ids: list[str]) -> set[str]:
        """Return the subset of ``evidence_ids`` that exist for this incident."""
        ...

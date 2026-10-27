"""Hypotheses and proposed actions. Enums here are what evals grade (ARCH §2, §9)."""

from enum import StrEnum
from typing import Self

from pydantic import BaseModel, Field, model_validator

from firebreak.models.common import new_id
from firebreak.models.evidence import Finding


class RootCauseCategory(StrEnum):
    BAD_DEPLOY = "bad_deploy"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    SLOW_QUERY = "slow_query"
    DEPENDENCY_FAILURE = "dependency_failure"
    MEMORY_LEAK = "memory_leak"
    MISCONFIGURATION = "misconfiguration"
    CAPACITY = "capacity"
    UNKNOWN = "unknown"


class Hypothesis(BaseModel):
    id: str = Field(default_factory=lambda: new_id("hy"))
    root_cause_category: RootCauseCategory
    explanation: str
    supporting_findings: list[str] = Field(min_length=1)
    contradicting_findings: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    rank: int = Field(ge=1)


def finding_ref(index: int) -> str:
    """Reference to ``findings[index]`` of the same report, as used in ``Hypothesis.*_findings``."""
    return f"f{index}"


class InvestigationReport(BaseModel):
    """Findings plus ranked hypotheses for one incident.

    Returned by the single-agent baseline (M3) and later by the synthesizer (M5).

    ``Hypothesis.supporting_findings`` / ``contradicting_findings`` hold refs ``f<index>``
    into ``findings``. Ranks are exactly ``1..len(hypotheses)``.
    """

    findings: list[Finding] = Field(min_length=1)
    hypotheses: list[Hypothesis] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def _check_refs_and_ranks(self) -> Self:
        valid = {finding_ref(i) for i in range(len(self.findings))}
        for h in self.hypotheses:
            unknown = [
                r for r in (*h.supporting_findings, *h.contradicting_findings) if r not in valid
            ]
            if unknown:
                raise ValueError(
                    f"hypothesis rank {h.rank} cites unknown findings {unknown}; "
                    f"valid refs are f0..f{len(self.findings) - 1}"
                )
        ranks = sorted(h.rank for h in self.hypotheses)
        if ranks != list(range(1, len(self.hypotheses) + 1)):
            raise ValueError(
                f"hypothesis ranks must be exactly 1..{len(self.hypotheses)}, got {ranks}"
            )
        return self


class ActionType(StrEnum):
    ROLLBACK = "rollback"
    RESTART = "restart"
    SCALE = "scale"
    TOGGLE_FLAG = "toggle_flag"
    REVERT_CONFIG = "revert_config"
    NONE = "none"


class RiskTier(StrEnum):
    READ = "read"
    REVERSIBLE = "reversible"
    IRREVERSIBLE = "irreversible"


class ProposedAction(BaseModel):
    id: str = Field(default_factory=lambda: new_id("act"))
    action: ActionType
    target: str
    params: dict[str, str | int | float | bool] = Field(default_factory=dict)
    rationale: str
    hypothesis_id: str
    idempotency_key: str = Field(default_factory=lambda: new_id("idem"))
    risk_tier: RiskTier | None = Field(
        default=None, description="Set ONLY by the policy engine, never by a model (ADR-004)"
    )

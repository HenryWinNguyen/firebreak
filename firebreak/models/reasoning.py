"""Hypotheses and proposed actions. Enums here are what evals grade (ARCH §2, §9)."""

from enum import StrEnum

from pydantic import BaseModel, Field

from firebreak.models.common import new_id


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

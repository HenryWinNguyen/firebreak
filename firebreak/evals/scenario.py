"""Eval scenario ground truth as data, loaded from YAML (ARCH §9)."""

import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from firebreak.models.evidence import EvidenceKind
from firebreak.models.reasoning import ActionType, RootCauseCategory


class ScenarioError(Exception):
    """A scenario file is unreadable, invalid, or conflicts with another."""


# Chaos flags from CONTRACTS 2.3 (S1 and S7 have no flag of their own).
CHAOS_FLAGS = frozenset(
    {
        "orders.error_rate",
        "orders.db_pool_size",
        "orders.n_plus_one",
        "inventory.unavailable",
        "orders.leak_kb_per_request",
        "orders.redis_host",
    }
)

# Evidence kinds the CONTRACTS 3.4 read tools can produce (`config` is only the service graph;
# there are no trace or config-history tools).
TOOL_REACHABLE_KINDS = frozenset(
    {
        EvidenceKind.METRIC_SERIES,
        EvidenceKind.LOG_LINES,
        EvidenceKind.DEPLOY,
        EvidenceKind.CONFIG,
    }
)


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Deploy(_Strict):
    service: str
    version: str


class Setup(_Strict):
    prior_deploy: Deploy | None = None
    chaos: dict[str, str | int | float | bool] = Field(default_factory=dict)
    deploy: Deploy | None = None

    @field_validator("chaos")
    @classmethod
    def _known_flags(
        cls, v: dict[str, str | int | float | bool]
    ) -> dict[str, str | int | float | bool]:
        unknown = sorted(set(v) - CHAOS_FLAGS)
        if unknown:
            raise ValueError(f"unknown chaos flags {unknown}; known: {sorted(CHAOS_FLAGS)}")
        return v


class Load(_Strict):
    """Traffic profile.

    ``rps`` counts ALL gateway requests per second, products and orders together. Following the
    M1 generator (tests/integration/m1/test_scenarios.py), 1 in 5 requests is
    ``GET /api/products`` and the other 4 in 5 are ``POST /api/orders`` with
    ``quantity = cart_size``.
    """

    rps: int = Field(gt=0)
    duration_s: int = Field(gt=0)
    cart_size: int = Field(default=1, ge=1, le=20)
    hold: Literal["until_teardown", "duration"] = Field(
        default="until_teardown",
        description=(
            "until_teardown: load runs until the scenario is torn down. duration: load stops "
            "after duration_s (the fault's effect persists without new requests)."
        ),
    )


class ExpectedAlert(_Strict):
    alertname: str
    service: str


class AcceptedAction(_Strict):
    action: ActionType
    target: str


class Expect(_Strict):
    alerts: list[ExpectedAlert] = Field(min_length=1, description="Any-of")
    root_cause_category: RootCauseCategory
    must_cite_kinds: list[EvidenceKind] = Field(default_factory=list)
    accepted_actions: list[AcceptedAction] = Field(min_length=1, description="Any-of")

    @field_validator("must_cite_kinds")
    @classmethod
    def _reachable(cls, v: list[EvidenceKind]) -> list[EvidenceKind]:
        bad = [k.value for k in v if k not in TOOL_REACHABLE_KINDS]
        if bad:
            raise ValueError(f"must_cite_kinds not producible by the read tools: {bad}")
        return v


class Teardown(_Strict):
    reset_chaos: bool = True


class Scenario(_Strict):
    """Ground truth for one fault.

    Setup order when applied: reset -> prior_deploy -> chaos flags -> deploy.
    """

    id: str
    title: str
    setup: Setup
    load: Load
    expect: Expect
    teardown: Teardown = Field(default_factory=Teardown)


def _expected_stem(scenario_id: str) -> str:
    """``S1`` -> ``s01`` (zero-padded to two digits); other ids -> lowercased."""
    m = re.fullmatch(r"S(\d+)", scenario_id)
    return f"s{int(m.group(1)):02d}" if m else scenario_id.lower()


def load_scenario(path: Path) -> Scenario:
    try:
        data: Any = yaml.safe_load(path.read_text())
        scenario = Scenario.model_validate(data)
    except (OSError, UnicodeDecodeError, yaml.YAMLError, ValidationError) as exc:
        raise ScenarioError(f"bad scenario file {path}: {exc}") from exc
    expected_stem = _expected_stem(scenario.id)
    if path.stem != expected_stem:
        raise ScenarioError(
            f"scenario file {path.name} must be named {expected_stem}.yaml for id {scenario.id!r}"
        )
    return scenario


def load_scenarios(directory: Path) -> dict[str, Scenario]:
    """Load every ``*.yaml`` in ``directory`` keyed by id; fail loudly on any problem."""
    out: dict[str, Scenario] = {}
    sources: dict[str, Path] = {}
    for path in sorted(directory.glob("*.yaml")):
        scenario = load_scenario(path)
        if scenario.id in out:
            raise ScenarioError(
                f"duplicate scenario id {scenario.id!r} in {path} and {sources[scenario.id]}"
            )
        out[scenario.id] = scenario
        sources[scenario.id] = path
    if not out:
        raise ScenarioError(f"no scenario files found in {directory}")
    return out

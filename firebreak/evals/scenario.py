"""Eval scenario ground truth as data, loaded from YAML (ARCH §9)."""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from firebreak.models.evidence import EvidenceKind
from firebreak.models.reasoning import ActionType, RootCauseCategory


class ScenarioError(Exception):
    """A scenario file is unreadable, invalid, or conflicts with another."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Deploy(_Strict):
    service: str
    version: str


class Setup(_Strict):
    chaos: dict[str, str | int | float | bool] = Field(default_factory=dict)
    deploy: Deploy | None = None


class Load(_Strict):
    rps: int = Field(gt=0)
    duration_s: int = Field(gt=0)
    cart_size: int = Field(default=1, ge=1, le=20)


class ExpectedAlert(_Strict):
    alertname: str
    service: str


class Expect(_Strict):
    alerts: list[ExpectedAlert] = Field(min_length=1, description="Any-of")
    root_cause_category: RootCauseCategory
    must_cite_kinds: list[EvidenceKind] = Field(default_factory=list)
    action: ActionType | None = None
    action_target: str | None = None


class Teardown(_Strict):
    reset_chaos: bool = True


class Scenario(_Strict):
    id: str
    title: str
    setup: Setup
    load: Load
    expect: Expect
    teardown: Teardown = Field(default_factory=Teardown)


def load_scenario(path: Path) -> Scenario:
    try:
        data: Any = yaml.safe_load(path.read_text())
        return Scenario.model_validate(data)
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise ScenarioError(f"bad scenario file {path}: {exc}") from exc


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

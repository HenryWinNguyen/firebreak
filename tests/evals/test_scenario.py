from pathlib import Path

import pytest

from firebreak.evals.scenario import ScenarioError, load_scenario, load_scenarios
from firebreak.models.reasoning import RootCauseCategory

SCENARIO_DIR = Path(__file__).resolve().parents[2] / "evals" / "scenarios"

EXPECTED = {
    "S1": RootCauseCategory.BAD_DEPLOY,
    "S2": RootCauseCategory.RESOURCE_EXHAUSTION,
    "S3": RootCauseCategory.SLOW_QUERY,
    "S4": RootCauseCategory.DEPENDENCY_FAILURE,
    "S5": RootCauseCategory.MEMORY_LEAK,
    "S6": RootCauseCategory.MISCONFIGURATION,
    "S7": RootCauseCategory.CAPACITY,
}

VALID = """id: X1
title: t
setup: {chaos: {a.b: true}}
load: {rps: 1, duration_s: 1, cart_size: 1}
expect:
  alerts: [{alertname: A, service: s}]
  root_cause_category: capacity
"""


def test_all_shipped_scenarios_load() -> None:
    scenarios = load_scenarios(SCENARIO_DIR)
    assert {k: v.expect.root_cause_category for k, v in scenarios.items()} == EXPECTED


def test_s1_has_deploy_and_s7_has_no_flags() -> None:
    scenarios = load_scenarios(SCENARIO_DIR)
    assert scenarios["S1"].setup.deploy is not None
    assert scenarios["S1"].setup.deploy.service == "orders"
    assert scenarios["S7"].setup.chaos == {}


def test_bad_yaml_fails(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text("id: [unclosed")
    with pytest.raises(ScenarioError):
        load_scenarios(tmp_path)


def test_unknown_field_fails(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text(VALID + "bogus: 1\n")
    with pytest.raises(ScenarioError):
        load_scenario(tmp_path / "a.yaml")


def test_duplicate_id_fails(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text(VALID)
    (tmp_path / "b.yaml").write_text(VALID)
    with pytest.raises(ScenarioError, match="duplicate"):
        load_scenarios(tmp_path)


def test_empty_dir_fails(tmp_path: Path) -> None:
    with pytest.raises(ScenarioError):
        load_scenarios(tmp_path)

from pathlib import Path

import pytest

from firebreak.evals.scenario import (
    TOOL_REACHABLE_KINDS,
    ScenarioError,
    load_scenario,
    load_scenarios,
)
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

VALID = """id: x1
title: t
setup: {chaos: {orders.n_plus_one: true}}
load: {rps: 1, duration_s: 1, cart_size: 1}
expect:
  alerts: [{alertname: A, service: s}]
  root_cause_category: capacity
  accepted_actions: [{action: scale, target: orders}]
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
    (tmp_path / "x1.yaml").write_text(VALID + "bogus: 1\n")
    with pytest.raises(ScenarioError):
        load_scenario(tmp_path / "x1.yaml")


def test_duplicate_id_in_second_file_fails(tmp_path: Path) -> None:
    # The stem rule makes a true duplicate unreachable; a copy under another name is rejected.
    (tmp_path / "x1.yaml").write_text(VALID)
    (tmp_path / "x1_copy.yaml").write_text(VALID)
    with pytest.raises(ScenarioError):
        load_scenarios(tmp_path)


def test_empty_dir_fails(tmp_path: Path) -> None:
    with pytest.raises(ScenarioError):
        load_scenarios(tmp_path)


def test_filename_must_match_id(tmp_path: Path) -> None:
    (tmp_path / "other.yaml").write_text(VALID)
    with pytest.raises(ScenarioError, match="must be named"):
        load_scenarios(tmp_path)


def test_invalid_utf8_fails(tmp_path: Path) -> None:
    (tmp_path / "x1.yaml").write_bytes(b"\xff\xfe\x00bad")
    with pytest.raises(ScenarioError):
        load_scenario(tmp_path / "x1.yaml")


def test_valid_fixture_loads(tmp_path: Path) -> None:
    (tmp_path / "x1.yaml").write_text(VALID)
    assert load_scenario(tmp_path / "x1.yaml").id == "x1"


def test_chaos_flag_typo_fails(tmp_path: Path) -> None:
    (tmp_path / "x1.yaml").write_text(VALID.replace("orders.n_plus_one", "orders.n_plus_on"))
    with pytest.raises(ScenarioError, match="unknown chaos flags"):
        load_scenario(tmp_path / "x1.yaml")


def test_unreachable_cite_kind_fails(tmp_path: Path) -> None:
    (tmp_path / "x1.yaml").write_text(VALID + "  must_cite_kinds: [trace]\n")
    with pytest.raises(ScenarioError):
        load_scenario(tmp_path / "x1.yaml")


def test_empty_accepted_actions_fails(tmp_path: Path) -> None:
    (tmp_path / "x1.yaml").write_text(
        VALID.replace("accepted_actions: [{action: scale, target: orders}]", "accepted_actions: []")
    )
    with pytest.raises(ScenarioError):
        load_scenario(tmp_path / "x1.yaml")


def test_shipped_cite_kinds_are_tool_reachable() -> None:
    for sc in load_scenarios(SCENARIO_DIR).values():
        assert set(sc.expect.must_cite_kinds) <= TOOL_REACHABLE_KINDS, sc.id


def test_s6_s3_s2_s1_updates() -> None:
    sc = load_scenarios(SCENARIO_DIR)
    assert sc["S3"].load.cart_size == 5
    assert sc["S2"].load.rps == 150
    assert sc["S6"].setup.chaos == {"orders.redis_host": "10.255.255.1"}
    assert sc["S1"].setup.chaos == {} and sc["S1"].setup.prior_deploy is not None


def test_load_hold_defaults_and_validates() -> None:
    from pydantic import ValidationError

    from firebreak.evals.scenario import Load

    assert Load(rps=1, duration_s=1).hold == "until_teardown"
    assert Load(rps=1, duration_s=1, hold="duration").hold == "duration"
    with pytest.raises(ValidationError):
        Load(rps=1, duration_s=1, hold="forever")  # type: ignore[arg-type]


def test_only_s5_holds_for_a_duration() -> None:
    scs = load_scenarios(SCENARIO_DIR)
    assert {k for k, v in scs.items() if v.load.hold == "duration"} == {"S5"}

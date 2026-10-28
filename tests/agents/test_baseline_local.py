"""The baseline-local config (T-047): the same agent as baseline, on a local model."""

import yaml

from firebreak.agents import baseline
from firebreak.agents.config import CONFIGS_DIR, load_agent_config
from firebreak.evals.runner import _agent_config
from tests.agents.test_baseline import fake_read_registry


def test_baseline_local_differs_from_baseline_only_in_name_and_model() -> None:
    base = yaml.safe_load((CONFIGS_DIR / "baseline.yaml").read_text())
    local = yaml.safe_load((CONFIGS_DIR / "baseline-local.yaml").read_text())
    assert local["name"] == "baseline-local" and local["model"] == "qwen3:8b"
    assert "fallback_model" not in local
    for key in ("name", "model", "fallback_model"):
        base.pop(key, None)
        local.pop(key, None)
    assert base == local  # same prompt, tools, limits and budget guard


def test_baseline_local_loads() -> None:
    cfg = load_agent_config(CONFIGS_DIR / "baseline-local.yaml", fake_read_registry())
    assert cfg.model == "qwen3:8b" and cfg.fallback_model is None and cfg.budget_usd == 0.50


def test_agent_config_swaps_and_restores_the_investigate_config() -> None:
    original = baseline.BASELINE_CONFIG
    with _agent_config("baseline-local"):
        assert baseline.BASELINE_CONFIG == CONFIGS_DIR / "baseline-local.yaml"
    assert original == baseline.BASELINE_CONFIG

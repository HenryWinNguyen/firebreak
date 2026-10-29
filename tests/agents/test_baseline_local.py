"""The baseline-local config (T-047): the same agent as baseline, on a local model."""

import pytest
import yaml
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from firebreak.agents import baseline
from firebreak.agents.baseline import investigate
from firebreak.agents.config import CONFIGS_DIR, load_agent_config
from firebreak.models.llm import LLMResponse, Usage
from firebreak.tools.settings import ToolSettings
from tests.agents.test_baseline import fake_read_registry, seed
from tests.tools.fakes import ScriptedLLM


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


@pytest.mark.db
async def test_investigate_with_baseline_local_config_requests_the_local_model(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    inc = await seed(sessions)
    refusal = LLMResponse(model="qwen3:8b", content=[], stop_reason="refusal", usage=Usage())
    for path, model, fallback in (
        (CONFIGS_DIR / "baseline-local.yaml", "qwen3:8b", None),
        (baseline.BASELINE_CONFIG, "claude-sonnet-5-5", "claude-haiku-5-5"),  # the default
    ):
        llm = ScriptedLLM([refusal])
        await investigate(
            inc.id,
            llm=llm,
            sessions=sessions,
            tools_settings=ToolSettings(),
            registry=fake_read_registry(),
            config_path=path,
        )
        assert [(r.model, r.fallback_model) for r in llm.requests] == [(model, fallback)]

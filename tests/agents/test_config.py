from pathlib import Path

import pytest

from firebreak.agents.config import (
    CONFIGS_DIR,
    PROMPTS_DIR,
    AgentConfigError,
    load_agent_config,
    load_agent_configs,
    resolve_output_schema,
)
from firebreak.models.evidence import SpecialistReport
from firebreak.models.reasoning import Hypothesis
from tests.tools.fakes import make_registry

VALID = """\
name: {name}
model: m
system_prompt: {prompt}
tools: {tools}
output_schema: {schema}
"""


def _write(
    tmp_path: Path,
    *,
    name: str = "agent",
    prompt: str = "echo.md",
    tools: str = "[echo]",
    schema: str = "SpecialistReport",
    file_name: str | None = None,
) -> Path:
    p = tmp_path / f"{file_name or name}.yaml"
    p.write_text(VALID.format(name=name, prompt=prompt, tools=tools, schema=schema))
    return p


def test_example_echo_config_loads() -> None:
    cfgs = load_agent_configs(make_registry())
    assert "echo" in cfgs
    echo = cfgs["echo"]
    assert echo.tools == ["echo"] and echo.output_schema == "SpecialistReport"
    assert (CONFIGS_DIR / "echo.yaml").is_file() and (PROMPTS_DIR / "echo.md").is_file()


def test_valid_config(tmp_path: Path) -> None:
    cfg = load_agent_config(_write(tmp_path), make_registry())
    assert cfg.name == "agent" and cfg.max_steps == 8


@pytest.mark.parametrize(
    ("kw", "match"),
    [
        ({"tools": "[echo, nope]"}, "unknown tool 'nope'"),
        ({"tools": "[echo, restart_service]"}, "only have READ tools"),
        ({"tools": "[echo, echo]"}, "duplicate tools"),
        ({"schema": "NoSuchModel"}, "output_schema 'NoSuchModel'"),
        ({"schema": "Severity"}, "not a pydantic model"),
        ({"prompt": "missing.md"}, "does not exist"),
        ({"prompt": "../config.py"}, "escapes"),
        ({"file_name": "other"}, "must match the file name"),
    ],
)
def test_invalid_configs_fail_at_load(tmp_path: Path, kw: dict[str, str], match: str) -> None:
    with pytest.raises(AgentConfigError, match=match):
        load_agent_config(_write(tmp_path, **kw), make_registry())


def test_schema_violation_fails(tmp_path: Path) -> None:
    p = tmp_path / "agent.yaml"
    p.write_text("name: agent\nmodel: m\nsystem_prompt: echo.md\noutput_schema: X\nmax_steps: 0\n")
    with pytest.raises(AgentConfigError, match="max_steps"):
        load_agent_config(p, make_registry())


@pytest.mark.parametrize("text", ["[not, a, mapping]", "name: [unclosed"])
def test_bad_yaml_fails(tmp_path: Path, text: str) -> None:
    p = tmp_path / "agent.yaml"
    p.write_text(text)
    with pytest.raises(AgentConfigError):
        load_agent_config(p, make_registry())


def test_one_bad_file_fails_the_whole_load(tmp_path: Path) -> None:
    _write(tmp_path, name="good")
    _write(tmp_path, name="bad", tools="[nope]")
    with pytest.raises(AgentConfigError, match=r"bad\.yaml"):
        load_agent_configs(make_registry(), configs_dir=tmp_path)


def test_resolve_output_schema() -> None:
    assert resolve_output_schema("SpecialistReport") is SpecialistReport
    assert resolve_output_schema("Hypothesis") is Hypothesis

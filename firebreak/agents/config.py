"""Load and validate agent configs (``firebreak/agents/configs/*.yaml``) into ``AgentConfig``.

Everything that can be checked statically fails here, at load time, not in the
middle of an incident: unknown tools, WRITE tools on an allowlist, an unknown
output schema, and a missing or out-of-tree system prompt.
"""

import importlib
import pkgutil
from pathlib import Path

import yaml
from pydantic import BaseModel, ValidationError

import firebreak.models
from firebreak.models.agents import AgentConfig
from firebreak.models.llm import ToolAccess
from firebreak.tools.registry import ToolRegistry

AGENTS_DIR = Path(__file__).resolve().parent
CONFIGS_DIR = AGENTS_DIR / "configs"
PROMPTS_DIR = AGENTS_DIR / "prompts"


class AgentConfigError(ValueError):
    """An agent config is invalid. The message names the file and the problem."""


def resolve_output_schema(name: str) -> type[BaseModel]:
    """Find the pydantic model called ``name`` in ``firebreak.models``'s modules."""
    found: list[type[BaseModel]] = []
    for mod_info in pkgutil.iter_modules(firebreak.models.__path__):
        module = importlib.import_module(f"firebreak.models.{mod_info.name}")
        obj = getattr(module, name, None)
        if (
            isinstance(obj, type)
            and issubclass(obj, BaseModel)
            and obj.__module__ == module.__name__
        ):
            found.append(obj)
    if not found:
        raise AgentConfigError(
            f"output_schema {name!r} is not a pydantic model in firebreak.models"
        )
    if len(found) > 1:
        raise AgentConfigError(f"output_schema {name!r} is ambiguous in firebreak.models")
    return found[0]


def prompt_path(cfg: AgentConfig, prompts_dir: Path = PROMPTS_DIR) -> Path:
    """Resolve ``cfg.system_prompt``, refusing anything outside ``prompts_dir``."""
    base = prompts_dir.resolve()
    path = (base / cfg.system_prompt).resolve()
    if not path.is_relative_to(base):
        raise AgentConfigError(
            f"agent {cfg.name!r}: system_prompt {cfg.system_prompt!r} escapes {base}"
        )
    return path


def validate_agent_config(
    cfg: AgentConfig, registry: ToolRegistry, *, prompts_dir: Path = PROMPTS_DIR
) -> AgentConfig:
    """Cross-check a parsed config against the tool registry, models and prompts."""
    if len(set(cfg.tools)) != len(cfg.tools):
        raise AgentConfigError(f"agent {cfg.name!r}: duplicate tools in allowlist")
    for name in cfg.tools:
        tool = registry.get(name)
        if tool is None:
            raise AgentConfigError(f"agent {cfg.name!r}: unknown tool {name!r}")
        if tool.access is not ToolAccess.READ:
            raise AgentConfigError(
                f"agent {cfg.name!r}: tool {name!r} is {tool.access}; agents may only "
                "have READ tools (ARCH §6)"
            )
    resolve_output_schema(cfg.output_schema)
    path = prompt_path(cfg, prompts_dir)
    if not path.is_file():
        raise AgentConfigError(f"agent {cfg.name!r}: system prompt {path} does not exist")
    return cfg


def load_agent_config(
    path: Path, registry: ToolRegistry, *, prompts_dir: Path = PROMPTS_DIR
) -> AgentConfig:
    try:
        data = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as e:
        raise AgentConfigError(f"{path}: cannot read config: {e}") from e
    if not isinstance(data, dict):
        raise AgentConfigError(f"{path}: expected a mapping at the top level")
    try:
        cfg = AgentConfig.model_validate(data)
    except ValidationError as e:
        raise AgentConfigError(f"{path}: {e}") from e
    if cfg.name != path.stem:
        raise AgentConfigError(f"{path}: name {cfg.name!r} must match the file name")
    try:
        return validate_agent_config(cfg, registry, prompts_dir=prompts_dir)
    except AgentConfigError as e:
        raise AgentConfigError(f"{path}: {e}") from e


def load_agent_configs(
    registry: ToolRegistry,
    *,
    configs_dir: Path = CONFIGS_DIR,
    prompts_dir: Path = PROMPTS_DIR,
) -> dict[str, AgentConfig]:
    """Load every ``*.yaml`` in ``configs_dir``. Any invalid file fails the whole load."""
    return {
        p.stem: load_agent_config(p, registry, prompts_dir=prompts_dir)
        for p in sorted(configs_dir.glob("*.yaml"))
    }

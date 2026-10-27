from typing import Any

import pytest
from pydantic import BaseModel

from firebreak.models.llm import ToolAccess
from firebreak.tools.registry import ToolRegistry
from tests.tools.fakes import EchoArgs, NoArgs, echo, make_registry

READ = ToolAccess.READ


def test_input_schema_generated_from_args_model() -> None:
    tool = make_registry().get("echo")
    assert tool is not None
    assert tool.spec.input_schema == EchoArgs.model_json_schema()
    assert tool.spec.input_schema["required"] == ["text"]
    assert tool.spec.input_schema["additionalProperties"] is False
    assert tool.access is ToolAccess.READ


def test_access_is_required() -> None:
    register: Any = ToolRegistry().register  # call it without `access` on purpose
    with pytest.raises(TypeError, match="access"):
        register(name="t", description="x", args_model=NoArgs, handler=echo)


def test_duplicate_name_rejected() -> None:
    reg = make_registry()
    with pytest.raises(ValueError, match="already registered"):
        reg.register(name="echo", description="x", args_model=EchoArgs, handler=echo, access=READ)


def test_args_model_must_forbid_extra() -> None:
    class Loose(BaseModel):
        text: str

    with pytest.raises(ValueError, match="extra='forbid'"):
        ToolRegistry().register(
            name="loose", description="x", args_model=Loose, handler=echo, access=READ
        )


@pytest.mark.parametrize(
    "kw", [{"timeout_s": 0}, {"max_output_chars": 0}, {"max_output_chars": 4000}]
)
def test_bad_limits_rejected(kw: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        ToolRegistry().register(
            name="t",
            description="x",
            args_model=NoArgs,
            handler=echo,
            access=READ,
            **kw,  # type: ignore[arg-type]
        )


def test_bad_tool_name_rejected() -> None:
    with pytest.raises(ValueError):
        ToolRegistry().register(
            name="Bad Name", description="x", args_model=NoArgs, handler=echo, access=READ
        )


def test_specs_hide_write_and_unknown() -> None:
    reg = make_registry()
    assert [s.name for s in reg.specs(["echo", "restart_service", "ghost"])] == ["echo"]

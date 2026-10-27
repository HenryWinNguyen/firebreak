"""Tool declarations: a pydantic args model, an async handler and a ``ToolAccess``.

``ToolSpec.input_schema`` (what the model sees) is generated from the args
model, so the schema the model is shown and the validation the gateway runs
can't drift apart.
"""

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from firebreak.models.evidence import EvidenceKind
from firebreak.models.llm import ToolAccess, ToolSpec
from firebreak.tools.context import ToolContext

#: Hard ceiling of ``Evidence.summary`` (firebreak/models/evidence.py).
EVIDENCE_SUMMARY_MAX = 4000
DEFAULT_TIMEOUT_S = 10.0
DEFAULT_MAX_OUTPUT_CHARS = 3500


@dataclass(frozen=True)
class ToolOutput:
    """What a handler returns. ``content`` is the raw (untrusted) payload."""

    kind: EvidenceKind
    query: str
    content: str


Handler = Callable[[Any, ToolContext], Awaitable[ToolOutput]]


@dataclass(frozen=True)
class RegisteredTool:
    spec: ToolSpec
    args_model: type[BaseModel]
    handler: Handler
    timeout_s: float
    max_output_chars: int

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def access(self) -> ToolAccess:
        return self.spec.access


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, RegisteredTool] = {}

    def register(
        self,
        *,
        name: str,
        description: str,
        args_model: type[BaseModel],
        handler: Handler,
        access: ToolAccess = ToolAccess.READ,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        max_output_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
    ) -> RegisteredTool:
        """Declare a tool. Raises ``ValueError`` on a bad declaration (fail at startup)."""
        if name in self._tools:
            raise ValueError(f"tool {name!r} is already registered")
        if args_model.model_config.get("extra") != "forbid":
            raise ValueError(
                f"tool {name!r}: args model {args_model.__name__} must set extra='forbid' "
                "so unexpected arguments are rejected, not ignored"
            )
        if timeout_s <= 0:
            raise ValueError(f"tool {name!r}: timeout_s must be > 0")
        if not 0 < max_output_chars <= EVIDENCE_SUMMARY_MAX - 200:
            raise ValueError(
                f"tool {name!r}: max_output_chars must be in 1..{EVIDENCE_SUMMARY_MAX - 200}"
            )
        spec = ToolSpec(
            name=name,
            description=description,
            input_schema=args_model.model_json_schema(),
            access=access,
        )
        tool = RegisteredTool(
            spec=spec,
            args_model=args_model,
            handler=handler,
            timeout_s=timeout_s,
            max_output_chars=max_output_chars,
        )
        self._tools[name] = tool
        return tool

    def get(self, name: str) -> RegisteredTool | None:
        return self._tools.get(name)

    def __contains__(self, name: object) -> bool:
        return name in self._tools

    def names(self) -> list[str]:
        return sorted(self._tools)

    def specs(self, names: Iterable[str]) -> list[ToolSpec]:
        """Specs for ``names`` that exist and are READ. WRITE tools are never exposed."""
        out: list[ToolSpec] = []
        for n in names:
            tool = self._tools.get(n)
            if tool is not None and tool.access is ToolAccess.READ:
                out.append(tool.spec)
        return out

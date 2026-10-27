"""Provider-neutral LLM call contract. The agent runtime depends on this, not on a SDK."""

from enum import StrEnum
from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field


class ToolAccess(StrEnum):
    READ = "read"
    WRITE = "write"  # never exposed to agents (ARCH §6)


class ToolSpec(BaseModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    description: str
    input_schema: dict[str, Any] = Field(description="JSON Schema for the tool's arguments")
    access: ToolAccess = ToolAccess.READ


class Usage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    cost_usd: float = 0.0

    def __add__(self, other: "Usage") -> "Usage":
        return Usage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            cache_read_tokens=self.cache_read_tokens + other.cache_read_tokens,
            cache_write_tokens=self.cache_write_tokens + other.cache_write_tokens,
            cost_usd=self.cost_usd + other.cost_usd,
        )


class LLMRequest(BaseModel):
    model: str
    system: str
    messages: list[dict[str, Any]] = Field(description="Anthropic Messages API message dicts")
    tools: list[ToolSpec] = Field(default_factory=list)
    max_tokens: int = Field(default=4096, gt=0)


class LLMResponse(BaseModel):
    model: str
    content: list[dict[str, Any]] = Field(description="Anthropic content block dicts")
    stop_reason: Literal["end_turn", "tool_use", "max_tokens", "stop_sequence", "refusal"]
    usage: Usage


class LLMClient(Protocol):
    """Implemented by firebreak/llm (T-031). Retries, fallback and record/replay live behind it."""

    async def complete(self, request: LLMRequest) -> LLMResponse: ...

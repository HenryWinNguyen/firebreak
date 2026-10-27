"""Agent configuration and run results (ARCH §5)."""

from pydantic import BaseModel, Field

from firebreak.models.llm import Usage


class AgentConfig(BaseModel):
    """Loaded from firebreak/agents/configs/<name>.yaml."""

    name: str
    model: str
    fallback_model: str | None = None
    system_prompt: str = Field(description="Path relative to firebreak/agents/prompts/")
    tools: list[str] = Field(default_factory=list, description="Tool allowlist")
    max_steps: int = Field(default=8, ge=1, le=50)
    max_output_tokens: int = Field(default=4096, gt=0)
    budget_usd: float = Field(default=0.15, gt=0)
    output_schema: str = Field(description="Name of a pydantic model in firebreak.models")


class ToolCallRecord(BaseModel):
    id: str
    agent: str
    tool: str
    arguments: dict[str, object]
    ok: bool
    error: str | None = None
    evidence_id: str | None = None
    duration_ms: int


class AgentRunResult(BaseModel):
    agent: str
    incident_id: str
    output: dict[str, object] = Field(description="Validated output_schema instance, dumped")
    steps: int
    tool_calls: list[ToolCallRecord]
    usage: Usage
    stopped_reason: str = Field(description="final_output | max_steps | budget | error")

"""The bounded tool-use loop every Firebreak agent runs (ARCH §5.1).

Bounds and guarantees:

- At most ``cfg.max_steps`` LLM calls, repair turns included.
- Cost is summed after every call; once ``cost_usd > cfg.budget_usd`` the run
  stops with ``stopped_reason="budget"`` and makes no further calls.
- Tool calls in one response run in parallel through the ``ToolGateway``. Every
  result, refusal or failure goes back to the model as a ``tool_result``.
- The final answer must parse into ``cfg.output_schema`` and every cited
  ``evidence_id`` must exist for this incident (``EvidenceSink.exists``).
  The model gets exactly **one** repair turn covering both checks.

``stopped_reason`` is ``final_output`` (``output`` is the validated schema),
``max_steps``, ``budget`` or ``error`` (``output`` is ``{}`` in all three).
LLM client errors and ``EvidenceSink.exists`` errors are infrastructure
failures: they're logged and propagate to the caller (the workflow retries).
"""

import asyncio
import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from firebreak.agents.config import PROMPTS_DIR, prompt_path, resolve_output_schema
from firebreak.models.agents import AgentConfig, AgentRunResult, ToolCallRecord
from firebreak.models.common import new_id
from firebreak.models.llm import LLMClient, LLMRequest, LLMResponse, Usage
from firebreak.tools.context import ToolContext
from firebreak.tools.errors import TooManyToolCalls
from firebreak.tools.gateway import ToolGateway, ToolResult
from firebreak.tools.untrusted import UNTRUSTED_DATA_POLICY

logger = logging.getLogger(__name__)

#: More tool_use blocks than this in one response are refused (not executed).
MAX_TOOL_CALLS_PER_STEP = 16


@dataclass
class _RunState:
    steps: int = 0
    usage: Usage = field(default_factory=Usage)
    tool_calls: list[ToolCallRecord] = field(default_factory=list)
    repair_used: bool = False


def build_system_prompt(prompt: str, schema: type[BaseModel]) -> str:
    """The agent's prompt plus runtime-owned rules: untrusted data and the output format."""
    return (
        f"{prompt.rstrip()}\n\n"
        f"## Untrusted data\n{UNTRUSTED_DATA_POLICY}\n\n"
        "## Final answer\nWhen you are done, reply with only a JSON object (no prose, "
        f"no code fence) that validates against this JSON Schema for {schema.__name__}:\n"
        f"{json.dumps(schema.model_json_schema(), sort_keys=True)}\n"
        "Cite evidence only by the evidence_id values shown on tool results."
    )


def iter_cited_evidence_ids(value: object) -> Iterator[str]:
    """Every value under an ``evidence_ids`` (list) or ``evidence_id`` (str) key, anywhere."""
    if isinstance(value, dict):
        for k, v in value.items():
            if k == "evidence_ids" and isinstance(v, list):
                yield from (x for x in v if isinstance(x, str))
            elif k == "evidence_id" and isinstance(v, str):
                yield v
            else:
                yield from iter_cited_evidence_ids(v)
    elif isinstance(value, list):
        for v in value:
            yield from iter_cited_evidence_ids(v)


def _text_of(response: LLMResponse) -> str:
    return "".join(
        str(b.get("text", "")) for b in response.content if b.get("type") == "text"
    ).strip()


def _strip_fence(text: str) -> str:
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 2 and lines[-1].strip() == "```":
            return "\n".join(lines[1:-1]).strip()
    return text


async def _check_output(
    text: str, schema: type[BaseModel], ctx: ToolContext
) -> tuple[dict[str, object] | None, str | None]:
    """Return ``(output, None)`` if valid, else ``(None, problem for the repair turn)``."""
    try:
        parsed = schema.model_validate_json(_strip_fence(text))
    except ValidationError as e:
        problems = "; ".join(
            f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}"
            for err in e.errors(include_url=False, include_input=False)
        )
        return None, f"Your final answer is not valid {schema.__name__} JSON: {problems}"

    output = parsed.model_dump(mode="json")
    cited = sorted(set(iter_cited_evidence_ids(output)))
    if cited:
        try:
            known = await ctx.evidence.exists(ctx.incident_id, cited)
        except Exception:
            logger.exception("evidence existence check failed incident=%s", ctx.incident_id)
            raise
        missing = [i for i in cited if i not in known]
        if missing:
            return None, (
                f"Your final answer cites evidence ids that do not exist for this incident: "
                f"{missing}. Cite only evidence_id values shown on tool results, or drop "
                "the claims that depend on them."
            )
    return output, None


async def _run_tools(
    cfg: AgentConfig,
    response: LLMResponse,
    ctx: ToolContext,
    gateway: ToolGateway,
) -> list[ToolResult]:
    blocks = [b for b in response.content if b.get("type") == "tool_use"]
    allowed, excess = blocks[:MAX_TOOL_CALLS_PER_STEP], blocks[MAX_TOOL_CALLS_PER_STEP:]

    async def one(block: dict[str, Any]) -> ToolResult:
        return await gateway.invoke(
            cfg,
            str(block.get("name", "")),
            block.get("input"),
            ctx,
            call_id=str(block.get("id") or new_id("tc")),
        )

    results = list(await asyncio.gather(*(one(b) for b in allowed)))
    for b in excess:
        err = TooManyToolCalls(
            f"more than {MAX_TOOL_CALLS_PER_STEP} tool calls in one turn; this one was not run"
        )
        results.append(
            gateway.error_result(
                cfg,
                str(b.get("name", "")),
                b.get("input"),
                err,
                ctx,
                call_id=str(b.get("id") or new_id("tc")),
            )
        )
    return results


def _tool_results_message(results: list[ToolResult]) -> dict[str, Any]:
    return {
        "role": "user",
        "content": [
            {
                "type": "tool_result",
                "tool_use_id": r.record.id,
                "content": r.content,
                "is_error": r.is_error,
            }
            for r in results
        ],
    }


async def run_agent(
    cfg: AgentConfig,
    task: str,
    ctx: ToolContext,
    llm: LLMClient,
    gateway: ToolGateway,
    *,
    prompts_dir: Path = PROMPTS_DIR,
) -> AgentRunResult:
    schema = resolve_output_schema(cfg.output_schema)
    prompt = await asyncio.to_thread(prompt_path(cfg, prompts_dir).read_text)
    system = build_system_prompt(prompt, schema)
    tools = gateway.specs_for(cfg)
    messages: list[dict[str, Any]] = [{"role": "user", "content": task}]
    state = _RunState()

    def finish(reason: str, output: dict[str, object] | None = None) -> AgentRunResult:
        log = logger.info if reason == "final_output" else logger.warning
        log(
            "agent run finished agent=%s incident=%s reason=%s steps=%d cost_usd=%.4f",
            cfg.name,
            ctx.incident_id,
            reason,
            state.steps,
            state.usage.cost_usd,
        )
        return AgentRunResult(
            agent=cfg.name,
            incident_id=ctx.incident_id,
            output=output or {},
            steps=state.steps,
            tool_calls=state.tool_calls,
            usage=state.usage,
            stopped_reason=reason,
        )

    while state.steps < cfg.max_steps:
        state.steps += 1
        response = await llm.complete(
            LLMRequest(
                model=cfg.model,
                system=system,
                messages=messages,
                tools=tools,
                max_tokens=cfg.max_output_tokens,
            )
        )
        state.usage = state.usage + response.usage
        if state.usage.cost_usd > cfg.budget_usd:
            return finish("budget")
        if response.stop_reason == "refusal":
            logger.warning("model refused agent=%s incident=%s", cfg.name, ctx.incident_id)
            return finish("error")

        messages.append({"role": "assistant", "content": response.content})
        if any(b.get("type") == "tool_use" for b in response.content):
            if state.steps >= cfg.max_steps:
                break  # no step left to read the results; don't run the tools
            results = await _run_tools(cfg, response, ctx, gateway)
            state.tool_calls.extend(r.record for r in results)
            messages.append(_tool_results_message(results))
            continue

        output, problem = await _check_output(_text_of(response), schema, ctx)
        if output is not None:
            return finish("final_output", output)
        assert problem is not None
        if state.repair_used:
            logger.warning(
                "final output invalid after repair agent=%s incident=%s: %s",
                cfg.name,
                ctx.incident_id,
                problem,
            )
            return finish("error")
        state.repair_used = True
        messages.append(
            {
                "role": "user",
                "content": f"{problem}\nReply again with only the corrected JSON object. "
                "This is your only chance to fix it.",
            }
        )

    return finish("max_steps")

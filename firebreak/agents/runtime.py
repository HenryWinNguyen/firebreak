"""The bounded tool-use loop every Firebreak agent runs (ARCH §5.1).

Bounds and guarantees:

- At most ``cfg.max_steps`` LLM calls, repair turns included.
- Cost is summed after every call; once ``cost_usd > cfg.budget_usd`` the run
  stops with ``stopped_reason="budget"`` and makes no further calls. A
  non-finite or negative reported cost counts as over budget (fail closed).
- Tool calls in one response run in parallel through the ``ToolGateway``. Every
  result, refusal or failure goes back to the model as a ``tool_result``, and
  every tool_use gets exactly one tool_result, even if the gateway itself fails.
- The final answer must parse into ``cfg.output_schema`` and every cited
  ``evidence_id`` must exist for this incident (``EvidenceSink.exists``).
- The model gets exactly **one** repair turn per run. It covers an invalid final
  answer, cited ids that don't exist, and a malformed tool turn (a tool_use with
  a missing, empty or duplicate id). Ids are never invented, and a malformed
  turn's tool_use blocks are never echoed back without results.

``stopped_reason`` is ``final_output`` (``output`` is the validated schema),
``max_steps``, ``budget`` or ``error`` (``output`` is ``{}`` in all three, and
``error`` says why when the reason is ``error``). LLM client errors and
``EvidenceSink.exists`` errors or timeouts are infrastructure failures: they're
logged and propagate to the caller (the workflow retries).
"""

import asyncio
import json
import logging
import math
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from firebreak.agents.config import PROMPTS_DIR, prompt_path, resolve_output_schema
from firebreak.models.agents import AgentConfig, AgentRunResult, ToolCallRecord
from firebreak.models.llm import LLMClient, LLMRequest, LLMResponse, Usage
from firebreak.tools.context import ToolContext
from firebreak.tools.errors import (
    MAX_ERROR_TEXT_CHARS,
    ToolInternalError,
    TooManyToolCalls,
    cap_text,
    format_validation_error,
)
from firebreak.tools.gateway import ToolGateway, ToolResult
from firebreak.tools.untrusted import UNTRUSTED_DATA_POLICY

logger = logging.getLogger(__name__)

#: More tool_use blocks than this in one response are refused (not executed).
MAX_TOOL_CALLS_PER_STEP = 16
MAX_TOOL_USE_ID_CHARS = 128
DEFAULT_EVIDENCE_TIMEOUT_S = 10.0
_REPAIR_SUFFIX = "This is your only chance to fix it."


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
    """Every value under an ``evidence_ids`` (list) or ``evidence_id`` (str) key, anywhere.

    Limitation: citations are found by key name only. An output schema that
    refers to evidence under any other key is not checked here, so output
    schemas must use ``evidence_ids`` / ``evidence_id`` for evidence references.
    (``Hypothesis.supporting_findings`` refers to findings, not evidence.)
    """
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
    text: str, schema: type[BaseModel], ctx: ToolContext, *, evidence_timeout_s: float
) -> tuple[dict[str, object] | None, str]:
    """Return ``(output, "")`` if valid, else ``(None, problem for the repair turn)``."""
    try:
        parsed = schema.model_validate_json(_strip_fence(text))
    except ValidationError as e:
        return None, (
            f"Your final answer is not valid {schema.__name__} JSON: {format_validation_error(e)}"
        )

    output = parsed.model_dump(mode="json")
    cited = sorted(set(iter_cited_evidence_ids(output)))
    if cited:
        try:
            async with asyncio.timeout(evidence_timeout_s):
                known = await ctx.evidence.exists(ctx.incident_id, cited)
        except Exception:
            logger.exception("evidence existence check failed incident=%s", ctx.incident_id)
            raise
        missing = [i for i in cited if i not in known]
        if missing:
            shown = cap_text(", ".join(repr(i[:64]) for i in missing), 500)
            return None, (
                "Your final answer cites evidence ids that do not exist for this incident: "
                f"{shown}. Cite only evidence_id values shown on tool results, or drop "
                "the claims that depend on them."
            )
    return output, ""


def _tool_use_problem(blocks: list[dict[str, Any]]) -> str | None:
    """Why this tool turn is malformed, or None. Ids are never invented or repaired."""
    seen: set[str] = set()
    for b in blocks:
        tid = b.get("id")
        if not isinstance(tid, str) or not tid.strip() or len(tid) > MAX_TOOL_USE_ID_CHARS:
            return "a tool call had a missing, empty or invalid id"
        if tid in seen:
            return "two tool calls had the same id"
        seen.add(tid)
    return None


def _block_name(block: dict[str, Any]) -> str:
    name = block.get("name")
    return name if isinstance(name, str) else ""


async def _run_tools(
    cfg: AgentConfig,
    blocks: list[dict[str, Any]],
    ctx: ToolContext,
    gateway: ToolGateway,
) -> list[ToolResult]:
    """Run one turn's tool calls in parallel. Returns exactly one result per block, in order.

    Ids were already validated by ``_tool_use_problem``. An unexpected exception
    from the gateway becomes that call's tool error and never aborts the sibling
    calls. Cancellation propagates, and ``gather`` then cancels the siblings.
    """
    allowed, excess = blocks[:MAX_TOOL_CALLS_PER_STEP], blocks[MAX_TOOL_CALLS_PER_STEP:]

    async def one(block: dict[str, Any]) -> ToolResult:
        name, call_id = _block_name(block), str(block["id"])
        try:
            return await gateway.invoke(cfg, name, block.get("input"), ctx, call_id=call_id)
        except Exception as e:
            logger.exception(
                "gateway raised agent=%s tool=%r incident=%s call=%s",
                cfg.name,
                name[:64],
                ctx.incident_id,
                call_id,
            )
            err = ToolInternalError(
                f"internal error while running the tool ({type(e).__name__}); "
                "the call did not complete"
            )
            return gateway.error_result(cfg, name, block.get("input"), err, ctx, call_id=call_id)

    results = list(await asyncio.gather(*(one(b) for b in allowed)))
    for b in excess:
        err = TooManyToolCalls(
            f"more than {MAX_TOOL_CALLS_PER_STEP} tool calls in one turn; this one was not run"
        )
        results.append(
            gateway.error_result(
                cfg, _block_name(b), b.get("input"), err, ctx, call_id=str(b["id"])
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


def _assistant_blocks(content: list[dict[str, Any]], *, drop_tool_use: bool) -> list[Any]:
    """Blocks worth echoing back: no empty text blocks (and no tool_use, if asked)."""
    out: list[Any] = []
    for b in content:
        if drop_tool_use and b.get("type") == "tool_use":
            continue
        if b.get("type") == "text" and not str(b.get("text", "")).strip():
            continue
        out.append(b)
    return out


def _append_user_text(messages: list[dict[str, Any]], text: str) -> None:
    """Add user text, merging into the last message if it is already a user turn."""
    last = messages[-1]
    if last["role"] != "user":
        messages.append({"role": "user", "content": text})
        return
    content = last["content"]
    blocks = [{"type": "text", "text": content}] if isinstance(content, str) else list(content)
    blocks.append({"type": "text", "text": text})
    last["content"] = blocks


def _over_budget(step: Usage, total: Usage, budget_usd: float) -> bool:
    if not math.isfinite(step.cost_usd) or step.cost_usd < 0:
        return True  # an unknowable cost is treated as an exhausted budget
    return not math.isfinite(total.cost_usd) or total.cost_usd > budget_usd


async def run_agent(
    cfg: AgentConfig,
    task: str,
    ctx: ToolContext,
    llm: LLMClient,
    gateway: ToolGateway,
    *,
    prompts_dir: Path = PROMPTS_DIR,
    evidence_timeout_s: float = DEFAULT_EVIDENCE_TIMEOUT_S,
) -> AgentRunResult:
    schema = resolve_output_schema(cfg.output_schema)
    prompt = await asyncio.to_thread(prompt_path(cfg, prompts_dir).read_text)
    system = build_system_prompt(prompt, schema)
    tools = gateway.specs_for(cfg)
    messages: list[dict[str, Any]] = [{"role": "user", "content": task}]
    state = _RunState()

    def finish(
        reason: str, output: dict[str, object] | None = None, error: str | None = None
    ) -> AgentRunResult:
        if error is not None:
            error = cap_text(error, MAX_ERROR_TEXT_CHARS)
        log = logger.info if reason == "final_output" else logger.warning
        log(
            "agent run finished agent=%s incident=%s reason=%s steps=%d cost_usd=%.4f error=%s",
            cfg.name,
            ctx.incident_id,
            reason,
            state.steps,
            state.usage.cost_usd,
            error,
        )
        return AgentRunResult(
            agent=cfg.name,
            incident_id=ctx.incident_id,
            output=output or {},
            steps=state.steps,
            tool_calls=state.tool_calls,
            usage=state.usage,
            stopped_reason=reason,
            error=error,
        )

    while state.steps < cfg.max_steps:
        state.steps += 1
        response = await llm.complete(
            LLMRequest(
                model=cfg.model,
                fallback_model=cfg.fallback_model,
                system=system,
                messages=messages,
                tools=tools,
                max_tokens=cfg.max_output_tokens,
            )
        )
        state.usage = state.usage + response.usage
        if _over_budget(response.usage, state.usage, cfg.budget_usd):
            return finish("budget")
        if response.stop_reason == "refusal":
            return finish("error", error="the model refused to answer (stop_reason=refusal)")

        tool_blocks = [b for b in response.content if b.get("type") == "tool_use"]
        if tool_blocks:
            problem = _tool_use_problem(tool_blocks)
            if problem is not None:
                # Malformed turn: echo back only its non-tool blocks, never unmatched tool_use.
                if state.repair_used:
                    return finish("error", error=f"malformed tool turn after repair: {problem}")
                state.repair_used = True
                kept = _assistant_blocks(response.content, drop_tool_use=True)
                if kept:
                    messages.append({"role": "assistant", "content": kept})
                _append_user_text(
                    messages,
                    f"Your last turn was rejected and none of its tool calls ran: {problem}. "
                    f"Every tool call needs a unique, non-empty id. {_REPAIR_SUFFIX}",
                )
                continue
            messages.append(
                {
                    "role": "assistant",
                    "content": _assistant_blocks(response.content, drop_tool_use=False),
                }
            )
            if state.steps >= cfg.max_steps:
                break  # no step left to read the results; don't run the tools
            results = await _run_tools(cfg, tool_blocks, ctx, gateway)
            state.tool_calls.extend(r.record for r in results)
            messages.append(_tool_results_message(results))
            continue

        output, problem = await _check_output(
            _text_of(response), schema, ctx, evidence_timeout_s=evidence_timeout_s
        )
        if output is not None:
            return finish("final_output", output)
        if state.repair_used:
            return finish("error", error=f"final answer invalid after repair: {problem}")
        state.repair_used = True
        kept = _assistant_blocks(response.content, drop_tool_use=False)
        if kept:
            messages.append({"role": "assistant", "content": kept})
        _append_user_text(
            messages,
            f"{problem}\nReply again with only the corrected JSON object. {_REPAIR_SUFFIX}",
        )

    return finish("max_steps")

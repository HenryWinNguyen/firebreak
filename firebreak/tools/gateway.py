"""The tool gateway: the single chokepoint every agent tool call goes through (ARCH §6).

``ToolGateway.invoke`` runs, in order:

1. allowlist check against the agent config            -> ``ToolNotAllowed``
   (then: is the tool registered at all)                -> ``UnknownTool``
2. access classification: anything but READ is refused  -> ``WriteToolRefused``
3. argument validation against the pydantic args model  -> ``InvalidToolArgs``
4. handler, under a per-tool timeout                    -> ``ToolTimeout`` / ``ToolHandlerFailed``
5. size cap (summary shown to the model; raw stored)    -> ``InvalidToolOutput``
6. ``EvidenceSink.record``                              -> ``EvidenceRecordFailed``
7. wrap the summary in an untrusted-data block (ARCH §8)
8. ``ToolCallRecord`` + a log line

The WRITE refusal deliberately happens *before* argument validation (the spec
lists it after): a WRITE tool is refused whatever its arguments, and the model
never gets validation feedback that would help it shape a WRITE call.

Every refusal and failure becomes a ``ToolResult`` with ``is_error=True``
that the runtime returns to the model as a tool error. ``invoke`` never raises
for a refused or failing call. Only cancellation (``BaseException``) propagates.
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass

from pydantic import BaseModel, ValidationError

from firebreak.models.agents import AgentConfig, ToolCallRecord
from firebreak.models.common import new_id
from firebreak.models.evidence import Evidence, EvidenceKind
from firebreak.models.llm import ToolAccess, ToolSpec
from firebreak.tools.context import ToolContext
from firebreak.tools.errors import (
    EvidenceRecordFailed,
    InvalidToolArgs,
    InvalidToolOutput,
    ToolError,
    ToolErrorCode,
    ToolHandlerFailed,
    ToolNotAllowed,
    ToolTimeout,
    UnknownTool,
    WriteToolRefused,
)
from firebreak.tools.registry import RegisteredTool, ToolOutput, ToolRegistry
from firebreak.tools.untrusted import wrap_untrusted

logger = logging.getLogger(__name__)

DEFAULT_MAX_RAW_CHARS = 2_000_000
_TRUNCATION_NOTE = "\n[... truncated {omitted} chars; the full payload is stored as evidence]"
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_MAX_DETAIL_CHARS = 1000


@dataclass(frozen=True)
class ToolResult:
    """Outcome of one tool call. ``content`` goes verbatim into the model's tool_result block."""

    record: ToolCallRecord
    content: str
    is_error: bool
    error_code: ToolErrorCode | None = None
    evidence: Evidence | None = None


def cap(text: str, limit: int) -> str:
    """Truncate ``text`` to ``limit`` chars and append a truncation note (< 100 chars)."""
    if len(text) <= limit:
        return text
    return text[:limit] + _TRUNCATION_NOTE.format(omitted=len(text) - limit)


def _shown_name(name: str) -> str:
    """A model-supplied tool name, made safe to echo back."""
    return repr(name[:64])


class ToolGateway:
    def __init__(
        self, registry: ToolRegistry, *, max_raw_chars: int = DEFAULT_MAX_RAW_CHARS
    ) -> None:
        self._registry = registry
        self._max_raw_chars = max_raw_chars

    def specs_for(self, agent_cfg: AgentConfig) -> list[ToolSpec]:
        """Tools the model is shown: allowlisted, registered and READ only."""
        return self._registry.specs(agent_cfg.tools)

    async def invoke(
        self,
        agent_cfg: AgentConfig,
        tool_name: str,
        raw_args: object,
        ctx: ToolContext,
        *,
        call_id: str | None = None,
    ) -> ToolResult:
        call_id = call_id or new_id("tc")
        started = time.monotonic()
        try:
            tool = self._authorize(agent_cfg, tool_name)
            args = self._validate(tool, raw_args)
            output = await self._run(tool, args, ctx)
            evidence = await self._record(tool, output, ctx, call_id)
        except ToolError as err:
            return self.error_result(
                agent_cfg, tool_name, raw_args, err, ctx, call_id=call_id, started=started
            )

        record = ToolCallRecord(
            id=call_id,
            agent=agent_cfg.name,
            tool=tool.name,
            arguments=_record_args(raw_args),
            ok=True,
            evidence_id=evidence.id,
            duration_ms=_elapsed_ms(started),
        )
        logger.info(
            "tool call ok agent=%s tool=%s incident=%s call=%s evidence=%s ms=%d",
            agent_cfg.name,
            tool.name,
            ctx.incident_id,
            call_id,
            evidence.id,
            record.duration_ms,
        )
        content = wrap_untrusted(evidence.summary, source=tool.name, evidence_id=evidence.id)
        return ToolResult(record=record, content=content, is_error=False, evidence=evidence)

    def error_result(
        self,
        agent_cfg: AgentConfig,
        tool_name: str,
        raw_args: object,
        err: ToolError,
        ctx: ToolContext,
        *,
        call_id: str,
        started: float | None = None,
    ) -> ToolResult:
        """Turn a ``ToolError`` into the tool error the model sees, and log it."""
        logger.warning(
            "tool call refused agent=%s tool=%s incident=%s call=%s code=%s: %s",
            agent_cfg.name,
            tool_name[:64],
            ctx.incident_id,
            call_id,
            err.code,
            err.message,
        )
        content = f"tool_error code={err.code}: {err.message}"
        if err.untrusted_detail:
            source = tool_name if _SAFE_TOKEN.match(tool_name) else "unknown"
            content += "\n" + wrap_untrusted(
                cap(err.untrusted_detail, _MAX_DETAIL_CHARS), source=source
            )
        record = ToolCallRecord(
            id=call_id,
            agent=agent_cfg.name,
            tool=tool_name,
            arguments=_record_args(raw_args),
            ok=False,
            error=f"{err.code}: {err.message}",
            duration_ms=_elapsed_ms(started) if started is not None else 0,
        )
        return ToolResult(record=record, content=content, is_error=True, error_code=err.code)

    # --- steps --------------------------------------------------------------------------

    def _authorize(self, agent_cfg: AgentConfig, tool_name: str) -> RegisteredTool:
        if tool_name not in agent_cfg.tools:
            available = [s.name for s in self.specs_for(agent_cfg)]
            raise ToolNotAllowed(
                f"tool {_shown_name(tool_name)} is not available to agent "
                f"{agent_cfg.name!r}. Available tools: {available}"
            )
        tool = self._registry.get(tool_name)
        if tool is None:
            raise UnknownTool(f"tool {_shown_name(tool_name)} does not exist")
        # Fail closed: anything that isn't explicitly READ is refused.
        if tool.access is not ToolAccess.READ:
            raise WriteToolRefused(
                f"tool {tool.name!r} changes the system and can never be called by an agent. "
                "Propose the action in your output instead; a human approves execution."
            )
        return tool

    def _validate(self, tool: RegisteredTool, raw_args: object) -> BaseModel:
        if not isinstance(raw_args, dict):
            raise InvalidToolArgs(
                f"arguments for {tool.name!r} must be a JSON object, got {type(raw_args).__name__}"
            )
        try:
            return tool.args_model.model_validate(raw_args)
        except ValidationError as e:
            # include_input=False: never echo the (possibly injected) values back verbatim.
            problems = "; ".join(
                f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}"
                for err in e.errors(include_url=False, include_input=False)
            )
            raise InvalidToolArgs(f"invalid arguments for {tool.name!r}: {problems}") from e

    async def _run(self, tool: RegisteredTool, args: BaseModel, ctx: ToolContext) -> ToolOutput:
        try:
            async with asyncio.timeout(tool.timeout_s):
                output = await tool.handler(args, ctx)
        except TimeoutError as e:
            raise ToolTimeout(f"tool {tool.name!r} timed out after {tool.timeout_s:g}s") from e
        except ToolError:
            raise  # a handler may raise a typed refusal itself
        except Exception as e:
            logger.warning(
                "tool handler raised tool=%s incident=%s", tool.name, ctx.incident_id, exc_info=True
            )
            raise ToolHandlerFailed(
                f"tool {tool.name!r} failed ({type(e).__name__})", untrusted_detail=str(e)
            ) from e

        if not (
            isinstance(output, ToolOutput)
            and isinstance(output.kind, EvidenceKind)
            and isinstance(output.query, str)
            and isinstance(output.content, str)
        ):
            logger.error("tool %r returned %r, not a valid ToolOutput", tool.name, type(output))
            raise InvalidToolOutput(f"tool {tool.name!r} returned a malformed result")
        return output

    async def _record(
        self, tool: RegisteredTool, output: ToolOutput, ctx: ToolContext, call_id: str
    ) -> Evidence:
        summary = cap(output.content, tool.max_output_chars)
        raw = cap(output.content, self._max_raw_chars)
        ev_id = new_id("ev")
        evidence = Evidence(
            id=ev_id,
            incident_id=ctx.incident_id,
            tool_call_id=call_id,
            kind=output.kind,
            query=output.query,
            summary=summary,
            raw_ref=f"evidence/{ctx.incident_id}/{ev_id}",
        )
        try:
            stored = await ctx.evidence.record(evidence, raw)
        except Exception as e:
            logger.error(
                "evidence record failed tool=%s incident=%s call=%s",
                tool.name,
                ctx.incident_id,
                call_id,
                exc_info=True,
            )
            raise EvidenceRecordFailed(
                f"the result of {tool.name!r} could not be stored as evidence, so it was "
                "discarded (uncited results can't be used). You may retry the call."
            ) from e
        if not (
            isinstance(stored, Evidence)
            and stored.incident_id == ctx.incident_id
            and _SAFE_TOKEN.match(stored.id)
        ):
            logger.error("evidence sink returned an invalid record for call=%s", call_id)
            raise EvidenceRecordFailed(f"the result of {tool.name!r} could not be stored")
        return stored


def _record_args(raw_args: object) -> dict[str, object]:
    if isinstance(raw_args, dict):
        return {str(k): v for k, v in raw_args.items()}
    return {"_invalid_arguments": repr(raw_args)[:200]}


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)

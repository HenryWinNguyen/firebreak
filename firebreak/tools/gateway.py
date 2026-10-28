"""The tool gateway: the single chokepoint every agent tool call goes through (ARCH §6).

``ToolGateway.invoke`` runs, in order:

1. allowlist check against the agent config            -> ``ToolNotAllowed``
   (then: is the tool registered at all)                -> ``UnknownTool``
2. access classification: anything but READ is refused  -> ``WriteToolRefused``
3. argument validation against the pydantic args model  -> ``InvalidToolArgs``
4. handler, under a per-tool timeout                    -> ``ToolTimeout`` / ``ToolHandlerFailed``
5. output checks: shape, hard length limit, query length -> ``InvalidToolOutput``
6. render the summary the model sees: defang, then cap (``render_untrusted``)
7. ``EvidenceSink.record`` (full payload), under a timeout -> ``EvidenceRecordFailed``
8. wrap the summary in an untrusted-data block (ARCH §8)
9. ``ToolCallRecord`` + a log line

The WRITE refusal deliberately happens *before* argument validation (the spec
lists it after): a WRITE tool is refused whatever its arguments, and the model
never gets validation feedback that would help it shape a WRITE call.

Every refusal and failure becomes a ``ToolResult`` with ``is_error=True``
that the runtime returns to the model as a tool error. ``invoke`` never raises,
except ``CancelledError`` when the calling task itself is being cancelled. A
handler that raises ``CancelledError`` on its own gets ``handler_failed``, and
any unexpected exception inside the gateway becomes ``internal_error``.

Model-facing error text is always gateway-authored. Anything a handler supplies
(exception text, or the message of a ``ToolError`` it raised) is shown only as
capped ``untrusted_detail`` inside an untrusted-data block. A handler can't pick
the error code either: a ``ToolError`` it raises maps to ``invalid_args`` (if it
is an ``InvalidToolArgs``) or ``handler_failed``.
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
    ToolInternalError,
    ToolNotAllowed,
    ToolTimeout,
    UnknownTool,
    WriteToolRefused,
    cap_text,
    format_validation_error,
)
from firebreak.tools.registry import EVIDENCE_SUMMARY_MAX, RegisteredTool, ToolOutput, ToolRegistry
from firebreak.tools.untrusted import render_untrusted, wrap_untrusted

logger = logging.getLogger(__name__)

#: Handler content longer than this many characters (not bytes) is rejected outright.
DEFAULT_MAX_CONTENT_CHARS = 1_000_000
#: ``Evidence.query`` must be reproducible, so an over-long one is rejected, not truncated.
MAX_QUERY_CHARS = 4000
DEFAULT_EVIDENCE_TIMEOUT_S = 10.0
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


def _shown_name(name: str) -> str:
    """A model-supplied tool name, made safe to echo back."""
    return repr(name[:64])


def _outer_cancel_requested() -> bool:
    """True if the current task is being cancelled from outside (not a stray CancelledError)."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


class ToolGateway:
    def __init__(
        self,
        registry: ToolRegistry,
        *,
        max_content_chars: int = DEFAULT_MAX_CONTENT_CHARS,
        evidence_timeout_s: float = DEFAULT_EVIDENCE_TIMEOUT_S,
    ) -> None:
        self._registry = registry
        self._max_content_chars = max_content_chars
        self._evidence_timeout_s = evidence_timeout_s

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
            return await self._invoke(agent_cfg, tool_name, raw_args, ctx, call_id, started)
        except ToolError as err:
            return self.error_result(
                agent_cfg, tool_name, raw_args, err, ctx, call_id=call_id, started=started
            )
        except Exception as e:
            logger.exception(
                "gateway internal error agent=%s tool=%r incident=%s call=%s",
                agent_cfg.name,
                tool_name[:64],
                ctx.incident_id,
                call_id,
            )
            internal = ToolInternalError(
                f"internal error while running the tool ({type(e).__name__}); "
                "the call did not complete"
            )
            return self.error_result(
                agent_cfg, tool_name, raw_args, internal, ctx, call_id=call_id, started=started
            )

    async def _invoke(
        self,
        agent_cfg: AgentConfig,
        tool_name: str,
        raw_args: object,
        ctx: ToolContext,
        call_id: str,
        started: float,
    ) -> ToolResult:
        tool = self._authorize(agent_cfg, tool_name)
        args = self._validate(tool, raw_args)
        output = await self._run(tool, args, ctx)
        evidence = await self._record(tool, output, ctx, call_id)
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
        content = wrap_untrusted(
            evidence.summary,
            source=tool.name,
            evidence_id=evidence.id,
            max_chars=EVIDENCE_SUMMARY_MAX,
        )
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
        code = err.code
        message = cap_text(str(getattr(err, "message", None) or type(err).__name__))
        detail = getattr(err, "untrusted_detail", None)
        logger.warning(
            "tool call refused agent=%s tool=%r incident=%s call=%s code=%s: %s",
            agent_cfg.name,
            tool_name[:64],
            ctx.incident_id,
            call_id,
            code,
            message,
        )
        content = f"tool_error code={code}: {message}"
        if isinstance(detail, str) and detail:
            source = tool_name if _SAFE_TOKEN.match(tool_name) else "unknown"
            content += "\n" + wrap_untrusted(detail, source=source, max_chars=_MAX_DETAIL_CHARS)
        record = ToolCallRecord(
            id=call_id,
            agent=agent_cfg.name,
            tool=tool_name,
            arguments=_record_args(raw_args),
            ok=False,
            error=f"{code}: {message}",
            duration_ms=_elapsed_ms(started) if started is not None else 0,
        )
        return ToolResult(record=record, content=content, is_error=True, error_code=code)

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
            raise InvalidToolArgs(
                f"invalid arguments for {tool.name!r}: {format_validation_error(e)}"
            ) from e
        except Exception as e:
            # A validator raised something pydantic doesn't convert (TypeError, KeyError...).
            logger.warning("args validator raised tool=%s", tool.name, exc_info=True)
            raise InvalidToolArgs(
                f"invalid arguments for {tool.name!r} (validator raised {type(e).__name__})"
            ) from e

    async def _run(self, tool: RegisteredTool, args: BaseModel, ctx: ToolContext) -> ToolOutput:
        deadline = asyncio.timeout(tool.timeout_s)
        try:
            async with deadline:
                output = await tool.handler(args, ctx)
        except asyncio.CancelledError as e:
            if _outer_cancel_requested():
                raise  # the run is being cancelled: propagate
            raise self._handler_failed(tool, ctx, e) from e
        except ToolError as e:
            raise self._handler_refused(tool, e) from e
        except TimeoutError as e:
            if deadline.expired():
                raise ToolTimeout(f"tool {tool.name!r} timed out after {tool.timeout_s:g}s") from e
            raise self._handler_failed(tool, ctx, e) from e
        except Exception as e:
            raise self._handler_failed(tool, ctx, e) from e

        if not (
            isinstance(output, ToolOutput)
            and isinstance(output.kind, EvidenceKind)
            and isinstance(output.query, str)
            and isinstance(output.content, str)
            and (output.raw is None or isinstance(output.raw, str))
        ):
            logger.error("tool %r returned %r, not a valid ToolOutput", tool.name, type(output))
            raise InvalidToolOutput(f"tool {tool.name!r} returned a malformed result")
        if max(len(output.content), len(output.raw or "")) > self._max_content_chars:
            logger.error(
                "tool %r returned %d chars of content, %d of raw (limit %d chars)",
                tool.name,
                len(output.content),
                len(output.raw or ""),
                self._max_content_chars,
            )
            raise InvalidToolOutput(
                f"tool {tool.name!r} returned more than {self._max_content_chars} characters; "
                "narrow the query"
            )
        if len(output.query) > MAX_QUERY_CHARS:
            logger.error("tool %r returned a %d-char query", tool.name, len(output.query))
            raise InvalidToolOutput(f"tool {tool.name!r} returned an over-long query")
        return output

    @staticmethod
    def _handler_refused(tool: RegisteredTool, e: ToolError) -> ToolError:
        """A ToolError raised by a handler: its code and text are not trusted."""
        cls: type[ToolError]
        if isinstance(e, InvalidToolArgs):
            cls = InvalidToolArgs
        elif isinstance(e, ToolTimeout):
            cls = ToolTimeout  # a backend timeout reaches the model as `timeout`
        else:
            cls = ToolHandlerFailed
        text = getattr(e, "message", None) or str(e)
        extra = getattr(e, "untrusted_detail", None)
        detail = "\n".join(x for x in (text, extra) if isinstance(x, str) and x)
        return cls(
            f"tool {tool.name!r} rejected the call ({cls.code})", untrusted_detail=detail or None
        )

    @staticmethod
    def _handler_failed(tool: RegisteredTool, ctx: ToolContext, e: BaseException) -> ToolError:
        logger.warning(
            "tool handler raised tool=%s incident=%s", tool.name, ctx.incident_id, exc_info=e
        )
        return ToolHandlerFailed(
            f"tool {tool.name!r} failed ({type(e).__name__})", untrusted_detail=str(e)
        )

    async def _record(
        self, tool: RegisteredTool, output: ToolOutput, ctx: ToolContext, call_id: str
    ) -> Evidence:
        # The size cap is applied last, on the exact (defanged) text the model will see.
        summary = render_untrusted(output.content, tool.max_output_chars)
        ev_id = new_id("ev")
        evidence = Evidence(
            id=ev_id,
            incident_id=ctx.incident_id,
            tool_call_id=call_id,
            kind=output.kind,
            query=output.query,
            summary=summary,
            # Proposed key; the sink may replace it (CONTRACTS §1, v1.1).
            raw_ref=f"evidence/{ctx.incident_id}/{ev_id}",
        )
        try:
            async with asyncio.timeout(self._evidence_timeout_s):
                stored = await ctx.evidence.record(
                    evidence, output.raw if output.raw is not None else output.content
                )
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

"""Typed tool-call failures (ARCH §6).

The gateway raises these internally and converts every one of them into a
tool error for the model. None of them ever escapes ``ToolGateway.invoke``.
"""

from enum import StrEnum


class ToolErrorCode(StrEnum):
    UNKNOWN_TOOL = "unknown_tool"
    NOT_ALLOWED = "not_allowed"
    WRITE_REFUSED = "write_refused"
    INVALID_ARGS = "invalid_args"
    TIMEOUT = "timeout"
    HANDLER_FAILED = "handler_failed"
    INVALID_OUTPUT = "invalid_output"
    EVIDENCE_FAILED = "evidence_failed"
    TOO_MANY_CALLS = "too_many_calls"


class ToolError(Exception):
    """A tool call that was refused or failed. ``message`` is gateway-authored (trusted).

    ``untrusted_detail`` carries text that may have come from the target system
    (an adapter's exception message, say). It's only ever shown to the model
    inside an untrusted-data block.
    """

    code: ToolErrorCode

    def __init__(self, message: str, *, untrusted_detail: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.untrusted_detail = untrusted_detail


class UnknownTool(ToolError):
    code = ToolErrorCode.UNKNOWN_TOOL


class ToolNotAllowed(ToolError):
    """The tool isn't on the agent's allowlist (ARCH §6 step 1, a PolicyViolation)."""

    code = ToolErrorCode.NOT_ALLOWED


class WriteToolRefused(ToolError):
    """WRITE tools are never callable by agents, whatever the config says (ARCH §6 step 3)."""

    code = ToolErrorCode.WRITE_REFUSED


class InvalidToolArgs(ToolError):
    code = ToolErrorCode.INVALID_ARGS


class ToolTimeout(ToolError):
    code = ToolErrorCode.TIMEOUT


class ToolHandlerFailed(ToolError):
    code = ToolErrorCode.HANDLER_FAILED


class InvalidToolOutput(ToolError):
    code = ToolErrorCode.INVALID_OUTPUT


class EvidenceRecordFailed(ToolError):
    code = ToolErrorCode.EVIDENCE_FAILED


class TooManyToolCalls(ToolError):
    code = ToolErrorCode.TOO_MANY_CALLS

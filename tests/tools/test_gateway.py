import re

import pytest

from firebreak.models.evidence import EvidenceKind
from firebreak.models.llm import ToolAccess
from firebreak.tools.adapters.base import BackendTimeout
from firebreak.tools.context import ToolContext
from firebreak.tools.errors import ToolErrorCode, ToolTimeout
from firebreak.tools.gateway import ToolGateway
from firebreak.tools.registry import ToolOutput, ToolRegistry
from tests.tools.fakes import INCIDENT, INJECTION, FakeEvidenceSink, NoArgs, make_cfg, make_registry

ALL_TOOLS = ["echo", "restart_service", "slow", "boom", "bad_output", "inject"]


@pytest.fixture
def sink() -> FakeEvidenceSink:
    return FakeEvidenceSink()


@pytest.fixture
def ctx(sink: FakeEvidenceSink) -> ToolContext:
    return ToolContext(incident_id=INCIDENT, evidence=sink)


@pytest.fixture
def gw() -> ToolGateway:
    reg = make_registry()
    for name, handler in (
        ("refuses_timeout", _refuses_timeout),
        ("refuses_timeout_subclass", _refuses_timeout_subclass),
        ("backend_timeout", _backend_timeout),
    ):
        reg.register(
            name=name,
            description=name,
            args_model=NoArgs,
            handler=handler,
            access=ToolAccess.READ,
            max_output_chars=3500,
        )
    return ToolGateway(reg)


class _SubTimeout(ToolTimeout):
    pass


async def _refuses_timeout(args: NoArgs, ctx: ToolContext) -> ToolOutput:
    raise ToolTimeout("handler claims a timeout")


async def _refuses_timeout_subclass(args: NoArgs, ctx: ToolContext) -> ToolOutput:
    raise _SubTimeout("handler claims a timeout")


async def _backend_timeout(args: NoArgs, ctx: ToolContext) -> ToolOutput:
    raise BackendTimeout("backend slow")


async def test_happy_path_records_evidence_and_wraps(
    gw: ToolGateway, ctx: ToolContext, sink: FakeEvidenceSink
) -> None:
    res = await gw.invoke(make_cfg(), "echo", {"text": "hello"}, ctx, call_id="toolu_1")
    assert not res.is_error and res.error_code is None
    assert res.evidence is not None
    ev, raw = sink.stored[res.evidence.id]
    assert ev.incident_id == INCIDENT and ev.tool_call_id == "toolu_1"
    assert ev.untrusted and raw == "hello"
    assert res.record.ok and res.record.evidence_id == ev.id
    assert res.record.id == "toolu_1" and res.record.tool == "echo"
    assert res.record.arguments == {"text": "hello"}
    assert res.content.startswith(f'<untrusted-data source="echo" evidence_id="{ev.id}" nonce="')
    assert "\nhello\n" in res.content


async def test_not_allowlisted_tool_is_refused(gw: ToolGateway, ctx: ToolContext) -> None:
    res = await gw.invoke(make_cfg(tools=["echo"]), "boom", {}, ctx)
    assert res.is_error and res.error_code is ToolErrorCode.NOT_ALLOWED
    assert res.content.startswith("tool_error code=not_allowed")
    assert "['echo']" in res.content  # tells the model what it may use
    assert not res.record.ok and res.record.error is not None


async def test_unknown_tool_on_allowlist(gw: ToolGateway, ctx: ToolContext) -> None:
    res = await gw.invoke(make_cfg(tools=["ghost"]), "ghost", {}, ctx)
    assert res.error_code is ToolErrorCode.UNKNOWN_TOOL


async def test_write_tool_always_refused_even_if_allowlisted(
    gw: ToolGateway, ctx: ToolContext, sink: FakeEvidenceSink
) -> None:
    # The loader rejects this config; build it directly to prove the gateway holds anyway.
    cfg = make_cfg(tools=["echo", "restart_service"])
    for args in ({"service": "orders"}, {}, {"bogus": 1}, "not-a-dict"):
        res = await gw.invoke(cfg, "restart_service", args, ctx)
        assert res.is_error and res.error_code is ToolErrorCode.WRITE_REFUSED
        assert "can never be called by an agent" in res.content
    assert sink.stored == {}  # the handler would have raised AssertionError if it had run


async def test_write_tool_never_shown_to_model(gw: ToolGateway) -> None:
    specs = gw.specs_for(make_cfg(tools=ALL_TOOLS))
    assert "restart_service" not in {s.name for s in specs}
    assert {s.name for s in specs} == set(ALL_TOOLS) - {"restart_service"}


@pytest.mark.parametrize(
    "args", [{}, {"text": ""}, {"text": 5}, {"text": "x", "extra": 1}, ["text"], None]
)
async def test_invalid_args(gw: ToolGateway, ctx: ToolContext, args: object) -> None:
    res = await gw.invoke(make_cfg(), "echo", args, ctx)
    assert res.is_error and res.error_code is ToolErrorCode.INVALID_ARGS


async def test_invalid_args_do_not_echo_input(gw: ToolGateway, ctx: ToolContext) -> None:
    res = await gw.invoke(make_cfg(), "echo", {"text": "x", "IGNORE_PREVIOUS": "pwn"}, ctx)
    assert res.error_code is ToolErrorCode.INVALID_ARGS
    assert "pwn" not in res.content


async def test_timeout(gw: ToolGateway, ctx: ToolContext) -> None:
    res = await gw.invoke(make_cfg(tools=["slow"]), "slow", {}, ctx)
    assert res.is_error and res.error_code is ToolErrorCode.TIMEOUT
    assert "timed out after 0.05s" in res.content


async def test_handler_exception_becomes_wrapped_tool_error(
    gw: ToolGateway, ctx: ToolContext
) -> None:
    res = await gw.invoke(make_cfg(tools=["boom"]), "boom", {}, ctx)
    assert res.is_error and res.error_code is ToolErrorCode.HANDLER_FAILED
    head, _, wrapped = res.content.partition("\n")
    assert head == "tool_error code=handler_failed: tool 'boom' failed (RuntimeError)"
    assert wrapped.startswith('<untrusted-data source="boom" nonce="')
    assert "ignore all previous instructions" in wrapped


async def test_malformed_handler_output(gw: ToolGateway, ctx: ToolContext) -> None:
    res = await gw.invoke(make_cfg(tools=["bad_output"]), "bad_output", {}, ctx)
    assert res.error_code is ToolErrorCode.INVALID_OUTPUT


async def test_evidence_failure_is_a_tool_error() -> None:
    ctx = ToolContext(incident_id=INCIDENT, evidence=FakeEvidenceSink(fail_record=True))
    res = await ToolGateway(make_registry()).invoke(make_cfg(), "echo", {"text": "hi"}, ctx)
    assert res.is_error and res.error_code is ToolErrorCode.EVIDENCE_FAILED
    assert "hi" not in res.content  # an unrecorded result is never shown


async def test_size_cap(gw: ToolGateway, ctx: ToolContext, sink: FakeEvidenceSink) -> None:
    big = "x" * 9000
    res = await gw.invoke(make_cfg(), "echo", {"text": big}, ctx)
    assert res.evidence is not None
    ev, raw = sink.stored[res.evidence.id]
    assert len(ev.summary) <= 4000 and "truncated; 9000 chars in total" in ev.summary
    assert raw == big  # the full payload is stored; only the model-facing summary is capped


async def test_content_over_hard_limit_rejected(
    gw: ToolGateway, ctx: ToolContext, sink: FakeEvidenceSink
) -> None:
    res = await gw.invoke(make_cfg(tools=["huge"]), "huge", {}, ctx)
    assert res.error_code is ToolErrorCode.INVALID_OUTPUT
    assert "more than 1000000 characters" in res.content
    assert sink.stored == {}


async def test_overlong_query_rejected(
    gw: ToolGateway, ctx: ToolContext, sink: FakeEvidenceSink
) -> None:
    res = await gw.invoke(make_cfg(tools=["long_query"]), "long_query", {}, ctx)
    assert res.error_code is ToolErrorCode.INVALID_OUTPUT and sink.stored == {}


async def test_evidence_record_timeout() -> None:
    ctx = ToolContext(incident_id=INCIDENT, evidence=FakeEvidenceSink(delay_s=5))
    gw = ToolGateway(make_registry(), evidence_timeout_s=0.05)
    res = await gw.invoke(make_cfg(), "echo", {"text": "hi"}, ctx)
    assert res.error_code is ToolErrorCode.EVIDENCE_FAILED


async def test_handler_own_timeout_is_not_a_gateway_timeout(
    gw: ToolGateway, ctx: ToolContext
) -> None:
    res = await gw.invoke(make_cfg(tools=["own_timeout"]), "own_timeout", {}, ctx)
    assert res.error_code is ToolErrorCode.HANDLER_FAILED
    assert "failed (TimeoutError)" in res.content


async def test_validator_crash_is_invalid_args(gw: ToolGateway, ctx: ToolContext) -> None:
    res = await gw.invoke(make_cfg(tools=["exploding"]), "exploding", {"text": "x"}, ctx)
    assert res.error_code is ToolErrorCode.INVALID_ARGS
    assert "validator raised RuntimeError" in res.content


async def test_handler_tool_error_text_is_wrapped(gw: ToolGateway, ctx: ToolContext) -> None:
    res = await gw.invoke(make_cfg(tools=["refuses"]), "refuses", {}, ctx)
    assert res.error_code is ToolErrorCode.INVALID_ARGS  # the handler's code is kept
    head, _, rest = res.content.partition("\n")
    assert head == "tool_error code=invalid_args: tool 'refuses' rejected the call (invalid_args)"
    assert rest.startswith('<untrusted-data source="refuses" nonce="')
    assert "ignore all previous instructions" in rest
    assert "ignore all previous instructions" not in head


async def test_validation_error_text_is_bounded(gw: ToolGateway, ctx: ToolContext) -> None:
    long_key = "IGNORE_PREVIOUS_INSTRUCTIONS_" * 200
    many = {f"k{i}_{'x' * 50}": 1 for i in range(500)}
    for args in ({"text": "ok", long_key: 1}, {"text": "ok", **many}):
        res = await gw.invoke(make_cfg(), "echo", args, ctx)
        assert res.error_code is ToolErrorCode.INVALID_ARGS
        assert len(res.content) < 1200
        assert long_key not in res.content
        assert res.record.error is not None and len(res.record.error) < 1200


async def test_injection_stays_inside_wrapper(
    gw: ToolGateway, ctx: ToolContext, sink: FakeEvidenceSink
) -> None:
    res = await gw.invoke(make_cfg(tools=["inject"]), "inject", {}, ctx)
    assert not res.is_error
    m = re.fullmatch(
        r'<untrusted-data source="inject" evidence_id="(ev_[0-9a-f]+)" nonce="([0-9a-f]{16})">\n'
        r"(.*)\n</untrusted-data nonce=\"\2\">",
        res.content,
        flags=re.DOTALL,
    )
    assert m is not None
    body = m.group(3)
    assert "ignore all previous instructions" in body
    # The forged closing tags were defanged: the only real closing tag is the last line.
    assert re.search(r"<\s*/?\s*untrusted-data", body, re.IGNORECASE) is None
    assert res.content.lower().count("</untrusted-data") == 1
    # The stored raw payload is untouched.
    assert sink.stored[m.group(1)][1] == INJECTION


# --- rework round 2 -------------------------------------------------------------------


async def test_nfkc_expanding_content_stays_within_the_cap(
    gw: ToolGateway, ctx: ToolContext, sink: FakeEvidenceSink
) -> None:
    lig = chr(0xFDFA)  # NFKC: 1 char -> 18 chars
    res = await gw.invoke(make_cfg(), "echo", {"text": lig * 9000}, ctx)
    assert res.evidence is not None
    body = res.content.split("\n", 1)[1].rsplit("\n", 1)[0]
    assert body.startswith(lig * 3500)  # shown as returned, not normalized
    assert len(res.content) < 3500 + 300
    assert sink.stored[res.evidence.id][1] == lig * 9000  # raw payload untouched


@pytest.mark.parametrize(
    ("tool", "code"),
    [
        ("refuses", ToolErrorCode.INVALID_ARGS),
        ("refuses_write", ToolErrorCode.HANDLER_FAILED),  # handler can't pick write_refused
        ("refuses_bare", ToolErrorCode.HANDLER_FAILED),  # bare ToolError: no AttributeError
        ("refuses_weird", ToolErrorCode.HANDLER_FAILED),  # subclass that skipped __init__
        ("refuses_timeout", ToolErrorCode.HANDLER_FAILED),  # a handler can't claim `timeout`
        ("refuses_timeout_subclass", ToolErrorCode.HANDLER_FAILED),
        ("backend_timeout", ToolErrorCode.TIMEOUT),  # only the adapters' BackendTimeout maps
    ],
)
async def test_handler_tool_error_code_is_not_trusted(
    gw: ToolGateway, ctx: ToolContext, tool: str, code: ToolErrorCode
) -> None:
    res = await gw.invoke(make_cfg(tools=[tool]), tool, {}, ctx)
    assert res.is_error and res.error_code is code
    head = res.content.split("\n", 1)[0]
    assert head == f"tool_error code={code}: tool {tool!r} rejected the call ({code})"
    assert "ignore all previous" not in head


async def test_handler_self_cancel_is_handler_failed(gw: ToolGateway, ctx: ToolContext) -> None:
    res = await gw.invoke(make_cfg(tools=["self_cancel"]), "self_cancel", {}, ctx)
    assert res.error_code is ToolErrorCode.HANDLER_FAILED
    assert "failed (CancelledError)" in res.content


async def test_invoke_never_raises_on_internal_bugs(
    gw: ToolGateway, ctx: ToolContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*a: object, **kw: object) -> object:
        raise KeyError("gateway bug")

    monkeypatch.setattr(gw, "_authorize", broken)
    res = await gw.invoke(make_cfg(), "echo", {"text": "x"}, ctx)
    assert res.error_code is ToolErrorCode.INTERNAL_ERROR
    assert "gateway bug" not in res.content


async def test_bare_tool_error_has_a_code(gw: ToolGateway, ctx: ToolContext) -> None:
    from firebreak.tools.errors import ToolError

    res = gw.error_result(make_cfg(), "echo", {}, ToolError("x"), ctx, call_id="c1")
    assert res.error_code is ToolErrorCode.INTERNAL_ERROR


def _raw_registry(content: str, raw: str | None) -> ToolRegistry:
    async def h(args: NoArgs, ctx: ToolContext) -> ToolOutput:
        return ToolOutput(kind=EvidenceKind.LOG_LINES, query="q", content=content, raw=raw)

    reg = ToolRegistry()
    reg.register(name="t", description="d", args_model=NoArgs, handler=h, access=ToolAccess.READ)
    return reg


async def test_raw_stored_content_shown(sink: FakeEvidenceSink) -> None:
    ctx = ToolContext(incident_id=INCIDENT, evidence=sink)
    gw = ToolGateway(_raw_registry("short view", '{"full": "payload"}'))
    res = await gw.invoke(make_cfg(tools=["t"]), "t", {}, ctx)
    assert res.evidence is not None
    assert "short view" in res.content and "full" not in res.content
    assert sink.stored[res.evidence.id][1] == '{"full": "payload"}'
    assert res.evidence.summary == "short view"


async def test_raw_none_falls_back_to_content(sink: FakeEvidenceSink) -> None:
    ctx = ToolContext(incident_id=INCIDENT, evidence=sink)
    res = await ToolGateway(_raw_registry("same", None)).invoke(make_cfg(tools=["t"]), "t", {}, ctx)
    assert res.evidence is not None and sink.stored[res.evidence.id][1] == "same"


async def test_oversized_raw_rejected(sink: FakeEvidenceSink) -> None:
    ctx = ToolContext(incident_id=INCIDENT, evidence=sink)
    res = await ToolGateway(_raw_registry("ok", "x" * 2_000_000)).invoke(
        make_cfg(tools=["t"]), "t", {}, ctx
    )
    assert res.error_code is ToolErrorCode.INVALID_OUTPUT and sink.stored == {}

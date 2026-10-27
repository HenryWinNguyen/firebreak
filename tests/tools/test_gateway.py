import re

import pytest

from firebreak.tools.context import ToolContext
from firebreak.tools.errors import ToolErrorCode
from firebreak.tools.gateway import ToolGateway
from tests.tools.fakes import INCIDENT, INJECTION, FakeEvidenceSink, make_cfg, make_registry

ALL_TOOLS = ["echo", "restart_service", "slow", "boom", "bad_output", "inject"]


@pytest.fixture
def sink() -> FakeEvidenceSink:
    return FakeEvidenceSink()


@pytest.fixture
def ctx(sink: FakeEvidenceSink) -> ToolContext:
    return ToolContext(incident_id=INCIDENT, evidence=sink)


@pytest.fixture
def gw() -> ToolGateway:
    return ToolGateway(make_registry())


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


async def test_size_cap(ctx: ToolContext, sink: FakeEvidenceSink) -> None:
    gw = ToolGateway(make_registry(), max_raw_chars=8000)
    big = "x" * 9000
    res = await gw.invoke(make_cfg(), "echo", {"text": big}, ctx)
    assert res.evidence is not None
    ev, raw = sink.stored[res.evidence.id]
    assert len(ev.summary) <= 4000 and "truncated 5500 chars" in ev.summary
    assert raw.startswith("x" * 8000) and "truncated 1000 chars" in raw


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

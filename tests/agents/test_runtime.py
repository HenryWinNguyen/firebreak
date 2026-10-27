import asyncio
import json
import re
from typing import Any

import pytest

from firebreak.agents.runtime import MAX_TOOL_CALLS_PER_STEP, iter_cited_evidence_ids, run_agent
from firebreak.models.agents import AgentConfig, AgentRunResult
from firebreak.models.evidence import Evidence, EvidenceKind
from firebreak.models.llm import LLMRequest, LLMResponse, ToolAccess, Usage
from firebreak.tools.context import ToolContext
from firebreak.tools.gateway import ToolGateway
from firebreak.tools.registry import ToolOutput
from firebreak.tools.untrusted import UNTRUSTED_DATA_POLICY
from tests.tools.fakes import (
    INCIDENT,
    FakeEvidenceSink,
    NoArgs,
    ScriptedLLM,
    Step,
    cite_last,
    evidence_ids_in,
    last_tool_results,
    make_cfg,
    make_registry,
    report_json,
    text_response,
    tool_response,
)

ECHO = ("toolu_1", "echo", {"text": "orders 5xx at 12:01"})


class Harness:
    def __init__(self, script: list[Step], **cfg: Any) -> None:
        self.sink = FakeEvidenceSink()
        self.ctx = ToolContext(incident_id=INCIDENT, evidence=self.sink)
        self.llm = ScriptedLLM(script)
        self.registry = make_registry()
        self.gateway = ToolGateway(self.registry)
        self.cfg: AgentConfig = make_cfg(**cfg)

    async def run(self) -> AgentRunResult:
        return await run_agent(self.cfg, "is orders erroring?", self.ctx, self.llm, self.gateway)


def tool_errors(req: LLMRequest) -> list[str]:
    return [r["content"] for r in last_tool_results(req) if r["is_error"]]


async def test_happy_path() -> None:
    h = Harness([tool_response(ECHO), cite_last()])
    res = await h.run()

    assert res.stopped_reason == "final_output" and res.error is None
    assert res.agent == "echo" and res.incident_id == INCIDENT
    assert res.steps == 2 and len(h.llm.requests) == 2
    assert res.usage.cost_usd == pytest.approx(0.002) and res.usage.input_tokens == 20
    [call] = res.tool_calls
    assert call.ok and call.id == "toolu_1" and call.evidence_id in h.sink.stored
    assert res.output["findings"][0]["evidence_ids"] == [call.evidence_id]  # type: ignore[index]
    assert h.sink.exists_calls == [(INCIDENT, [call.evidence_id])]

    first, second = h.llm.requests
    assert [t.name for t in first.tools] == ["echo"]
    assert first.model == "fake-model" and first.max_tokens == 4096
    assert "You are the echo agent" in first.system
    assert UNTRUSTED_DATA_POLICY in first.system and '"SpecialistReport"' in first.system
    assert first.messages == [{"role": "user", "content": "is orders erroring?"}]
    # second turn: assistant tool_use echoed back, then the tool_result for the same id
    assert second.messages[1]["role"] == "assistant"
    [tr] = last_tool_results(second)
    assert tr["tool_use_id"] == "toolu_1" and tr["is_error"] is False
    assert tr["content"].startswith("<untrusted-data ")


async def test_max_steps() -> None:
    h = Harness([tool_response(ECHO)] * 3, max_steps=3)
    res = await h.run()
    assert res.stopped_reason == "max_steps"
    assert res.steps == 3 and len(h.llm.requests) == 3 and res.output == {}
    # the last step's tool calls are not run: nothing would ever read the result
    assert len(res.tool_calls) == 2 and len(h.sink.stored) == 2


async def test_budget_stop() -> None:
    h = Harness([tool_response(ECHO, cost=0.03)] * 5, budget_usd=0.05)
    res = await h.run()
    assert res.stopped_reason == "budget"
    assert res.steps == 2 and len(h.llm.requests) == 2  # no call after the budget is crossed
    assert res.usage.cost_usd == pytest.approx(0.06) and res.output == {}
    assert len(res.tool_calls) == 1  # the over-budget step's tools never ran


async def test_budget_beats_final_output() -> None:
    h = Harness([tool_response(ECHO), cite_last(cost=1.0)], budget_usd=0.05)
    res = await h.run()
    assert res.stopped_reason == "budget" and res.output == {}


async def test_budget_exactly_reached_is_not_exceeded() -> None:
    h = Harness([tool_response(ECHO, cost=0.025), cite_last(cost=0.025)], budget_usd=0.05)
    assert (await h.run()).stopped_reason == "final_output"


async def test_repair_turn_success() -> None:
    h = Harness([tool_response(ECHO), text_response("I think orders is down."), cite_last()])
    res = await h.run()
    assert res.stopped_reason == "final_output" and res.steps == 3
    repair = h.llm.requests[2].messages[-1]
    assert repair["role"] == "user" and "not valid SpecialistReport JSON" in repair["content"]
    assert "only chance" in repair["content"]


async def test_repair_turn_failure() -> None:
    h = Harness(
        [tool_response(ECHO), text_response("nope"), text_response('{"agent": "echo"}')],
        max_steps=8,
    )
    res = await h.run()
    assert res.stopped_reason == "error" and res.output == {}
    assert res.steps == 3 and len(h.llm.requests) == 3  # exactly one repair turn
    assert res.error is not None and res.error.startswith("final answer invalid after repair")


async def test_code_fenced_json_is_accepted() -> None:
    def fenced(req: LLMRequest) -> LLMResponse:
        return text_response(f"```json\n{report_json(evidence_ids_in(req))}\n```")

    assert (await Harness([tool_response(ECHO), fenced]).run()).stopped_reason == "final_output"


async def test_hallucinated_evidence_id_rejected() -> None:
    lie = cite_last(extra=["ev_made_up"])
    h = Harness([tool_response(ECHO), lie, lie], max_steps=8)
    res = await h.run()
    assert res.stopped_reason == "error" and res.output == {}
    assert "ev_made_up" in h.llm.requests[2].messages[-1]["content"]
    assert all("ev_made_up" in ids for _, ids in h.sink.exists_calls)


async def test_hallucinated_evidence_id_fixed_by_repair() -> None:
    h = Harness([tool_response(ECHO), cite_last(extra=["ev_made_up"]), cite_last()])
    res = await h.run()
    assert res.stopped_reason == "final_output"
    assert "ev_made_up" not in json.dumps(res.output)


async def test_evidence_from_another_incident_rejected() -> None:
    h = Harness([text_response(report_json(["ev_other"]))] * 2)
    h.sink.stored["ev_other"] = (
        Evidence(
            id="ev_other",
            incident_id="inc_someone_else",
            tool_call_id="tc_x",
            kind=EvidenceKind.LOG_LINES,
            query="q",
            summary="s",
            raw_ref="r",
        ),
        "raw",
    )
    res = await h.run()
    assert res.stopped_reason == "error"
    assert h.sink.exists_calls[0] == (INCIDENT, ["ev_other"])


async def test_non_allowlisted_tool_returns_tool_error_and_run_continues() -> None:
    h = Harness([tool_response(("toolu_x", "boom", {})), tool_response(ECHO), cite_last()])
    res = await h.run()
    assert res.stopped_reason == "final_output"
    [err] = tool_errors(h.llm.requests[1])
    assert err.startswith("tool_error code=not_allowed")
    assert [c.ok for c in res.tool_calls] == [False, True]


async def test_write_tool_refused() -> None:
    # Bypass the loader on purpose: even an allowlisted WRITE tool must be refused.
    h = Harness(
        [
            tool_response(("toolu_w", "restart_service", {"service": "orders"})),
            tool_response(ECHO),
            cite_last(),
        ],
        tools=["echo", "restart_service"],
    )
    res = await h.run()
    assert res.stopped_reason == "final_output"
    assert "restart_service" not in [t.name for t in h.llm.requests[0].tools]
    [err] = tool_errors(h.llm.requests[1])
    assert err.startswith("tool_error code=write_refused")
    assert res.tool_calls[0].tool == "restart_service" and not res.tool_calls[0].ok


async def test_tool_timeout_is_a_tool_error() -> None:
    h = Harness([tool_response(("toolu_s", "slow", {}), ECHO), cite_last()], tools=["echo", "slow"])
    res = await h.run()
    assert res.stopped_reason == "final_output"
    results = last_tool_results(h.llm.requests[1])
    assert [r["tool_use_id"] for r in results] == ["toolu_s", "toolu_1"]  # order preserved
    assert results[0]["is_error"] and "code=timeout" in results[0]["content"]
    assert not results[1]["is_error"]


async def test_injection_in_tool_result_stays_wrapped() -> None:
    h = Harness(
        [
            tool_response(("toolu_i", "inject", {})),
            # the "compromised" model obeys the injected text...
            tool_response(("toolu_r", "restart_service", {"service": "orders"})),
            cite_last(),
        ],
        tools=["echo", "inject"],
    )
    res = await h.run()
    [tr] = last_tool_results(h.llm.requests[1])
    content = tr["content"]
    m = re.fullmatch(
        r'<untrusted-data source="inject" evidence_id="ev_\w+" nonce="(\w+)">\n(.*)\n'
        r'</untrusted-data nonce="\1">',
        content,
        re.DOTALL,
    )
    assert m is not None
    assert "ignore all previous instructions" in m.group(2)
    assert content.lower().count("</untrusted-data") == 1
    # ...and the follow-up WRITE call goes nowhere
    [err] = tool_errors(h.llm.requests[2])
    assert "code=not_allowed" in err
    assert res.stopped_reason == "final_output"


async def test_tool_calls_run_in_parallel() -> None:
    h = Harness([], tools=["meet"])
    arrived = 0
    both = asyncio.Event()

    async def meet(args: NoArgs, ctx: ToolContext) -> ToolOutput:
        nonlocal arrived
        arrived += 1
        if arrived == 2:
            both.set()
        await both.wait()  # deadlocks (-> timeout) unless both calls run concurrently
        return ToolOutput(kind=EvidenceKind.METRIC_SERIES, query="q", content="ok")

    h.registry.register(
        name="meet",
        description="x",
        args_model=NoArgs,
        handler=meet,
        access=ToolAccess.READ,
        timeout_s=1.0,
    )
    h.llm.script = [tool_response(("a", "meet", {}), ("b", "meet", {})), cite_last()]
    res = await h.run()
    assert res.stopped_reason == "final_output"
    assert [c.ok for c in res.tool_calls] == [True, True]


async def test_too_many_tool_calls_in_one_turn() -> None:
    n = MAX_TOOL_CALLS_PER_STEP + 2
    calls = [(f"t{i}", "echo", {"text": str(i)}) for i in range(n)]
    h = Harness([tool_response(*calls), cite_last()])
    res = await h.run()
    results = last_tool_results(h.llm.requests[1])
    assert len(results) == n
    assert sum(r["is_error"] for r in results) == 2
    assert "code=too_many_calls" in results[-1]["content"]
    assert len(h.sink.stored) == MAX_TOOL_CALLS_PER_STEP
    assert res.stopped_reason == "final_output"


async def test_model_refusal_stops_with_error() -> None:
    refusal = LLMResponse(model="fake", content=[], stop_reason="refusal", usage=Usage())
    res = await Harness([refusal]).run()
    assert res.stopped_reason == "error"
    assert res.error == "the model refused to answer (stop_reason=refusal)"


async def test_llm_errors_propagate() -> None:
    class Down:
        async def complete(self, request: LLMRequest) -> LLMResponse:
            raise ConnectionError("provider down")

    h = Harness([])
    with pytest.raises(ConnectionError):
        await run_agent(h.cfg, "t", h.ctx, Down(), h.gateway)


def test_iter_cited_evidence_ids() -> None:
    out = {
        "findings": [{"evidence_ids": ["ev_a", "ev_b"]}, {"evidence_ids": ["ev_a"]}],
        "nested": {"evidence_id": "ev_c", "other": [{"evidence_ids": ["ev_d"]}]},
        "evidence_ids": "not-a-list",
    }
    assert sorted(set(iter_cited_evidence_ids(out))) == ["ev_a", "ev_b", "ev_c", "ev_d"]


# --- rework round 1 -------------------------------------------------------------------


async def test_fallback_model_is_passed_to_the_client() -> None:
    h = Harness([tool_response(ECHO), cite_last()], fallback_model="fake-fallback")
    await h.run()
    assert [r.fallback_model for r in h.llm.requests] == ["fake-fallback", "fake-fallback"]
    h2 = Harness([tool_response(ECHO), cite_last()])
    await h2.run()
    assert h2.llm.requests[0].fallback_model is None


def _raw_tool_turn(*blocks: dict[str, Any]) -> LLMResponse:
    return LLMResponse(
        model="fake",
        content=[{"type": "text", "text": "checking"}, *blocks],
        stop_reason="tool_use",
        usage=Usage(cost_usd=0.001),
    )


def _no_tool_use_echoed(req: LLMRequest) -> bool:
    return all(
        not (
            isinstance(m["content"], list)
            and any(b.get("type") == "tool_use" for b in m["content"])
        )
        for m in req.messages
    )


@pytest.mark.parametrize(
    "blocks",
    [
        ({"type": "tool_use", "name": "echo", "input": {"text": "a"}},),  # missing id
        ({"type": "tool_use", "id": "", "name": "echo", "input": {"text": "a"}},),  # empty
        ({"type": "tool_use", "id": "  ", "name": "echo", "input": {"text": "a"}},),  # blank
        ({"type": "tool_use", "id": 7, "name": "echo", "input": {"text": "a"}},),  # not a str
        ({"type": "tool_use", "id": "x" * 129, "name": "echo", "input": {"text": "a"}},),  # long
        (
            {"type": "tool_use", "id": "dup", "name": "echo", "input": {"text": "a"}},
            {"type": "tool_use", "id": "dup", "name": "echo", "input": {"text": "b"}},
        ),
    ],
)
async def test_malformed_tool_ids_use_the_repair_turn(blocks: tuple[dict[str, Any], ...]) -> None:
    h = Harness([_raw_tool_turn(*blocks), tool_response(ECHO), cite_last()])
    res = await h.run()
    assert res.stopped_reason == "final_output"
    retry = h.llm.requests[1]
    # Nothing ran, no ids were invented, and the bad tool_use blocks were not echoed back.
    assert h.sink.stored.keys() == {res.tool_calls[0].evidence_id}
    assert [c.id for c in res.tool_calls] == ["toolu_1"]
    assert _no_tool_use_echoed(retry)
    assert retry.messages[-2] == {
        "role": "assistant",
        "content": [{"type": "text", "text": "checking"}],
    }
    assert "none of its tool calls ran" in retry.messages[-1]["content"]


async def test_malformed_tool_ids_twice_is_an_error() -> None:
    bad = _raw_tool_turn({"type": "tool_use", "name": "echo", "input": {"text": "a"}})
    h = Harness([bad, bad])
    res = await h.run()
    assert res.stopped_reason == "error"
    assert (
        res.error
        == "malformed tool turn after repair: a tool call had a missing, empty or invalid id"
    )
    assert res.tool_calls == [] and h.sink.stored == {}


async def test_malformed_tool_turn_shares_the_single_repair() -> None:
    bad = _raw_tool_turn({"type": "tool_use", "name": "echo", "input": {"text": "a"}})
    h = Harness([bad, text_response("not json")], max_steps=8)
    res = await h.run()
    assert res.stopped_reason == "error" and len(h.llm.requests) == 2


class _FlakyGateway(ToolGateway):
    async def invoke(self, agent_cfg: AgentConfig, tool_name: str, *a: Any, **kw: Any) -> Any:
        if tool_name == "boom":
            raise RuntimeError("gateway bug")
        return await super().invoke(agent_cfg, tool_name, *a, **kw)


async def test_unexpected_gateway_exception_is_a_per_call_tool_error() -> None:
    h = Harness([tool_response(("t_boom", "boom", {}), ECHO), cite_last()], tools=["echo", "boom"])
    h.gateway = _FlakyGateway(h.registry)
    res = await h.run()
    assert res.stopped_reason == "final_output"
    results = last_tool_results(h.llm.requests[1])
    assert [r["tool_use_id"] for r in results] == ["t_boom", "toolu_1"]
    assert results[0]["is_error"] and "code=internal_error" in results[0]["content"]
    assert "gateway bug" not in results[0]["content"]
    assert not results[1]["is_error"]


async def test_cancelling_the_run_cancels_sibling_tool_calls() -> None:
    h = Harness([], tools=["hang"])
    started: list[str] = []
    cancelled: list[str] = []
    both_started = asyncio.Event()

    async def hang(args: NoArgs, ctx: ToolContext) -> ToolOutput:
        me = f"call{len(started)}"
        started.append(me)
        if len(started) == 2:
            both_started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append(me)
            raise
        raise AssertionError("unreachable")

    h.registry.register(
        name="hang",
        description="x",
        args_model=NoArgs,
        handler=hang,
        access=ToolAccess.READ,
        timeout_s=60,
    )
    h.llm.script = [tool_response(("a", "hang", {}), ("b", "hang", {}))]
    task = asyncio.create_task(h.run())
    await asyncio.wait_for(both_started.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sorted(cancelled) == ["call0", "call1"]


async def test_empty_end_turn_is_not_echoed() -> None:
    empty = LLMResponse(model="fake", content=[], stop_reason="end_turn", usage=Usage())
    blank = text_response("   ")
    for first in (empty, blank):
        h = Harness([tool_response(ECHO), first, cite_last()])
        res = await h.run()
        assert res.stopped_reason == "final_output"
        retry = h.llm.requests[2]
        assert all(m["content"] not in ([], "") for m in retry.messages)
        assert all(
            not (m["role"] == "assistant" and m["content"] == [{"type": "text", "text": "   "}])
            for m in retry.messages
        )
        # the repair text is merged into the trailing user turn (after the tool results)
        last = retry.messages[-1]
        assert last["role"] == "user" and retry.messages[-2]["role"] == "assistant"
        assert last["content"][0]["type"] == "tool_result"
        assert "only chance" in last["content"][-1]["text"]


@pytest.mark.parametrize("bad_cost", [float("nan"), float("inf"), -1.0])
async def test_non_finite_or_negative_cost_is_over_budget(bad_cost: float) -> None:
    h = Harness([tool_response(ECHO, cost=bad_cost), cite_last()])
    res = await h.run()
    assert res.stopped_reason == "budget" and len(h.llm.requests) == 1


async def test_evidence_exists_timeout_propagates() -> None:
    h = Harness([tool_response(ECHO), cite_last()])
    h.sink.delay_s = 0.0
    sink = h.sink

    original_exists = sink.exists

    async def slow_exists(incident_id: str, ids: list[str]) -> set[str]:
        await asyncio.sleep(5)
        return await original_exists(incident_id, ids)

    sink.exists = slow_exists  # type: ignore[method-assign]
    with pytest.raises(TimeoutError):
        await run_agent(h.cfg, "t", h.ctx, h.llm, h.gateway, evidence_timeout_s=0.05)


async def test_repair_problem_text_is_bounded() -> None:
    long_key = "X" * 5000
    h = Harness([text_response(json.dumps({long_key: 1})), text_response("{}")])
    res = await h.run()
    assert res.stopped_reason == "error"
    repair = h.llm.requests[1].messages[-1]["content"]
    text = repair if isinstance(repair, str) else repair[-1]["text"]
    assert len(text) < 1300 and long_key not in text
    assert res.error is not None and len(res.error) < 1100


# --- rework round 2 -------------------------------------------------------------------


async def test_tool_use_id_of_exactly_128_chars_is_accepted() -> None:
    tid = "x" * 128
    h = Harness([tool_response((tid, "echo", {"text": "a"})), cite_last()])
    res = await h.run()
    assert res.stopped_reason == "final_output" and res.tool_calls[0].id == tid


async def test_max_tokens_tool_turn_is_not_executed() -> None:
    cut = LLMResponse(
        model="fake",
        content=[{"type": "tool_use", "id": "toolu_cut", "name": "echo", "input": {"text": "a"}}],
        stop_reason="max_tokens",
        usage=Usage(cost_usd=0.001),
    )
    h = Harness([cut, tool_response(ECHO), cite_last()])
    res = await h.run()
    assert res.stopped_reason == "final_output"
    assert [c.id for c in res.tool_calls] == ["toolu_1"]  # the cut-off call never ran
    retry = h.llm.requests[1]
    assert _no_tool_use_echoed(retry)
    assert "max_tokens" in retry.messages[-1]["content"][-1]["text"]  # merged into the task turn


async def test_max_tokens_tool_turn_twice_is_an_error() -> None:
    cut = LLMResponse(
        model="fake",
        content=[{"type": "tool_use", "id": "toolu_cut", "name": "echo", "input": {"text": "a"}}],
        stop_reason="max_tokens",
        usage=Usage(cost_usd=0.001),
    )
    h = Harness([cut, cut])
    res = await h.run()
    assert res.stopped_reason == "error" and res.tool_calls == []
    assert res.error is not None and "max_tokens" in res.error


async def test_handler_self_cancel_does_not_affect_siblings() -> None:
    h = Harness(
        [tool_response(("t_c", "self_cancel", {}), ECHO), cite_last()],
        tools=["echo", "self_cancel"],
    )
    res = await h.run()
    assert res.stopped_reason == "final_output"
    results = last_tool_results(h.llm.requests[1])
    assert [r["tool_use_id"] for r in results] == ["t_c", "toolu_1"]
    assert "code=handler_failed" in results[0]["content"] and not results[1]["is_error"]


class _StrayCancelGateway(ToolGateway):
    async def invoke(self, agent_cfg: AgentConfig, tool_name: str, *a: Any, **kw: Any) -> Any:
        if tool_name == "boom":
            raise asyncio.CancelledError  # not a cancellation of the run
        return await super().invoke(agent_cfg, tool_name, *a, **kw)


async def test_stray_cancelled_error_from_gateway_is_a_per_call_error() -> None:
    h = Harness([tool_response(("t_boom", "boom", {}), ECHO), cite_last()], tools=["echo", "boom"])
    h.gateway = _StrayCancelGateway(h.registry)
    res = await h.run()
    assert res.stopped_reason == "final_output"
    results = last_tool_results(h.llm.requests[1])
    assert len(results) == 2
    assert "code=internal_error" in results[0]["content"] and not results[1]["is_error"]


async def test_cancelling_the_run_leaves_no_tool_task_running() -> None:
    h = Harness([], tools=["hang", "self_cancel"])
    gate = asyncio.Event()
    finished: list[str] = []

    async def hang(args: NoArgs, ctx: ToolContext) -> ToolOutput:
        gate.set()
        try:
            await asyncio.sleep(60)
        finally:
            finished.append("hang")
        raise AssertionError("unreachable")

    h.registry.register(
        name="hang",
        description="x",
        args_model=NoArgs,
        handler=hang,
        access=ToolAccess.READ,
        timeout_s=60,
    )
    h.llm.script = [tool_response(("a", "hang", {}), ("b", "self_cancel", {}))]
    before = asyncio.all_tasks()
    task = asyncio.create_task(h.run())
    await asyncio.wait_for(gate.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished == ["hang"]
    assert asyncio.all_tasks() - before - {asyncio.current_task()} == set()

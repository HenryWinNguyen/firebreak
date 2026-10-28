"""OpenAICompatLLMClient and RoutingLLMClient tests over httpx.MockTransport. No network."""

import json
import logging
import os
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from firebreak.llm import (
    FixtureStore,
    LLMConnectionRefused,
    LLMError,
    LLMMode,
    LLMRetriesExhausted,
    LLMSettings,
    OpenAICompatLLMClient,
    PricingError,
    PricingTable,
    RecordingLLMClient,
    RoutingLLMClient,
    TokenBucket,
    UnknownModelError,
    build_llm_client,
)
from firebreak.models.llm import LLMRequest, LLMResponse, ToolSpec, Usage

LOCAL = "qwen3:8b"
SONNET = "claude-sonnet-5-5"
HAIKU = "claude-haiku-5-5"

TOOLS = [
    ToolSpec(name="a_tool", description="a", input_schema={"type": "object"}),
    ToolSpec(name="b_tool", description="b", input_schema={"type": "object"}),
]


def chat_json(
    *,
    content: str | None = "hello",
    tool_calls: list[dict[str, Any]] | None = None,
    finish: str = "stop",
    model: str = LOCAL,
    usage: dict[str, int] | None = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls is not None:
        message["tool_calls"] = tool_calls
    return {
        "id": "chatcmpl-1",
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
        "usage": usage or {"prompt_tokens": 120, "completion_tokens": 30, "total_tokens": 150},
    }


def call(call_id: str, name: str, arguments: str) -> dict[str, Any]:
    return {"id": call_id, "type": "function", "function": {"name": name, "arguments": arguments}}


class Fake:
    def __init__(self, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        self.requests: list[httpx.Request] = []
        self.bodies: list[dict[str, Any]] = []
        self._handler = handler

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.bodies.append(json.loads(request.content))
        return self._handler(request)


def ok(body: dict[str, Any] | None = None) -> Fake:
    return Fake(lambda r: httpx.Response(200, json=body or chat_json()))


def make_client(
    fake: Callable[[httpx.Request], httpx.Response], **settings: Any
) -> tuple[OpenAICompatLLMClient, list[float]]:
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    http = httpx.AsyncClient(transport=httpx.MockTransport(fake))
    client = OpenAICompatLLMClient(
        LLMSettings(**settings),
        client=http,
        limiter=TokenBucket(6000),
        sleep=sleep,
        rng=random.Random(0),
    )
    return client, sleeps


def req(
    messages: list[dict[str, Any]] | None = None,
    model: str = LOCAL,
    fallback_model: str | None = None,
) -> LLMRequest:
    return LLMRequest(
        model=model,
        fallback_model=fallback_model,
        system="You are an SRE.",
        messages=messages or [{"role": "user", "content": "why 500s?"}],
        tools=TOOLS,
    )


# ---- translation: request ----


async def test_request_translation_text_and_tools() -> None:
    fake = ok()
    client, _ = make_client(fake)
    await client.complete(req())
    assert fake.requests[0].url == "http://localhost:11434/v1/chat/completions"
    body = fake.bodies[0]
    assert body["model"] == LOCAL and body["max_tokens"] == 4096 and body["stream"] is False
    assert body["messages"] == [
        {"role": "system", "content": "You are an SRE."},
        {"role": "user", "content": "why 500s?"},
    ]
    assert body["tools"][0] == {
        "type": "function",
        "function": {"name": "a_tool", "description": "a", "parameters": {"type": "object"}},
    }
    assert len(body["tools"]) == 2
    assert "authorization" not in fake.requests[0].headers


async def test_multi_tool_turn_and_tool_result_round_trip() -> None:
    messages: list[dict[str, Any]] = [
        {"role": "user", "content": "go"},
        {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "checking"},
                {"type": "tool_use", "id": "call_1", "name": "a_tool", "input": {"x": 1}},
                {"type": "tool_use", "id": "call_2", "name": "b_tool", "input": {}},
            ],
        },
        {
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "call_1", "content": "res one"},
                {
                    "type": "tool_result",
                    "tool_use_id": "call_2",
                    "content": [{"type": "text", "text": "bad"}],
                    "is_error": True,
                },
                {"type": "text", "text": "now answer"},
            ],
        },
    ]
    fake = ok()
    client, _ = make_client(fake)
    await client.complete(req(messages))
    sent = fake.bodies[0]["messages"]
    assert sent[2] == {
        "role": "assistant",
        "content": "checking",
        "tool_calls": [
            call("call_1", "a_tool", '{"x": 1}'),
            call("call_2", "b_tool", "{}"),
        ],
    }
    assert sent[3] == {"role": "tool", "tool_call_id": "call_1", "content": "res one"}
    assert sent[4] == {"role": "tool", "tool_call_id": "call_2", "content": "[error] bad"}
    assert sent[5] == {"role": "user", "content": "now answer"}
    assert len(sent) == 6


async def test_assistant_tool_only_turn_has_null_content() -> None:
    messages: list[dict[str, Any]] = [
        {"role": "user", "content": "go"},
        {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "c1", "name": "a_tool", "input": {}}],
        },
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "c1", "content": "r"}]},
    ]
    fake = ok()
    client, _ = make_client(fake)
    await client.complete(req(messages))
    sent = fake.bodies[0]["messages"]
    assert sent[2]["content"] is None and sent[2]["tool_calls"][0]["id"] == "c1"
    assert sent[3] == {"role": "tool", "tool_call_id": "c1", "content": "r"}
    assert len(sent) == 4  # a tool-result-only user turn adds no empty user message


async def test_unsupported_block_is_an_error() -> None:
    client, _ = make_client(ok())
    messages = [{"role": "user", "content": [{"type": "image", "source": {}}]}]
    with pytest.raises(LLMError, match="unsupported content block"):
        await client.complete(req(messages))


# ---- translation: response ----


async def test_text_reply_usage_and_zero_cost() -> None:
    client, _ = make_client(ok())
    resp = await client.complete(req())
    assert resp.content == [{"type": "text", "text": "hello"}]
    assert resp.stop_reason == "end_turn" and resp.model == LOCAL
    assert resp.usage == Usage(input_tokens=120, output_tokens=30, cost_usd=0.0)


async def test_tool_calls_become_tool_use_blocks_with_provider_ids() -> None:
    body = chat_json(
        content=None,
        tool_calls=[call("call_a", "a_tool", '{"q": "up"}'), call("call_b", "b_tool", "{}")],
        finish="tool_calls",
    )
    client, _ = make_client(ok(body))
    resp = await client.complete(req())
    assert resp.stop_reason == "tool_use"
    assert resp.content == [
        {"type": "tool_use", "id": "call_a", "name": "a_tool", "input": {"q": "up"}},
        {"type": "tool_use", "id": "call_b", "name": "b_tool", "input": {}},
    ]


@pytest.mark.parametrize(
    ("finish", "expected"),
    [
        ("stop", "end_turn"),
        ("tool_calls", "tool_use"),
        ("length", "max_tokens"),
        ("content_filter", "refusal"),
    ],
)
async def test_finish_reason_mapping(finish: str, expected: str) -> None:
    calls = [call("c1", "a_tool", "{}")] if finish == "tool_calls" else None
    client, _ = make_client(ok(chat_json(tool_calls=calls, finish=finish)))
    assert (await client.complete(req())).stop_reason == expected


async def test_stop_with_tool_calls_is_tool_use() -> None:
    body = chat_json(tool_calls=[call("c1", "a_tool", "{}")], finish="stop")  # Ollama does this
    client, _ = make_client(ok(body))
    assert (await client.complete(req())).stop_reason == "tool_use"


async def test_unknown_finish_reason_and_empty_choices_are_errors() -> None:
    client, _ = make_client(ok(chat_json(finish="weird")))
    with pytest.raises(LLMError, match="finish_reason 'weird'"):
        await client.complete(req())
    client, _ = make_client(ok({"choices": [], "usage": {}}))
    with pytest.raises(LLMError, match="no choices"):
        await client.complete(req())


async def test_unpriced_model_is_a_typed_error_not_free() -> None:
    client, _ = make_client(ok(chat_json(model="mystery")))
    with pytest.raises(PricingError):
        await client.complete(req(model="mystery"))


# ---- malformed tool-call arguments ----


@pytest.mark.parametrize(
    ("raw", "expected_input"),
    [
        ('{"q": ', '{"q": '),  # invalid JSON: raw string kept
        ("not json at all", "not json at all"),
        ("", ""),
        ("[1, 2]", [1, 2]),  # valid JSON, not an object
        ('"str"', "str"),
        ("null", None),
    ],
)
async def test_malformed_tool_args_are_kept_not_dropped(
    raw: str, expected_input: Any, caplog: pytest.LogCaptureFixture
) -> None:
    body = chat_json(tool_calls=[call("c1", "a_tool", raw)], finish="tool_calls")
    client, _ = make_client(ok(body))
    with caplog.at_level(logging.WARNING):
        resp = await client.complete(req())
    block = resp.content[-1]
    assert block["type"] == "tool_use" and block["id"] == "c1" and block["name"] == "a_tool"
    assert block["input"] == expected_input
    assert not isinstance(block["input"], dict)  # the gateway rejects it as invalid arguments
    assert resp.stop_reason == "tool_use"
    LLMResponse.model_validate(resp.model_dump())
    assert any("arguments" in r.message for r in caplog.records if r.levelno == logging.WARNING)


async def test_malformed_args_are_rejected_by_the_gateway_as_invalid_args() -> None:
    from firebreak.models.agents import AgentConfig
    from firebreak.tools.context import ToolContext
    from firebreak.tools.gateway import ToolGateway
    from tests.tools.fakes import make_registry

    body = chat_json(tool_calls=[call("c1", "echo", '{"text": ')], finish="tool_calls")
    client, _ = make_client(ok(body))
    resp = await client.complete(req())
    block = resp.content[-1]
    cfg = AgentConfig(
        name="a",
        model="m",
        system_prompt="echo.md",
        tools=["echo"],
        output_schema="SpecialistReport",
    )

    class Sink:
        async def add(self, *a: Any, **k: Any) -> Any:
            raise AssertionError("no evidence for a rejected call")

    ctx = ToolContext(incident_id="inc", evidence=Sink())  # type: ignore[arg-type]
    result = await ToolGateway(make_registry()).invoke(
        cfg, "echo", block["input"], ctx, call_id="c1"
    )
    assert not result.record.ok
    assert "must be a JSON object" in result.content


async def test_malformed_args_echo_back_as_the_raw_string() -> None:
    messages: list[dict[str, Any]] = [
        {"role": "user", "content": "go"},
        {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "c1", "name": "a_tool", "input": '{"q": '}],
        },
        {
            "role": "user",
            "content": [{"type": "tool_result", "tool_use_id": "c1", "content": "tool_error"}],
        },
    ]
    fake = ok()
    client, _ = make_client(fake)
    await client.complete(req(messages))
    assert fake.bodies[0]["messages"][2]["tool_calls"][0]["function"]["arguments"] == '{"q": '


# ---- errors, retries, settings ----


async def test_connection_refused_says_to_start_ollama() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("[Errno 61] Connection refused", request=request)

    client, sleeps = make_client(refuse)
    with pytest.raises(LLMConnectionRefused) as exc_info:
        await client.complete(req())
    msg = str(exc_info.value)
    assert "ollama serve" in msg and f"ollama pull {LOCAL}" in msg
    assert "http://localhost:11434/v1" in msg
    assert sleeps == []  # a refused connection is not retried


async def test_retries_on_429_with_retry_after_then_succeeds() -> None:
    responses = [
        httpx.Response(429, headers={"retry-after": "2"}, text="busy"),
        httpx.Response(200, json=chat_json()),
    ]
    client, sleeps = make_client(lambda r: responses.pop(0))
    resp = await client.complete(req())
    assert resp.stop_reason == "end_turn" and sleeps == [2.0]


async def test_retries_exhausted_and_timeout_are_retryable() -> None:
    def slow(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    client, sleeps = make_client(slow, max_retries=2)
    with pytest.raises(LLMRetriesExhausted):
        await client.complete(req())
    assert len(sleeps) == 2


async def test_4xx_is_a_non_retried_typed_error() -> None:
    client, sleeps = make_client(lambda r: httpx.Response(404, text="model 'qwen3:8b' not found"))
    with pytest.raises(LLMError, match="HTTP 404") as exc_info:
        await client.complete(req())
    assert exc_info.value.status_code == 404 and sleeps == []


async def test_bad_json_body_is_an_error() -> None:
    client, _ = make_client(lambda r: httpx.Response(200, text="<html>"))
    with pytest.raises(LLMError, match="not JSON"):
        await client.complete(req())


async def test_api_key_is_sent_when_set_and_never_in_repr() -> None:
    fake = ok()
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake))
    settings = LLMSettings(openai_api_key=SecretStr("sk-secret"), openai_base_url="http://h/v1/")
    client = OpenAICompatLLMClient(settings, client=http, limiter=TokenBucket(6000))
    await client.complete(req())
    assert fake.requests[0].headers["authorization"] == "Bearer sk-secret"
    assert str(fake.requests[0].url) == "http://h/v1/chat/completions"
    assert "sk-secret" not in repr(settings)


def test_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("BASE_URL", "API_KEY", "TIMEOUT_SECONDS"):
        monkeypatch.delenv(f"FIREBREAK_LLM_OPENAI_{var}", raising=False)
    s = LLMSettings(_env_file=None)  # type: ignore[call-arg]
    assert s.openai_base_url == "http://localhost:11434/v1"
    assert s.openai_api_key is None and s.openai_timeout_seconds == 300
    monkeypatch.setenv("FIREBREAK_LLM_OPENAI_BASE_URL", "http://x:1/v1")
    assert LLMSettings(_env_file=None).openai_base_url == "http://x:1/v1"  # type: ignore[call-arg]


# ---- pricing and routing ----


def test_pricing_providers() -> None:
    table = PricingTable.load(LLMSettings().pricing_path)
    assert table.provider_for(SONNET) == "anthropic"
    assert table.provider_for(LOCAL) == "openai_compat"
    assert table.cost_usd(LOCAL, Usage(input_tokens=10**6, output_tokens=10**6)) == 0.0
    with pytest.raises(UnknownModelError, match="mystery"):
        table.provider_for("mystery")


class Stub:
    def __init__(self, name: str, fail: bool = False) -> None:
        self.name = name
        self.fail = fail
        self.requests: list[LLMRequest] = []

    async def complete(self, request: LLMRequest) -> LLMResponse:
        self.requests.append(request)
        if self.fail:
            raise LLMRetriesExhausted(f"{self.name} down", retryable=True)
        return LLMResponse(
            model=request.model,
            content=[{"type": "text", "text": self.name}],
            stop_reason="end_turn",
            usage=Usage(),
        )


def router(local: Stub, remote: Stub) -> RoutingLLMClient:
    return RoutingLLMClient(
        LLMSettings(),
        factories={"openai_compat": lambda: local, "anthropic": lambda: remote},
    )


async def test_routes_by_model() -> None:
    local, remote = Stub("local"), Stub("remote")
    r = router(local, remote)
    assert (await r.complete(req(model=LOCAL))).content[0]["text"] == "local"
    assert (await r.complete(req(model=SONNET))).content[0]["text"] == "remote"
    assert len(local.requests) == 1 and len(remote.requests) == 1


async def test_unknown_model_and_unknown_fallback_are_typed_errors() -> None:
    local, remote = Stub("local"), Stub("remote")
    r = router(local, remote)
    with pytest.raises(UnknownModelError):
        await r.complete(req(model="mystery"))
    with pytest.raises(UnknownModelError):
        await r.complete(req(model=LOCAL, fallback_model="mystery"))
    assert local.requests == [] and remote.requests == []


async def test_fallback_across_providers() -> None:
    local, remote = Stub("local", fail=True), Stub("remote")
    resp = await router(local, remote).complete(req(model=LOCAL, fallback_model=HAIKU))
    assert resp.content[0]["text"] == "remote" and resp.model == HAIKU
    assert local.requests[0].fallback_model is None  # the primary client never sees the fallback
    assert remote.requests[0].model == HAIKU and remote.requests[0].fallback_model is None


async def test_same_provider_fallback_is_left_to_the_client() -> None:
    local, remote = Stub("local"), Stub("remote")
    await router(local, remote).complete(req(model=SONNET, fallback_model=HAIKU))
    assert remote.requests[0].fallback_model == HAIKU and local.requests == []


async def test_clients_are_built_lazily() -> None:
    built: list[str] = []

    def factory(name: str) -> Callable[[], Stub]:
        def make() -> Stub:
            built.append(name)
            return Stub(name)

        return make

    r = RoutingLLMClient(
        LLMSettings(), factories={"openai_compat": factory("local"), "anthropic": factory("remote")}
    )
    await r.complete(req(model=LOCAL))
    assert built == ["local"]  # an all-local run never builds the Anthropic client


async def test_routing_end_to_end_to_ollama_over_mock_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = ok()
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake))
    settings = LLMSettings()
    r = RoutingLLMClient(
        settings,
        factories={
            "openai_compat": lambda: OpenAICompatLLMClient(
                settings, client=http, limiter=TokenBucket(6000)
            ),
        },
    )
    resp = await r.complete(req())
    assert resp.usage.cost_usd == 0.0 and len(fake.requests) == 1


# ---- record / replay ----


async def test_record_replay_for_the_local_provider(tmp_path: Path) -> None:
    fake = ok()
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake))
    live = OpenAICompatLLMClient(LLMSettings(), client=http, limiter=TokenBucket(6000))
    store = FixtureStore(tmp_path)
    recorded = await RecordingLLMClient(live, store).complete(req())

    def boom(*a: Any, **k: Any) -> Any:
        raise AssertionError("replay must not build a live client")

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(OpenAICompatLLMClient, "__init__", boom)
        monkeypatch.setattr(RoutingLLMClient, "__init__", boom)
        replay = build_llm_client(LLMSettings(mode=LLMMode.REPLAY, fixture_dir=tmp_path))
        assert await replay.complete(req()) == recorded
    finally:
        monkeypatch.undo()
    assert len(fake.requests) == 1  # the replay made no HTTP call


# ---- optional real Ollama ----


@pytest.mark.integration
@pytest.mark.skipif(os.environ.get("FIREBREAK_OLLAMA") != "1", reason="set FIREBREAK_OLLAMA=1")
async def test_real_ollama_round_trip() -> None:
    client = OpenAICompatLLMClient(LLMSettings())
    resp = await client.complete(
        LLMRequest(
            model=LOCAL,
            system="Answer in one word.",
            messages=[{"role": "user", "content": "Say hi."}],
            max_tokens=256,
        )
    )
    assert resp.stop_reason in {"end_turn", "max_tokens"} and resp.usage.cost_usd == 0.0

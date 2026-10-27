"""AnthropicLLMClient tests over a fake HTTP transport. No network."""

import json
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import anthropic
import httpx2
import pytest

from firebreak.llm import (
    AnthropicLLMClient,
    FixtureMissing,
    FixtureStore,
    LLMError,
    LLMMode,
    LLMRetriesExhausted,
    LLMSettings,
    PricingTable,
    RecordingLLMClient,
    ReplayLLMClient,
    TokenBucket,
    build_llm_client,
    fixture_key,
)
from firebreak.models.llm import LLMRequest, ToolSpec, Usage

OPUS = "claude-opus-5-5"
SONNET = "claude-sonnet-5-5"
HAIKU = "claude-haiku-5-5"


def message_json(model: str, **usage: int) -> dict[str, Any]:
    return {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [{"type": "text", "text": "hello"}],
        "stop_reason": "end_turn",
        "stop_sequence": None,
        "usage": {"input_tokens": 100, "output_tokens": 50, **usage},
    }


class Fake:
    """Scripted transport: a list of (status, body) per call, or a callable."""

    def __init__(self, handler: Callable[[httpx2.Request], httpx2.Response]) -> None:
        self.requests: list[dict[str, Any]] = []
        self._handler = handler

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(json.loads(request.content))
        return self._handler(request)


def make_client(
    fake: Fake, *, fallbacks: dict[str, str] | None = None, **settings: Any
) -> tuple[AnthropicLLMClient, list[float]]:
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    http = httpx2.AsyncClient(transport=httpx2.MockTransport(fake))
    sdk = anthropic.AsyncAnthropic(api_key="test", http_client=http, max_retries=0)
    client = AnthropicLLMClient(
        LLMSettings(**settings),
        client=sdk,
        fallbacks=fallbacks,
        limiter=TokenBucket(6000),
        sleep=sleep,
        rng=random.Random(0),
    )
    return client, sleeps


def req(model: str = OPUS) -> LLMRequest:
    return LLMRequest(
        model=model,
        system="You are an SRE.",
        messages=[{"role": "user", "content": "why 500s?"}],
        tools=[
            ToolSpec(name="a_tool", description="a", input_schema={"type": "object"}),
            ToolSpec(name="b_tool", description="b", input_schema={"type": "object"}),
        ],
    )


def err(status: int) -> httpx2.Response:
    return httpx2.Response(
        status, json={"type": "error", "error": {"type": "x", "message": "boom"}}
    )


async def test_request_conversion_adds_cache_control() -> None:
    fake = Fake(lambda r: httpx2.Response(200, json=message_json(OPUS)))
    client, _ = make_client(fake)
    resp = await client.complete(req())
    body = fake.requests[0]
    assert body["system"] == [
        {"type": "text", "text": "You are an SRE.", "cache_control": {"type": "ephemeral"}}
    ]
    assert body["tools"][-1]["cache_control"] == {"type": "ephemeral"}
    assert "access" not in body["tools"][0]
    assert body["messages"] == [{"role": "user", "content": "why 500s?"}]
    assert resp.content == [{"type": "text", "text": "hello"}]
    assert resp.stop_reason == "end_turn"


async def test_retries_then_succeeds_with_jittered_backoff() -> None:
    statuses = iter([429, 529, 500])

    def handler(_: httpx2.Request) -> httpx2.Response:
        status = next(statuses, 200)
        return err(status) if status != 200 else httpx2.Response(200, json=message_json(OPUS))

    fake = Fake(handler)
    client, sleeps = make_client(fake)
    resp = await client.complete(req())
    assert len(fake.requests) == 4
    assert len(sleeps) == 3
    for attempt, delay in enumerate(sleeps):
        assert 0 <= delay <= 2**attempt  # base 1s, full jitter
    assert resp.model == OPUS


async def test_exhausted_retries_without_fallback_raises() -> None:
    fake = Fake(lambda r: err(503))
    client, sleeps = make_client(fake, fallbacks={})
    with pytest.raises(LLMRetriesExhausted) as ei:
        await client.complete(req())
    assert ei.value.status_code == 503
    assert len(fake.requests) == 5  # 1 + 4 retries
    assert len(sleeps) == 4


async def test_fallback_model_after_retries_exhausted() -> None:
    def handler(r: httpx2.Request) -> httpx2.Response:
        model = json.loads(r.content)["model"]
        if model == OPUS:
            return err(529)
        return httpx2.Response(200, json=message_json(SONNET))

    fake = Fake(handler)
    client, _ = make_client(fake, fallbacks={OPUS: SONNET})
    resp = await client.complete(req())
    assert [r["model"] for r in fake.requests] == [OPUS] * 5 + [SONNET]
    assert resp.model == SONNET
    # cost priced at the model that actually answered
    assert resp.usage.cost_usd == pytest.approx((100 * 2.0 + 50 * 10.0) / 1e6)


@pytest.mark.parametrize("status", [400, 401, 404])
async def test_non_retryable_raises_typed_error_immediately(status: int) -> None:
    fake = Fake(lambda r: err(status))
    client, sleeps = make_client(fake, fallbacks={OPUS: SONNET})
    with pytest.raises(LLMError) as ei:
        await client.complete(req())
    assert not isinstance(ei.value, LLMRetriesExhausted)
    assert ei.value.status_code == status
    assert isinstance(ei.value.__cause__, anthropic.APIStatusError)
    assert len(fake.requests) == 1
    assert sleeps == []


async def test_unsupported_stop_reason_raises() -> None:
    body = message_json(OPUS)
    body["stop_reason"] = "pause_turn"
    client, _ = make_client(Fake(lambda r: httpx2.Response(200, json=body)))
    with pytest.raises(LLMError, match="pause_turn"):
        await client.complete(req())


async def test_cost_math_with_cache_tokens() -> None:
    body = message_json(
        OPUS,
        input_tokens=1000,
        output_tokens=500,
        cache_read_input_tokens=10_000,
        cache_creation_input_tokens=2000,
    )
    client, _ = make_client(Fake(lambda r: httpx2.Response(200, json=body)))
    resp = await client.complete(req())
    assert resp.usage.cache_read_tokens == 10_000
    assert resp.usage.cache_write_tokens == 2000
    expected = (1000 * 4.0 + 2000 * 5.0 + 10_000 * 0.20 + 500 * 20.0) / 1e6
    assert resp.usage.cost_usd == pytest.approx(expected)


def test_haiku_long_prompt_tier_and_unknown_model(tmp_path: Path) -> None:
    table = PricingTable.load(LLMSettings().pricing_path)
    short = table.cost_usd(HAIKU, Usage(input_tokens=1000, output_tokens=1000))
    assert short == pytest.approx((1000 * 0.10 + 1000 * 0.50) / 1e6)
    long = table.cost_usd(HAIKU, Usage(input_tokens=200_000, output_tokens=1000))
    assert long == pytest.approx((200_000 * 0.50 + 1000 * 2.50) / 1e6)
    with pytest.raises(LLMError):
        table.cost_usd("claude-unknown", Usage())


async def test_record_then_replay_round_trip(tmp_path: Path) -> None:
    fake = Fake(lambda r: httpx2.Response(200, json=message_json(OPUS)))
    live, _ = make_client(fake)
    store = FixtureStore(tmp_path / "fx")
    recorded = await RecordingLLMClient(live, store).complete(req())

    key = fixture_key(req())
    assert (tmp_path / "fx" / f"{key}.json").exists()

    replayed = await ReplayLLMClient(store).complete(req())
    assert replayed == recorded
    assert len(fake.requests) == 1  # replay made no call


async def test_replay_missing_fixture_raises_with_key(tmp_path: Path) -> None:
    client = ReplayLLMClient(FixtureStore(tmp_path))
    with pytest.raises(FixtureMissing) as ei:
        await client.complete(req())
    assert ei.value.key == fixture_key(req())
    assert ei.value.key in str(ei.value)


def test_fixture_key_is_canonical_and_ignores_max_tokens() -> None:
    a = req()
    b = req().model_copy(update={"max_tokens": 1})
    assert fixture_key(a) == fixture_key(b)
    c = req().model_copy(update={"messages": [{"content": "why 500s?", "role": "user"}]})
    assert fixture_key(a) == fixture_key(c)  # key order irrelevant
    d = req().model_copy(update={"system": "other"})
    assert fixture_key(a) != fixture_key(d)
    assert len(fixture_key(a)) == 64


def test_build_client_modes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    replay = build_llm_client(LLMSettings(mode=LLMMode.REPLAY, fixture_dir=tmp_path))
    assert isinstance(replay, ReplayLLMClient)
    rec = build_llm_client(LLMSettings(mode=LLMMode.RECORD, fixture_dir=tmp_path))
    assert isinstance(rec, RecordingLLMClient)
    live = build_llm_client(LLMSettings(mode=LLMMode.LIVE))
    assert isinstance(live, AnthropicLLMClient)


def test_mode_read_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIREBREAK_LLM_MODE", "replay")
    assert LLMSettings().mode is LLMMode.REPLAY


async def test_token_bucket_waits_when_empty() -> None:
    now = [0.0]
    slept: list[float] = []

    async def sleep(s: float) -> None:
        slept.append(s)
        now[0] += s

    bucket = TokenBucket(60, capacity=2, clock=lambda: now[0], sleep=sleep)
    for _ in range(3):
        await bucket.acquire()
    assert slept == [pytest.approx(1.0)]  # 60 rpm = 1/s; third request waits 1s

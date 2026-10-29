"""AnthropicLLMClient tests over a fake HTTP transport. No network."""

import json
import logging
import math
import os
import random
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import format_datetime
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
    RoutingLLMClient,
    TokenBucket,
    build_llm_client,
    fixture_key,
)
from firebreak.models.llm import LLMRequest, LLMResponse, ToolSpec, Usage

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
    fake: Fake, *, now: datetime | None = None, **settings: Any
) -> tuple[AnthropicLLMClient, list[float]]:
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    http = httpx2.AsyncClient(transport=httpx2.MockTransport(fake))
    sdk = anthropic.AsyncAnthropic(api_key="test", http_client=http, max_retries=0)
    client = AnthropicLLMClient(
        LLMSettings(**settings),
        client=sdk,
        clock=(lambda: now) if now else (lambda: datetime.now(UTC)),
        limiter=TokenBucket(6000),
        sleep=sleep,
        rng=random.Random(0),
    )
    return client, sleeps


def req(model: str = OPUS, fallback_model: str | None = None) -> LLMRequest:
    return LLMRequest(
        model=model,
        fallback_model=fallback_model,
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
    client, sleeps = make_client(fake)
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
    client, _ = make_client(fake)
    resp = await client.complete(req(fallback_model=SONNET))
    assert [r["model"] for r in fake.requests] == [OPUS] * 5 + [SONNET]
    assert resp.model == SONNET
    # cost priced at the model that actually answered
    assert resp.usage.cost_usd == pytest.approx((100 * 2.0 + 50 * 10.0) / 1e6)


async def test_no_fallback_when_fallback_model_unset() -> None:
    # No built-in opus->sonnet or sonnet->haiku default: unset means never.
    for model in (OPUS, SONNET):
        fake = Fake(lambda r: err(529))
        client, _ = make_client(fake)
        with pytest.raises(LLMRetriesExhausted):
            await client.complete(req(model))
        assert {r["model"] for r in fake.requests} == {model}
        assert len(fake.requests) == 5


async def test_fallback_only_to_the_requested_fallback_model() -> None:
    fake = Fake(lambda r: err(503))
    client, _ = make_client(fake)
    with pytest.raises(LLMRetriesExhausted):
        await client.complete(req(OPUS, fallback_model=HAIKU))
    assert [r["model"] for r in fake.requests] == [OPUS] * 5 + [HAIKU] * 5  # no chain beyond it


async def test_unpriced_answering_model_uses_requested_model_price() -> None:
    body = message_json("claude-opus-5-5-20991231")
    client, _ = make_client(Fake(lambda r: httpx2.Response(200, json=body)))
    resp = await client.complete(req(OPUS, fallback_model=HAIKU))
    assert resp.usage.cost_usd == pytest.approx((100 * 4.0 + 50 * 20.0) / 1e6)  # Opus, not Haiku


async def test_unknown_model_costs_nan_and_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    body = message_json("mystery-model")
    client, _ = make_client(Fake(lambda r: httpx2.Response(200, json=body)))
    with caplog.at_level(logging.WARNING, logger="firebreak.llm.client"):
        a = await client.complete(req("mystery-model"))
        b = await client.complete(req("mystery-model"))
    assert math.isnan(a.usage.cost_usd) and math.isnan(b.usage.cost_usd)
    assert len([r for r in caplog.records if "no pricing" in r.getMessage()]) == 1


async def test_max_retries_zero_makes_exactly_one_attempt() -> None:
    fake = Fake(lambda r: err(529))
    client, sleeps = make_client(fake, max_retries=0)
    with pytest.raises(LLMRetriesExhausted):
        await client.complete(req())
    assert len(fake.requests) == 1 and sleeps == []


def retry_after_response(status: int, value: str) -> httpx2.Response:
    return httpx2.Response(
        status,
        headers={"retry-after": value},
        json={"type": "error", "error": {"type": "x", "message": "slow down"}},
    )


@pytest.mark.parametrize("status", [429, 529])
async def test_retry_after_seconds_is_honored(status: int) -> None:
    calls = iter([retry_after_response(status, "7")])
    fake = Fake(lambda r: next(calls, None) or httpx2.Response(200, json=message_json(OPUS)))
    client, sleeps = make_client(fake)
    await client.complete(req())
    assert sleeps == [7.0]


async def test_retry_after_is_capped_by_max_backoff() -> None:
    calls = iter([retry_after_response(429, "3600")])
    fake = Fake(lambda r: next(calls, None) or httpx2.Response(200, json=message_json(OPUS)))
    client, sleeps = make_client(fake, backoff_max_seconds=10)
    await client.complete(req())
    assert sleeps == [10.0]


async def test_retry_after_http_date_is_honored() -> None:
    now = datetime(2026, 10, 27, 12, 0, 0, tzinfo=UTC)
    when = format_datetime(datetime(2026, 10, 27, 12, 0, 5, tzinfo=UTC), usegmt=True)
    calls = iter([retry_after_response(529, when)])
    fake = Fake(lambda r: next(calls, None) or httpx2.Response(200, json=message_json(OPUS)))
    client, sleeps = make_client(fake, now=now)
    await client.complete(req())
    assert sleeps == [5.0]


async def test_retry_after_ignored_on_other_statuses_and_garbage() -> None:
    calls = iter([retry_after_response(500, "25"), retry_after_response(429, "soon")])
    fake = Fake(lambda r: next(calls, None) or httpx2.Response(200, json=message_json(OPUS)))
    client, sleeps = make_client(fake)
    await client.complete(req())
    assert len(sleeps) == 2 and all(d <= 2 for d in sleeps)  # jittered backoff, not 25


def test_fixture_write_is_atomic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FixtureStore(tmp_path)
    response = LLMResponse(
        model=OPUS,
        content=[{"type": "text", "text": "hi"}],
        stop_reason="end_turn",
        usage=Usage(),
    )
    key = store.save(req(), response)
    original = store.path_for(key).read_text()

    replaced: list[tuple[str, str]] = []
    real_replace = os.replace

    def spy(src: Any, dst: Any) -> None:
        assert Path(src).exists() and Path(src).parent == tmp_path and str(src) != str(dst)
        replaced.append((str(src), str(dst)))
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy)
    store.save(req(), response)
    assert replaced == [(replaced[0][0], str(store.path_for(key)))]
    assert store.path_for(key).read_text() == original
    assert [p.name for p in tmp_path.iterdir()] == [f"{key}.json"]  # no temp file left

    def boom(src: Any, dst: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError, match="disk full"):
        store.save(req().model_copy(update={"system": "other"}), response)
    assert [p.name for p in tmp_path.iterdir()] == [f"{key}.json"]  # failed write cleaned up


@pytest.mark.parametrize("status", [400, 401, 404])
async def test_non_retryable_raises_typed_error_immediately(status: int) -> None:
    fake = Fake(lambda r: err(status))
    client, sleeps = make_client(fake)
    with pytest.raises(LLMError) as ei:
        await client.complete(req(fallback_model=SONNET))
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
    assert isinstance(live, RoutingLLMClient)  # picks Anthropic or openai_compat per model


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

"""T-044: fixture keys that survive replay, strict replay errors, and the shared clock."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from firebreak.llm.client import (
    AnthropicLLMClient,
    KeyLoggingRecorder,
    StrictReplayLLMClient,
    build_llm_client,
)
from firebreak.llm.fixtures import (
    FixtureStore,
    StaleRecordingError,
    first_difference,
    fixture_key,
)
from firebreak.llm.settings import LLMMode, LLMSettings
from firebreak.models.common import deterministic
from firebreak.models.llm import LLMRequest, LLMResponse, ToolSpec, Usage
from firebreak.tools.untrusted import wrap_untrusted

AT = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def req(*, system: str = "sys", tool_text: str = "ERROR db timeout", extra: str = "") -> LLMRequest:
    block = wrap_untrusted(tool_text, source="logql_query", evidence_id="ev_1", max_chars=1000)
    return LLMRequest(
        model="claude-sonnet-5-5",
        system=system,
        messages=[
            {"role": "user", "content": "investigate" + extra},
            {"role": "assistant", "content": [{"type": "tool_use", "id": "t1", "name": "x"}]},
            {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "t1", "content": block}],
            },
        ],
        tools=[ToolSpec(name="x", description="d", input_schema={"type": "object"})],
    )


def resp(text: str = "ok") -> LLMResponse:
    return LLMResponse(
        model="claude-sonnet-5-5",
        content=[{"type": "text", "text": text}],
        stop_reason="end_turn",
        usage=Usage(input_tokens=1, output_tokens=1, cost_usd=0.001),
    )


class Scripted:
    async def complete(self, request: LLMRequest) -> LLMResponse:
        return resp()


def test_key_ignores_untrusted_nonces_but_not_content() -> None:
    a, b = req(), req()  # each wrap draws a fresh random nonce
    assert a.messages != b.messages
    assert fixture_key(a) == fixture_key(b)
    assert fixture_key(req(tool_text="ERROR other")) != fixture_key(a)
    assert fixture_key(req(system="sys2")) != fixture_key(a)


def test_key_keeps_nonce_structure() -> None:
    """Two blocks sharing one nonce vs two distinct nonces are different conversations."""
    one = 'x <d nonce="0123456789abcdef"> </d nonce="0123456789abcdef">'
    two = 'x <d nonce="0123456789abcdef"> </d nonce="fedcba9876543210">'
    r1 = LLMRequest(model="m", system="s", messages=[{"role": "user", "content": one}])
    r2 = LLMRequest(model="m", system="s", messages=[{"role": "user", "content": two}])
    assert fixture_key(r1) != fixture_key(r2)


def test_first_difference_names_the_part() -> None:
    base = req()
    assert "system prompt changed" in first_difference(base, req(system="other"))
    assert first_difference(base, req(extra="!")).startswith("messages[0] (user)")
    assert first_difference(base, req(tool_text="new")).startswith("messages[2] (user)")
    assert "model changed" in first_difference(
        base, base.model_copy(update={"model": "claude-opus-5-5"})
    )
    shorter = base.model_copy(update={"messages": base.messages[:1]})
    assert first_difference(base, shorter) == "1 messages now, 3 recorded"
    renamed = base.model_copy(
        update={"tools": [ToolSpec(name="y", description="d", input_schema={})]}
    )
    assert "tool list changed" in first_difference(base, renamed)
    redescribed = base.model_copy(
        update={"tools": [ToolSpec(name="x", description="new", input_schema={})]}
    )
    assert "tool definition 'x' changed" in first_difference(base, redescribed)


async def test_recorder_then_strict_replay(tmp_path: Path) -> None:
    store = FixtureStore(tmp_path)
    rec = KeyLoggingRecorder(Scripted(), store)
    await rec.complete(req())
    await rec.complete(req(extra=" more"))
    assert rec.keys == [fixture_key(req()), fixture_key(req(extra=" more"))]
    assert store.load_request(rec.keys[0]) is not None
    assert store.load_request("0" * 64) is None

    replay = StrictReplayLLMClient(store, scenario="s01", recorded_keys=rec.keys)
    assert await replay.complete(req()) == resp()  # fresh nonces, same key
    with pytest.raises(StaleRecordingError) as ei:
        await replay.complete(req(extra=" changed"))
    e = ei.value
    assert e.scenario == "s01" and e.step == "llm step 2"
    assert e.detail.startswith("messages[0] (user) differs")
    assert "s01" in str(e) and "llm step 2" in str(e) and e.key in str(e)

    with pytest.raises(StaleRecordingError, match="only 2 LLM steps"):
        await replay.complete(req(extra=" third"))


async def test_strict_replay_check_runs_first(tmp_path: Path) -> None:
    def check() -> None:
        raise StaleRecordingError(scenario="s", step="tool call 1 (x)", detail="d")

    replay = StrictReplayLLMClient(FixtureStore(tmp_path), scenario="s", check=check)
    with pytest.raises(StaleRecordingError, match=r"tool call 1 \(x\)"):
        await replay.complete(req())


def test_build_replay_client_is_strict(tmp_path: Path) -> None:
    client = build_llm_client(LLMSettings(mode=LLMMode.REPLAY, fixture_dir=tmp_path / "s07"))
    assert isinstance(client, StrictReplayLLMClient) and client.scenario == "s07"


def test_default_clock_is_the_shared_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    client = AnthropicLLMClient(LLMSettings())
    with deterministic("seed", at=AT):
        assert client._clock() == AT

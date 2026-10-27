"""AnthropicLLMClient: Messages API conversion, retries, fallback, cost, rate limiting."""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Any, cast

import anthropic

from firebreak.llm.errors import LLMError, LLMRetriesExhausted
from firebreak.llm.fixtures import FixtureStore
from firebreak.llm.pricing import PricingTable
from firebreak.llm.ratelimit import TokenBucket
from firebreak.llm.replay import RecordingLLMClient, ReplayLLMClient
from firebreak.llm.settings import LLMMode, LLMSettings
from firebreak.models.llm import LLMClient, LLMRequest, LLMResponse, Usage

log = logging.getLogger(__name__)

_CACHE = {"type": "ephemeral"}
_OVERLOADED = 529
_STOP_REASONS = {"end_turn", "tool_use", "max_tokens", "stop_sequence", "refusal"}

# Model -> fallback used once retries on the primary are exhausted (ARCH 5.3: Opus -> Sonnet).
DEFAULT_FALLBACKS: dict[str, str] = {
    "claude-opus-5-5": "claude-sonnet-5-5",
    "claude-sonnet-5-5": "claude-haiku-5-5",
}


def to_api_params(request: LLMRequest) -> dict[str, Any]:
    """LLMRequest -> Messages API kwargs, with cache_control on system and tool definitions."""
    params: dict[str, Any] = {
        "model": request.model,
        "max_tokens": request.max_tokens,
        "messages": request.messages,
    }
    if request.system:
        params["system"] = [{"type": "text", "text": request.system, "cache_control": _CACHE}]
    if request.tools:
        tools: list[dict[str, Any]] = [
            {"name": t.name, "description": t.description, "input_schema": t.input_schema}
            for t in request.tools
        ]
        tools[-1]["cache_control"] = _CACHE  # breakpoint after the last tool caches all of them
        params["tools"] = tools
    return params


def _status(exc: Exception) -> int | None:
    return exc.status_code if isinstance(exc, anthropic.APIStatusError) else None


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, anthropic.APIStatusError):
        return exc.status_code in (408, 409, 429, _OVERLOADED) or exc.status_code >= 500
    return isinstance(exc, anthropic.APIConnectionError)  # includes timeouts


class AnthropicLLMClient:
    def __init__(
        self,
        settings: LLMSettings | None = None,
        *,
        client: anthropic.AsyncAnthropic | None = None,
        pricing: PricingTable | None = None,
        fallbacks: dict[str, str] | None = None,
        limiter: TokenBucket | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._settings = settings or LLMSettings()
        # The SDK's own retries are disabled so this class is the single retry policy.
        self._client = client or anthropic.AsyncAnthropic(
            max_retries=0, timeout=self._settings.timeout_seconds
        )
        self._pricing = pricing or PricingTable.load(self._settings.pricing_path)
        self._fallbacks = DEFAULT_FALLBACKS if fallbacks is None else fallbacks
        self._limiter = limiter or TokenBucket(self._settings.requests_per_minute)
        self._sleep = sleep
        self._rng = rng or random.Random()

    async def complete(self, request: LLMRequest) -> LLMResponse:
        try:
            return await self._complete_with_retries(request)
        except LLMRetriesExhausted as primary_exc:
            fallback = self._fallbacks.get(request.model)
            if fallback is None:
                raise
            log.warning(
                "llm retries exhausted on %s (%s); falling back to %s",
                request.model,
                primary_exc,
                fallback,
            )
            return await self._complete_with_retries(request.model_copy(update={"model": fallback}))

    async def _complete_with_retries(self, request: LLMRequest) -> LLMResponse:
        max_retries = self._settings.max_retries
        attempt = 0
        while True:
            await self._limiter.acquire()
            try:
                message = await self._client.messages.create(**to_api_params(request))
            except Exception as exc:
                if not _is_retryable(exc):
                    raise LLMError(
                        f"anthropic call failed for {request.model}: {exc}",
                        status_code=_status(exc),
                    ) from exc
                if attempt >= max_retries:
                    raise LLMRetriesExhausted(
                        f"{request.model} still failing after {max_retries} retries: {exc}",
                        status_code=_status(exc),
                        retryable=True,
                    ) from exc
                delay = self._backoff(attempt)
                log.warning(
                    "llm retryable error on %s (attempt %d/%d, status=%s): %s; sleeping %.2fs",
                    request.model,
                    attempt + 1,
                    max_retries + 1,
                    _status(exc),
                    exc,
                    delay,
                )
                attempt += 1
                await self._sleep(delay)
                continue
            return self._to_response(message, requested_model=request.model)

    def _backoff(self, attempt: int) -> float:
        """Full-jitter exponential backoff."""
        ceiling = min(
            self._settings.backoff_max_seconds, self._settings.backoff_base_seconds * (2**attempt)
        )
        return self._rng.uniform(0, ceiling)

    def _to_response(
        self, message: anthropic.types.Message, *, requested_model: str
    ) -> LLMResponse:
        stop_reason = message.stop_reason
        if stop_reason not in _STOP_REASONS:
            raise LLMError(f"unsupported stop_reason {stop_reason!r} from {requested_model}")
        model = message.model or requested_model
        usage = Usage(
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
            cache_read_tokens=message.usage.cache_read_input_tokens or 0,
            cache_write_tokens=message.usage.cache_creation_input_tokens or 0,
        )
        usage.cost_usd = self._pricing.cost_usd(model, usage)
        return LLMResponse(
            model=model,
            content=[b.model_dump(mode="json", exclude_none=True) for b in message.content],
            stop_reason=cast(Any, stop_reason),
            usage=usage,
        )


def build_llm_client(settings: LLMSettings | None = None) -> LLMClient:
    """Factory honoring FIREBREAK_LLM_MODE=live|record|replay."""
    settings = settings or LLMSettings()
    store = FixtureStore(settings.fixture_dir)
    if settings.mode is LLMMode.REPLAY:
        return ReplayLLMClient(store)
    live = AnthropicLLMClient(settings)
    if settings.mode is LLMMode.RECORD:
        return RecordingLLMClient(live, store)
    return live

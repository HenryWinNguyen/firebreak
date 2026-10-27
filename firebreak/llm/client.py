"""AnthropicLLMClient: Messages API conversion, retries, fallback, cost, rate limiting."""

import asyncio
import logging
import math
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
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

_RETRY_AFTER_STATUSES = (429, _OVERLOADED)


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


def _retry_after_seconds(exc: Exception, now: datetime) -> float | None:
    """The server's Retry-After (delta-seconds or HTTP date) on 429/529, else None."""
    if (
        not isinstance(exc, anthropic.APIStatusError)
        or exc.status_code not in _RETRY_AFTER_STATUSES
    ):
        return None
    raw = exc.response.headers.get("retry-after")
    if raw is None:
        return None
    raw = raw.strip()
    try:
        seconds = float(raw)
    except ValueError:
        try:
            when = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            log.warning("ignoring unparseable Retry-After header %r", raw)
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - now).total_seconds()
    if not math.isfinite(seconds):
        log.warning("ignoring non-finite Retry-After header %r", raw)
        return None
    return max(0.0, seconds)


class AnthropicLLMClient:
    def __init__(
        self,
        settings: LLMSettings | None = None,
        *,
        client: anthropic.AsyncAnthropic | None = None,
        pricing: PricingTable | None = None,
        limiter: TokenBucket | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._settings = settings or LLMSettings()
        # The SDK's own retries are disabled so this class is the single retry policy.
        self._client = client or anthropic.AsyncAnthropic(
            max_retries=0, timeout=self._settings.timeout_seconds
        )
        self._pricing = pricing or PricingTable.load(self._settings.pricing_path)
        self._clock = clock
        self._unpriced_models: set[str] = set()
        self._limiter = limiter or TokenBucket(self._settings.requests_per_minute)
        self._sleep = sleep
        self._rng = rng or random.Random()

    async def complete(self, request: LLMRequest) -> LLMResponse:
        try:
            return await self._complete_with_retries(request)
        except LLMRetriesExhausted as primary_exc:
            fallback = request.fallback_model
            if fallback is None or fallback == request.model:
                raise
            log.warning(
                "llm retries exhausted on %s (%s); falling back to %s",
                request.model,
                primary_exc,
                fallback,
            )
            return await self._complete_with_retries(
                request.model_copy(update={"model": fallback, "fallback_model": None})
            )

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
                delay = self._retry_delay(exc, attempt)
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

    def _retry_delay(self, exc: Exception, attempt: int) -> float:
        """Retry-After when the server sent one (capped by the max backoff), else jitter."""
        retry_after = _retry_after_seconds(exc, self._clock())
        if retry_after is None:
            return self._backoff(attempt)
        return min(retry_after, self._settings.backoff_max_seconds)

    def _backoff(self, attempt: int) -> float:
        """Full-jitter exponential backoff."""
        ceiling = min(
            self._settings.backoff_max_seconds, self._settings.backoff_base_seconds * (2**attempt)
        )
        return self._rng.uniform(0, ceiling)

    def _cost_usd(self, answered_model: str, requested_model: str, usage: Usage) -> float:
        """Price by the answering model; if unpriced, by the model this call requested.

        A model in neither place logs one warning and costs NaN, which the runtime's
        budget check treats as over budget (fail closed).
        """
        for model in (answered_model, requested_model):
            if model in self._pricing.models:
                if model != answered_model:
                    self._warn_once(
                        answered_model,
                        "no pricing for %s; using the requested model %s price",
                        answered_model,
                        requested_model,
                    )
                return self._pricing.cost_usd(model, usage)
        self._warn_once(
            answered_model,
            "no pricing for %s or requested model %s; cost is NaN (fails the budget check)",
            answered_model,
            requested_model,
        )
        return math.nan

    def _warn_once(self, key: str, msg: str, *args: str) -> None:
        if key not in self._unpriced_models:
            self._unpriced_models.add(key)
            log.warning(msg, *args)

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
        usage.cost_usd = self._cost_usd(model, requested_model, usage)
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

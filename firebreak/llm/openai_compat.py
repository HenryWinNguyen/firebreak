"""OpenAICompatLLMClient: an OpenAI-style ``/chat/completions`` client (local Ollama).

``LLMRequest``/``LLMResponse`` stay Anthropic-shaped; this module translates both ways.

Malformed tool-call arguments (small models emit them): the ``arguments`` string is parsed
as JSON. If it is invalid JSON, or valid JSON that is not an object, the ``tool_use`` block
is still returned, with ``input`` set to the raw string (invalid JSON) or the parsed
non-object value. The tool gateway rejects any non-dict input as invalid arguments, so the
model sees an error result for that call and can retry. Nothing is dropped or repaired, and
a warning is logged. When such a block is echoed back in the history, it is sent with
``arguments`` ``"{}"`` (servers reject non-object arguments); the error tool_result carries the
explanation. A missing or empty tool-call id becomes ``call_<index>`` (warning logged).
"""

import asyncio
import json
import logging
import math
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, cast

import httpx

from firebreak.llm.errors import LLMConnectionRefused, LLMError, LLMRetriesExhausted
from firebreak.llm.pricing import PricingTable
from firebreak.llm.ratelimit import TokenBucket
from firebreak.llm.settings import LLMSettings
from firebreak.models.common import utcnow
from firebreak.models.llm import LLMRequest, LLMResponse, Usage

log = logging.getLogger(__name__)

_CHARS_PER_TOKEN = 3  # conservative: real text is ~4 chars/token, so this over-estimates
_TRUNCATION_RATIO = 0.98


class ContextOverflowError(LLMError):
    """The prompt (plus reply budget) does not fit the server's context window.

    Defined here, not in ``errors.py``, to keep T-050 inside its owned paths. Never retried and
    never triggers the fallback model: a different model does not fix a too-long conversation.
    """


_FINISH_REASONS = {
    "stop": "end_turn",
    "tool_calls": "tool_use",
    "function_call": "tool_use",
    "length": "max_tokens",
    "content_filter": "refusal",
}
_RETRY_STATUSES = (408, 409, 429)


def _text_of_result(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [str(b.get("text", "")) if isinstance(b, dict) else str(b) for b in content]
        return "\n".join(parts)
    return json.dumps(content)


def _arguments_string(value: Any, *, name: Any = None) -> str:
    """Anthropic ``input`` -> OpenAI ``arguments`` JSON-object string.

    A malformed call (``input`` not a dict) is echoed as ``"{}"``: servers such as Ollama parse
    history arguments into a map and answer 400 on anything else. The gateway's error
    tool_result already tells the model what was wrong with the call.
    """
    if isinstance(value, dict):
        return json.dumps(value)
    log.warning("echoing malformed arguments of tool call %r as {} (was %.100r)", name, value)
    return "{}"


def _convert_message(msg: dict[str, Any]) -> list[dict[str, Any]]:
    role = msg.get("role")
    content = msg.get("content")
    if isinstance(content, str):
        return [{"role": role, "content": content}]
    if not isinstance(content, list):
        raise LLMError(f"unsupported message content of type {type(content).__name__}")
    texts: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    tool_msgs: list[dict[str, Any]] = []
    for block in content:
        btype = block.get("type")
        if btype == "text":
            texts.append(str(block.get("text", "")))
        elif btype == "tool_use":
            tool_calls.append(
                {
                    "id": block.get("id"),
                    "type": "function",
                    "function": {
                        "name": block.get("name"),
                        "arguments": _arguments_string(
                            block.get("input", {}), name=block.get("name")
                        ),
                    },
                }
            )
        elif btype == "tool_result":
            body = _text_of_result(block.get("content", ""))
            if block.get("is_error"):
                body = f"[error] {body}"
            tool_msgs.append(
                {"role": "tool", "tool_call_id": block.get("tool_use_id"), "content": body}
            )
        else:
            raise LLMError(f"unsupported content block type {btype!r} for an openai_compat model")
    out: list[dict[str, Any]] = list(tool_msgs)  # tool results must directly follow the call
    text = "\n".join(texts)
    if role == "assistant":
        entry: dict[str, Any] = {"role": "assistant", "content": text or None}
        if tool_calls:
            entry["tool_calls"] = tool_calls
        out.append(entry)
    elif texts or not tool_msgs:
        out.append({"role": role, "content": text})
    return out


def to_chat_params(request: LLMRequest) -> dict[str, Any]:
    """LLMRequest -> ``/chat/completions`` JSON body."""
    messages: list[dict[str, Any]] = []
    if request.system:
        messages.append({"role": "system", "content": request.system})
    for msg in request.messages:
        messages.extend(_convert_message(msg))
    params: dict[str, Any] = {
        "model": request.model,
        "messages": messages,
        "max_tokens": request.max_tokens,
        "stream": False,
    }
    if request.tools:
        params["tools"] = [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_schema,
                },
            }
            for t in request.tools
        ]
    return params


def estimate_prompt_tokens(params: dict[str, Any]) -> int:
    """Conservative prompt-size estimate: serialized messages + tool schemas, chars / 3."""
    chars = len(json.dumps(params["messages"])) + len(json.dumps(params.get("tools", [])))
    return math.ceil(chars / _CHARS_PER_TOKEN)


def _parse_arguments(raw: Any, *, name: Any, call_id: Any) -> Any:
    """The tool input. Malformed values are passed through unchanged for the gateway to reject."""
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        log.warning("tool call %r (%r) has non-string arguments %r", name, call_id, raw)
        return raw
    try:
        parsed = json.loads(raw)
    except ValueError:
        log.warning("tool call %r (%r) has invalid JSON arguments: %.200r", name, call_id, raw)
        return raw
    if not isinstance(parsed, dict):
        log.warning("tool call %r (%r) arguments are not an object: %.200r", name, call_id, raw)
    return parsed


def _retry_after(response: httpx.Response, now: datetime) -> float | None:
    raw = response.headers.get("retry-after")
    if raw is None or response.status_code not in (429, 529):
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
        return None
    return max(0.0, seconds)


class _Retryable(Exception):
    def __init__(self, message: str, *, status: int | None, response: httpx.Response | None):
        super().__init__(message)
        self.status = status
        self.response = response


class OpenAICompatLLMClient:
    def __init__(
        self,
        settings: LLMSettings | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        pricing: PricingTable | None = None,
        limiter: TokenBucket | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._settings = settings or LLMSettings()
        headers = {}
        if self._settings.openai_api_key is not None:
            headers["Authorization"] = f"Bearer {self._settings.openai_api_key.get_secret_value()}"
        self._client = client or httpx.AsyncClient(
            timeout=self._settings.openai_timeout_seconds, headers=headers
        )
        self._headers = headers
        self._url = self._settings.openai_base_url.rstrip("/") + "/chat/completions"
        self._pricing = pricing or PricingTable.load(self._settings.pricing_path)
        self._limiter = limiter or TokenBucket(self._settings.requests_per_minute)
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._clock = clock

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

    async def _post(self, request: LLMRequest) -> dict[str, Any]:
        base = self._settings.openai_base_url
        try:
            response = await self._client.post(
                self._url, json=to_chat_params(request), headers=self._headers
            )
        except httpx.ConnectError as exc:
            raise LLMConnectionRefused(
                f"cannot connect to {base} for model {request.model!r}: {exc}. Start the "
                f"server (`ollama serve`) and pull the model (`ollama pull {request.model}`), "
                "or set FIREBREAK_LLM_OPENAI_BASE_URL."
            ) from exc
        except httpx.TimeoutException as exc:
            raise _Retryable(f"timeout after {exc!r}", status=None, response=None) from exc
        except httpx.TransportError as exc:
            raise _Retryable(f"transport error: {exc!r}", status=None, response=None) from exc
        status = response.status_code
        if status in _RETRY_STATUSES or status >= 500:
            raise _Retryable(
                f"HTTP {status}: {response.text[:200]}", status=status, response=response
            )
        if status >= 400:
            raise LLMError(
                f"openai_compat call failed for {request.model}: HTTP {status}: "
                f"{response.text[:300]}",
                status_code=status,
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise LLMError(
                f"openai_compat reply for {request.model} is not JSON: {response.text[:200]}"
            ) from exc
        if not isinstance(body, dict):
            raise LLMError(f"openai_compat reply for {request.model} is not a JSON object")
        return body

    async def _complete_with_retries(self, request: LLMRequest) -> LLMResponse:
        max_retries = self._settings.openai_max_retries
        attempt = 0
        while True:
            self._check_preflight(request)
            await self._limiter.acquire()
            try:
                body = await self._post(request)
            except _Retryable as exc:
                if attempt >= max_retries:
                    raise LLMRetriesExhausted(
                        f"{request.model} still failing after {max_retries} retries: {exc}",
                        status_code=exc.status,
                        retryable=True,
                    ) from exc
                delay = self._retry_delay(exc, attempt)
                log.warning(
                    "llm retryable error on %s (attempt %d/%d, status=%s): %s; sleeping %.2fs",
                    request.model,
                    attempt + 1,
                    max_retries + 1,
                    exc.status,
                    exc,
                    delay,
                )
                attempt += 1
                await self._sleep(delay)
                continue
            self._check_truncation(body, request.model)
            return self._to_response(body, requested_model=request.model)

    def _check_preflight(self, request: LLMRequest) -> None:
        limit = self._settings.openai_context_tokens
        estimate = estimate_prompt_tokens(to_chat_params(request))
        if estimate + request.max_tokens > limit:
            raise ContextOverflowError(
                f"context overflow for {request.model}: estimated prompt {estimate} tokens + "
                f"max_tokens {request.max_tokens} > FIREBREAK_LLM_OPENAI_CONTEXT_TOKENS={limit}; "
                "request not sent (the server would silently truncate it). Raise "
                "OLLAMA_CONTEXT_LENGTH and FIREBREAK_LLM_OPENAI_CONTEXT_TOKENS together, or "
                "shorten the conversation."
            )

    def _check_truncation(self, body: dict[str, Any], model: str) -> None:
        usage = body.get("usage")
        if not isinstance(usage, dict):
            return
        prompt = int(usage.get("prompt_tokens") or 0)
        completion = int(usage.get("completion_tokens") or 0)
        limit = self._settings.openai_context_tokens
        if prompt + completion >= _TRUNCATION_RATIO * limit:
            raise ContextOverflowError(
                f"context overflow for {model}: prompt_tokens {prompt} + completion_tokens "
                f"{completion} = {prompt + completion} >= 98% of "
                f"FIREBREAK_LLM_OPENAI_CONTEXT_TOKENS={limit}; the server almost certainly "
                "truncated the prompt, so the reply cannot be trusted. Raise "
                "OLLAMA_CONTEXT_LENGTH and FIREBREAK_LLM_OPENAI_CONTEXT_TOKENS together."
            )

    def _retry_delay(self, exc: _Retryable, attempt: int) -> float:
        if exc.response is not None:
            after = _retry_after(exc.response, self._clock())
            if after is not None:
                return min(after, self._settings.backoff_max_seconds)
        ceiling = min(
            self._settings.backoff_max_seconds, self._settings.backoff_base_seconds * (2**attempt)
        )
        return self._rng.uniform(0, ceiling)

    def _to_response(self, body: dict[str, Any], *, requested_model: str) -> LLMResponse:
        choices = body.get("choices")
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            raise LLMError(f"openai_compat reply for {requested_model} has no choices")
        choice = choices[0]
        message = choice.get("message")
        if not isinstance(message, dict):
            raise LLMError(f"openai_compat reply for {requested_model} has no message")
        content: list[dict[str, Any]] = []
        text = message.get("content")
        if isinstance(text, str) and text:
            content.append({"type": "text", "text": text})
        tool_calls = message.get("tool_calls") or []
        if not isinstance(tool_calls, list):
            raise LLMError(f"openai_compat tool_calls for {requested_model} is not a list")
        for index, call in enumerate(tool_calls):
            fn = call.get("function") if isinstance(call, dict) else None
            if not isinstance(fn, dict):
                raise LLMError(f"openai_compat tool call without a function: {call!r:.200}")
            call_id = call.get("id")
            if not isinstance(call_id, str) or not call_id.strip():
                call_id = f"call_{index}"
                log.warning(
                    "tool call %r from %s had no id; using %s",
                    fn.get("name"),
                    requested_model,
                    call_id,
                )
            content.append(
                {
                    "type": "tool_use",
                    "id": call_id,
                    "name": fn.get("name") or "",
                    "input": _parse_arguments(
                        fn.get("arguments", ""), name=fn.get("name"), call_id=call_id
                    ),
                }
            )
        if not content:
            raise LLMError(
                f"empty completion from {requested_model}: no text and no tool calls "
                f"(finish_reason={choice.get('finish_reason')!r})"
            )
        finish = choice.get("finish_reason")
        if finish not in _FINISH_REASONS:
            raise LLMError(f"unsupported finish_reason {finish!r} from {requested_model}")
        stop_reason = _FINISH_REASONS[finish]
        if tool_calls and stop_reason == "end_turn":
            stop_reason = "tool_use"  # some servers (Ollama) say "stop" even with tool calls
        raw_usage = body.get("usage")
        usage_d = raw_usage if isinstance(raw_usage, dict) else {}
        usage = Usage(
            input_tokens=int(usage_d.get("prompt_tokens") or 0),
            output_tokens=int(usage_d.get("completion_tokens") or 0),
        )
        model = body.get("model") or requested_model
        # Priced by the requested model: a local server may echo a different tag name.
        usage.cost_usd = self._pricing.cost_usd(requested_model, usage)
        return LLMResponse(
            model=model if isinstance(model, str) else requested_model,
            content=content,
            stop_reason=cast(Any, stop_reason),
            usage=usage,
        )

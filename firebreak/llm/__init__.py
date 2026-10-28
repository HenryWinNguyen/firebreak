"""LLM client (T-031): Anthropic wrapper, retries, fallback, cost, record/replay."""

from firebreak.llm.client import AnthropicLLMClient, build_llm_client
from firebreak.llm.errors import (
    FixtureMissing,
    LLMConnectionRefused,
    LLMError,
    LLMRetriesExhausted,
    PricingError,
    UnknownModelError,
)
from firebreak.llm.fixtures import FixtureStore, fixture_key
from firebreak.llm.openai_compat import OpenAICompatLLMClient
from firebreak.llm.pricing import PricingTable
from firebreak.llm.ratelimit import TokenBucket
from firebreak.llm.replay import RecordingLLMClient, ReplayLLMClient
from firebreak.llm.routing import RoutingLLMClient
from firebreak.llm.settings import LLMMode, LLMSettings

__all__ = [
    "AnthropicLLMClient",
    "FixtureMissing",
    "FixtureStore",
    "LLMConnectionRefused",
    "LLMError",
    "LLMMode",
    "LLMRetriesExhausted",
    "LLMSettings",
    "OpenAICompatLLMClient",
    "PricingError",
    "PricingTable",
    "RecordingLLMClient",
    "ReplayLLMClient",
    "RoutingLLMClient",
    "TokenBucket",
    "UnknownModelError",
    "build_llm_client",
    "fixture_key",
]

"""RoutingLLMClient: send each request to the provider that serves its model.

The provider comes from ``pricing.yaml`` (``provider: anthropic | openai_compat``). A model
that is not listed is an ``UnknownModelError``; it never defaults to a provider or to $0.
Clients are built lazily, so an all-local run never constructs the Anthropic client.
"""

import logging
from collections.abc import Callable, Mapping

from firebreak.llm.errors import LLMRetriesExhausted
from firebreak.llm.pricing import PricingTable, Provider
from firebreak.llm.settings import LLMSettings
from firebreak.models.llm import LLMClient, LLMRequest, LLMResponse

log = logging.getLogger(__name__)


class RoutingLLMClient:
    def __init__(
        self,
        settings: LLMSettings | None = None,
        *,
        pricing: PricingTable | None = None,
        factories: Mapping[Provider, Callable[[], LLMClient]] | None = None,
    ) -> None:
        self._settings = settings or LLMSettings()
        self._pricing = pricing or PricingTable.load(self._settings.pricing_path)
        self._factories: dict[Provider, Callable[[], LLMClient]] = dict(
            factories or self._default_factories()
        )
        self._clients: dict[Provider, LLMClient] = {}

    def _default_factories(self) -> dict[Provider, Callable[[], LLMClient]]:
        def anthropic_client() -> LLMClient:
            from firebreak.llm.client import AnthropicLLMClient

            return AnthropicLLMClient(self._settings, pricing=self._pricing)

        def openai_client() -> LLMClient:
            from firebreak.llm.openai_compat import OpenAICompatLLMClient

            return OpenAICompatLLMClient(self._settings, pricing=self._pricing)

        return {"anthropic": anthropic_client, "openai_compat": openai_client}

    def _client(self, provider: Provider) -> LLMClient:
        if provider not in self._clients:
            self._clients[provider] = self._factories[provider]()
        return self._clients[provider]

    async def complete(self, request: LLMRequest) -> LLMResponse:
        primary = self._pricing.provider_for(request.model)
        fallback = request.fallback_model
        if fallback is None or fallback == request.model:
            return await self._client(primary).complete(request)
        fallback_provider = self._pricing.provider_for(fallback)  # unknown fallback fails now
        if fallback_provider == primary:
            return await self._client(primary).complete(request)  # the client falls back itself
        try:
            return await self._client(primary).complete(
                request.model_copy(update={"fallback_model": None})
            )
        except LLMRetriesExhausted as exc:
            log.warning(
                "llm retries exhausted on %s (%s); falling back to %s via %s",
                request.model,
                exc,
                fallback,
                fallback_provider,
            )
            return await self._client(fallback_provider).complete(
                request.model_copy(update={"model": fallback, "fallback_model": None})
            )

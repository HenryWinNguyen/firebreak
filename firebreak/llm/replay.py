"""Record and replay clients."""

from firebreak.llm.fixtures import FixtureStore
from firebreak.models.llm import LLMClient, LLMRequest, LLMResponse


class ReplayLLMClient:
    """Serves recorded fixtures. Raises FixtureMissing (with the key) on a miss; never calls out."""

    def __init__(self, store: FixtureStore) -> None:
        self._store = store

    async def complete(self, request: LLMRequest) -> LLMResponse:
        return self._store.load(request)


class RecordingLLMClient:
    """Wraps a live client and writes every response to the fixture store."""

    def __init__(self, inner: LLMClient, store: FixtureStore) -> None:
        self._inner = inner
        self._store = store

    async def complete(self, request: LLMRequest) -> LLMResponse:
        response = await self._inner.complete(request)
        self._store.save(request, response)
        return response

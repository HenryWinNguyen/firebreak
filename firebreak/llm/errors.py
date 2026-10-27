"""Typed errors raised by the LLM client. Nothing here is ever swallowed."""


class LLMError(Exception):
    """Base class. `status_code` is set when the failure came from an HTTP response."""

    def __init__(self, message: str, *, status_code: int | None = None, retryable: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable


class LLMRetriesExhausted(LLMError):
    """A retryable failure persisted through every attempt (and the fallback, if any)."""


class PricingError(LLMError):
    """No price is configured for a model. Cost is never guessed."""


class FixtureMissing(LLMError):
    """Replay mode found no fixture for this request. `key` is the sha256 fixture key."""

    def __init__(self, key: str, path: str):
        super().__init__(f"no LLM fixture for key {key} (looked at {path})")
        self.key = key
        self.path = path

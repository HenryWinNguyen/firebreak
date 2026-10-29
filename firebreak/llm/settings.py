"""LLM client settings (`FIREBREAK_LLM_*`)."""

from enum import StrEnum
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class LLMMode(StrEnum):
    LIVE = "live"
    RECORD = "record"
    REPLAY = "replay"


class LLMSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FIREBREAK_LLM_", env_file=".env", extra="ignore")

    mode: LLMMode = LLMMode.LIVE
    fixture_dir: Path = Path("tests/fixtures/llm")
    pricing_path: Path = Path(__file__).parent / "pricing.yaml"
    requests_per_minute: float = Field(default=50.0, gt=0)
    max_retries: int = Field(default=4, ge=0, le=4, description="Retries after the first attempt")
    backoff_base_seconds: float = Field(default=1.0, gt=0)
    backoff_max_seconds: float = Field(default=30.0, gt=0)
    timeout_seconds: float = Field(default=120.0, gt=0)
    # OpenAI-compatible provider (local Ollama by default). Ollama needs no key.
    openai_base_url: str = "http://localhost:11434/v1"
    openai_api_key: SecretStr | None = None
    openai_timeout_seconds: float = Field(default=300.0, gt=0)
    # Retries after the first attempt for the local provider. Kept at 1 (not max_retries) because
    # each attempt can take up to openai_timeout_seconds; 5 x 300s would stall a hung model ~25 min.
    openai_max_retries: int = Field(default=1, ge=0, le=4)
    # Must equal the server's OLLAMA_CONTEXT_LENGTH (Ollama defaults to 4096 and silently truncates
    # longer prompts). Prompt size is estimated conservatively as chars / 3.
    openai_context_tokens: int = Field(default=32768, gt=0)

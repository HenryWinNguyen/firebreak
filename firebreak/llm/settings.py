"""LLM client settings (`FIREBREAK_LLM_*`)."""

from enum import StrEnum
from pathlib import Path

from pydantic import Field
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

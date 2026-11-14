"""Worker/client settings for Temporal (CONTRACTS §1, orchestration rule)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class TemporalSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FIREBREAK_", env_file=".env", extra="ignore")

    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    #: Seconds in-flight activities get to finish on SIGTERM before the worker cancels them.
    worker_shutdown_grace_s: int = 30

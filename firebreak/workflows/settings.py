"""Worker and client settings (``FIREBREAK_TEMPORAL_*``)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class TemporalSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FIREBREAK_TEMPORAL_", env_file=".env", extra="ignore"
    )

    address: str = "localhost:7233"
    namespace: str = "default"
    #: Seconds a stopping worker waits for in-flight activities before cancelling them.
    graceful_shutdown_s: int = 30

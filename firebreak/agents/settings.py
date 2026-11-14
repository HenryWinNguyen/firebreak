"""Settings for the multi-agent activities. Env prefix ``FIREBREAK_AGENTS_``."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class AgentSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FIREBREAK_AGENTS_", env_file=".env", extra="ignore"
    )

    #: Directory name under ``firebreak/agents/configs/`` that holds ``<agent>.yaml`` files.
    profile: str = "multi-local"

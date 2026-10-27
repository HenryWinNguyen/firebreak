"""Process-wide settings. Track-specific settings live in their own package (CONTRACTS §4)."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FIREBREAK_", env_file=".env", extra="ignore")

    env: str = "dev"
    database_url: str = "postgresql+asyncpg://firebreak:firebreak@localhost:5432/firebreak"
    webhook_token: SecretStr = SecretStr("dev-webhook-token")

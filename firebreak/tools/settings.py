"""Settings for the read-tool adapters (CONTRACTS §3.4, §4). Env prefix ``FIREBREAK_TOOLS_``."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_shopfloor_urls() -> dict[str, str]:
    # Ports from CONTRACTS §2.1.
    return {
        "gateway": "http://localhost:8100",
        "orders": "http://localhost:8101",
        "inventory": "http://localhost:8102",
    }


class ToolSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FIREBREAK_TOOLS_", env_file=".env", extra="ignore"
    )

    prometheus_url: str = "http://localhost:9090"
    loki_url: str = "http://localhost:3100"
    shopfloor_urls: dict[str, str] = Field(default_factory=_default_shopfloor_urls)
    #: Bearer token for ``GET /admin/deploys`` only. The adapter can't reach any other admin path.
    shopfloor_admin_token: SecretStr = SecretStr("dev-admin-token")
    #: Per-request HTTP timeout. Keep it below each tool's gateway timeout.
    http_timeout_s: float = Field(default=5.0, gt=0)

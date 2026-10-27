"""Alerts: the normalized form every ingest source produces (ARCH §3)."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from firebreak.models.common import Severity, new_id


class AlertSource(StrEnum):
    ALERTMANAGER = "alertmanager"
    SENTRY = "sentry"
    GENERIC = "generic"


class AlertStatus(StrEnum):
    FIRING = "firing"
    RESOLVED = "resolved"


class Alert(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: new_id("al"))
    source: AlertSource
    fingerprint: str = Field(min_length=1, description="Stable per alert rule + label set")
    alertname: str
    service: str
    severity: Severity
    status: AlertStatus = AlertStatus.FIRING
    summary: str
    labels: dict[str, str] = Field(default_factory=dict)
    started_at: datetime
    ended_at: datetime | None = None

    @property
    def idempotency_key(self) -> str:
        """Replaying the same webhook must be a no-op; this is the dedupe key."""
        return f"{self.source}:{self.fingerprint}:{self.started_at.isoformat()}"


# --- Alertmanager webhook payload (external format, v4) -------------------------------


class AlertmanagerAlert(BaseModel):
    status: AlertStatus
    labels: dict[str, str]
    annotations: dict[str, str] = Field(default_factory=dict)
    startsAt: datetime
    endsAt: datetime | None = None
    fingerprint: str


class AlertmanagerWebhook(BaseModel):
    version: str = "4"
    status: AlertStatus
    receiver: str
    alerts: list[AlertmanagerAlert]

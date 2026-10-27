"""Normalize external webhook payloads into ``Alert``s (ARCH §3)."""

import hashlib
import json
from datetime import datetime

from pydantic import BaseModel, Field

from firebreak.models.alert import (
    Alert,
    AlertmanagerWebhook,
    AlertSource,
    AlertStatus,
)
from firebreak.models.common import Severity


class NormalizationError(ValueError):
    """The payload is well-formed but cannot become an ``Alert`` (maps to HTTP 422)."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


class GenericWebhook(BaseModel):
    """Payload for ``POST /webhooks/generic``: one alert in already-normalized shape."""

    alertname: str = Field(min_length=1)
    service: str = Field(min_length=1)
    severity: Severity = Severity.WARNING
    summary: str = ""
    status: AlertStatus = AlertStatus.FIRING
    labels: dict[str, str] = Field(default_factory=dict)
    fingerprint: str | None = None
    started_at: datetime
    ended_at: datetime | None = None


def _label_fingerprint(alertname: str, service: str, labels: dict[str, str]) -> str:
    blob = json.dumps([alertname, service, sorted(labels.items())])
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def normalize_alertmanager(webhook: AlertmanagerWebhook) -> list[Alert]:
    """Convert every alert in the webhook, or raise ``NormalizationError`` listing all problems.

    ``alertname`` and ``service`` labels are required. ``severity`` defaults to ``warning``
    when absent but an unrecognised value is an error. Nothing is silently dropped.
    """
    alerts: list[Alert] = []
    problems: list[str] = []
    for i, am in enumerate(webhook.alerts):
        where = f"alerts[{i}] (fingerprint={am.fingerprint})"
        missing = [k for k in ("alertname", "service") if not am.labels.get(k)]
        if missing:
            problems.append(f"{where}: missing required label(s): {', '.join(missing)}")
            continue
        raw_severity = am.labels.get("severity", Severity.WARNING.value)
        try:
            severity = Severity(raw_severity)
        except ValueError:
            problems.append(f"{where}: invalid severity label {raw_severity!r}")
            continue
        summary = am.annotations.get("summary") or am.annotations.get("description") or ""
        alerts.append(
            Alert(
                source=AlertSource.ALERTMANAGER,
                fingerprint=am.fingerprint,
                alertname=am.labels["alertname"],
                service=am.labels["service"],
                severity=severity,
                status=am.status,
                summary=summary,
                labels=dict(am.labels),
                started_at=am.startsAt,
                ended_at=am.endsAt,
            )
        )
    if problems:
        raise NormalizationError(problems)
    return alerts


def normalize_generic(payload: GenericWebhook) -> list[Alert]:
    return [
        Alert(
            source=AlertSource.GENERIC,
            fingerprint=payload.fingerprint
            or _label_fingerprint(payload.alertname, payload.service, payload.labels),
            alertname=payload.alertname,
            service=payload.service,
            severity=payload.severity,
            status=payload.status,
            summary=payload.summary,
            labels=dict(payload.labels),
            started_at=payload.started_at,
            ended_at=payload.ended_at,
        )
    ]

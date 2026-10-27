"""Normalize external webhook payloads into ``Alert``s (ARCH §3)."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, Field, StringConstraints

from firebreak.models.alert import (
    Alert,
    AlertmanagerWebhook,
    AlertSource,
    AlertStatus,
)
from firebreak.models.common import Severity

MAX_ALERTS = 500
MAX_LABELS = 64
MAX_VALUE_LEN = 2000
_ZERO_YEAR = 1  # Alertmanager sends 0001-01-01T00:00:00Z for "no end"

Short = Annotated[str, StringConstraints(min_length=1, max_length=256)]
Long = Annotated[str, StringConstraints(max_length=MAX_VALUE_LEN)]


class NormalizationError(ValueError):
    """The payload is well-formed but cannot become an ``Alert`` (maps to HTTP 422)."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


class GenericWebhook(BaseModel):
    """Payload for ``POST /webhooks/generic``: one alert in already-normalized shape."""

    alertname: Short
    service: Short
    severity: Severity = Severity.WARNING
    summary: Long = ""
    status: AlertStatus = AlertStatus.FIRING
    labels: dict[Short, Long] = Field(default_factory=dict, max_length=MAX_LABELS)
    fingerprint: Short | None = None
    started_at: AwareDatetime
    ended_at: AwareDatetime | None = None


def _label_fingerprint(alertname: str, service: str, labels: dict[str, str]) -> str:
    blob = json.dumps([alertname, service, sorted(labels.items())])
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def _utc(dt: datetime) -> datetime:
    return dt.astimezone(UTC)


def normalize_alertmanager(webhook: AlertmanagerWebhook) -> list[Alert]:
    """Convert every alert in the webhook, or raise ``NormalizationError`` listing all problems.

    ``alertname`` and ``service`` labels are required. ``severity`` defaults to ``warning``
    when absent but an unrecognised value is an error. Nothing is silently dropped.
    """
    if len(webhook.alerts) > MAX_ALERTS:
        raise NormalizationError([f"too many alerts: {len(webhook.alerts)} > {MAX_ALERTS}"])
    alerts: list[Alert] = []
    problems: list[str] = []
    for i, am in enumerate(webhook.alerts):
        where = f"alerts[{i}] (fingerprint={am.fingerprint})"
        missing = [k for k in ("alertname", "service") if not am.labels.get(k)]
        if missing:
            problems.append(f"{where}: missing required label(s): {', '.join(missing)}")
            continue
        if len(am.labels) > MAX_LABELS or any(
            len(k) > 256 or len(v) > MAX_VALUE_LEN for k, v in am.labels.items()
        ):
            problems.append(f"{where}: labels exceed size limits")
            continue
        if any(len(v) > MAX_VALUE_LEN for v in am.annotations.values()):
            problems.append(f"{where}: annotations exceed size limits")
            continue
        if am.startsAt.tzinfo is None or (am.endsAt is not None and am.endsAt.tzinfo is None):
            problems.append(f"{where}: timestamps must include a timezone offset")
            continue
        raw_severity = am.labels.get("severity", Severity.WARNING.value).lower()
        try:
            severity = Severity(raw_severity)
        except ValueError:
            problems.append(f"{where}: invalid severity label {raw_severity!r}")
            continue
        ends = am.endsAt
        ended_at = None if ends is None or ends.year <= _ZERO_YEAR else _utc(ends)
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
                started_at=_utc(am.startsAt),
                ended_at=ended_at,
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
            started_at=_utc(payload.started_at),
            ended_at=_utc(payload.ended_at) if payload.ended_at else None,
        )
    ]

from datetime import UTC, datetime

import pytest

from firebreak.ingest.normalize import (
    GenericWebhook,
    NormalizationError,
    normalize_alertmanager,
    normalize_generic,
)
from firebreak.models.alert import (
    AlertmanagerAlert,
    AlertmanagerWebhook,
    AlertSource,
    AlertStatus,
)
from firebreak.models.common import Severity

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _am(labels: dict[str, str], fp: str = "fp1", status: AlertStatus = AlertStatus.FIRING):
    return AlertmanagerAlert(
        status=status,
        labels=labels,
        annotations={"summary": "boom"},
        startsAt=T0,
        fingerprint=fp,
    )


def _hook(*alerts: AlertmanagerAlert) -> AlertmanagerWebhook:
    return AlertmanagerWebhook(status=AlertStatus.FIRING, receiver="firebreak", alerts=list(alerts))


def test_alertmanager_maps_labels_and_annotations() -> None:
    [a] = normalize_alertmanager(
        _hook(_am({"alertname": "HighErrorRate", "service": "orders", "severity": "critical"}))
    )
    assert a.source is AlertSource.ALERTMANAGER
    assert (a.alertname, a.service, a.severity) == ("HighErrorRate", "orders", Severity.CRITICAL)
    assert a.summary == "boom"
    assert a.fingerprint == "fp1"
    assert a.started_at == T0


def test_missing_service_is_an_error_naming_the_alert() -> None:
    with pytest.raises(NormalizationError) as exc:
        normalize_alertmanager(
            _hook(
                _am({"alertname": "A", "service": "s"}, fp="ok"),
                _am({"alertname": "B", "severity": "critical"}, fp="bad"),
            )
        )
    assert len(exc.value.problems) == 1
    assert "alerts[1]" in exc.value.problems[0]
    assert "service" in exc.value.problems[0]


def test_invalid_severity_is_an_error() -> None:
    with pytest.raises(NormalizationError, match="severity"):
        normalize_alertmanager(_hook(_am({"alertname": "A", "service": "s", "severity": "bad"})))


def test_missing_severity_defaults_to_warning() -> None:
    [a] = normalize_alertmanager(_hook(_am({"alertname": "A", "service": "s"})))
    assert a.severity is Severity.WARNING


def test_resolved_status_preserved() -> None:
    [a] = normalize_alertmanager(
        _hook(_am({"alertname": "A", "service": "s"}, status=AlertStatus.RESOLVED))
    )
    assert a.status is AlertStatus.RESOLVED


def test_generic_fingerprint_is_stable() -> None:
    p = GenericWebhook(alertname="A", service="s", labels={"k": "v"}, started_at=T0)
    [a1], [a2] = normalize_generic(p), normalize_generic(p)
    assert a1.fingerprint == a2.fingerprint
    assert a1.idempotency_key == a2.idempotency_key
    assert a1.source is AlertSource.GENERIC

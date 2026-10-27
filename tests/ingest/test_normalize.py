from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

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


@pytest.mark.parametrize(
    "ts", ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00+00:00", "2026-01-01T02:00:00+02:00"]
)
def test_timezones_produce_same_key(ts: str) -> None:
    am = AlertmanagerAlert(
        status=AlertStatus.FIRING,
        labels={"alertname": "A", "service": "s"},
        startsAt=datetime.fromisoformat(ts),
        fingerprint="f",
    )
    g = GenericWebhook(
        alertname="A", service="s", fingerprint="f", started_at=datetime.fromisoformat(ts)
    )
    [a] = normalize_alertmanager(_hook(am))
    [b] = normalize_generic(g)
    assert a.started_at.utcoffset() == timedelta(0)
    assert a.idempotency_key == "alertmanager:f:2026-01-01T00:00:00+00:00"
    assert b.idempotency_key == "generic:f:2026-01-01T00:00:00+00:00"


def test_naive_generic_rejected() -> None:
    with pytest.raises(ValidationError):
        GenericWebhook(alertname="A", service="s", started_at=datetime(2026, 1, 1))


def test_naive_alertmanager_rejected() -> None:
    am = AlertmanagerAlert(
        status=AlertStatus.FIRING,
        labels={"alertname": "A", "service": "s"},
        startsAt=datetime(2026, 1, 1),
        fingerprint="f",
    )
    with pytest.raises(NormalizationError, match="timezone"):
        normalize_alertmanager(_hook(am))


def test_zero_ends_at_becomes_none() -> None:
    am = AlertmanagerAlert(
        status=AlertStatus.FIRING,
        labels={"alertname": "A", "service": "s"},
        startsAt=T0,
        endsAt=datetime(1, 1, 1, tzinfo=UTC),
        fingerprint="f",
    )
    [a] = normalize_alertmanager(_hook(am))
    assert a.ended_at is None


def test_real_ends_at_kept_in_utc() -> None:
    am = AlertmanagerAlert(
        status=AlertStatus.RESOLVED,
        labels={"alertname": "A", "service": "s"},
        startsAt=T0,
        endsAt=datetime.fromisoformat("2026-01-01T03:00:00+02:00"),
        fingerprint="f",
    )
    [a] = normalize_alertmanager(_hook(am))
    assert a.ended_at == datetime(2026, 1, 1, 1, tzinfo=UTC)


def test_severity_lowercased() -> None:
    [a] = normalize_alertmanager(
        _hook(_am({"alertname": "A", "service": "s", "severity": "CRITICAL"}))
    )
    assert a.severity is Severity.CRITICAL


def test_too_many_alerts_rejected() -> None:
    hook = _hook(*[_am({"alertname": "A", "service": "s"}, fp=str(i)) for i in range(501)])
    with pytest.raises(NormalizationError, match="too many"):
        normalize_alertmanager(hook)


def test_oversized_label_rejected() -> None:
    with pytest.raises(NormalizationError, match="size"):
        normalize_alertmanager(_hook(_am({"alertname": "A", "service": "s", "x": "y" * 2001})))


def test_explicit_generic_fingerprint_used() -> None:
    [a] = normalize_generic(
        GenericWebhook(alertname="A", service="s", fingerprint="mine", started_at=T0)
    )
    assert a.fingerprint == "mine"


def test_description_fallback_summary() -> None:
    am = AlertmanagerAlert(
        status=AlertStatus.FIRING,
        labels={"alertname": "A", "service": "s"},
        annotations={"description": "long text"},
        startsAt=T0,
        fingerprint="f",
    )
    [a] = normalize_alertmanager(_hook(am))
    assert a.summary == "long text"

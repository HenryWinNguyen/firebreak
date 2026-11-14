"""Webhook -> signal-with-start of IncidentWorkflow (T-066)."""

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from temporalio.client import Client, WorkflowExecutionStatus

import firebreak.api.temporal as api_temporal
from firebreak.api.app import app
from firebreak.api.deps import get_alert_repo
from firebreak.api.temporal import IncidentSignaller, SignalSettings
from firebreak.models.common import Severity
from firebreak.models.incident import IncidentState
from firebreak.models.orchestration import AlertAdded
from tests.api.conftest import RecordingSignaller
from tests.api.test_webhooks import AUTH, FakeGrouper, MemoryRepo, am_alert, am_hook, grouper  # noqa: F401
from tests.workflows.test_grouping_window import WindowStub
from tests.workflows.test_incident import env_client, make_workers

WARN_LABELS = {"alertname": "Slow", "service": "orders", "severity": "warning"}


@pytest.fixture
async def client(grouper: FakeGrouper) -> httpx.AsyncClient:  # type: ignore[misc]  # noqa: F811
    repo = MemoryRepo()
    app.dependency_overrides[get_alert_repo] = lambda: repo
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c


async def post(c: httpx.AsyncClient, *alerts: dict) -> httpx.Response:  # type: ignore[type-arg]
    return await c.post("/webhooks/alertmanager", json=am_hook(*alerts), headers=AUTH)


async def test_created_incident_is_signalled(
    client: httpx.AsyncClient, signaller: RecordingSignaller
) -> None:
    r = await post(client, am_alert("a"))
    assert r.status_code == 200
    alert_id = r.json()["results"][0]["alert_id"]
    assert signaller.calls == [
        ("inc_fake", AlertAdded(alert_id=alert_id, severity=Severity.CRITICAL))
    ]


async def test_duplicate_is_signalled_again(
    client: httpx.AsyncClient, signaller: RecordingSignaller
) -> None:
    await post(client, am_alert("a"))
    r = await post(client, am_alert("a"))
    assert r.json()["results"][0]["decision"]["duplicate"] is True
    assert len(signaller.calls) == 2
    assert signaller.calls[0] == signaller.calls[1]


async def test_resolved_is_not_signalled(
    client: httpx.AsyncClient, signaller: RecordingSignaller
) -> None:
    r = await post(client, am_alert("a", status="resolved"))
    assert r.status_code == 200
    assert signaller.calls == []


async def test_signal_error_still_2xx_and_logged(
    client: httpx.AsyncClient,
    signaller: RecordingSignaller,
    caplog: pytest.LogCaptureFixture,
) -> None:
    signaller.error = ConnectionError("temporal down")
    with caplog.at_level(logging.ERROR):
        r = await post(client, am_alert("a"), am_alert("b"))
    assert r.status_code == 200
    assert len(signaller.calls) == 2  # a generic error does not stop the other alert
    errors = [x for x in caplog.records if x.levelno == logging.ERROR]
    assert len(errors) == 2
    alert_id = r.json()["results"][0]["alert_id"]
    assert "inc_fake" in errors[0].getMessage() and alert_id in errors[0].getMessage()
    assert errors[0].exc_info is not None


async def test_webhook_to_workflow_in_grouping(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End to end: webhook -> signal-with-start on a real test server -> workflow in GROUPING."""
    stub = WindowStub(datetime.now(UTC) + timedelta(hours=1))
    async with env_client() as (_, tclient):

        async def fake_connect(*_a: object, **_k: object) -> Client:
            return tclient

        monkeypatch.setattr(api_temporal, "connect", fake_connect)
        real = IncidentSignaller(SignalSettings(temporal_signal_timeout_s=5))
        app.dependency_overrides[api_temporal.get_signaller] = lambda: real
        wf, act = make_workers(tclient, stub)
        async with wf, act:
            r = await post(client, am_alert("a", labels=WARN_LABELS))
            assert r.status_code == 200
            handle = tclient.get_workflow_handle("incident-inc_fake")
            for _ in range(100):
                if stub.states:
                    break
                await asyncio.sleep(0.05)
            assert stub.states == [IncidentState.GROUPING]
            desc = await handle.describe()
            assert desc.status == WorkflowExecutionStatus.RUNNING
            await handle.terminate("test over")


async def test_connect_hang_is_bounded(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def hang(*_a: object, **_k: object) -> Client:
        await asyncio.sleep(3600)
        raise AssertionError("unreachable")

    monkeypatch.setattr(api_temporal, "connect", hang)
    real = IncidentSignaller(SignalSettings(temporal_signal_timeout_s=0.2))
    app.dependency_overrides[api_temporal.get_signaller] = lambda: real
    start = time.monotonic()
    with caplog.at_level(logging.ERROR):
        r = await post(client, am_alert("a"), am_alert("b"), am_alert("c"))
    assert r.status_code == 200
    assert time.monotonic() - start < 2  # one bounded attempt; the rest are skipped
    assert len([x for x in caplog.records if x.levelno == logging.ERROR]) == 3


async def test_concurrent_webhooks_with_hanging_connect_share_one_bound(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempts = 0

    async def hang(*_a: object, **_k: object) -> Client:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(3600)
        raise AssertionError("unreachable")

    monkeypatch.setattr(api_temporal, "connect", hang)
    real = IncidentSignaller(SignalSettings(temporal_signal_timeout_s=0.3))
    app.dependency_overrides[api_temporal.get_signaller] = lambda: real
    start = time.monotonic()
    rs = await asyncio.gather(*(post(client, am_alert(f"fp{i}")) for i in range(10)))
    assert all(r.status_code == 200 for r in rs)
    assert time.monotonic() - start < 1.0  # ~one bound, not 10 x 0.3 s
    assert attempts == 1  # one shared connect attempt


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def test_connect_failure_fails_fast_then_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    connects = 0
    sent: list[str] = []
    fake_client = object()

    async def flaky_connect(*_a: object, **_k: object) -> object:
        nonlocal connects
        connects += 1
        if connects == 1:
            raise ConnectionError("down")
        return fake_client

    async def fake_signal(c: object, incident_id: str, alert: AlertAdded) -> None:
        assert c is fake_client
        sent.append(incident_id)

    monkeypatch.setattr(api_temporal, "connect", flaky_connect)
    monkeypatch.setattr(api_temporal, "signal_alert_added", fake_signal)
    clock = FakeClock()
    s = IncidentSignaller(
        SignalSettings(temporal_signal_timeout_s=1, temporal_connect_retry_s=20), clock=clock
    )
    a = AlertAdded(alert_id="al_1", severity=Severity.WARNING)
    with pytest.raises(ConnectionError):
        await s.signal("inc_1", a)
    clock.now += 10  # inside the negative-cache window: no new connect attempt
    with pytest.raises(ConnectionError):
        await s.signal("inc_1", a)
    assert connects == 1
    clock.now += 11  # window over: retried, and the failure was not cached forever
    await s.signal("inc_1", a)
    assert connects == 2 and sent == ["inc_1"]
    await s.signal("inc_1", a)  # connected client is cached
    assert connects == 2 and sent == ["inc_1", "inc_1"]


async def test_hanging_signal_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    async def ok_connect(*_a: object, **_k: object) -> object:
        return object()

    async def hang_signal(*_a: object, **_k: object) -> None:
        await asyncio.sleep(3600)

    monkeypatch.setattr(api_temporal, "connect", ok_connect)
    monkeypatch.setattr(api_temporal, "signal_alert_added", hang_signal)
    s = IncidentSignaller(SignalSettings(temporal_signal_timeout_s=0.2))
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        await s.signal("inc_1", AlertAdded(alert_id="al_1", severity=Severity.WARNING))
    assert time.monotonic() - start < 1.0

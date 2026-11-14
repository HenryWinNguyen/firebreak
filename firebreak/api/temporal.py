"""Signalling ``IncidentWorkflow`` from the API (T-066).

The Temporal client is created lazily and cached, so the API starts and accepts webhooks with
Temporal down. Connecting and each signal are bounded by ``asyncio.wait_for`` so a dead
Temporal cannot hold a webhook past Alertmanager's timeout.
"""

import asyncio
import logging
import time
from collections.abc import Callable

from pydantic_settings import BaseSettings, SettingsConfigDict
from temporalio.client import Client

from firebreak.models.orchestration import AlertAdded
from firebreak.workflows.client import connect, signal_alert_added

log = logging.getLogger(__name__)


class SignalSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FIREBREAK_", env_file=".env", extra="ignore")

    #: Total seconds one ``signal`` call (connect plus signal) may take.
    temporal_signal_timeout_s: float = 5.0
    #: After a failed connect, signals fail fast for this many seconds, then connect is retried.
    temporal_connect_retry_s: float = 15.0


class IncidentSignaller:
    """Sends ``AlertAdded`` to the incident's workflow. Raises on failure (callers log it).

    Each ``signal`` call (connect and signal together) is bounded by ``temporal_signal_timeout_s``,
    however many webhooks run concurrently: they share one in-flight connect attempt and each
    waits on it with its own bound. A failed connect is remembered for
    ``temporal_connect_retry_s`` (signals fail fast), then the next call retries.
    """

    def __init__(
        self,
        settings: SignalSettings | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        settings = settings or SignalSettings()
        self._timeout = settings.temporal_signal_timeout_s
        self._retry_after = settings.temporal_connect_retry_s
        self._clock = clock
        self._client: Client | None = None
        self._connecting: asyncio.Task[Client] | None = None
        self._failed_until = 0.0

    async def _connect(self) -> Client:
        try:
            client = await asyncio.wait_for(connect(), timeout=self._timeout)
        except Exception:
            self._failed_until = self._clock() + self._retry_after
            raise
        finally:
            self._connecting = None
        self._client = client
        return client

    async def _get_client(self) -> Client:
        if self._client is not None:
            return self._client
        if self._clock() < self._failed_until:
            raise ConnectionError("Temporal connect failed recently; not retrying yet")
        if self._connecting is None:
            task = asyncio.ensure_future(self._connect())
            # Waiters see the failure; this keeps "exception never retrieved" quiet if none do.
            task.add_done_callback(lambda t: t.cancelled() or t.exception())
            self._connecting = task
        # shield: one waiter timing out must not cancel the attempt the others share
        return await asyncio.shield(self._connecting)

    async def _signal(self, incident_id: str, alert: AlertAdded) -> None:
        client = await self._get_client()
        await signal_alert_added(client, incident_id, alert)

    async def signal(self, incident_id: str, alert: AlertAdded) -> None:
        await asyncio.wait_for(self._signal(incident_id, alert), timeout=self._timeout)

    async def close(self) -> None:
        # temporalio's Client has no close(); dropping the reference releases the connection.
        self._client = None
        if self._connecting is not None:
            self._connecting.cancel()


_signaller: IncidentSignaller | None = None


def get_signaller() -> IncidentSignaller:
    """The process-wide signaller (no connection is made until the first signal)."""
    global _signaller
    if _signaller is None:
        _signaller = IncidentSignaller()
    return _signaller


async def close_signaller() -> None:
    global _signaller
    signaller, _signaller = _signaller, None
    if signaller is not None:
        await signaller.close()

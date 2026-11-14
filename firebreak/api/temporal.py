"""Signalling ``IncidentWorkflow`` from the API (T-066).

The Temporal client is created lazily and cached, so the API starts and accepts webhooks with
Temporal down. Connecting and each signal are bounded by ``asyncio.wait_for`` so a dead
Temporal cannot hold a webhook past Alertmanager's timeout.
"""

import asyncio
import logging

from pydantic_settings import BaseSettings, SettingsConfigDict
from temporalio.client import Client

from firebreak.models.orchestration import AlertAdded
from firebreak.workflows.client import connect, signal_alert_added

log = logging.getLogger(__name__)


class SignalSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FIREBREAK_", env_file=".env", extra="ignore")

    #: Seconds allowed for connecting to Temporal, and again for each signal.
    temporal_signal_timeout_s: float = 5.0


class IncidentSignaller:
    """Sends ``AlertAdded`` to the incident's workflow. Raises on failure (callers log it)."""

    def __init__(self, settings: SignalSettings | None = None) -> None:
        self._timeout = (settings or SignalSettings()).temporal_signal_timeout_s
        self._client: Client | None = None
        self._lock = asyncio.Lock()

    async def _get_client(self) -> Client:
        async with self._lock:
            if self._client is None:
                self._client = await asyncio.wait_for(connect(), timeout=self._timeout)
            return self._client

    async def signal(self, incident_id: str, alert: AlertAdded) -> None:
        client = await self._get_client()
        await asyncio.wait_for(
            signal_alert_added(client, incident_id, alert), timeout=self._timeout
        )

    async def close(self) -> None:
        # temporalio's Client has no close(); dropping the reference releases the connection.
        self._client = None


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

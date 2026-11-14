"""API-specific fixtures. DB fixtures (``migrated_url``, ``engine``, ``sessions``) are in the
root ``tests/conftest.py``."""

from collections.abc import Iterator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from firebreak.api.app import app
from firebreak.api.temporal import get_signaller
from firebreak.db.repositories import AlertRepository
from firebreak.models.orchestration import AlertAdded


@pytest.fixture
def alert_repo(sessions: async_sessionmaker[AsyncSession]) -> AlertRepository:
    return AlertRepository(sessions)


class RecordingSignaller:
    """Stands in for ``IncidentSignaller``: records calls, optionally raises."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, AlertAdded]] = []
        self.error: Exception | None = None

    async def signal(self, incident_id: str, alert: AlertAdded) -> None:
        self.calls.append((incident_id, alert))
        if self.error is not None:
            raise self.error


@pytest.fixture(autouse=True)
def signaller() -> Iterator[RecordingSignaller]:
    """Never let a webhook test reach a real Temporal (a live one may be running)."""
    fake = RecordingSignaller()
    app.dependency_overrides[get_signaller] = lambda: fake
    yield fake
    app.dependency_overrides.pop(get_signaller, None)

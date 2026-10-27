"""API-specific fixtures. DB fixtures (``migrated_url``, ``engine``, ``sessions``) are in the
root ``tests/conftest.py``."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from firebreak.api.deps import IngestAlertRepository


@pytest.fixture
def alert_repo(sessions: async_sessionmaker[AsyncSession]) -> IngestAlertRepository:
    return IngestAlertRepository(sessions)

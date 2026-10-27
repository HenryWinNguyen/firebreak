"""API-specific fixtures. DB fixtures (``migrated_url``, ``engine``, ``sessions``) are in the
root ``tests/conftest.py``."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from firebreak.db.repositories import AlertRepository


@pytest.fixture
def alert_repo(sessions: async_sessionmaker[AsyncSession]) -> AlertRepository:
    return AlertRepository(sessions)

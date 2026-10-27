"""M2-specific fixtures. Migration/truncation fixtures come from the root ``tests/conftest.py``."""

from collections.abc import AsyncIterator

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from firebreak.api.app import app
from firebreak.api.auth import get_settings
from firebreak.api.deps import _build_alert_repo, _build_grouper, get_engine
from firebreak.config import Settings


@pytest.fixture
def settings(migrated_url: str) -> Settings:
    """Settings injected into the real app: test database, otherwise the configured values."""
    return Settings(database_url=migrated_url)


@pytest.fixture
def auth(settings: Settings) -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.webhook_token.get_secret_value()}"}


@pytest.fixture
async def client(settings: Settings, engine: AsyncEngine) -> AsyncIterator[httpx.AsyncClient]:
    """The real app with real wiring (deps.py); ``engine`` truncates the tables first."""
    app.dependency_overrides[get_settings] = lambda: settings
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c
    app.dependency_overrides.clear()
    app_engine = get_engine(settings.database_url)
    get_engine.cache_clear()
    _build_alert_repo.cache_clear()
    _build_grouper.cache_clear()
    await app_engine.dispose()

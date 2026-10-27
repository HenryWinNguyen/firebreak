import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from firebreak.api.app import app
from firebreak.api.auth import get_settings
from firebreak.api.deps import _build_alert_repo, _build_grouper, get_engine
from firebreak.config import Settings

DB_URL = os.environ.get("FIREBREAK_TEST_DATABASE_URL")
ALEMBIC_INI = Path(__file__).resolve().parents[3] / "firebreak" / "db" / "alembic.ini"
MIGRATIONS = ALEMBIC_INI.parent / "migrations"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    skip = pytest.mark.skip(reason="FIREBREAK_TEST_DATABASE_URL is not set")
    for item in items:
        if "db" in item.keywords and not DB_URL:
            item.add_marker(skip)


def _upgrade(sync_conn: Connection) -> None:
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("script_location", str(MIGRATIONS))
    cfg.attributes["connection"] = sync_conn
    command.upgrade(cfg, "head")


async def _migrate(url: str) -> None:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
            await conn.run_sync(_upgrade)
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def migrated_url() -> Iterator[str]:
    """Fresh schema built by running the real Alembic migrations once per session."""
    if not DB_URL:
        pytest.skip("FIREBREAK_TEST_DATABASE_URL is not set")
    asyncio.run(_migrate(DB_URL))
    yield DB_URL


@pytest.fixture
async def truncated(migrated_url: str) -> AsyncIterator[str]:
    eng = create_async_engine(migrated_url, poolclass=NullPool)
    async with eng.begin() as conn:
        await conn.execute(text("TRUNCATE alerts, incidents, incident_alerts CASCADE"))
    await eng.dispose()
    yield migrated_url


@pytest.fixture
async def client(truncated: str) -> AsyncIterator[httpx.AsyncClient]:
    """The real app with real wiring (deps.py), pointed at the test database via Settings."""
    app.dependency_overrides[get_settings] = lambda: Settings(database_url=truncated)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c
    app.dependency_overrides.clear()
    engine = get_engine(truncated)
    get_engine.cache_clear()
    _build_alert_repo.cache_clear()
    _build_grouper.cache_clear()
    await engine.dispose()

import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from firebreak.db.engine import make_session_factory

DB_URL = os.environ.get("FIREBREAK_TEST_DATABASE_URL")
ALEMBIC_INI = Path(__file__).resolve().parents[2] / "firebreak" / "db" / "alembic.ini"
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
async def engine(migrated_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(migrated_url, poolclass=NullPool)
    async with eng.begin() as conn:
        await conn.execute(
            text("TRUNCATE alerts, incidents, incident_alerts, evidence, evidence_blobs CASCADE")
        )
    yield eng
    await eng.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return make_session_factory(engine)

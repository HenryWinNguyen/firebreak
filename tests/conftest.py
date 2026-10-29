import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from firebreak.db.engine import make_session_factory
from firebreak.db.tables import Base

DB_URL = os.environ.get("FIREBREAK_TEST_DATABASE_URL")
ALEMBIC_INI = Path(__file__).resolve().parents[1] / "firebreak" / "db" / "alembic.ini"
MIGRATIONS = ALEMBIC_INI.parent / "migrations"


SKIP_REASON = (
    "FIREBREAK_TEST_DATABASE_URL is not set; export e.g. "
    "FIREBREAK_TEST_DATABASE_URL=postgresql+asyncpg://firebreak:firebreak@localhost:55432/firebreak_test"
)


def _require_test_database(url: str) -> None:
    """Hard error unless the database name ends in ``_test`` (the fixture runs DROP SCHEMA).

    A ``database``/``dbname`` query parameter would override the path name at connect time,
    so it is refused outright; ``_migrate`` also re-checks ``current_database()`` after connecting.
    """
    parsed = make_url(url)
    if {"database", "dbname"} & {k.lower() for k in parsed.query}:
        raise RuntimeError(
            "refusing to run: FIREBREAK_TEST_DATABASE_URL must not set the database in its "
            "query string; put it in the path"
        )
    name = parsed.database or ""
    if not name.endswith("_test"):
        raise RuntimeError(
            f"refusing to run: FIREBREAK_TEST_DATABASE_URL database {name!r} must end in "
            "'_test' because the test fixtures DROP SCHEMA"
        )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    skip = pytest.mark.skip(reason=SKIP_REASON)
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
            actual = (await conn.execute(text("SELECT current_database()"))).scalar_one()
            if not str(actual).endswith("_test"):
                raise RuntimeError(f"refusing to DROP SCHEMA: connected to {actual!r}, not *_test")
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
            await conn.run_sync(_upgrade)
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def migrated_url() -> Iterator[str]:
    """Fresh schema built by running the real Alembic migrations once per session."""
    if not DB_URL:
        pytest.skip(SKIP_REASON)
    _require_test_database(DB_URL)
    asyncio.run(_migrate(DB_URL))
    yield DB_URL


@pytest.fixture
async def engine(migrated_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(migrated_url, poolclass=NullPool)
    async with eng.begin() as conn:
        await conn.execute(
            text("TRUNCATE " + ", ".join(t.name for t in Base.metadata.sorted_tables) + " CASCADE")
        )
    yield eng
    await eng.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return make_session_factory(engine)

"""Async engine and session factory built from ``Settings.database_url``."""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from firebreak.config import Settings


def make_engine(database_url: str | None = None) -> AsyncEngine:
    url = database_url if database_url is not None else Settings().database_url
    return create_async_engine(url, pool_pre_ping=True)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)

"""Alembic environment (async).

Callers may pass a live sync connection via ``config.attributes["connection"]`` (used by
tests); otherwise the URL is taken from ``Settings.database_url``.
"""

import asyncio

from alembic import context
from sqlalchemy.engine import Connection

from firebreak.db.engine import make_engine
from firebreak.db.tables import Base

config = context.config
target_metadata = Base.metadata


def _run(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def _run_async() -> None:
    engine = make_engine()
    try:
        async with engine.connect() as connection:
            await connection.run_sync(_run)
            await connection.commit()
    finally:
        await engine.dispose()


def run_migrations_online() -> None:
    connection = config.attributes.get("connection")
    if connection is not None:
        _run(connection)
    else:
        asyncio.run(_run_async())


if context.is_offline_mode():
    raise RuntimeError("offline migrations are not supported; run against a database")
run_migrations_online()

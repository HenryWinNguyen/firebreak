"""FastAPI dependencies for ingest. Tests override these via ``dependency_overrides``."""

from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncEngine

from firebreak.api.auth import get_settings
from firebreak.config import Settings
from firebreak.db.engine import make_engine, make_session_factory
from firebreak.db.repositories import AlertRepository
from firebreak.ingest.grouping import PostgresIncidentGrouper
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.models.incident import IncidentGrouper

# Kept for importers that still use the old name; the method now lives on AlertRepository.
IngestAlertRepository = AlertRepository


_engines: list[AsyncEngine] = []


@lru_cache
def get_engine(database_url: str) -> AsyncEngine:
    """One engine per database URL, shared by the repository and the grouper."""
    engine = make_engine(database_url)
    _engines.append(engine)
    return engine


async def dispose_engines() -> None:
    """Dispose every engine built by ``get_engine`` and drop the cached wiring (shutdown)."""
    engines = list(_engines)
    _engines.clear()
    get_engine.cache_clear()
    _build_alert_repo.cache_clear()
    _build_grouper.cache_clear()
    for engine in engines:
        await engine.dispose()


@lru_cache
def _build_alert_repo(database_url: str) -> AlertRepository:
    return AlertRepository(make_session_factory(get_engine(database_url)))


@lru_cache
def _build_grouper(database_url: str) -> PostgresIncidentGrouper:
    return PostgresIncidentGrouper(
        make_session_factory(get_engine(database_url)), ServiceGraph.load()
    )


def get_alert_repo(
    settings: Annotated[Settings, Depends(get_settings)],
) -> AlertRepository:
    return _build_alert_repo(settings.database_url)


def get_grouper(settings: Annotated[Settings, Depends(get_settings)]) -> IncidentGrouper:
    """The real Postgres grouper, built once from ``database_url`` and the service graph."""
    return _build_grouper(settings.database_url)

"""FastAPI dependencies for ingest. Tests override these via ``dependency_overrides``."""

from functools import lru_cache

from fastapi import HTTPException, status

from firebreak.db.engine import make_engine, make_session_factory
from firebreak.db.repositories import AlertRepository
from firebreak.models.incident import IncidentGrouper


@lru_cache
def get_alert_repo() -> AlertRepository:
    return AlertRepository(make_session_factory(make_engine()))


def get_grouper() -> IncidentGrouper:
    """The real grouper is wired in T-024; until then ingest is unavailable, loudly."""
    raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "incident grouper not configured")

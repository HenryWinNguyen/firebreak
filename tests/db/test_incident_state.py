import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from firebreak.db.repositories import IncidentNotFoundError, IncidentRepository
from firebreak.models.incident import IncidentState
from tests.db.test_repositories import make_incident

pytestmark = pytest.mark.db

Sessions = async_sessionmaker[AsyncSession]


async def test_set_state_updates_state_and_updated_at(sessions: Sessions) -> None:
    repo = IncidentRepository(sessions)
    inc = await repo.create(make_incident())
    await repo.set_state(inc.id, IncidentState.INVESTIGATING)
    got = await repo.get(inc.id)
    assert got is not None
    assert got.state == IncidentState.INVESTIGATING
    assert got.updated_at > inc.updated_at
    # idempotent
    await repo.set_state(inc.id, IncidentState.INVESTIGATING)
    again = await repo.get(inc.id)
    assert again is not None and again.state == IncidentState.INVESTIGATING


async def test_set_state_unknown_incident_raises(sessions: Sessions) -> None:
    with pytest.raises(IncidentNotFoundError):
        await IncidentRepository(sessions).set_state("inc_missing", IncidentState.TRIAGE)

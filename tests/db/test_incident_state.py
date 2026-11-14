import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from firebreak.db.repositories import IncidentNotFoundError, IncidentRepository
from firebreak.models.common import Severity, utcnow
from firebreak.models.incident import Incident, IncidentState

pytestmark = pytest.mark.db

Sessions = async_sessionmaker[AsyncSession]


def make_incident() -> Incident:
    now = utcnow()
    return Incident(
        severity=Severity.CRITICAL,
        cluster="checkout",
        services=["orders"],
        alert_ids=[],
        opened_at=now,
        window_closes_at=now,
    )


async def test_set_state_updates_state_and_updated_at(sessions: Sessions) -> None:
    repo = IncidentRepository(sessions)
    inc = await repo.create(make_incident())
    await repo.set_state(inc.id, IncidentState.INVESTIGATING)
    got = await repo.get(inc.id)
    assert got is not None
    assert got.state is IncidentState.INVESTIGATING
    assert got.updated_at > inc.updated_at
    await repo.set_state(inc.id, IncidentState.REPORT_ONLY)  # idempotent / absolute
    await repo.set_state(inc.id, IncidentState.REPORT_ONLY)
    got = await repo.get(inc.id)
    assert got is not None and got.state is IncidentState.REPORT_ONLY


async def test_set_state_unknown_incident(sessions: Sessions) -> None:
    with pytest.raises(IncidentNotFoundError):
        await IncidentRepository(sessions).set_state("inc_missing", IncidentState.TRIAGE)

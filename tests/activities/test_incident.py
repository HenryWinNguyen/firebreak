import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from firebreak.activities.incident import IncidentActivities
from firebreak.db.repositories import AlertRepository, IncidentRepository
from firebreak.models.incident import IncidentState
from firebreak.models.orchestration import StateUpdate
from tests.db.test_repositories import make_alert, make_incident

pytestmark = pytest.mark.db

Sessions = async_sessionmaker[AsyncSession]


async def test_load_context_builds_summary_from_incident_and_alerts(sessions: Sessions) -> None:
    alert = make_alert(summary="queue backing up")
    await AlertRepository(sessions).insert_if_new(alert)
    inc = await IncidentRepository(sessions).create(make_incident(alert_ids=[alert.id]))
    acts = IncidentActivities(sessions)
    ctx = await ActivityEnvironment().run(acts.load_context, inc.id)
    assert ctx.incident_id == inc.id
    assert ctx.services == ["orders"]
    assert "queue backing up" in ctx.summary
    assert "untrusted" in ctx.summary.lower()  # alert text is wrapped by build_task
    assert ctx.triage is None and ctx.reports == []


async def test_load_context_unknown_incident_is_non_retryable(sessions: Sessions) -> None:
    acts = IncidentActivities(sessions)
    with pytest.raises(ApplicationError) as exc:
        await ActivityEnvironment().run(acts.load_context, "inc_missing")
    assert exc.value.non_retryable


async def test_set_incident_state_persists(sessions: Sessions) -> None:
    inc = await IncidentRepository(sessions).create(make_incident())
    acts = IncidentActivities(sessions)
    await ActivityEnvironment().run(
        acts.set_incident_state, StateUpdate(incident_id=inc.id, state=IncidentState.PLANNING)
    )
    got = await IncidentRepository(sessions).get(inc.id)
    assert got is not None and got.state == IncidentState.PLANNING


async def test_set_incident_state_unknown_incident_is_non_retryable(sessions: Sessions) -> None:
    acts = IncidentActivities(sessions)
    with pytest.raises(ApplicationError) as exc:
        await ActivityEnvironment().run(
            acts.set_incident_state,
            StateUpdate(incident_id="inc_missing", state=IncidentState.TRIAGE),
        )
    assert exc.value.non_retryable

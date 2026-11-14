from datetime import datetime

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from firebreak.activities.incident import IncidentActivities
from firebreak.db.repositories import AlertRepository, IncidentRepository
from firebreak.models.alert import Alert
from firebreak.models.incident import Incident, IncidentState
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


async def test_load_context_carries_aware_window_closes_at(sessions: Sessions) -> None:
    inc = await IncidentRepository(sessions).create(make_incident())
    ctx = await ActivityEnvironment().run(IncidentActivities(sessions).load_context, inc.id)
    assert ctx.window_closes_at == inc.window_closes_at
    assert ctx.window_closes_at is not None and ctx.window_closes_at.tzinfo is not None


async def test_load_context_naive_window_fails_validation(sessions: Sessions) -> None:
    naive = make_incident().model_copy(update={"window_closes_at": datetime(2026, 1, 1, 12, 0)})

    class NaiveRepo:
        async def get(self, incident_id: str) -> Incident:
            return naive

        async def list_alerts(self, incident_id: str) -> list[Alert]:
            return []

    acts = IncidentActivities(sessions)
    acts._repo = NaiveRepo()  # type: ignore[assignment]
    with pytest.raises(ValidationError):
        await ActivityEnvironment().run(acts.load_context, naive.id)

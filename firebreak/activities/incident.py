"""Incident activities: ``load_context`` and ``set_incident_state`` (T-064).

Both only touch Postgres. They are idempotent, so Temporal can retry them freely:
``load_context`` is a read and ``set_incident_state`` is "set to X". An unknown incident can
never succeed on retry, so ``IncidentNotFoundError`` becomes a non-retryable ``ApplicationError``.
"""

from temporalio import activity
from temporalio.exceptions import ApplicationError

from firebreak.agents.baseline import build_task
from firebreak.db.repositories import IncidentNotFoundError, IncidentRepository, SessionFactory
from firebreak.models.orchestration import (
    ACTIVITY_LOAD_CONTEXT,
    ACTIVITY_SET_INCIDENT_STATE,
    IncidentContext,
    StateUpdate,
)

INCIDENT_NOT_FOUND = "IncidentNotFound"


def _not_found(e: IncidentNotFoundError) -> ApplicationError:
    return ApplicationError(str(e), type=INCIDENT_NOT_FOUND, non_retryable=True)


class IncidentActivities:
    def __init__(self, sessions: SessionFactory) -> None:
        self._repo = IncidentRepository(sessions)

    @activity.defn(name=ACTIVITY_LOAD_CONTEXT)
    async def load_context(self, incident_id: str) -> IncidentContext:
        try:
            incident = await self._repo.get(incident_id)
            if incident is None:
                raise IncidentNotFoundError(incident_id)
            alerts = await self._repo.list_alerts(incident_id)
        except IncidentNotFoundError as e:
            raise _not_found(e) from e
        return IncidentContext(
            incident_id=incident.id,
            summary=build_task(incident, alerts),
            services=list(incident.services),
            # AwareDatetime: a naive value fails validation loudly (workflow fails visibly).
            window_closes_at=incident.window_closes_at,
        )

    @activity.defn(name=ACTIVITY_SET_INCIDENT_STATE)
    async def set_incident_state(self, update: StateUpdate) -> None:
        try:
            await self._repo.set_state(update.incident_id, update.state)
        except IncidentNotFoundError as e:
            raise _not_found(e) from e

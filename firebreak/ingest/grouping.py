"""Postgres-backed incident grouper (ARCH §3).

An alert joins the open incident of its dependency cluster if that incident's
window has not closed; otherwise it opens a new incident. The whole decision
runs in one transaction behind a per-cluster ``pg_advisory_xact_lock`` so two
alerts arriving together for one cluster produce a single incident. All reads and
writes go through the session-taking ``*_in`` repository methods.
"""

import logging
from datetime import timedelta

from sqlalchemy import select, text

from firebreak.db.repositories import AlertRepository, IncidentRepository, SessionFactory
from firebreak.db.tables import IncidentAlertRow
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.models.alert import Alert, AlertStatus
from firebreak.models.common import Severity, utcnow
from firebreak.models.incident import GroupingDecision, Incident, IncidentState

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_SECONDS = 60.0

_SEVERITY_RANK = {Severity.INFO: 0, Severity.WARNING: 1, Severity.CRITICAL: 2}


class PostgresIncidentGrouper:
    """Implements ``firebreak.models.incident.IncidentGrouper``."""

    def __init__(
        self,
        sessions: SessionFactory,
        graph: ServiceGraph,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
    ) -> None:
        self._sessions = sessions
        self._graph = graph
        self._window = timedelta(seconds=window_seconds)

    async def assign(self, alert: Alert) -> GroupingDecision:
        cluster = self._graph.cluster_of(alert.service)
        resolved = alert.status is AlertStatus.RESOLVED
        async with self._sessions() as session, session.begin():
            # Serialise all grouping decisions for this cluster until commit.
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:cluster))"), {"cluster": cluster}
            )
            # Take "now" after the lock so a waiter sees the winner's window.
            now = utcnow()

            alert_id = await AlertRepository.insert_if_new_in(session, alert)

            # The one query kept here: no repository method answers "is this alert linked".
            existing = (
                await session.execute(
                    select(IncidentAlertRow.incident_id).where(
                        IncidentAlertRow.alert_id == alert_id
                    )
                )
            ).scalar_one_or_none()
            if existing is not None:
                return GroupingDecision(incident_id=existing, created=False, duplicate=True)

            open_inc = await IncidentRepository.find_open_for_cluster_in(
                session, cluster, now, for_update=True
            )

            if open_inc is None:
                if resolved:
                    # A resolved alert never opens an incident; it stays stored, unlinked.
                    logger.info("resolved alert %s: no open incident in %s", alert_id, cluster)
                    return GroupingDecision(
                        incident_id=None, created=False, duplicate=alert_id != alert.id
                    )
                critical = alert.severity is Severity.CRITICAL
                incident = await IncidentRepository.create_in(
                    session,
                    Incident(
                        state=IncidentState.GROUPING,
                        severity=alert.severity,
                        cluster=cluster,
                        services=[alert.service],
                        alert_ids=[alert_id],
                        opened_at=now,
                        window_closes_at=now if critical else now + self._window,
                        updated_at=now,
                    ),
                )
                incident_id, created = incident.id, True
            else:
                incident_id, created = open_inc.id, False
                # attach_alert_in links the alert and appends the service atomically.
                await IncidentRepository.attach_alert_in(
                    session, incident_id, alert_id, alert.service
                )
                if not resolved:
                    # The row is locked FOR UPDATE since the read above, so its services plus
                    # the new one is the complete list: no concurrent attach can be lost.
                    window = open_inc.window_closes_at
                    if alert.severity is Severity.CRITICAL:
                        # Investigate right away: close the window now.
                        window = min(window, now)
                    await IncidentRepository.update_rollup_in(
                        session,
                        incident_id,
                        severity=max(
                            open_inc.severity, alert.severity, key=_SEVERITY_RANK.__getitem__
                        ),
                        window_closes_at=window,
                        services=[*open_inc.services, alert.service],
                    )
            logger.info(
                "alert %s -> incident %s (cluster=%s created=%s)",
                alert_id,
                incident_id,
                cluster,
                created,
            )
            return GroupingDecision(incident_id=incident_id, created=created, duplicate=False)

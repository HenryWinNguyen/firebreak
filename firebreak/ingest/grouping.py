"""Postgres-backed incident grouper (ARCH §3).

An alert joins the open incident of its dependency cluster if that incident's
window has not closed; otherwise it opens a new incident. The whole decision
runs in one transaction behind a per-cluster ``pg_advisory_xact_lock`` so two
alerts arriving together for one cluster produce a single incident.
"""

import logging
from datetime import timedelta

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from firebreak.db.repositories import SessionFactory
from firebreak.db.tables import AlertRow, IncidentAlertRow, IncidentRow
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.models.alert import Alert
from firebreak.models.common import Severity, new_id, utcnow
from firebreak.models.incident import TERMINAL_STATES, GroupingDecision, IncidentState

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
        async with self._sessions() as session, session.begin():
            # Serialise all grouping decisions for this cluster until commit.
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:cluster))"), {"cluster": cluster}
            )
            # Take "now" after the lock so a waiter sees the winner's window.
            now = utcnow()

            alert_id = await self._ensure_alert(session, alert)

            existing = (
                await session.execute(
                    select(IncidentAlertRow.incident_id).where(
                        IncidentAlertRow.alert_id == alert_id
                    )
                )
            ).scalar_one_or_none()
            if existing is not None:
                return GroupingDecision(incident_id=existing, created=False, duplicate=True)

            open_inc = (
                await session.execute(
                    select(IncidentRow)
                    .where(
                        IncidentRow.cluster == cluster,
                        IncidentRow.window_closes_at > now,
                        IncidentRow.state.not_in([s.value for s in TERMINAL_STATES]),
                    )
                    .order_by(IncidentRow.opened_at.desc(), IncidentRow.id.desc())
                    .limit(1)
                    .with_for_update()
                )
            ).scalar_one_or_none()

            critical = alert.severity is Severity.CRITICAL
            if open_inc is None:
                incident_id = new_id("inc")
                session.add(
                    IncidentRow(
                        id=incident_id,
                        state=IncidentState.GROUPING.value,
                        severity=alert.severity.value,
                        cluster=cluster,
                        services=[alert.service],
                        opened_at=now,
                        window_closes_at=now if critical else now + self._window,
                        updated_at=now,
                    )
                )
                await session.flush()
                created = True
            else:
                incident_id = open_inc.id
                severity = max(
                    Severity(open_inc.severity), alert.severity, key=_SEVERITY_RANK.__getitem__
                )
                services = list(open_inc.services)
                if alert.service not in services:
                    services.append(alert.service)
                values: dict[str, object] = {
                    "severity": severity.value,
                    "services": services,
                    "updated_at": now,
                }
                if critical:
                    # Investigate right away: close the window now.
                    values["window_closes_at"] = min(open_inc.window_closes_at, now)
                await session.execute(
                    update(IncidentRow).where(IncidentRow.id == incident_id).values(**values)
                )
                created = False

            session.add(
                IncidentAlertRow(incident_id=incident_id, alert_id=alert_id, service=alert.service)
            )
            logger.info(
                "alert %s -> incident %s (cluster=%s created=%s)",
                alert_id,
                incident_id,
                cluster,
                created,
            )
            return GroupingDecision(incident_id=incident_id, created=created, duplicate=False)

    @staticmethod
    async def _ensure_alert(session: AsyncSession, alert: Alert) -> str:
        """Insert the alert if new; return the stored alert's id either way."""
        await session.execute(
            pg_insert(AlertRow)
            .values(
                id=alert.id,
                idempotency_key=alert.idempotency_key,
                source=alert.source.value,
                fingerprint=alert.fingerprint,
                alertname=alert.alertname,
                service=alert.service,
                severity=alert.severity.value,
                status=alert.status.value,
                summary=alert.summary,
                labels=alert.labels,
                started_at=alert.started_at,
                ended_at=alert.ended_at,
            )
            .on_conflict_do_nothing(index_elements=[AlertRow.idempotency_key])
        )
        stored: str = (
            await session.execute(
                select(AlertRow.id).where(AlertRow.idempotency_key == alert.idempotency_key)
            )
        ).scalar_one()
        return stored

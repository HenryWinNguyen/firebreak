"""Repositories used by ingest, grouping and the tool gateway.

Each method runs in its own transaction on a session from the injected factory.
"""

from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from firebreak.db.tables import (
    AlertRow,
    EvidenceBlobRow,
    EvidenceRow,
    IncidentAlertRow,
    IncidentRow,
)
from firebreak.models.alert import Alert
from firebreak.models.common import Severity, utcnow
from firebreak.models.evidence import Evidence, EvidenceKind
from firebreak.models.incident import TERMINAL_STATES, Incident, IncidentState

SessionFactory = async_sessionmaker[AsyncSession]


class PersistenceError(Exception):
    """Base class for typed persistence errors."""


class IncidentNotFoundError(PersistenceError):
    def __init__(self, incident_id: str) -> None:
        super().__init__(f"incident not found: {incident_id}")
        self.incident_id = incident_id


class AlertNotFoundError(PersistenceError):
    def __init__(self, alert_ids: list[str]) -> None:
        super().__init__(f"alerts not found: {alert_ids}")
        self.alert_ids = alert_ids


class AlertRepository:
    def __init__(self, sessions: SessionFactory) -> None:
        self._sessions = sessions

    async def insert_if_new(self, alert: Alert) -> bool:
        """Insert the alert. Return False if its idempotency key already exists.

        Race-safe: relies on ``ON CONFLICT DO NOTHING`` against the unique key.
        """
        stmt = (
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
            .returning(AlertRow.id)
        )
        async with self._sessions() as session, session.begin():
            return (await session.execute(stmt)).first() is not None


class IncidentRepository:
    def __init__(self, sessions: SessionFactory) -> None:
        self._sessions = sessions

    async def find_open_for_cluster(self, cluster: str, now: datetime) -> Incident | None:
        """Newest non-terminal incident in ``cluster`` whose window is still open.

        The window is open while ``now < window_closes_at``; at exactly
        ``window_closes_at`` it is closed.
        """
        terminal = [s.value for s in TERMINAL_STATES]
        stmt = (
            select(IncidentRow)
            .where(
                IncidentRow.cluster == cluster,
                IncidentRow.window_closes_at > now,
                IncidentRow.state.not_in(terminal),
            )
            .order_by(IncidentRow.opened_at.desc(), IncidentRow.id.desc())
            .limit(1)
        )
        async with self._sessions() as session:
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                return None
            return await self._to_model(session, row)

    async def create(self, incident: Incident) -> Incident:
        """Insert the incident and link ``incident.alert_ids`` (alerts must already exist)."""
        async with self._sessions() as session, session.begin():
            session.add(
                IncidentRow(
                    id=incident.id,
                    state=incident.state.value,
                    severity=incident.severity.value,
                    cluster=incident.cluster,
                    services=list(incident.services),
                    opened_at=incident.opened_at,
                    window_closes_at=incident.window_closes_at,
                    updated_at=incident.updated_at,
                )
            )
            await session.flush()
            if incident.alert_ids:
                found = {
                    r.id: r.service
                    for r in (
                        await session.execute(
                            select(AlertRow.id, AlertRow.service).where(
                                AlertRow.id.in_(incident.alert_ids)
                            )
                        )
                    ).all()
                }
                missing = [a for a in incident.alert_ids if a not in found]
                if missing:
                    raise AlertNotFoundError(missing)
                session.add_all(
                    IncidentAlertRow(incident_id=incident.id, alert_id=a, service=found[a])
                    for a in dict.fromkeys(incident.alert_ids)
                )
        return incident

    async def attach_alert(self, incident_id: str, alert_id: str, service: str) -> None:
        """Link an alert to an incident (idempotent) and add ``service`` to its services."""
        async with self._sessions() as session, session.begin():
            # The UPDATE takes the row lock, which serialises concurrent attaches.
            updated = await session.execute(
                update(IncidentRow)
                .where(IncidentRow.id == incident_id)
                .values(
                    services=func.array_append(IncidentRow.services, service),
                    updated_at=utcnow(),
                )
                .where(~IncidentRow.services.contains([service]))
                .returning(IncidentRow.id)
            )
            if updated.first() is None:
                # Either the incident is missing or the service is already listed.
                touched = await session.execute(
                    update(IncidentRow)
                    .where(IncidentRow.id == incident_id)
                    .values(updated_at=utcnow())
                    .returning(IncidentRow.id)
                )
                if touched.first() is None:
                    raise IncidentNotFoundError(incident_id)
            await session.execute(
                pg_insert(IncidentAlertRow)
                .values(incident_id=incident_id, alert_id=alert_id, service=service)
                .on_conflict_do_nothing()
            )

    async def get(self, incident_id: str) -> Incident | None:
        async with self._sessions() as session:
            row = await session.get(IncidentRow, incident_id)
            if row is None:
                return None
            return await self._to_model(session, row)

    @staticmethod
    async def _to_model(session: AsyncSession, row: IncidentRow) -> Incident:
        alert_ids = (
            (
                await session.execute(
                    select(IncidentAlertRow.alert_id)
                    .where(IncidentAlertRow.incident_id == row.id)
                    .order_by(IncidentAlertRow.attached_at, IncidentAlertRow.alert_id)
                )
            )
            .scalars()
            .all()
        )
        return Incident(
            id=row.id,
            state=IncidentState(row.state),
            severity=Severity(row.severity),
            cluster=row.cluster,
            services=list(row.services),
            alert_ids=list(alert_ids),
            opened_at=row.opened_at,
            window_closes_at=row.window_closes_at,
            updated_at=row.updated_at,
        )


class PostgresEvidenceSink:
    """Implements ``firebreak.models.evidence.EvidenceSink`` on Postgres."""

    def __init__(self, sessions: SessionFactory) -> None:
        self._sessions = sessions

    async def record(self, evidence: Evidence, raw: str) -> Evidence:
        """Store the raw payload under ``evidence.raw_ref`` and the evidence row.

        Idempotent per evidence id and raw_ref (retries neither duplicate nor overwrite).
        """
        async with self._sessions() as session, session.begin():
            await session.execute(
                pg_insert(EvidenceBlobRow)
                .values(key=evidence.raw_ref, content=raw)
                .on_conflict_do_nothing()
            )
            await session.execute(
                pg_insert(EvidenceRow)
                .values(
                    id=evidence.id,
                    incident_id=evidence.incident_id,
                    tool_call_id=evidence.tool_call_id,
                    kind=evidence.kind.value,
                    query=evidence.query,
                    summary=evidence.summary,
                    raw_ref=evidence.raw_ref,
                    untrusted=evidence.untrusted,
                )
                .on_conflict_do_nothing(index_elements=[EvidenceRow.id])
            )
        return evidence

    async def exists(self, incident_id: str, evidence_ids: list[str]) -> set[str]:
        if not evidence_ids:
            return set()
        stmt = select(EvidenceRow.id).where(
            EvidenceRow.incident_id == incident_id, EvidenceRow.id.in_(evidence_ids)
        )
        async with self._sessions() as session:
            return set((await session.execute(stmt)).scalars().all())

    async def get_raw(self, raw_ref: str) -> str | None:
        async with self._sessions() as session:
            blob = await session.get(EvidenceBlobRow, raw_ref)
            return None if blob is None else blob.content

    async def get(self, evidence_id: str) -> Evidence | None:
        async with self._sessions() as session:
            row = await session.get(EvidenceRow, evidence_id)
            if row is None:
                return None
            return Evidence(
                id=row.id,
                incident_id=row.incident_id,
                tool_call_id=row.tool_call_id,
                kind=EvidenceKind(row.kind),
                query=row.query,
                summary=row.summary,
                raw_ref=row.raw_ref,
                untrusted=row.untrusted,
            )

"""Repositories used by ingest, grouping and the tool gateway.

Each public method runs in its own transaction on a session from the injected factory.
The ``*_in`` variants take a caller-owned session instead, for callers (the grouper) that
need several operations in one transaction; they never commit. Each query exists once, in
the ``*_in`` method, and the plain method is a thin wrapper around it.

Datetimes passed to any method must be timezone-aware (``ValueError`` otherwise).
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
from firebreak.models.alert import Alert, AlertStatus
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


class EvidenceConflictError(PersistenceError):
    """A retry reused an evidence id with a different incident or different raw payload."""

    def __init__(self, evidence_id: str, reason: str) -> None:
        super().__init__(f"evidence {evidence_id} conflicts with the stored record: {reason}")
        self.evidence_id = evidence_id


def _require_aware(**values: datetime | None) -> None:
    for name, value in values.items():
        if value is not None and value.utcoffset() is None:
            raise ValueError(f"{name} must be a timezone-aware datetime")


class AlertRepository:
    def __init__(self, sessions: SessionFactory) -> None:
        self._sessions = sessions

    async def insert_if_new(self, alert: Alert) -> bool:
        """Insert the alert. Return False if its idempotency key already exists."""
        async with self._sessions() as session, session.begin():
            return (await self.insert_or_get(alert))[1]

    async def insert_or_get(self, alert: Alert) -> tuple[str, bool]:
        """Insert if the key is new. Return ``(stored_id, inserted)`` in one transaction."""
        async with self._sessions() as session, session.begin():
            stored_id = await self.insert_if_new_in(session, alert)
        return stored_id, stored_id == alert.id

    @staticmethod
    async def get_status_in(session: AsyncSession, alert_id: str) -> AlertStatus | None:
        """Stored status of an alert, or None if it does not exist."""
        value = (
            await session.execute(select(AlertRow.status).where(AlertRow.id == alert_id))
        ).scalar_one_or_none()
        return None if value is None else AlertStatus(value)

    @staticmethod
    async def insert_if_new_in(session: AsyncSession, alert: Alert) -> str:
        """Insert the alert if its idempotency key is new; return the stored id either way.

        Race-safe: relies on ``ON CONFLICT DO NOTHING`` against the unique key. The returned
        id equals ``alert.id`` when this call inserted the row.
        """
        _require_aware(started_at=alert.started_at, ended_at=alert.ended_at)
        inserted = (
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
                .returning(AlertRow.id)
            )
        ).scalar_one_or_none()
        if inserted is not None:
            return inserted
        stored: str = (
            await session.execute(
                select(AlertRow.id).where(AlertRow.idempotency_key == alert.idempotency_key)
            )
        ).scalar_one()
        return stored

    async def mark_resolved(self, idempotency_key: str, ended_at: datetime) -> bool:
        async with self._sessions() as session, session.begin():
            return await self.mark_resolved_in(session, idempotency_key, ended_at)

    @staticmethod
    async def mark_resolved_in(
        session: AsyncSession, idempotency_key: str, ended_at: datetime
    ) -> bool:
        """Set ``status=resolved`` and ``ended_at`` on a stored firing alert.

        Returns False if no alert has this key. Idempotent: an already-resolved alert keeps
        its original ``ended_at`` and the call still returns True.
        """
        _require_aware(ended_at=ended_at)
        updated = await session.execute(
            update(AlertRow)
            .where(
                AlertRow.idempotency_key == idempotency_key,
                AlertRow.status != AlertStatus.RESOLVED.value,
            )
            .values(status=AlertStatus.RESOLVED.value, ended_at=ended_at)
            .returning(AlertRow.id)
        )
        if updated.first() is not None:
            return True
        exists = await session.execute(
            select(AlertRow.id).where(AlertRow.idempotency_key == idempotency_key)
        )
        return exists.first() is not None


class IncidentRepository:
    def __init__(self, sessions: SessionFactory) -> None:
        self._sessions = sessions

    async def find_open_for_cluster(self, cluster: str, now: datetime) -> Incident | None:
        """Newest non-terminal incident in ``cluster`` whose window is still open."""
        async with self._sessions() as session:
            return await self.find_open_for_cluster_in(session, cluster, now, for_update=False)

    @classmethod
    async def find_open_for_cluster_in(
        cls, session: AsyncSession, cluster: str, now: datetime, *, for_update: bool
    ) -> Incident | None:
        """Newest non-terminal incident in ``cluster`` whose window is still open.

        The window is open while ``now < window_closes_at``; at exactly
        ``window_closes_at`` it is closed. ``for_update`` takes ``FOR UPDATE`` on the row.
        """
        _require_aware(now=now)
        stmt = (
            select(IncidentRow)
            .where(
                IncidentRow.cluster == cluster,
                IncidentRow.window_closes_at > now,
                IncidentRow.state.not_in([s.value for s in TERMINAL_STATES]),
            )
            .order_by(IncidentRow.opened_at.desc(), IncidentRow.id.desc())
            .limit(1)
        )
        if for_update:
            stmt = stmt.with_for_update()
        row = (await session.execute(stmt)).scalar_one_or_none()
        if row is None:
            return None
        return await cls._to_model(session, row)

    @staticmethod
    async def find_incident_for_alert_in(session: AsyncSession, alert_id: str) -> str | None:
        """Id of the incident this alert is linked to, or None if it is unlinked."""
        return (
            await session.execute(
                select(IncidentAlertRow.incident_id).where(IncidentAlertRow.alert_id == alert_id)
            )
        ).scalar_one_or_none()

    async def create(self, incident: Incident) -> Incident:
        """Insert the incident and link ``incident.alert_ids`` (alerts must already exist)."""
        async with self._sessions() as session, session.begin():
            return await self.create_in(session, incident)

    @staticmethod
    async def create_in(session: AsyncSession, incident: Incident) -> Incident:
        """Insert the incident and link its alerts; ``AlertNotFoundError`` if any is missing."""
        _require_aware(
            opened_at=incident.opened_at,
            window_closes_at=incident.window_closes_at,
            updated_at=incident.updated_at,
        )
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
            await session.flush()
        return incident

    async def attach_alert(self, incident_id: str, alert_id: str, service: str) -> None:
        """Link an alert to an incident (idempotent) and add ``service`` to its services."""
        async with self._sessions() as session, session.begin():
            await self.attach_alert_in(session, incident_id, alert_id, service)

    @staticmethod
    async def attach_alert_in(
        session: AsyncSession, incident_id: str, alert_id: str, service: str
    ) -> None:
        """Raises ``IncidentNotFoundError`` / ``AlertNotFoundError`` for unknown ids."""
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
        alert_exists = await session.execute(select(AlertRow.id).where(AlertRow.id == alert_id))
        if alert_exists.first() is None:
            # Checked before the link insert so the caller's transaction is not poisoned by an
            # IntegrityError. The caller decides whether to roll back.
            raise AlertNotFoundError([alert_id])
        await session.execute(
            pg_insert(IncidentAlertRow)
            .values(incident_id=incident_id, alert_id=alert_id, service=service)
            .on_conflict_do_nothing()
        )

    @staticmethod
    async def update_rollup_in(
        session: AsyncSession,
        incident_id: str,
        *,
        severity: Severity,
        window_closes_at: datetime,
        services: list[str],
    ) -> None:
        """Overwrite severity, window close time and services; bump ``updated_at``.

        The caller computes the new values (typically after ``find_open_for_cluster_in(...,
        for_update=True)``). Raises ``IncidentNotFoundError`` if the incident is missing.
        """
        _require_aware(window_closes_at=window_closes_at)
        updated = await session.execute(
            update(IncidentRow)
            .where(IncidentRow.id == incident_id)
            .values(
                severity=severity.value,
                window_closes_at=window_closes_at,
                services=list(dict.fromkeys(services)),
                updated_at=utcnow(),
            )
            .returning(IncidentRow.id)
        )
        if updated.first() is None:
            raise IncidentNotFoundError(incident_id)

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

        Idempotent: an identical retry returns normally. Reusing an id (or raw_ref) with a
        different incident or raw payload raises ``EvidenceConflictError`` and stores nothing.
        """
        async with self._sessions() as session, session.begin():
            await session.execute(
                pg_insert(EvidenceBlobRow)
                .values(key=evidence.raw_ref, content=raw)
                .on_conflict_do_nothing()
            )
            inserted = await session.execute(
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
                .returning(EvidenceRow.id)
            )
            blob = await session.get(EvidenceBlobRow, evidence.raw_ref)
            if blob is None or blob.content != raw:
                raise EvidenceConflictError(
                    evidence.id, f"raw payload for {evidence.raw_ref!r} differs from the stored one"
                )
            if inserted.first() is None:
                existing = await session.get(EvidenceRow, evidence.id)
                if existing is None:
                    raise PersistenceError(f"evidence {evidence.id} vanished during record")
                if existing.incident_id != evidence.incident_id:
                    raise EvidenceConflictError(
                        evidence.id, f"already recorded for incident {existing.incident_id}"
                    )
                if existing.raw_ref != evidence.raw_ref:
                    raise EvidenceConflictError(
                        evidence.id, f"already recorded with raw_ref {existing.raw_ref!r}"
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

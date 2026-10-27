"""Ingest pipeline: dedupe, then hand new firing alerts to the grouper."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from firebreak.models.alert import Alert, AlertStatus
from firebreak.models.common import utcnow
from firebreak.models.incident import GroupingDecision, IncidentGrouper


class AlertStore(Protocol):
    async def insert_or_get(self, alert: Alert) -> tuple[str, bool]:
        """Insert the alert if its idempotency key is new. Return ``(stored_id, inserted)``."""
        ...

    async def mark_resolved(self, idempotency_key: str, ended_at: datetime) -> bool: ...


@dataclass(frozen=True)
class IngestResult:
    alert_id: str  # the STORED alert id (differs from ``alert.id`` for a replayed alert)
    decision: GroupingDecision


class ResolveMissingError(RuntimeError):
    """A resolved update found no stored alert after the insert reported a conflict."""


async def ingest(
    alerts: list[Alert], repo: AlertStore, grouper: IncidentGrouper
) -> list[IngestResult]:
    """Return one result per alert, in order.

    - Resolved alert: stored if new; if an alert with the same key is already stored (the
      normal case: it was stored as firing) it is updated via ``mark_resolved``. Never sent
      to the grouper, so ``incident_id=None``, ``created=False`` and ``duplicate=False``
      (a resolved notification is a state change, not a duplicate).
    - Firing alert: stored if new, then ALWAYS sent to the grouper, even when it was already
      stored. The grouper is idempotent, so replay recovers an alert that was inserted but
      never grouped (grouper error or crash mid-batch) and reports ``duplicate=True`` for
      one already linked.

    Exceptions from the repo or grouper propagate; replaying the webhook is the retry.
    """
    results: list[IngestResult] = []
    for alert in alerts:
        stored_id, inserted = await repo.insert_or_get(alert)
        if alert.status is AlertStatus.RESOLVED:
            if not inserted and not await repo.mark_resolved(
                alert.idempotency_key, alert.ended_at or utcnow()
            ):
                raise ResolveMissingError(
                    f"alert {alert.idempotency_key} vanished while being marked resolved"
                )
            decision = GroupingDecision(incident_id=None, created=False, duplicate=False)
        else:
            decision = await grouper.assign(alert)
        results.append(IngestResult(alert_id=stored_id, decision=decision))
    return results

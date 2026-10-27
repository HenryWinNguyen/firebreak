"""Ingest pipeline: dedupe, then hand new firing alerts to the grouper."""

from typing import Protocol

from firebreak.models.alert import Alert, AlertStatus
from firebreak.models.incident import GroupingDecision, IncidentGrouper


class AlertInserter(Protocol):
    async def insert_if_new(self, alert: Alert) -> bool: ...


async def ingest(
    alerts: list[Alert], repo: AlertInserter, grouper: IncidentGrouper
) -> list[GroupingDecision]:
    """Return one decision per alert, in order.

    - Resolved alert: stored if new, never sent to the grouper (``incident_id=None``).
    - Firing alert: stored if new, then ALWAYS sent to the grouper, even when it was already
      stored. The grouper is idempotent, so replay recovers an alert that was inserted but
      never grouped (grouper error or crash mid-batch) and reports ``duplicate=True`` for
      one already linked.

    Exceptions from the repo or grouper propagate; replaying the webhook is the retry.
    """
    decisions: list[GroupingDecision] = []
    for alert in alerts:
        is_new = await repo.insert_if_new(alert)
        if alert.status is AlertStatus.RESOLVED:
            decisions.append(
                GroupingDecision(incident_id=None, created=False, duplicate=not is_new)
            )
        else:
            decisions.append(await grouper.assign(alert))
    return decisions

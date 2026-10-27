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

    - Duplicate (``insert_if_new`` is False): ``duplicate=True``, not sent to the grouper.
    - New resolved alert: stored, but never opens an incident (not sent to the grouper).
    - New firing alert: stored first, then assigned by the grouper.

    ``incident_id`` is ``""`` when no incident is involved (duplicate or resolved).
    Exceptions from the repo or grouper propagate; alerts already inserted stay inserted
    and a replay will report them as duplicates.
    """
    decisions: list[GroupingDecision] = []
    for alert in alerts:
        if not await repo.insert_if_new(alert):
            decisions.append(GroupingDecision(incident_id="", created=False, duplicate=True))
        elif alert.status is AlertStatus.RESOLVED:
            decisions.append(GroupingDecision(incident_id="", created=False, duplicate=False))
        else:
            decisions.append(await grouper.assign(alert))
    return decisions

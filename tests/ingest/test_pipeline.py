from datetime import UTC, datetime

from firebreak.ingest.pipeline import ingest
from firebreak.models.alert import Alert, AlertSource, AlertStatus
from firebreak.models.common import Severity
from firebreak.models.incident import GroupingDecision

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def make_alert(fp: str, status: AlertStatus = AlertStatus.FIRING) -> Alert:
    return Alert(
        source=AlertSource.GENERIC,
        fingerprint=fp,
        alertname="A",
        service="s",
        severity=Severity.WARNING,
        status=status,
        summary="x",
        started_at=T0,
    )


class FakeRepo:
    def __init__(self) -> None:
        self.keys: set[str] = set()
        self.inserted_before_assign: list[bool] = []

    async def insert_if_new(self, alert: Alert) -> bool:
        if alert.idempotency_key in self.keys:
            return False
        self.keys.add(alert.idempotency_key)
        return True


class FakeGrouper:
    def __init__(self, repo: FakeRepo) -> None:
        self.repo = repo
        self.seen: list[Alert] = []

    async def assign(self, alert: Alert) -> GroupingDecision:
        self.repo.inserted_before_assign.append(alert.idempotency_key in self.repo.keys)
        self.seen.append(alert)
        return GroupingDecision(incident_id="inc_1", created=len(self.seen) == 1, duplicate=False)


async def test_new_alerts_go_to_grouper_after_insert() -> None:
    repo = FakeRepo()
    grouper = FakeGrouper(repo)
    decisions = await ingest([make_alert("a"), make_alert("b")], repo, grouper)
    assert [d.incident_id for d in decisions] == ["inc_1", "inc_1"]
    assert [d.created for d in decisions] == [True, False]
    assert repo.inserted_before_assign == [True, True]


async def test_duplicates_reported_and_not_grouped() -> None:
    repo = FakeRepo()
    grouper = FakeGrouper(repo)
    await ingest([make_alert("a")], repo, grouper)
    decisions = await ingest([make_alert("a"), make_alert("b")], repo, grouper)
    assert [d.duplicate for d in decisions] == [True, False]
    assert [a.fingerprint for a in grouper.seen] == ["a", "b"]


async def test_resolved_alert_stored_but_not_grouped() -> None:
    repo = FakeRepo()
    grouper = FakeGrouper(repo)
    [d] = await ingest([make_alert("a", AlertStatus.RESOLVED)], repo, grouper)
    assert not d.duplicate and not d.created
    assert grouper.seen == []
    assert len(repo.keys) == 1


async def test_grouper_error_propagates() -> None:
    class Boom:
        async def assign(self, alert: Alert) -> GroupingDecision:
            raise RuntimeError("boom")

    import pytest

    with pytest.raises(RuntimeError, match="boom"):
        await ingest([make_alert("a")], FakeRepo(), Boom())

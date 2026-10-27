from datetime import UTC, datetime

import pytest

from firebreak.ingest.pipeline import ResolveMissingError, ingest
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
        self.ids: dict[str, str] = {}
        self.resolved: list[tuple[str, datetime]] = []

    async def insert_or_get(self, alert: Alert) -> tuple[str, bool]:
        if alert.idempotency_key in self.keys:
            return self.ids[alert.idempotency_key], False
        self.keys.add(alert.idempotency_key)
        self.ids[alert.idempotency_key] = alert.id
        return alert.id, True

    async def mark_resolved(self, idempotency_key: str, ended_at: datetime) -> bool:
        self.resolved.append((idempotency_key, ended_at))
        return idempotency_key in self.keys


class FakeGrouper:
    """Idempotent like the real grouper: re-assigning a linked alert reports duplicate=True."""

    def __init__(self, repo: FakeRepo, fail_times: int = 0) -> None:
        self.repo = repo
        self.fail_times = fail_times
        self.calls: list[Alert] = []
        self.linked: set[str] = set()

    async def assign(self, alert: Alert) -> GroupingDecision:
        self.repo.inserted_before_assign.append(alert.idempotency_key in self.repo.keys)
        self.calls.append(alert)
        if self.fail_times:
            self.fail_times -= 1
            raise RuntimeError("boom")
        dup = alert.idempotency_key in self.linked
        self.linked.add(alert.idempotency_key)
        return GroupingDecision(
            incident_id="inc_1", created=not dup and len(self.linked) == 1, duplicate=dup
        )


async def test_new_alerts_go_to_grouper_after_insert() -> None:
    repo = FakeRepo()
    grouper = FakeGrouper(repo)
    decisions = [
        r.decision for r in await ingest([make_alert("a"), make_alert("b")], repo, grouper)
    ]
    assert [d.incident_id for d in decisions] == ["inc_1", "inc_1"]
    assert [d.created for d in decisions] == [True, False]
    assert repo.inserted_before_assign == [True, True]


async def test_replay_reports_duplicate_via_grouper() -> None:
    repo = FakeRepo()
    grouper = FakeGrouper(repo)
    await ingest([make_alert("a")], repo, grouper)
    results = await ingest([make_alert("a"), make_alert("b")], repo, grouper)
    decisions = [r.decision for r in results]
    assert [d.duplicate for d in decisions] == [True, False]
    assert decisions[0].incident_id == "inc_1"


async def test_replay_reports_stored_alert_id() -> None:
    repo = FakeRepo()
    grouper = FakeGrouper(repo)
    [first] = await ingest([make_alert("a")], repo, grouper)
    [again] = await ingest([make_alert("a")], repo, grouper)
    assert again.alert_id == first.alert_id


async def test_new_resolved_alert_stored_but_not_grouped_or_updated() -> None:
    repo = FakeRepo()
    grouper = FakeGrouper(repo)
    [r] = await ingest([make_alert("a", AlertStatus.RESOLVED)], repo, grouper)
    d = r.decision
    assert d.incident_id is None and not d.duplicate and not d.created
    assert grouper.calls == [] and len(repo.keys) == 1 and repo.resolved == []


async def test_resolved_for_stored_firing_alert_calls_mark_resolved() -> None:
    repo = FakeRepo()
    grouper = FakeGrouper(repo)
    [fired] = await ingest([make_alert("a")], repo, grouper)
    resolved = make_alert("a", AlertStatus.RESOLVED)
    [r] = await ingest([resolved], repo, grouper)
    assert repo.resolved == [(resolved.idempotency_key, repo.resolved[0][1])]
    assert repo.resolved[0][1].utcoffset() is not None
    assert r.alert_id == fired.alert_id
    assert r.decision.incident_id is None and not r.decision.duplicate
    assert len(grouper.calls) == 1  # only the firing one


async def test_resolved_update_vanished_row_raises() -> None:
    repo = FakeRepo()
    repo.ids["x"] = "alr_x"

    class Lost(FakeRepo):
        async def insert_or_get(self, alert: Alert) -> tuple[str, bool]:
            return "alr_x", False

        async def mark_resolved(self, idempotency_key: str, ended_at: datetime) -> bool:
            return False

    with pytest.raises(ResolveMissingError):
        await ingest([make_alert("a", AlertStatus.RESOLVED)], Lost(), FakeGrouper(repo))


async def test_grouper_error_propagates() -> None:
    repo = FakeRepo()
    with pytest.raises(RuntimeError, match="boom"):
        await ingest([make_alert("a")], repo, FakeGrouper(repo, fail_times=1))


async def test_replay_after_grouper_failure_groups_the_alert() -> None:
    repo = FakeRepo()
    grouper = FakeGrouper(repo, fail_times=1)
    with pytest.raises(RuntimeError):
        await ingest([make_alert("a")], repo, grouper)
    assert len(repo.keys) == 1 and grouper.linked == set()  # inserted, never grouped
    [res] = await ingest([make_alert("a")], repo, grouper)
    d = res.decision
    assert d.incident_id == "inc_1" and not d.duplicate
    assert len(grouper.linked) == 1

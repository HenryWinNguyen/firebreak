"""M2 acceptance: an S10 alert storm is one incident; replaying it is a no-op."""

from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from firebreak.api.app import app
from firebreak.api.deps import get_grouper
from firebreak.models.alert import Alert
from firebreak.models.incident import GroupingDecision

pytestmark = pytest.mark.db

SERVICES = ["gateway", "orders", "inventory"]


def storm(n: int = 15) -> dict[str, Any]:
    alerts = [
        {
            "status": "firing",
            "labels": {
                "alertname": f"Storm{i}",
                "service": SERVICES[i % 3],
                "severity": "warning",
            },
            "annotations": {"summary": f"storm alert {i}"},
            "startsAt": "2026-01-01T00:00:00Z",
            "fingerprint": f"fp{i:02d}",
        }
        for i in range(n)
    ]
    return {"version": "4", "status": "firing", "receiver": "firebreak", "alerts": alerts}


async def counts(url: str) -> tuple[int, int, int]:
    """(incidents, incident_alerts, alerts) row counts."""
    eng = create_async_engine(url, poolclass=NullPool)
    try:
        async with eng.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT (SELECT count(*) FROM incidents),"
                        " (SELECT count(*) FROM incident_alerts),"
                        " (SELECT count(*) FROM alerts)"
                    )
                )
            ).one()
        return int(row[0]), int(row[1]), int(row[2])
    finally:
        await eng.dispose()


async def test_s10_storm_is_one_incident_and_replay_is_noop(
    client: httpx.AsyncClient, migrated_url: str, auth: dict[str, str]
) -> None:
    r = await client.post("/webhooks/alertmanager", json=storm(), headers=auth)
    assert r.status_code == 200
    results = r.json()["results"]
    assert len(results) == 15
    assert not any(x["decision"]["duplicate"] for x in results)
    assert sum(x["decision"]["created"] for x in results) == 1
    assert len({x["decision"]["incident_id"] for x in results}) == 1
    assert await counts(migrated_url) == (1, 15, 15)

    again = await client.post("/webhooks/alertmanager", json=storm(), headers=auth)
    assert again.status_code == 200
    replay = again.json()["results"]
    assert len(replay) == 15
    assert all(x["decision"]["duplicate"] for x in replay)
    assert not any(x["decision"]["created"] for x in replay)
    assert {x["decision"]["incident_id"] for x in replay} == {
        x["decision"]["incident_id"] for x in results
    }
    assert await counts(migrated_url) == (1, 15, 15)


class ExplodingGrouper:
    async def assign(self, alert: Alert) -> GroupingDecision:
        raise RuntimeError("grouper down")


async def test_replay_recovers_alert_stored_but_not_grouped(
    client: httpx.AsyncClient, migrated_url: str, auth: dict[str, str]
) -> None:
    payload = storm(1)
    app.dependency_overrides[get_grouper] = lambda: ExplodingGrouper()
    with pytest.raises(RuntimeError, match="grouper down"):
        await client.post("/webhooks/alertmanager", json=payload, headers=auth)
    # Alert persisted, but nothing grouped it.
    assert await counts(migrated_url) == (0, 0, 1)

    del app.dependency_overrides[get_grouper]  # real grouper again
    r = await client.post("/webhooks/alertmanager", json=payload, headers=auth)
    assert r.status_code == 200
    decision = r.json()["results"][0]["decision"]
    assert decision["created"] is True
    assert decision["duplicate"] is False
    assert await counts(migrated_url) == (1, 1, 1)

    # A third delivery is now a pure duplicate.
    r = await client.post("/webhooks/alertmanager", json=payload, headers=auth)
    assert r.json()["results"][0]["decision"]["duplicate"] is True
    assert await counts(migrated_url) == (1, 1, 1)

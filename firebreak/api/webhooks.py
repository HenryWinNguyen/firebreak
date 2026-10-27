"""``POST /webhooks/{alertmanager,generic}`` (ARCH §3, CONTRACTS §3.2)."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from firebreak.api.auth import require_webhook_token
from firebreak.api.deps import get_alert_repo, get_grouper
from firebreak.db.repositories import AlertRepository
from firebreak.ingest.normalize import (
    GenericWebhook,
    NormalizationError,
    normalize_alertmanager,
    normalize_generic,
)
from firebreak.ingest.pipeline import ingest
from firebreak.models.alert import Alert, AlertmanagerWebhook
from firebreak.models.incident import GroupingDecision, IncidentGrouper

router = APIRouter(prefix="/webhooks", dependencies=[Depends(require_webhook_token)])


class AlertResult(BaseModel):
    alert_id: str
    fingerprint: str
    decision: GroupingDecision


class IngestResponse(BaseModel):
    results: list[AlertResult]


async def _run(
    alerts: list[Alert], repo: AlertRepository, grouper: IncidentGrouper
) -> IngestResponse:
    decisions = await ingest(alerts, repo, grouper)
    return IngestResponse(
        results=[
            AlertResult(alert_id=a.id, fingerprint=a.fingerprint, decision=d)
            for a, d in zip(alerts, decisions, strict=True)
        ]
    )


@router.post("/alertmanager")
async def alertmanager_webhook(
    payload: AlertmanagerWebhook,
    repo: Annotated[AlertRepository, Depends(get_alert_repo)],
    grouper: Annotated[IncidentGrouper, Depends(get_grouper)],
) -> IngestResponse:
    try:
        alerts = normalize_alertmanager(payload)
    except NormalizationError as exc:
        raise HTTPException(422, exc.problems) from exc
    return await _run(alerts, repo, grouper)


@router.post("/generic")
async def generic_webhook(
    payload: GenericWebhook,
    repo: Annotated[AlertRepository, Depends(get_alert_repo)],
    grouper: Annotated[IncidentGrouper, Depends(get_grouper)],
) -> IngestResponse:
    return await _run(normalize_generic(payload), repo, grouper)

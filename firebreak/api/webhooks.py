"""``POST /webhooks/{alertmanager,generic}`` (ARCH §3, CONTRACTS §3.2)."""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import InterfaceError, OperationalError

from firebreak.api.auth import require_webhook_token
from firebreak.api.deps import get_alert_repo, get_grouper
from firebreak.ingest.normalize import (
    GenericWebhook,
    NormalizationError,
    normalize_alertmanager,
    normalize_generic,
)
from firebreak.ingest.pipeline import AlertStore, ingest
from firebreak.models.alert import Alert, AlertmanagerWebhook
from firebreak.models.incident import GroupingDecision, IncidentGrouper

log = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", dependencies=[Depends(require_webhook_token)])


class AlertResult(BaseModel):
    alert_id: str
    fingerprint: str
    decision: GroupingDecision


class IngestResponse(BaseModel):
    results: list[AlertResult]


async def _run(alerts: list[Alert], repo: AlertStore, grouper: IncidentGrouper) -> IngestResponse:
    try:
        results = await ingest(alerts, repo, grouper)
    except (OSError, OperationalError, InterfaceError) as exc:
        # Connection-level failures only; anything else propagates as a 500.
        log.exception("database unreachable during ingest of %d alert(s)", len(alerts))
        raise HTTPException(503, "database unavailable; retry the webhook delivery") from exc
    return IngestResponse(
        results=[
            AlertResult(alert_id=r.alert_id, fingerprint=a.fingerprint, decision=r.decision)
            for a, r in zip(alerts, results, strict=True)
        ]
    )


@router.post("/alertmanager")
async def alertmanager_webhook(
    payload: AlertmanagerWebhook,
    repo: Annotated[AlertStore, Depends(get_alert_repo)],
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
    repo: Annotated[AlertStore, Depends(get_alert_repo)],
    grouper: Annotated[IncidentGrouper, Depends(get_grouper)],
) -> IngestResponse:
    return await _run(normalize_generic(payload), repo, grouper)

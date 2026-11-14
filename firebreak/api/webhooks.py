"""``POST /webhooks/{alertmanager,generic}`` (ARCH §3, CONTRACTS §3.2)."""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import InterfaceError, OperationalError

from firebreak.api.auth import require_webhook_token
from firebreak.api.deps import get_alert_repo, get_grouper
from firebreak.api.temporal import IncidentSignaller, get_signaller
from firebreak.ingest.normalize import (
    GenericWebhook,
    NormalizationError,
    normalize_alertmanager,
    normalize_generic,
)
from firebreak.ingest.pipeline import AlertStore, IngestResult, ingest
from firebreak.models.alert import Alert, AlertmanagerWebhook, AlertStatus
from firebreak.models.incident import GroupingDecision, IncidentGrouper
from firebreak.models.orchestration import AlertAdded

log = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", dependencies=[Depends(require_webhook_token)])


class AlertResult(BaseModel):
    alert_id: str
    fingerprint: str
    decision: GroupingDecision


class IngestResponse(BaseModel):
    results: list[AlertResult]


async def _signal_all(
    alerts: list[Alert], results: list[IngestResult], signaller: IncidentSignaller
) -> None:
    """Signal-with-start each firing alert's incident. Failures are logged, never raised.

    Duplicates are re-signalled on purpose (idempotent; repairs an earlier failed signal).
    The alerts are already stored, so a dead Temporal must not fail the webhook.
    """
    timed_out = False
    for alert, result in zip(alerts, results, strict=True):
        incident_id = result.decision.incident_id
        if alert.status is not AlertStatus.FIRING or incident_id is None:
            continue
        if timed_out:
            # One bounded attempt already hung; don't hold the webhook for every alert.
            log.error(
                "not signalling workflow for incident %s alert %s: Temporal timed out earlier "
                "in this webhook",
                incident_id,
                result.alert_id,
            )
            continue
        try:
            await signaller.signal(
                incident_id, AlertAdded(alert_id=result.alert_id, severity=alert.severity)
            )
        except Exception as exc:
            timed_out = isinstance(exc, TimeoutError)
            log.exception(
                "failed to signal workflow for incident %s alert %s", incident_id, result.alert_id
            )


async def _run(
    alerts: list[Alert],
    repo: AlertStore,
    grouper: IncidentGrouper,
    signaller: IncidentSignaller,
) -> IngestResponse:
    try:
        results = await ingest(alerts, repo, grouper)
    except (OSError, OperationalError, InterfaceError) as exc:
        # Connection-level failures only; anything else propagates as a 500.
        log.exception("database unreachable during ingest of %d alert(s)", len(alerts))
        raise HTTPException(503, "database unavailable; retry the webhook delivery") from exc
    await _signal_all(alerts, results, signaller)
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
    signaller: Annotated[IncidentSignaller, Depends(get_signaller)],
) -> IngestResponse:
    try:
        alerts = normalize_alertmanager(payload)
    except NormalizationError as exc:
        raise HTTPException(422, exc.problems) from exc
    return await _run(alerts, repo, grouper, signaller)


@router.post("/generic")
async def generic_webhook(
    payload: GenericWebhook,
    repo: Annotated[AlertStore, Depends(get_alert_repo)],
    grouper: Annotated[IncidentGrouper, Depends(get_grouper)],
    signaller: Annotated[IncidentSignaller, Depends(get_signaller)],
) -> IngestResponse:
    return await _run(normalize_generic(payload), repo, grouper, signaller)

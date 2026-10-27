"""Admin auth, chaos flags and deploy history (CONTRACTS 2.2)."""

from __future__ import annotations

import hmac
import os
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from common.runtime import InvalidSettingError, RuntimeSettings, UnknownSettingError


def require_admin(authorization: Annotated[str | None, Header()] = None) -> None:
    token = os.environ.get("SHOPFLOOR_ADMIN_TOKEN", "")
    if not token:
        raise HTTPException(503, "admin disabled: SHOPFLOOR_ADMIN_TOKEN is not set")
    expected = f"Bearer {token}"
    if authorization is None or not hmac.compare_digest(authorization, expected):
        raise HTTPException(401, "invalid admin token")


class ChaosRequest(BaseModel):
    flag: str
    value: Any


class DeployRequest(BaseModel):
    version: str = Field(min_length=1)
    message: str = ""


def admin_router() -> APIRouter:
    router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)])

    @router.get("/chaos")
    async def get_chaos(request: Request) -> dict[str, Any]:
        runtime: RuntimeSettings = request.app.state.runtime
        return runtime.snapshot()

    @router.post("/chaos")
    async def set_chaos(req: ChaosRequest, request: Request) -> dict[str, Any]:
        runtime: RuntimeSettings = request.app.state.runtime
        try:
            runtime.set(req.flag, req.value)
        except UnknownSettingError:
            raise HTTPException(422, f"unknown flag {req.flag!r}") from None
        except InvalidSettingError as exc:
            raise HTTPException(422, str(exc)) from None
        request.app.state.logger.warning(
            f"chaos flag {req.flag} set to {req.value!r}", extra={"route": "/admin/chaos"}
        )
        return runtime.snapshot()

    @router.post("/chaos/reset")
    async def reset_chaos(request: Request) -> dict[str, Any]:
        runtime: RuntimeSettings = request.app.state.runtime
        runtime.reset()
        request.app.state.logger.warning("chaos flags reset", extra={"route": "/admin/chaos/reset"})
        return runtime.snapshot()

    @router.get("/deploys")
    async def deploys(request: Request) -> list[dict[str, str]]:
        history: list[dict[str, str]] = request.app.state.deploys
        return list(reversed(history))  # newest first

    @router.post("/deploy")
    async def deploy(req: DeployRequest, request: Request) -> dict[str, str]:
        app = request.app
        entry = record_deploy(app, req.version, req.message)
        app.state.logger.warning(
            f"deployed {req.version}: {req.message}", extra={"route": "/admin/deploy"}
        )
        return entry

    return router


def record_deploy(app: FastAPI, version: str, message: str) -> dict[str, str]:
    """Append to the deploy history, update app_info, and run the service's deploy hook."""
    previous: str = app.state.version
    entry = {
        "service": app.state.service,
        "version": version,
        "deployed_at": datetime.now(UTC).isoformat(),
        "message": message,
    }
    app.state.deploys.append(entry)
    app.state.version = version
    app.state.metrics.set_version(version)
    hook = app.state.on_deploy
    if hook is not None:
        hook(version, previous)
    return entry

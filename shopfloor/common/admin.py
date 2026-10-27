"""Admin auth and a stub router. T-012 replaces the stub bodies with chaos behavior."""

from __future__ import annotations

import hmac
import os
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from common.runtime import RuntimeSettings


def require_admin(authorization: Annotated[str | None, Header()] = None) -> None:
    token = os.environ.get("SHOPFLOOR_ADMIN_TOKEN", "")
    if not token:
        raise HTTPException(503, "admin disabled: SHOPFLOOR_ADMIN_TOKEN is not set")
    expected = f"Bearer {token}"
    if authorization is None or not hmac.compare_digest(authorization, expected):
        raise HTTPException(401, "invalid admin token")


def admin_router() -> APIRouter:
    router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)])

    @router.get("/chaos")
    async def get_chaos(request: Request) -> dict[str, Any]:
        runtime: RuntimeSettings = request.app.state.runtime
        return runtime.snapshot()

    @router.post("/chaos")
    async def set_chaos() -> None:
        raise HTTPException(501, "implemented in T-012")

    @router.post("/chaos/reset")
    async def reset_chaos() -> None:
        raise HTTPException(501, "implemented in T-012")

    @router.get("/deploys")
    async def deploys() -> None:
        raise HTTPException(501, "implemented in T-012")

    @router.post("/deploy")
    async def deploy() -> None:
        raise HTTPException(501, "implemented in T-012")

    return router

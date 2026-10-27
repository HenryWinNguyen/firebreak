"""Shared FastAPI skeleton: trace ids, metrics, JSON request logs, health, admin."""

from __future__ import annotations

import logging
import os
import time
import traceback
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from common.admin import admin_router
from common.errors import DownstreamError
from common.logging import (
    TRACE_HEADER,
    get_logger,
    new_trace_id,
    reset_trace_id,
    set_trace_id,
)
from common.metrics import Metrics
from common.pool import PoolExhaustedError
from common.runtime import RuntimeSettings


async def _downstream_handler(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, DownstreamError)
    request.state.error = str(exc)
    return JSONResponse(
        {"detail": exc.detail, "downstream": exc.service}, status_code=exc.response_status
    )


async def _pool_handler(request: Request, exc: Exception) -> Response:
    request.state.error = str(exc)
    return JSONResponse({"detail": str(exc)}, status_code=503)


def create_base_app(
    service: str,
    runtime: RuntimeSettings | None = None,
    lifespan: Any = None,
    version: str | None = None,
    on_deploy: Callable[[str, str], None] | None = None,
) -> FastAPI:
    version = version or os.environ.get("SHOPFLOOR_VERSION", "1.0.0")
    app = FastAPI(title=f"shopfloor-{service}", lifespan=lifespan)
    app.state.service = service
    app.state.version = version
    app.state.on_deploy = on_deploy  # called with (new_version, previous_version)
    app.state.deploys = [
        {
            "service": service,
            "version": version,
            "deployed_at": datetime.now(UTC).isoformat(),
            "message": "initial deploy",
        }
    ]
    app.state.metrics = Metrics(service, version)
    app.state.runtime = runtime or RuntimeSettings({})
    app.state.logger = get_logger(service)
    metrics: Metrics = app.state.metrics
    logger: logging.Logger = app.state.logger

    app.add_exception_handler(DownstreamError, _downstream_handler)
    app.add_exception_handler(PoolExhaustedError, _pool_handler)

    @app.middleware("http")
    async def observe(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        trace_id = request.headers.get(TRACE_HEADER) or new_trace_id()
        token = set_trace_id(trace_id)
        start = time.perf_counter()
        request.state.error = None
        try:
            try:
                response = await call_next(request)
            except Exception as exc:  # noqa: BLE001 (recorded and logged below as a 500)
                request.state.error = "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                ).strip()
                response = JSONResponse({"detail": "internal error"}, status_code=500)
            elapsed = time.perf_counter() - start
            route = getattr(request.scope.get("route"), "path", "unmatched")
            status = response.status_code
            metrics.observe(route, request.method, status, elapsed)
            fields: dict[str, Any] = {
                "route": route,
                "status": status,
                "latency_ms": round(elapsed * 1000, 2),
                "trace_id": trace_id,
            }
            if request.state.error:
                fields["error"] = request.state.error
            logger.log(
                logging.ERROR if status >= 500 else logging.INFO,
                f"{request.method} {route}",
                extra=fields,
            )
            response.headers[TRACE_HEADER] = trace_id
            return response
        finally:
            reset_trace_id(token)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    app.include_router(admin_router())
    return app

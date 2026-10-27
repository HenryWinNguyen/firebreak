"""Gateway: the public entry point. Calls orders only."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from common.app import create_base_app
from common.errors import call_service
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


class OrderRequest(BaseModel):
    product_id: int
    quantity: int = Field(default=1, ge=1)


def create_app(orders: httpx.AsyncClient | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        client = orders or httpx.AsyncClient(
            base_url=os.environ.get("ORDERS_URL", "http://orders:8101"), timeout=10.0
        )
        app.state.orders = client
        try:
            yield
        finally:
            await client.aclose()

    app = create_base_app("gateway", lifespan=lifespan)

    @app.get("/api/products")
    async def products() -> Any:
        return await call_service(app.state.orders, "orders", "GET", "/products")

    @app.post("/api/orders")
    async def place_order(req: OrderRequest) -> JSONResponse:
        body = await call_service(
            app.state.orders, "orders", "POST", "/orders", json=req.model_dump()
        )
        return JSONResponse(body, status_code=201)

    return app


app = create_app()

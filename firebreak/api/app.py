from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from firebreak.api.deps import dispose_engines
from firebreak.api.temporal import close_signaller
from firebreak.api.webhooks import router as webhooks_router


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    try:
        yield
    finally:
        try:
            await close_signaller()
        finally:
            await dispose_engines()


app = FastAPI(title="Firebreak", lifespan=lifespan)
app.include_router(webhooks_router)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}

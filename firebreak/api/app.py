from fastapi import FastAPI

from firebreak.api.webhooks import router as webhooks_router

app = FastAPI(title="Firebreak")
app.include_router(webhooks_router)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}

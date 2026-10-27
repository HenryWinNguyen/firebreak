from fastapi import FastAPI

app = FastAPI(title="Firebreak")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}

from fastapi.testclient import TestClient

from firebreak.api.app import app


def test_healthz() -> None:
    assert TestClient(app).get("/healthz").json() == {"status": "ok"}

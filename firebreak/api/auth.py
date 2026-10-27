"""Bearer-token auth for webhooks."""

import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from firebreak.config import Settings

_bearer = HTTPBearer(auto_error=False)


def get_settings() -> Settings:
    return Settings()


def require_webhook_token(
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    expected = settings.webhook_token.get_secret_value().encode()
    given = creds.credentials.encode() if creds is not None else b""
    # Always compare, so timing does not reveal whether a header was sent.
    ok = secrets.compare_digest(given, expected)
    if creds is None or not ok:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "missing or invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

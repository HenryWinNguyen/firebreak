"""Shopfloor read adapter. Exposes ONLY ``GET /admin/deploys``.

``/admin/chaos`` and every POST endpoint are unreachable: the shared guard
transport rejects any other path or method (see ``adapters/base.py``).
"""

import logging
from datetime import datetime
from typing import Protocol

import httpx
from pydantic import BaseModel, ValidationError

from firebreak.tools.adapters.base import BackendError, ReadOnlyClient
from firebreak.tools.errors import InvalidToolArgs

logger = logging.getLogger(__name__)

DEPLOYS_PATH = "/admin/deploys"


class Deploy(BaseModel):
    service: str
    version: str
    deployed_at: datetime
    message: str = ""


class DeploysAdapter(Protocol):
    async def recent_deploys(self, service: str, limit: int) -> list[Deploy]: ...


class ShopfloorAdapter:
    def __init__(
        self,
        urls: dict[str, str],
        *,
        admin_token: str,
        timeout_s: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._urls = {k: v.rstrip("/") for k, v in urls.items()}
        self._http = ReadOnlyClient(
            backend="shopfloor",
            allowed_paths=frozenset({DEPLOYS_PATH}),
            timeout_s=timeout_s,
            headers={"Authorization": f"Bearer {admin_token}"},
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def recent_deploys(self, service: str, limit: int) -> list[Deploy]:
        base = self._urls.get(service)
        if base is None:
            raise InvalidToolArgs(f"unknown service {service[:40]!r}; known: {sorted(self._urls)}")
        body = await self._http.get_json(base + DEPLOYS_PATH)
        if not isinstance(body, list):
            raise BackendError("shopfloor returned an unexpected deploys shape")
        try:
            deploys = [Deploy.model_validate(item) for item in body]
        except ValidationError as e:
            logger.warning("shopfloor deploys failed validation service=%s: %s", service, e)
            raise BackendError("shopfloor returned a malformed deploy record") from e
        # The endpoint is newest-first (CONTRACTS §2.2); don't rely on it for the cap.
        deploys.sort(key=lambda d: d.deployed_at, reverse=True)
        return deploys[:limit]

from __future__ import annotations

from typing import Any

import httpx

from common.logging import TRACE_HEADER, current_trace_id


class DownstreamError(Exception):
    """A call to another service failed. 4xx is passed through, anything else becomes 502."""

    def __init__(self, service: str, status: int, detail: str) -> None:
        super().__init__(f"{service} returned {status}: {detail}")
        self.service = service
        self.status = status
        self.detail = detail

    @property
    def response_status(self) -> int:
        return self.status if 400 <= self.status < 500 else 502


async def call_service(
    client: httpx.AsyncClient,
    service: str,
    method: str,
    path: str,
    json: Any = None,
) -> Any:
    """Call a downstream service, propagating the trace id. Raises DownstreamError."""
    trace_id = current_trace_id()
    headers = {TRACE_HEADER: trace_id} if trace_id else {}
    try:
        resp = await client.request(method, path, json=json, headers=headers)
    except httpx.HTTPError as exc:
        raise DownstreamError(service, 502, f"unreachable: {exc!r}") from exc
    if resp.status_code >= 400:
        try:
            detail = str(resp.json().get("detail", resp.text))
        except (ValueError, AttributeError):
            detail = resp.text
        raise DownstreamError(service, resp.status_code, detail)
    return resp.json()

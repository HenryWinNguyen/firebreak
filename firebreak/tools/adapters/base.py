"""A GET-only HTTP client shared by the read adapters, plus their typed errors.

Safety (CONTRACTS §3.4): no adapter may issue a non-GET request or reach a path it
wasn't built for (``/admin/chaos`` would leak the injected fault, i.e. eval ground
truth). That is enforced structurally: every client sends through
``_GuardTransport``, which refuses any request whose method isn't GET or whose path
isn't in the adapter's exact allowlist, whatever the calling code does. Redirects
are never followed.
"""

import logging
from collections.abc import Mapping
from typing import Any

import httpx

from firebreak.tools.errors import ToolHandlerFailed, ToolTimeout, cap_text

logger = logging.getLogger(__name__)

_MAX_DETAIL_CHARS = 500


class BackendError(ToolHandlerFailed):
    """The backend answered with an HTTP error, a ``status: error`` body or garbage."""


class BackendUnavailable(ToolHandlerFailed):
    """The backend couldn't be reached (connection refused, DNS, protocol error)."""


class BackendTimeout(ToolTimeout):
    """The backend didn't answer within the HTTP timeout."""


class ForbiddenRequest(ToolHandlerFailed):
    """The guard refused a request: not a GET, or a path outside the allowlist."""


class _GuardTransport(httpx.AsyncBaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport, allowed_paths: frozenset[str]) -> None:
        self._inner = inner
        self._allowed = allowed_paths

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            raise ForbiddenRequest(f"refused {request.method} request: adapters are GET-only")
        if request.url.path not in self._allowed:
            raise ForbiddenRequest(f"refused GET {request.url.path!r}: path is not allowlisted")
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


class ReadOnlyClient:
    """One shared ``httpx.AsyncClient`` that can only GET allowlisted paths."""

    def __init__(
        self,
        *,
        backend: str,
        allowed_paths: frozenset[str],
        timeout_s: float,
        headers: Mapping[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.backend = backend
        inner = transport if transport is not None else httpx.AsyncHTTPTransport()
        self._client = httpx.AsyncClient(
            transport=_GuardTransport(inner, allowed_paths),
            timeout=timeout_s,
            headers=dict(headers or {}),
            follow_redirects=False,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_json(self, url: str, params: Mapping[str, str] | None = None) -> Any:
        """GET ``url`` and return the parsed JSON body, or raise a typed ``ToolError``."""
        try:
            resp = await self._client.get(url, params=dict(params or {}))
        except httpx.TimeoutException as e:
            logger.warning("%s timed out url=%s", self.backend, url)
            raise BackendTimeout(f"{self.backend} did not answer in time") from e
        except httpx.HTTPError as e:
            logger.warning("%s unreachable url=%s: %s", self.backend, url, e)
            raise BackendUnavailable(
                f"{self.backend} is unreachable ({type(e).__name__})",
                untrusted_detail=str(e),
            ) from e

        if not resp.is_success:
            logger.warning("%s HTTP %d url=%s", self.backend, resp.status_code, url)
            raise BackendError(
                f"{self.backend} returned HTTP {resp.status_code}",
                untrusted_detail=cap_text(resp.text, _MAX_DETAIL_CHARS),
            )
        try:
            return resp.json()
        except ValueError as e:
            logger.warning("%s returned non-JSON url=%s", self.backend, url)
            raise BackendError(
                f"{self.backend} returned a non-JSON body",
                untrusted_detail=cap_text(resp.text, _MAX_DETAIL_CHARS),
            ) from e


def check_status(backend: str, body: Any) -> dict[str, Any]:
    """Validate a Prometheus/Loki envelope; raise ``BackendError`` on ``status: error``."""
    if not isinstance(body, dict):
        raise BackendError(f"{backend} returned an unexpected body shape")
    if body.get("status") != "success":
        raise BackendError(
            f"{backend} reported an error",
            untrusted_detail=cap_text(f"{body.get('errorType', '')}: {body.get('error', '')}"),
        )
    return body

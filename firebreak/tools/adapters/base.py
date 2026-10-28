"""A GET-only HTTP client shared by the read adapters, plus their typed errors.

Safety (CONTRACTS §3.4): no adapter may issue a non-GET request or reach a path it
wasn't built for (``/admin/chaos`` would leak the injected fault, i.e. eval ground
truth). That is enforced structurally: every client sends through
``_GuardTransport``, which refuses any request whose method isn't GET or whose path
isn't in the adapter's exact allowlist, whatever the calling code does. Redirects
are never followed.
"""

import json
import logging
import math
from collections.abc import Iterable, Mapping
from typing import Any

import httpx

from firebreak.tools.errors import ToolHandlerFailed, cap_text

logger = logging.getLogger(__name__)

_MAX_DETAIL_CHARS = 500
#: Hard cap on a backend response body (checked before parsing).
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
#: The decompressed body is read in pieces this big, checking the cap after each.
_READ_PIECE_BYTES = 64 * 1024
_DEFAULT_PORTS = {"http": 80, "https": 443}


def _origin(url: httpx.URL) -> tuple[str, str, int | None]:
    scheme = url.scheme.lower()
    return (scheme, url.host.lower(), url.port or _DEFAULT_PORTS.get(scheme))


def _parse_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):  # e.g. 1e999 -> inf, which json.dumps would write as Infinity
        raise ValueError(f"non-finite number {text[:20]!r}")
    return value


def _reject_constant(name: str) -> Any:
    raise ValueError(f"non-standard JSON constant {name}")


class BackendError(ToolHandlerFailed):
    """The backend answered with an HTTP error, a ``status: error`` body or garbage."""


class BackendUnavailable(ToolHandlerFailed):
    """The backend couldn't be reached (connection refused, DNS, protocol error)."""


class BackendTimeout(ToolHandlerFailed):
    """The backend didn't answer within the HTTP timeout.

    Deliberately not a ``ToolTimeout`` subclass: the gateway maps exactly this type to the
    ``timeout`` code, so no other handler-raised error can claim it.
    """


class ForbiddenRequest(ToolHandlerFailed):
    """The guard refused a request: not a GET, or a path outside the allowlist."""


class _GuardTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        inner: httpx.AsyncBaseTransport,
        allowed_paths: frozenset[str],
        allowed_origins: frozenset[tuple[str, str, int | None]],
    ) -> None:
        self._inner = inner
        self._allowed = allowed_paths
        self._origins = allowed_origins

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            raise ForbiddenRequest(f"refused {request.method} request: adapters are GET-only")
        # raw_path is the still-encoded path plus "?query"; compare the path part only, so
        # an encoded slash (admin%2Fdeploys) can't alias an allowlisted path.
        raw_path = request.url.raw_path.split(b"?", 1)[0].decode("ascii", "replace")
        if raw_path not in self._allowed:
            raise ForbiddenRequest(f"refused GET {raw_path!r}: path is not allowlisted")
        if _origin(request.url) not in self._origins:
            raise ForbiddenRequest(
                f"refused GET to {request.url.host!r}: host is not the configured backend"
            )
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
        base_urls: Iterable[str],
        timeout_s: float,
        headers: Mapping[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.backend = backend
        inner = transport if transport is not None else httpx.AsyncHTTPTransport()
        origins = frozenset(_origin(httpx.URL(u)) for u in base_urls)
        self._client = httpx.AsyncClient(
            transport=_GuardTransport(inner, allowed_paths, origins),
            timeout=timeout_s,
            headers=dict(headers or {}),
            follow_redirects=False,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_json(self, url: str, params: Mapping[str, str] | None = None) -> Any:
        """GET ``url`` and return the parsed JSON body, or raise a typed ``ToolError``."""
        try:
            async with self._client.stream("GET", url, params=dict(params or {})) as resp:
                body = await self._read_capped(resp, url)
        except httpx.TimeoutException as e:
            logger.warning("%s timed out url=%s", self.backend, url)
            raise BackendTimeout(f"{self.backend} did not answer in time") from e
        except httpx.HTTPError as e:
            logger.warning("%s unreachable url=%s: %s", self.backend, url, e)
            raise BackendUnavailable(
                f"{self.backend} is unreachable ({type(e).__name__})",
                untrusted_detail=str(e),
            ) from e

        text = body.decode("utf-8", errors="replace")
        if not resp.is_success:
            logger.warning("%s HTTP %d url=%s", self.backend, resp.status_code, url)
            raise BackendError(
                f"{self.backend} returned HTTP {resp.status_code}",
                untrusted_detail=cap_text(text, _MAX_DETAIL_CHARS),
            )
        try:
            return json.loads(body, parse_constant=_reject_constant, parse_float=_parse_float)
        except ValueError as e:  # includes JSONDecodeError and UnicodeDecodeError
            logger.warning("%s returned invalid JSON url=%s: %s", self.backend, url, e)
            raise BackendError(
                f"{self.backend} returned a non-JSON body",
                untrusted_detail=cap_text(text, _MAX_DETAIL_CHARS),
            ) from e

    async def _read_capped(self, resp: httpx.Response, url: str) -> bytes:
        """Read the body, refusing anything over ``MAX_RESPONSE_BYTES`` before it is parsed."""
        too_large = BackendError(f"{self.backend} response too large; narrow the query")
        declared = resp.headers.get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > MAX_RESPONSE_BYTES:
            logger.warning(
                "%s response too large (content-length=%s) url=%s", self.backend, declared, url
            )
            raise too_large
        buf = bytearray()
        async for chunk in resp.aiter_bytes(chunk_size=_READ_PIECE_BYTES):
            buf += chunk
            if len(buf) > MAX_RESPONSE_BYTES:
                logger.warning("%s response exceeded cap url=%s", self.backend, url)
                raise too_large
        return bytes(buf)


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

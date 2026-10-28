"""Loki read adapter (GET /loki/api/v1/query_range)."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

import httpx

from firebreak.tools.adapters.base import BackendError, ReadOnlyClient, check_status

_RANGE = "/loki/api/v1/query_range"


@dataclass(frozen=True)
class LogStream:
    labels: dict[str, str]
    #: ``(unix_nanoseconds, line)``; for metric LogQL queries the "line" is the value.
    entries: list[tuple[int, str]]


@dataclass(frozen=True)
class LogResult:
    result_type: str  # "streams" (log lines) or "matrix" (a metric LogQL query)
    streams: list[LogStream]
    raw: Any = field(repr=False, default=None)


class LogsAdapter(Protocol):
    async def query_range(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> LogResult: ...


class LokiAdapter:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._http = ReadOnlyClient(
            backend="loki",
            allowed_paths=frozenset({_RANGE}),
            base_urls=[self._base],
            timeout_s=timeout_s,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def query_range(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> LogResult:
        body = check_status(
            "loki",
            await self._http.get_json(
                self._base + _RANGE,
                {
                    "query": query,
                    "start": str(int(start.timestamp() * 1e9)),
                    "end": str(int(end.timestamp() * 1e9)),
                    "limit": str(limit),
                    "direction": "backward",
                },
            ),
        )
        data: Any = body.get("data")
        try:
            rtype = str(data["resultType"])
            streams: list[LogStream] = []
            for item in data["result"]:
                labels = dict(item.get("stream") or item.get("metric") or {})
                entries = [(_ns(v[0], rtype), str(v[1])) for v in item["values"]]
                streams.append(LogStream(labels, entries))
        except (KeyError, TypeError, ValueError, IndexError, AttributeError) as e:
            raise BackendError("loki returned an unexpected result shape") from e
        return LogResult(result_type=rtype, streams=streams, raw=data)


def _ns(ts: Any, rtype: str) -> int:
    # Streams carry nanosecond strings; matrix results carry float seconds.
    return int(float(ts) * 1e9) if rtype == "matrix" else int(ts)

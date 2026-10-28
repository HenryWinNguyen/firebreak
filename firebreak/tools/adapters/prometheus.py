"""Prometheus read adapter (GET /api/v1/query, /query_range, /label/__name__/values)."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

import httpx

from firebreak.tools.adapters.base import BackendError, ReadOnlyClient, check_status

_QUERY = "/api/v1/query"
_RANGE = "/api/v1/query_range"
_NAMES = "/api/v1/label/__name__/values"


@dataclass(frozen=True)
class Series:
    labels: dict[str, str]
    #: ``(unix_seconds, value-as-string)``
    points: list[tuple[float, str]]


@dataclass(frozen=True)
class MetricResult:
    result_type: str
    series: list[Series]
    #: The backend's full ``data`` JSON.
    raw: Any = field(repr=False, default=None)


class MetricsAdapter(Protocol):
    async def instant(self, query: str, at: datetime) -> MetricResult: ...

    async def range(
        self, query: str, start: datetime, end: datetime, step: str
    ) -> MetricResult: ...

    async def metric_names(self) -> list[str]: ...


class PrometheusAdapter:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._http = ReadOnlyClient(
            backend="prometheus",
            allowed_paths=frozenset({_QUERY, _RANGE, _NAMES}),
            base_urls=[self._base],
            timeout_s=timeout_s,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def instant(self, query: str, at: datetime) -> MetricResult:
        body = await self._http.get_json(
            self._base + _QUERY, {"query": query, "time": f"{at.timestamp():.3f}"}
        )
        return _parse(check_status("prometheus", body))

    async def range(self, query: str, start: datetime, end: datetime, step: str) -> MetricResult:
        body = await self._http.get_json(
            self._base + _RANGE,
            {
                "query": query,
                "start": f"{start.timestamp():.3f}",
                "end": f"{end.timestamp():.3f}",
                "step": step,
            },
        )
        return _parse(check_status("prometheus", body))

    async def metric_names(self) -> list[str]:
        body = check_status("prometheus", await self._http.get_json(self._base + _NAMES))
        data: Any = body.get("data")
        if not isinstance(data, list) or not all(isinstance(n, str) for n in data):
            raise BackendError("prometheus returned an unexpected label-values shape")
        return list(data)


def _parse(body: dict[str, Any]) -> MetricResult:
    data: Any = body.get("data")
    try:
        rtype = str(data["resultType"])
        result = data["result"]
        series: list[Series] = []
        if rtype == "vector":
            for item in result:
                series.append(Series(dict(item["metric"]), [_point(item["value"])]))
        elif rtype == "matrix":
            for item in result:
                series.append(Series(dict(item["metric"]), [_point(p) for p in item["values"]]))
        elif rtype in ("scalar", "string"):
            series.append(Series({}, [_point(result)]))
        else:
            raise BackendError(f"prometheus returned unsupported resultType {rtype[:40]!r}")
    except (KeyError, TypeError, ValueError, IndexError) as e:
        raise BackendError("prometheus returned an unexpected result shape") from e
    return MetricResult(result_type=rtype, series=series, raw=data)


def _point(p: Any) -> tuple[float, str]:
    return (float(p[0]), str(p[1]))

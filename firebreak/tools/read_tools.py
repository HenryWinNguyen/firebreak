"""The six read tools of CONTRACTS §3.4, registered in a ``ToolRegistry``.

All are ``ToolAccess.READ`` and run through the gateway. Query strings reach the
backend verbatim. Results are rendered compactly and sized to fit
``max_output_chars``; ``Evidence.query`` records the exact resolved query and window.
"""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from firebreak.ingest.service_graph import ServiceGraph
from firebreak.models.common import utcnow
from firebreak.models.evidence import EvidenceKind
from firebreak.models.llm import ToolAccess
from firebreak.tools.adapters.loki import LogResult, LogsAdapter, LokiAdapter
from firebreak.tools.adapters.prometheus import (
    MetricResult,
    MetricsAdapter,
    PrometheusAdapter,
    Series,
)
from firebreak.tools.adapters.shopfloor import DeploysAdapter, ShopfloorAdapter
from firebreak.tools.context import ToolContext
from firebreak.tools.registry import DEFAULT_MAX_OUTPUT_CHARS, Handler, ToolOutput, ToolRegistry
from firebreak.tools.settings import ToolSettings

logger = logging.getLogger(__name__)

Clock = Callable[[], datetime]

MAX_OUTPUT_CHARS = DEFAULT_MAX_OUTPUT_CHARS
#: Headroom under ``max_output_chars`` for the gateway's defanging.
_RENDER_BUDGET = MAX_OUTPUT_CHARS - 400
_MAX_SERIES = 50
_MAX_LINE_CHARS = 400
_POINT_STEPS = (10_000, 60, 30, 15, 8, 4, 2, 1)

Service = Literal["gateway", "orders", "inventory"]


# --- args -------------------------------------------------------------------------------


class PromqlQueryArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(min_length=1, max_length=2000, description="PromQL expression")
    at: AwareDatetime | None = Field(default=None, description="Evaluation time (UTC); default now")


class PromqlRangeArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(min_length=1, max_length=2000, description="PromQL expression")
    minutes_back: int = Field(default=15, ge=1, le=120)
    step_seconds: int = Field(default=15, ge=5, le=600)


class ListMetricsArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    match: str | None = Field(default=None, max_length=200, description="Metric-name prefix")


class LogqlQueryArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(
        min_length=1,
        max_length=2000,
        description=(
            'LogQL; must start with a stream selector, e.g. {service="orders", level="error"}'
        ),
    )
    minutes_back: int = Field(default=15, ge=1, le=120)
    limit: int = Field(default=100, ge=1, le=500)


class RecentDeploysArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    service: Service
    limit: int = Field(default=5, ge=1, le=20)


class NoArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --- adapters ---------------------------------------------------------------------------


@dataclass
class ReadAdapters:
    metrics: MetricsAdapter
    logs: LogsAdapter
    deploys: DeploysAdapter
    graph: ServiceGraph

    async def aclose(self) -> None:
        """Close the HTTP clients of the real adapters (fakes without ``aclose`` are skipped)."""
        for a in (self.metrics, self.logs, self.deploys):
            close = getattr(a, "aclose", None)
            if close is not None:
                await close()


def build_default_adapters(settings: ToolSettings) -> ReadAdapters:
    """Real adapters. The caller owns them and should ``await adapters.aclose()``."""
    t = settings.http_timeout_s
    return ReadAdapters(
        metrics=PrometheusAdapter(settings.prometheus_url, timeout_s=t),
        logs=LokiAdapter(settings.loki_url, timeout_s=t),
        deploys=ShopfloorAdapter(
            settings.shopfloor_urls,
            admin_token=settings.shopfloor_admin_token.get_secret_value(),
            timeout_s=t,
        ),
        graph=ServiceGraph.load(),
    )


# --- rendering --------------------------------------------------------------------------


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hms(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%H:%M:%SZ")


def _labels(labels: dict[str, str]) -> str:
    return "{" + ", ".join(f'{k}="{v}"' for k, v in sorted(labels.items())) + "}"


def _sample(points: list[tuple[float, str]], k: int) -> list[tuple[float, str]]:
    """At most ``k`` evenly spaced points, always keeping the first and the last."""
    if len(points) <= k:
        return points
    if k == 1:
        return [points[-1]]
    idx = {round(i * (len(points) - 1) / (k - 1)) for i in range(k)}
    return [points[i] for i in sorted(idx)]


def render_series(series: list[Series], budget: int = _RENDER_BUDGET) -> str:
    """``labels → [t, v], ...`` rows, downsampled/trimmed to fit ``budget`` with a note."""
    if not series:
        return "(no series returned)"
    total = len(series)
    max_pts = max(len(s.points) for s in series)
    shown = min(total, _MAX_SERIES)
    while True:
        for k in _POINT_STEPS:
            k = min(k, max_pts)
            rows = [
                f"{_labels(s.labels)} → "
                + ", ".join(f"[{_hms(t)}, {v}]" for t, v in _sample(s.points, k))
                for s in series[:shown]
            ]
            notes = []
            if shown < total:
                notes.append(f"showing {shown} of {total} series")
            if k < max_pts:
                notes.append(f"downsampled to at most {k} points per series")
            text = "\n".join(rows)
            if notes:
                text += "\n[truncated: " + "; ".join(notes) + "]"
            if len(text) <= budget:
                return text
            if k == 1:
                break
        if shown == 1:
            return text[: budget - 40] + "\n[truncated: output cut at size limit]"
        shown = max(1, shown // 2)


def render_logs(result: LogResult, budget: int = _RENDER_BUDGET) -> str:
    if result.result_type == "matrix":
        return render_series(
            [
                Series(s.labels, [(ns / 1e9, v) for ns, v in sorted(s.entries)])
                for s in result.streams
            ],
            budget,
        )
    entries = sorted(
        ((ns, s.labels, line) for s in result.streams for ns, line in s.entries),
        key=lambda e: e[0],
    )
    if not entries:
        return "(no log lines in window)"
    total = len(entries)
    lines: list[str] = []
    for ns, labels, line in entries:
        ts = datetime.fromtimestamp(ns / 1e9, UTC).strftime("%H:%M:%S.%f")[:-3]
        if len(line) > _MAX_LINE_CHARS:
            line = line[:_MAX_LINE_CHARS] + "…"
        tag = " ".join(f"{k}={v}" for k, v in sorted(labels.items()) if k != "service_name")
        lines.append(f"{ts} {tag} | {line}")
    # Keep the newest lines that fit; drop the oldest.
    kept: list[str] = []
    size = 0
    for row in reversed(lines):
        if size + len(row) + 1 > budget - 80 and kept:
            break
        kept.append(row[: budget - 80])
        size += len(row) + 1
    kept.reverse()
    text = "\n".join(kept)
    if len(kept) < total:
        text += f"\n[truncated: showing the newest {len(kept)} of {total} lines]"
    return text


# --- registry ---------------------------------------------------------------------------


def build_read_registry(
    settings: ToolSettings,
    adapters: ReadAdapters,
    *,
    clock: Clock | None = None,
) -> ToolRegistry:
    """Register exactly the six §3.4 tools.

    ``adapters`` is required so the caller owns the HTTP clients and can close them
    (``await adapters.aclose()``); ``build_default_adapters(settings)`` makes the real ones.
    """
    ad = adapters
    # Default to the shared clock so `deterministic()` freezes the query windows (replay).
    now = clock or utcnow

    def utc_now() -> datetime:
        t = now()
        if t.tzinfo is None:
            raise ValueError("clock must return an aware datetime")
        # Whole seconds, so Evidence.query reproduces the exact window that was requested.
        return t.astimezone(UTC).replace(microsecond=0)

    async def promql_query(args: PromqlQueryArgs, ctx: ToolContext) -> ToolOutput:
        at = args.at.astimezone(UTC).replace(microsecond=0) if args.at is not None else utc_now()
        result = await ad.metrics.instant(args.query, at)
        return ToolOutput(
            kind=EvidenceKind.METRIC_SERIES,
            query=f"promql_query {args.query} @ {_iso(at)}",
            content=_series_text(result),
            raw=json.dumps(result.raw, allow_nan=False),
        )

    async def promql_range(args: PromqlRangeArgs, ctx: ToolContext) -> ToolOutput:
        end = utc_now()
        start = end - timedelta(minutes=args.minutes_back)
        result = await ad.metrics.range(args.query, start, end, f"{args.step_seconds}s")
        return ToolOutput(
            kind=EvidenceKind.METRIC_SERIES,
            query=(
                f"promql_range {args.query} | start={_iso(start)} end={_iso(end)} "
                f"step={args.step_seconds}s"
            ),
            content=_series_text(result),
            raw=json.dumps(result.raw, allow_nan=False),
        )

    async def list_metrics(args: ListMetricsArgs, ctx: ToolContext) -> ToolOutput:
        names = await ad.metrics.metric_names()
        prefix = args.match or ""
        hits = sorted(n for n in names if n.startswith(prefix))
        text = "\n".join(hits) if hits else "(no metrics match)"
        if len(text) > _RENDER_BUDGET:
            kept: list[str] = []
            size = 0
            for n in hits:
                if size + len(n) + 1 > _RENDER_BUDGET - 80:
                    break
                kept.append(n)
                size += len(n) + 1
            text = "\n".join(kept) + (
                f"\n[truncated: showing {len(kept)} of {len(hits)} metrics; use a longer prefix]"
            )
        return ToolOutput(
            kind=EvidenceKind.METRIC_SERIES,
            query=f"list_metrics prefix={prefix!r}",
            content=text,
            raw=json.dumps(names, allow_nan=False),
        )

    async def logql_query(args: LogqlQueryArgs, ctx: ToolContext) -> ToolOutput:
        end = utc_now()
        start = end - timedelta(minutes=args.minutes_back)
        result = await ad.logs.query_range(args.query, start, end, args.limit)
        return ToolOutput(
            kind=EvidenceKind.LOG_LINES,
            query=(
                f"logql_query {args.query} | start={_iso(start)} end={_iso(end)} limit={args.limit}"
            ),
            content=render_logs(result),
            raw=json.dumps(result.raw, allow_nan=False),
        )

    async def recent_deploys(args: RecentDeploysArgs, ctx: ToolContext) -> ToolOutput:
        deploys = await ad.deploys.recent_deploys(args.service, args.limit)
        rows = [
            f"{_iso(d.deployed_at)} {d.service} {d.version} | {d.message[:200]}" for d in deploys
        ]
        return ToolOutput(
            kind=EvidenceKind.DEPLOY,
            query=f"recent_deploys service={args.service} limit={args.limit}",
            content="\n".join(rows) if rows else "(no deploys recorded)",
            raw=json.dumps([d.model_dump(mode="json") for d in deploys], allow_nan=False),
        )

    async def service_graph(args: NoArgs, ctx: ToolContext) -> ToolOutput:
        g = ad.graph
        parts = [f"cluster {name}: {', '.join(svcs)}" for name, svcs in g.clusters.items()]
        parts += [f"{a} -> {b}" for a, b in g.edges]
        return ToolOutput(
            kind=EvidenceKind.CONFIG,
            query="service_graph config/service_graph.yaml",
            content="\n".join(parts),
            raw=json.dumps({"clusters": g.clusters, "edges": g.edges}, allow_nan=False),
        )

    reg = ToolRegistry()

    def add(name: str, description: str, args_model: type[BaseModel], handler: Handler) -> None:
        reg.register(
            name=name,
            description=description,
            args_model=args_model,
            handler=handler,
            access=ToolAccess.READ,
            max_output_chars=MAX_OUTPUT_CHARS,
        )

    add(
        "promql_query",
        "Run an instant PromQL query against Prometheus (default: now).",
        PromqlQueryArgs,
        promql_query,
    )
    add(
        "promql_range",
        "Run a PromQL range query over the last minutes_back minutes.",
        PromqlRangeArgs,
        promql_range,
    )
    add(
        "list_metrics",
        "List Prometheus metric names, optionally filtered by name prefix.",
        ListMetricsArgs,
        list_metrics,
    )
    add(
        "logql_query",
        "Run a LogQL query against Loki over the last minutes_back minutes. The query must start "
        "with a stream selector in braces; bare filters like level=error are invalid. Labels: "
        "service (gateway, orders, inventory), level (error, warning, info). Examples: "
        '{service="orders", level="error"} or {service="gateway"} |= "timeout".',
        LogqlQueryArgs,
        logql_query,
    )
    add(
        "recent_deploys",
        "Most recent deploys of a Shopfloor service, newest first.",
        RecentDeploysArgs,
        recent_deploys,
    )
    add(
        "service_graph",
        "Shopfloor service dependency clusters and caller -> callee edges.",
        NoArgs,
        service_graph,
    )
    return reg


def _series_text(result: MetricResult) -> str:
    return render_series(result.series)

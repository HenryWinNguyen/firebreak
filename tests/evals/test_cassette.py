"""T-044 record/replay cassettes. Scripted fake LLM and fake adapters; no network, no API."""

import asyncio
import json
import re
import shutil
import socket
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import anthropic
import httpx
import httpx2
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from firebreak.agents.baseline import BASELINE_CONFIG, _summarize, build_task, investigate
from firebreak.agents.config import PROMPTS_DIR, load_agent_config
from firebreak.agents.runtime import run_agent
from firebreak.db.repositories import AlertRepository, IncidentRepository
from firebreak.db.tables import Base
from firebreak.evals.cassette import (
    Cassette,
    SecretInRecordingError,
    StaleRecordingError,
    find_secrets,
    record_run,
    recording_dir,
    replay_run,
    tool_key,
)
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.llm.client import AnthropicLLMClient, StrictReplayLLMClient, build_llm_client
from firebreak.llm.errors import FixtureMissing, LLMError
from firebreak.llm.ratelimit import TokenBucket
from firebreak.llm.settings import LLMMode, LLMSettings
from firebreak.models.alert import Alert, AlertSource
from firebreak.models.common import Severity, utcnow
from firebreak.models.incident import Incident
from firebreak.models.llm import LLMClient, LLMRequest, LLMResponse, Usage
from firebreak.models.reasoning import InvestigationReport
from firebreak.tools.adapters.base import BackendTimeout
from firebreak.tools.adapters.loki import LogResult, LogStream, LokiAdapter
from firebreak.tools.adapters.prometheus import MetricResult, PrometheusAdapter, Series
from firebreak.tools.adapters.shopfloor import Deploy, ShopfloorAdapter
from firebreak.tools.context import ToolContext
from firebreak.tools.gateway import ToolGateway
from firebreak.tools.read_tools import Clock, ReadAdapters, build_read_registry
from firebreak.tools.settings import ToolSettings
from tests.tools.fakes import FakeEvidenceSink, ScriptedLLM, evidence_ids_in, text_response

AT = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)
SCENARIO = "s01_bad_deploy"
API_KEY = "sk-ant-api03-SECRET-do-not-record-0123456789"
ADMIN_TOKEN = "shopfloor-admin-SECRET-0123456789"

# One parallel turn using all six read tools, then the final report.
CALLS: list[tuple[str, str, dict[str, Any]]] = [
    ("t1", "promql_query", {"query": 'rate(http_errors_total{service="orders"}[1m])'}),
    ("t2", "logql_query", {"query": '{service="orders"} |= "ERROR"', "limit": 50}),
    ("t3", "recent_deploys", {"service": "orders"}),
    ("t4", "service_graph", {}),
    ("t5", "list_metrics", {"match": "http_"}),
    ("t6", "promql_range", {"query": 'up{service="orders"}', "minutes_back": 30}),
]


# --- fakes ------------------------------------------------------------------------------


class Live:
    """Fake 'live' adapters. Earlier calls are slower, so they finish in reverse order."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.timeout_logs = False

    async def _delay(self, name: str) -> None:
        self.calls.append(name)
        await asyncio.sleep(max(0.0, 0.05 - 0.01 * len(self.calls)))

    async def instant(self, query: str, at: datetime) -> MetricResult:
        await self._delay("instant")
        raw = {"resultType": "vector", "result": [{"metric": {"service": "orders"}}]}
        return MetricResult(
            "vector", [Series({"service": "orders"}, [(at.timestamp(), "0.12")])], raw
        )

    async def range(self, query: str, start: datetime, end: datetime, step: str) -> MetricResult:
        await self._delay("range")
        pts = [(start.timestamp() + 60 * i, str(i)) for i in range(5)]
        return MetricResult(
            "matrix", [Series({"service": "orders"}, pts)], {"resultType": "matrix"}
        )

    async def metric_names(self) -> list[str]:
        await self._delay("names")
        return ["up", "http_errors_total", "http_requests_total"]

    async def query_range(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> LogResult:
        await self._delay("logs")
        if self.timeout_logs:
            raise BackendTimeout("loki did not answer in time")
        ns = int(end.timestamp() * 1e9) - 1_000_000_000
        streams = [
            LogStream({"service_name": "orders", "level": "error"}, [(ns, "ERROR db timeout")])
        ]
        return LogResult("streams", streams, {"resultType": "streams"})

    async def recent_deploys(self, service: str, limit: int) -> list[Deploy]:
        await self._delay("deploys")
        return [
            Deploy(service=service, version="v1.4.2-bad", deployed_at=AT - timedelta(minutes=7))
        ]


def live_adapters(live: Live) -> ReadAdapters:
    return ReadAdapters(metrics=live, logs=live, deploys=live, graph=ServiceGraph.load())


def report_for(evidence: list[str]) -> str:
    return json.dumps(
        {
            "findings": [
                {
                    "claim": "deploy v1.4.2-bad broke orders",
                    "evidence_ids": evidence,
                    "confidence": 0.9,
                    "agent": "baseline",
                }
            ],
            "hypotheses": [
                {
                    "root_cause_category": "bad_deploy",
                    "explanation": "errors began right after the deploy",
                    "supporting_findings": ["f0"],
                    "confidence": 0.85,
                    "rank": 1,
                }
            ],
        }
    )


def final(req: LLMRequest) -> LLMResponse:
    return text_response(report_for(evidence_ids_in(req)), cost=0.002)


def script() -> ScriptedLLM:
    tool_turn = LLMResponse(
        model="claude-sonnet-5-5",
        content=[{"type": "tool_use", "id": i, "name": n, "input": a} for i, n, a in CALLS],
        stop_reason="tool_use",
        usage=Usage(input_tokens=1200, output_tokens=90, cache_read_tokens=800, cost_usd=0.004),
    )
    return ScriptedLLM([tool_turn, final])


@dataclass
class Outcome:
    report: InvestigationReport | None
    error: str | None
    usage: Usage
    steps: int
    calls: list[tuple[str, str, bool, str | None, str | None]]
    evidence: dict[str, Any]
    incident_id: str


async def run_baseline(
    llm: LLMClient,
    adapters: ReadAdapters,
    *,
    prompts_dir: Path = PROMPTS_DIR,
    clock: Clock | None = None,
) -> Outcome:
    """The baseline agent end to end (runtime, real gateway, real read tools)."""
    registry = build_read_registry(ToolSettings(), adapters, clock=clock)
    cfg = load_agent_config(BASELINE_CONFIG, registry)
    now = utcnow()
    alert = Alert(
        source=AlertSource.ALERTMANAGER,
        fingerprint="fp-orders-5xx",
        alertname="HighErrorRate",
        service="orders",
        severity=Severity.CRITICAL,
        summary="orders 5xx above 5%",
        labels={"team": "shop"},
        started_at=now - timedelta(minutes=2),
    )
    inc = Incident(
        severity=Severity.CRITICAL,
        cluster="checkout",
        services=["orders"],
        alert_ids=[alert.id],
        window_closes_at=now + timedelta(seconds=60),
    )
    sink = FakeEvidenceSink()
    ctx = ToolContext(incident_id=inc.id, evidence=sink)
    run = await run_agent(
        cfg, build_task(inc, [alert]), ctx, llm, ToolGateway(registry), prompts_dir=prompts_dir
    )
    report, error = _summarize(run)
    return Outcome(
        report=report,
        error=error,
        usage=run.usage,
        steps=run.steps,
        calls=[(c.id, c.tool, c.ok, c.evidence_id, c.error) for c in run.tool_calls],
        evidence={k: (e.model_dump(), raw) for k, (e, raw) in sink.stored.items()},
        incident_id=inc.id,
    )


async def record(directory: Path, live: Live | None = None, at: datetime = AT) -> Outcome:
    live = live or Live()
    with record_run(
        directory,
        agent="baseline",
        scenario=SCENARIO,
        at=at,
        adapters=live_adapters(live),
        llm=script(),
        secrets=[API_KEY, ADMIN_TOKEN],
    ) as rec:
        return await run_baseline(rec.llm, rec.adapters)


@pytest.fixture
def no_network(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Every way out is a hard failure: sockets, httpx/httpx2 transports, a live LLM client."""
    attempts: list[str] = []

    def refuse(what: str) -> Any:
        def _raise(*a: Any, **kw: Any) -> Any:
            attempts.append(what)
            raise AssertionError(f"replay reached the network via {what}")

        return _raise

    monkeypatch.setattr(socket.socket, "connect", refuse("socket.connect"))
    monkeypatch.setattr(socket.socket, "connect_ex", refuse("socket.connect_ex"))
    monkeypatch.setattr(socket, "create_connection", refuse("socket.create_connection"))
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", refuse("httpx"))
    monkeypatch.setattr(httpx2.AsyncHTTPTransport, "handle_async_request", refuse("httpx2"))
    monkeypatch.setattr(AnthropicLLMClient, "__init__", refuse("AnthropicLLMClient"))
    monkeypatch.setattr(anthropic.AsyncAnthropic, "__init__", refuse("AsyncAnthropic"))
    yield attempts
    assert attempts == []


# --- round trip -------------------------------------------------------------------------


async def test_round_trip_is_identical_offline(tmp_path: Path, no_network: list[str]) -> None:
    directory = recording_dir("baseline", SCENARIO, root=tmp_path)
    live = Live()
    recorded = await record(directory, live)
    assert recorded.report is not None and recorded.error is None
    assert len(live.calls) == 5  # service_graph is config, not an adapter
    assert all(ok for _, _, ok, _, _ in recorded.calls)

    with replay_run(directory) as rp:
        assert isinstance(rp.llm, StrictReplayLLMClient)
        replayed = await run_baseline(rp.llm, rp.adapters)

    assert replayed.report == recorded.report
    assert replayed.incident_id == recorded.incident_id
    assert replayed.calls == recorded.calls  # same call ids -> same evidence ids
    assert replayed.evidence == recorded.evidence
    assert replayed.usage == recorded.usage and replayed.usage.cost_usd == pytest.approx(0.006)
    assert replayed.steps == recorded.steps == 2
    assert rp.llm.steps == 2 and rp.cassette.calls == 5


async def test_layout_and_meta(tmp_path: Path) -> None:
    directory = recording_dir("baseline", SCENARIO, root=tmp_path)
    await record(directory)
    assert directory == tmp_path / "baseline" / SCENARIO
    names = sorted(p.relative_to(directory).as_posix() for p in directory.rglob("*") if p.is_file())
    assert names[-2:] == ["meta.json", "tools.jsonl"]
    llm = [n for n in names if n.startswith("llm/")]
    assert len(llm) == 2 and all(re.fullmatch(r"llm/[0-9a-f]{64}\.json", n) for n in llm)
    assert not [p for p in (tmp_path / "baseline").iterdir() if p.name.startswith(".")]

    meta = json.loads((directory / "meta.json").read_text())
    assert meta["seed"] == f"{SCENARIO}@{AT.isoformat()}" and meta["at"].startswith(
        "2026-10-01T12:00:00"
    )
    assert meta["models"] == ["claude-sonnet-5-5", "fake"]  # requested + answering
    assert re.fullmatch(r"[0-9a-f]{64}", meta["prompt_sha256"])
    assert [f"llm/{k}.json" for k in meta["llm_steps"]] == sorted(llm, key=meta_order(meta))
    assert meta["tool_calls"] == 5

    lines = [json.loads(x) for x in (directory / "tools.jsonl").read_text().splitlines()]
    assert [x["seq"] for x in lines] == list(range(5))
    assert {x["method"] for x in lines} == {
        "metrics.instant",
        "metrics.range",
        "metrics.metric_names",
        "logs.query_range",
        "deploys.recent_deploys",
    }
    logs = next(x for x in lines if x["method"] == "logs.query_range")
    assert logs["args"] == {
        "query": '{service="orders"} |= "ERROR"',
        "start": "2026-10-01T11:45:00+00:00",
        "end": "2026-10-01T12:00:00+00:00",
        "limit": 50,
    }
    assert logs["key"] == tool_key("logs.query_range", logs["args"])


def meta_order(meta: dict[str, Any]) -> Any:
    order = {f"llm/{k}.json": i for i, k in enumerate(meta["llm_steps"])}
    return lambda n: order[n]


async def test_record_refuses_to_clobber_and_cleans_up_on_failure(tmp_path: Path) -> None:
    directory = tmp_path / "baseline" / SCENARIO
    await record(directory)
    with pytest.raises(FileExistsError):
        await record(directory)

    other = tmp_path / "baseline" / "s02"
    with (
        pytest.raises(RuntimeError, match="boom"),
        record_run(
            other,
            agent="baseline",
            scenario="s02",
            at=AT,
            adapters=live_adapters(Live()),
            llm=script(),
            secrets=[],
        ),
    ):
        raise RuntimeError("boom")
    assert not other.exists() and not list(other.parent.glob(".s02*"))


# --- stale detection --------------------------------------------------------------------


async def test_changed_prompt_line_is_stale(tmp_path: Path, no_network: list[str]) -> None:
    directory = tmp_path / "baseline" / SCENARIO
    await record(directory)
    prompts = tmp_path / "prompts"
    shutil.copytree(PROMPTS_DIR, prompts)
    text = (prompts / "baseline.md").read_text().splitlines()
    text[0] = text[0] + " (edited)"
    (prompts / "baseline.md").write_text("\n".join(text) + "\n")

    with pytest.raises(StaleRecordingError) as ei, replay_run(directory) as rp:
        await run_baseline(rp.llm, rp.adapters, prompts_dir=prompts)
    err = ei.value
    assert err.scenario == SCENARIO and err.step == "llm step 1"
    assert "system prompt changed" in str(err) and SCENARIO in str(err)


async def test_changed_tool_arg_is_stale(tmp_path: Path, no_network: list[str]) -> None:
    directory = tmp_path / "baseline" / SCENARIO
    await record(directory)

    # The read tools now compute their window one minute later: every adapter arg moves.
    with pytest.raises(StaleRecordingError) as ei, replay_run(directory) as rp:
        await run_baseline(rp.llm, rp.adapters, clock=lambda: AT + timedelta(minutes=1))
    err = ei.value
    assert err.scenario == SCENARIO and err.step.startswith("tool call ")
    assert "differ from the closest recorded call" in err.detail
    assert "recorded" in err.detail and SCENARIO in str(err)


async def test_extra_or_unknown_adapter_call_is_stale(tmp_path: Path) -> None:
    directory = tmp_path / "x"
    cas = Cassette.record(directory, live_adapters(Live()), scenario="x")
    await cas.adapters.deploys.recent_deploys("orders", 5)
    cas.save()

    replay = Cassette.replay(directory, scenario="x", graph=ServiceGraph.load())
    await replay.adapters.deploys.recent_deploys("orders", 5)
    # Recorded once; a second identical call is not in the cassette.
    with pytest.raises(StaleRecordingError, match="recorded 1 time") as ei:
        await replay.adapters.deploys.recent_deploys("orders", 5)
    assert ei.value.step == "tool call 2 (deploys.recent_deploys)"
    with pytest.raises(StaleRecordingError, match="never called in the recording"):
        await replay.adapters.metrics.metric_names()
    with pytest.raises(StaleRecordingError, match=r"limit=3 \(recorded 5\)"):
        await replay.adapters.deploys.recent_deploys("orders", 3)
    # The gateway would turn these into tool errors; the cassette keeps them for the LLM side.
    assert len(replay.stale) == 3
    with pytest.raises(StaleRecordingError, match="recorded 1 time"):
        replay.raise_if_stale()
    with pytest.raises(RuntimeError, match="no real adapters"):
        replay.real()


async def test_missing_recording_is_stale(tmp_path: Path) -> None:
    with (
        pytest.raises(StaleRecordingError, match=r"no meta\.json") as ei,
        replay_run(tmp_path / "baseline" / "nope"),
    ):
        pass
    assert ei.value.step == "setup"


async def test_replay_with_no_llm_transport_never_calls_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every Anthropic client the code could build has a transport that raises on any request.

    Replay mode (``FIREBREAK_LLM_MODE=replay`` via ``build_llm_client``) misses with a
    ``StaleRecordingError`` naming the scenario and step; the transport is never touched.
    """
    hits: list[httpx2.Request] = []

    def explode(request: httpx2.Request) -> httpx2.Response:
        hits.append(request)
        raise AssertionError("replay made a live LLM request")

    real_init = anthropic.AsyncAnthropic.__init__

    def raising_client(self: anthropic.AsyncAnthropic, *a: Any, **kw: Any) -> None:
        kw["http_client"] = httpx2.AsyncClient(transport=httpx2.MockTransport(explode))
        kw.setdefault("api_key", "test")
        real_init(self, *a, **kw)

    monkeypatch.setattr(anthropic.AsyncAnthropic, "__init__", raising_client)
    req = LLMRequest(
        model="claude-sonnet-5-5", system="s", messages=[{"role": "user", "content": "x"}]
    )

    replay = build_llm_client(
        LLMSettings(mode=LLMMode.REPLAY, fixture_dir=tmp_path / "llm"), scenario=SCENARIO
    )
    assert isinstance(replay, StrictReplayLLMClient)
    with pytest.raises(StaleRecordingError) as ei:
        await replay.complete(req)
    assert isinstance(ei.value, FixtureMissing)  # existing callers still catch it
    assert ei.value.scenario == SCENARIO and ei.value.step == "llm step 1"
    assert SCENARIO in str(ei.value) and "llm step 1" in str(ei.value)
    assert hits == []

    # Control: the patched transport does catch a live call (loudly, as an LLMError).
    live = build_llm_client(LLMSettings(mode=LLMMode.LIVE, max_retries=0))
    with pytest.raises(LLMError, match="Connection error"):
        await live.complete(req)
    assert len(hits) == 1


# --- errors and ordering ----------------------------------------------------------------


async def test_backend_error_is_recorded_and_replayed_as_the_same_type(tmp_path: Path) -> None:
    directory = tmp_path / "baseline" / SCENARIO
    live = Live()
    live.timeout_logs = True
    recorded = await record(directory, live)
    logs_call = next(c for c in recorded.calls if c[1] == "logql_query")
    assert logs_call[2] is False and logs_call[4] is not None and logs_call[4].startswith("timeout")

    entry = next(
        json.loads(x)
        for x in (directory / "tools.jsonl").read_text().splitlines()
        if "logs.query_range" in x
    )
    assert entry["error"]["type"] == "BackendTimeout" and "result" not in entry

    with replay_run(directory) as rp:
        replayed = await run_baseline(rp.llm, rp.adapters)
    assert replayed.calls == recorded.calls and replayed.evidence == recorded.evidence


async def test_unknown_exception_replays_with_same_name_and_text(tmp_path: Path) -> None:
    class Weird(RuntimeError):
        pass

    class Breaks(Live):
        async def metric_names(self) -> list[str]:
            raise Weird("parser blew up")

    directory = tmp_path / "x"
    cas = Cassette.record(directory, live_adapters(Breaks()), scenario="x")
    with pytest.raises(Weird):
        await cas.adapters.metrics.metric_names()
    cas.save()
    replay = Cassette.replay(directory, scenario="x")
    with pytest.raises(Exception, match="parser blew up") as ei:
        await replay.adapters.metrics.metric_names()
    assert type(ei.value).__name__ == "Weird"


async def test_deadline_cut_off_call_replays_as_a_hang(tmp_path: Path) -> None:
    class Hangs(Live):
        async def metric_names(self) -> list[str]:
            await asyncio.sleep(10)
            raise AssertionError("unreachable")

    cas = Cassette.record(tmp_path, live_adapters(Hangs()), scenario="t")
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await cas.adapters.metrics.metric_names()
    cas.save()
    assert json.loads((tmp_path / "tools.jsonl").read_text())["cancelled"] is True

    replay = Cassette.replay(tmp_path, scenario="t", graph=ServiceGraph.load())
    with pytest.raises(TimeoutError):  # the gateway's deadline fires again, no result leaks
        async with asyncio.timeout(0.05):
            await replay.adapters.metrics.metric_names()
    assert replay.stale == []


def test_tool_key_normalizes_timezones() -> None:
    from zoneinfo import ZoneInfo

    from firebreak.evals.cassette import _dt

    local = AT.astimezone(ZoneInfo("America/Chicago"))
    assert _dt(local) == _dt(AT) == "2026-10-01T12:00:00+00:00"
    a = tool_key("metrics.instant", {"query": "up", "at": _dt(AT)})
    b = tool_key("metrics.instant", {"at": _dt(local), "query": "up"})
    assert a == b
    with pytest.raises(ValueError, match="timezone-aware"):
        _dt(AT.replace(tzinfo=None))


async def test_results_are_released_in_call_start_order(tmp_path: Path) -> None:
    live = Live()
    cas = Cassette.record(tmp_path, live_adapters(live), scenario="o")
    done: list[str] = []

    async def go(name: str, coro: Any) -> None:
        await coro
        done.append(name)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(go("a", cas.adapters.metrics.metric_names()))  # slowest
        tg.create_task(go("b", cas.adapters.deploys.recent_deploys("orders", 1)))
        tg.create_task(go("c", cas.adapters.metrics.instant("up", AT)))  # fastest
    assert done == ["a", "b", "c"]


# --- secrets ----------------------------------------------------------------------------


def _anthropic_handler(request: httpx2.Request) -> httpx2.Response:
    body = json.loads(request.content)
    assert request.headers["authorization"] == f"Bearer {API_KEY}"
    if len(body["messages"]) == 1:
        content: list[dict[str, Any]] = [
            {"type": "tool_use", "id": i, "name": n, "input": a} for i, n, a in CALLS[:3]
        ]
        stop = "tool_use"
    else:
        ids = sorted(set(re.findall(r"ev_[0-9a-f]{32}", json.dumps(body["messages"]))))
        content = [{"type": "text", "text": report_for(ids)}]
        stop = "end_turn"
    return httpx2.Response(
        200,
        json={
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": body["model"],
            "content": content,
            "stop_reason": stop,
            "stop_sequence": None,
            "usage": {"input_tokens": 100, "output_tokens": 20},
        },
    )


def _backend_handler(echo_token: bool) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/admin/deploys":
            assert request.headers["authorization"] == f"Bearer {ADMIN_TOKEN}"
            msg = f"leaked {ADMIN_TOKEN}" if echo_token else "bump"
            return httpx.Response(
                200,
                json=[
                    {
                        "service": "orders",
                        "version": "v2",
                        "deployed_at": "2026-10-01T11:50:00Z",
                        "message": msg,
                    }
                ],
            )
        if path == "/loki/api/v1/query_range":
            return httpx.Response(
                200,
                json={
                    "status": "success",
                    "data": {
                        "resultType": "streams",
                        "result": [
                            {
                                "stream": {"service_name": "orders"},
                                "values": [["1759319940000000000", "ERROR x"]],
                            }
                        ],
                    },
                },
            )
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"metric": {"service": "orders"}, "value": [1759320000, "0.2"]}],
                },
            },
        )

    return handler


def real_adapters(echo_token: bool = False) -> ReadAdapters:
    t = httpx.MockTransport(_backend_handler(echo_token))
    return ReadAdapters(
        metrics=PrometheusAdapter("http://prom:9090", timeout_s=1, transport=t),
        logs=LokiAdapter("http://loki:3100", timeout_s=1, transport=t),
        deploys=ShopfloorAdapter(
            {"orders": "http://orders:8101"}, admin_token=ADMIN_TOKEN, timeout_s=1, transport=t
        ),
        graph=ServiceGraph.load(),
    )


def real_llm() -> AnthropicLLMClient:
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(_anthropic_handler))
    sdk = anthropic.AsyncAnthropic(api_key=API_KEY, http_client=http, max_retries=0)
    return AnthropicLLMClient(LLMSettings(max_retries=0), client=sdk, limiter=TokenBucket(6000))


async def test_secret_scan_with_real_clients(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real adapters and LLM client (mock transports) carry both secrets; files don't."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", API_KEY)
    monkeypatch.setenv("FIREBREAK_TOOLS_SHOPFLOOR_ADMIN_TOKEN", ADMIN_TOKEN)
    directory = tmp_path / "baseline" / SCENARIO
    adapters = real_adapters()
    with record_run(
        directory, agent="baseline", scenario=SCENARIO, at=AT, adapters=adapters, llm=real_llm()
    ) as rec:  # secrets=None: default_secrets() reads both from the environment
        out = await run_baseline(rec.llm, rec.adapters)
    await adapters.aclose()
    assert out.report is not None, out.error
    files = [p for p in directory.rglob("*") if p.is_file()]
    assert len(files) == 4  # 2 llm fixtures, tools.jsonl, meta.json
    for p in files:
        text = p.read_text()
        assert API_KEY not in text and ADMIN_TOKEN not in text, p
    assert find_secrets(directory, [API_KEY, ADMIN_TOKEN]) == []

    # And the scan does catch a planted one (raw or JSON-escaped).
    (directory / "tools.jsonl").write_text(json.dumps({"x": f'"{ADMIN_TOKEN}"'}) + "\n")
    assert find_secrets(directory, [ADMIN_TOKEN]) == [directory / "tools.jsonl"]


async def test_secret_in_backend_output_aborts_the_recording(tmp_path: Path) -> None:
    directory = tmp_path / "baseline" / SCENARIO
    adapters = real_adapters(echo_token=True)
    with (
        pytest.raises(SecretInRecordingError, match=r"tools\.jsonl"),
        record_run(
            directory,
            agent="baseline",
            scenario=SCENARIO,
            at=AT,
            adapters=adapters,
            llm=real_llm(),
            secrets=[API_KEY, ADMIN_TOKEN],
        ) as rec,
    ):
        await run_baseline(rec.llm, rec.adapters)
    await adapters.aclose()
    assert not directory.exists() and not list(directory.parent.glob(".*"))


# --- Postgres evidence sink (db) ----------------------------------------------------------


async def _seed_incident(sessions: Any) -> str:
    now = utcnow()
    alert = Alert(
        source=AlertSource.ALERTMANAGER,
        fingerprint="fp-orders-5xx",
        alertname="HighErrorRate",
        service="orders",
        severity=Severity.CRITICAL,
        summary="orders 5xx above 5%",
        started_at=now - timedelta(minutes=2),
    )
    assert await AlertRepository(sessions).insert_if_new(alert)
    inc = await IncidentRepository(sessions).create(
        Incident(
            severity=Severity.CRITICAL,
            cluster="checkout",
            services=["orders"],
            alert_ids=[alert.id],
            opened_at=now,
            window_closes_at=now + timedelta(seconds=60),
        )
    )
    return inc.id


async def _evidence_rows(engine: AsyncEngine) -> list[tuple[Any, ...]]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT id, incident_id, tool_call_id, kind, query, summary, raw_ref "
                "FROM evidence ORDER BY id"
            )
        )
        return [tuple(r) for r in rows]


@pytest.mark.db
async def test_round_trip_through_investigate_and_postgres(
    tmp_path: Path, engine: AsyncEngine, sessions: Any
) -> None:
    """``investigate`` + PostgresEvidenceSink: same report, usage, calls and evidence rows."""
    directory = tmp_path / "baseline" / SCENARIO
    with record_run(
        directory,
        agent="baseline",
        scenario=SCENARIO,
        at=AT,
        adapters=live_adapters(Live()),
        llm=script(),
        secrets=[API_KEY, ADMIN_TOKEN],
    ) as rec:
        inc_id = await _seed_incident(sessions)
        recorded = await investigate(
            inc_id,
            llm=rec.llm,
            sessions=sessions,
            tools_settings=ToolSettings(),
            registry=build_read_registry(ToolSettings(), rec.adapters),
        )
    recorded_rows = await _evidence_rows(engine)
    assert recorded.ok and len(recorded_rows) == 6

    async with engine.begin() as conn:
        tables = ", ".join(t.name for t in Base.metadata.sorted_tables)
        await conn.execute(text(f"TRUNCATE {tables} CASCADE"))

    with replay_run(directory) as rp:
        assert await _seed_incident(sessions) == inc_id
        replayed = await investigate(
            inc_id,
            llm=rp.llm,
            sessions=sessions,
            tools_settings=ToolSettings(),
            registry=build_read_registry(ToolSettings(), rp.adapters),
        )

    assert replayed.report == recorded.report
    assert replayed.usage == recorded.usage and replayed.steps == recorded.steps

    def no_timing(calls: list[Any]) -> list[Any]:
        return [c.model_copy(update={"duration_ms": 0}) for c in calls]

    assert no_timing(replayed.tool_calls) == no_timing(recorded.tool_calls)
    assert await _evidence_rows(engine) == recorded_rows


# --- T-046: hardening -------------------------------------------------------------------


def test_secret_scan_finds_base64_and_url_encoded_forms(tmp_path: Path) -> None:
    import base64
    import urllib.parse

    secret = "sk-ant-abc/def+ghi=jkl"
    forms = {
        "b64.txt": base64.b64encode(secret.encode()).decode(),
        "b64url.txt": base64.urlsafe_b64encode(secret.encode()).decode().rstrip("="),
        "pct.txt": urllib.parse.quote(secret, safe=""),
        "plus.txt": urllib.parse.quote_plus(secret),
        "raw.txt": secret,
    }
    for name, form in forms.items():
        (tmp_path / name).write_text(f"prefix {form} suffix")
    (tmp_path / "clean.txt").write_text("nothing to see")
    assert find_secrets(tmp_path, [secret]) == sorted(tmp_path / n for n in forms)


def test_short_secret_warns_and_is_not_scanned(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (tmp_path / "f.txt").write_text("hunter2 is here")
    with caplog.at_level("WARNING", logger="firebreak.evals.cassette"):
        assert find_secrets(tmp_path, ["hunter2"]) == []
    assert "too short to scan" in caplog.text
    assert "hunter2" not in caplog.text  # the value is never logged


async def test_overwrite_failure_mid_swap_keeps_the_old_recording(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = recording_dir("baseline", SCENARIO, root=tmp_path)
    await record(directory)
    before = {p.name: p.read_bytes() for p in directory.rglob("*") if p.is_file()}

    real_rename = Path.rename

    def flaky(self: Path, target: Any) -> Path:
        if ".recording." in self.name and Path(target) == directory:
            raise OSError("disk went away")
        return real_rename(self, target)

    monkeypatch.setattr(Path, "rename", flaky)
    with (
        pytest.raises(OSError, match="disk went away"),
        record_run(
            directory,
            agent="baseline",
            scenario=SCENARIO,
            at=AT,
            adapters=live_adapters(Live()),
            llm=script(),
            secrets=[],
            overwrite=True,
        ) as rec,
    ):
        await run_baseline(rec.llm, rec.adapters)
    monkeypatch.undo()

    after = {p.name: p.read_bytes() for p in directory.rglob("*") if p.is_file()}
    assert after == before
    assert not list(directory.parent.glob(".*"))  # staging and aside are gone
    with replay_run(directory) as rp:
        await run_baseline(rp.llm, rp.adapters)


async def test_overwrite_succeeds_and_staging_names_are_unique(tmp_path: Path) -> None:
    directory = recording_dir("baseline", SCENARIO, root=tmp_path)
    await record(directory)
    with record_run(
        directory,
        agent="baseline",
        scenario=SCENARIO,
        at=AT,
        adapters=live_adapters(Live()),
        llm=script(),
        secrets=[],
        overwrite=True,
    ) as rec:
        staging = rec.cassette.directory
        assert staging.name.startswith(f".{SCENARIO}.recording.") and staging != directory
        await run_baseline(rec.llm, rec.adapters)
    assert not list(directory.parent.glob(".*"))
    with replay_run(directory) as rp:
        await run_baseline(rp.llm, rp.adapters)


async def test_replay_fails_when_recorded_tool_calls_are_not_consumed(tmp_path: Path) -> None:
    directory = recording_dir("baseline", SCENARIO, root=tmp_path)
    await record(directory)
    with (
        pytest.raises(StaleRecordingError, match="ended after 0 tool calls") as ei,
        replay_run(directory),
    ):
        pass  # the run makes no adapter calls at all
    assert ei.value.step == "tool call 1"


def test_default_secrets_includes_the_openai_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from firebreak.evals.cassette import default_secrets

    key = "sk-openai-compat-0123456789abcdef"
    monkeypatch.delenv("FIREBREAK_LLM_OPENAI_API_KEY", raising=False)
    assert key not in default_secrets()
    monkeypatch.setenv("FIREBREAK_LLM_OPENAI_API_KEY", key)
    assert key in default_secrets()


def test_recording_containing_the_openai_key_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from firebreak.evals.cassette import default_secrets

    key = "sk-openai-compat-0123456789abcdef"
    monkeypatch.setenv("FIREBREAK_LLM_OPENAI_API_KEY", key)
    (tmp_path / "tools.jsonl").write_text(json.dumps({"x": key}) + "\n")
    assert find_secrets(tmp_path, default_secrets()) == [tmp_path / "tools.jsonl"]


# --- unique seed per recording (T-057) ---------------------------------------------------


async def test_rerecord_with_different_at_has_disjoint_evidence_ids(tmp_path: Path) -> None:
    first = await record(recording_dir("baseline", SCENARIO, root=tmp_path))
    second = await record(
        recording_dir("candidate", SCENARIO, root=tmp_path), at=AT + timedelta(seconds=1)
    )
    assert first.evidence and second.evidence
    assert not set(first.evidence) & set(second.evidence)
    assert first.incident_id != second.incident_id


async def test_seed_with_at_and_offset_round_trips_and_replays(tmp_path: Path) -> None:
    at = datetime(2026, 10, 29, 18, 4, 5, 123456, tzinfo=UTC)
    directory = recording_dir("baseline", SCENARIO, root=tmp_path)
    recorded = await record(directory, at=at)
    seed = json.loads((directory / "meta.json").read_text())["seed"]
    assert seed == f"{SCENARIO}@2026-10-29T18:04:05.123456+00:00"
    with replay_run(directory) as rp:
        assert rp.meta.seed == seed and rp.meta.at == at
        replayed = await run_baseline(rp.llm, rp.adapters)
    assert replayed.evidence == recorded.evidence and replayed.calls == recorded.calls


@pytest.fixture
def old_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """Behave as before T-057: ids seeded by the bare scenario."""
    import firebreak.evals.cassette as cassette_mod

    real = cassette_mod.deterministic

    def old(seed: str, *, at: datetime) -> Any:
        return real(seed.split("@")[0], at=at)

    monkeypatch.setattr(cassette_mod, "deterministic", old)


def _rewrite_seed_as_scenario(directory: Path) -> None:
    meta_path = directory / "meta.json"
    meta = json.loads(meta_path.read_text())
    meta["seed"] = SCENARIO
    meta_path.write_text(json.dumps(meta))


async def test_old_format_meta_seed_equal_scenario_still_replays(
    tmp_path: Path, old_format: None
) -> None:
    directory = recording_dir("baseline", SCENARIO, root=tmp_path)
    recorded = await record(directory)
    _rewrite_seed_as_scenario(directory)
    with replay_run(directory) as rp:
        assert rp.meta.seed == SCENARIO
        replayed = await run_baseline(rp.llm, rp.adapters)
    assert replayed.evidence == recorded.evidence and replayed.calls == recorded.calls


async def test_old_format_recordings_of_one_scenario_share_ids(
    tmp_path: Path, old_format: None
) -> None:
    """Replay seeds ids by meta.seed: old-format agents collide per scenario."""
    ids = []
    for agent in ("baseline", "candidate"):
        directory = recording_dir(agent, SCENARIO, root=tmp_path)
        await record(directory)
        _rewrite_seed_as_scenario(directory)
        with replay_run(directory) as rp:
            ids.append(set((await run_baseline(rp.llm, rp.adapters)).evidence))
    assert ids[0] and ids[0] == ids[1]

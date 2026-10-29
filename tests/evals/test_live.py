"""T-051 live record driver. No network, no Docker: MockTransport and fakes."""

import asyncio
import dataclasses
import logging
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from firebreak.evals.__main__ import main
from firebreak.evals.cassette import SecretInRecordingError
from firebreak.evals.live import (
    DirtyStackError,
    DriveConfig,
    DriveSettings,
    DriveState,
    IncidentSource,
    IncidentTimeoutError,
    LiveDriveError,
    LiveIncident,
    LoadGenerator,
    OrdersRssGuardError,
    apply_setup,
    drive_scenario,
    make_admin_client,
)
from firebreak.evals.runner import EvalRunError, RunOutcome, ScenarioOutcome, record_batch
from firebreak.evals.scenario import Scenario, load_scenarios
from firebreak.llm.errors import LLMError
from firebreak.models.common import utcnow
from firebreak.tools.settings import ToolSettings

SCENARIOS = load_scenarios(Path("evals/scenarios"))
S1, S2 = SCENARIOS["S1"], SCENARIOS["S2"]
CFG = DriveConfig(
    gateway_url="http://gw",
    orders_url="http://orders",
    inventory_url="http://inv",
    alertmanager_url="http://am",
    prometheus_url="http://prom",
    incident_timeout_s=0.3,
    clean_timeout_s=0.3,
    poll_interval_s=0.01,
    clean_polls=2,
    max_concurrency=2,
)


class FakeStack:
    """Records admin calls; alerts are scripted by the test."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []  # (host, path, body)
        self.am_alerts: list[dict[str, Any]] = []
        self.prom_alerts: list[dict[str, Any]] = []
        self.fail_paths: set[str] = set()
        self.auth: set[str | None] = set()
        self.gateway_hits = 0
        self.rss_bytes: float = 100 * 1024 * 1024
        self.rss_status = 200
        self.rss_empty = False
        self.rss_reads = 0
        self.rss_blind_script: Callable[[int], bool] | None = None  # read number -> blind?

    def handler(self, request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        if host == "gw":
            self.gateway_hits += 1
            if path == "/api/products":
                return httpx.Response(200, json=[{"id": 1}, {"id": 2}])
            return httpx.Response(500)
        if host == "am":
            return httpx.Response(200, json=self.am_alerts)
        if host == "prom" and path == "/api/v1/query":
            self.rss_reads += 1
            if self.rss_status != 200:
                return httpx.Response(self.rss_status)
            blind = self.rss_blind_script(self.rss_reads) if self.rss_blind_script else False
            result = [] if (self.rss_empty or blind) else [{"value": [0, str(self.rss_bytes)]}]
            return httpx.Response(200, json={"data": {"result": result}})
        if host == "prom":
            return httpx.Response(200, json={"data": {"alerts": self.prom_alerts}})
        self.auth.add(request.headers.get("authorization"))
        self.calls.append((host, path, request.content.decode()))
        if path in self.fail_paths:
            return httpx.Response(503)
        return httpx.Response(200, json={})

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))

    def fire(self, alertname: str, service: str) -> None:
        self.am_alerts.append(
            {"status": {"state": "active"}, "labels": {"alertname": alertname, "service": service}}
        )

    def clear(self) -> None:
        self.am_alerts.clear()
        self.prom_alerts.clear()


class FakeSource:
    def __init__(self, incidents: Sequence[LiveIncident] = ()) -> None:
        self.incidents = list(incidents)

    async def incidents_since(self, since: datetime) -> Sequence[LiveIncident]:
        return [i for i in self.incidents if i.opened_at > since]


def incident(inc_id: str, *alerts: tuple[str, str], after: float = 5.0) -> LiveIncident:
    return LiveIncident(inc_id, utcnow() + timedelta(seconds=after), alerts)


def outcome(sid: str) -> ScenarioOutcome:
    return ScenarioOutcome(sid, None)


def paths(stack: FakeStack) -> list[tuple[str, str]]:
    return [(h, p) for h, p, _ in stack.calls]


RESET = [
    ("orders", "/admin/chaos/reset"),
    ("inv", "/admin/chaos/reset"),
    ("orders", "/admin/deploy"),
]


async def drive(
    stack: FakeStack,
    sc: Scenario,
    source: IncidentSource,
    record: Any,
    state: DriveState | None = None,
    cfg: DriveConfig = CFG,
) -> ScenarioOutcome:
    async with stack.client() as plain, stack.client() as admin:
        return await drive_scenario(
            sc, record, cfg=cfg, admin=admin, plain=plain, incidents=source, state=state
        )


async def test_happy_path_order_and_teardown() -> None:
    stack = FakeStack()
    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    seen: list[str] = []

    async def record(inc_id: str) -> ScenarioOutcome:
        seen.append(inc_id)
        return outcome("S1")

    out = await drive(stack, S1, src, record)
    assert out.scenario_id == "S1" and seen == ["inc_1"]
    c = stack.calls
    assert [(h, p) for h, p, _ in c[:3]] == RESET
    assert "v1.0.0" in c[3][2]  # prior_deploy
    assert "v2.0.0-bad" in c[4][2]  # deploy
    assert [(h, p) for h, p, _ in c[5:]] == RESET  # teardown
    assert stack.gateway_hits > 1  # load ran


async def test_setup_order_reset_prior_chaos_deploy() -> None:
    stack = FakeStack()
    sc = S1.model_copy(
        update={
            "setup": S1.setup.model_copy(
                update={"chaos": {"orders.db_pool_size": 2, "inventory.unavailable": True}}
            )
        }
    )
    async with stack.client() as admin:
        await apply_setup(admin, sc, CFG)
    kinds = [
        "reset" if p.endswith("reset") else ("chaos" if p.endswith("chaos") else "deploy")
        for _, p, _ in stack.calls
    ]
    assert kinds == ["reset", "reset", "deploy", "deploy", "chaos", "chaos", "deploy"]
    chaos = [(h, b) for h, p, b in stack.calls if p == "/admin/chaos"]
    assert chaos[0][0] == "orders" and "orders.db_pool_size" in chaos[0][1]
    assert chaos[1][0] == "inv" and "inventory.unavailable" in chaos[1][1]


async def test_dirty_stack_refused_before_any_admin_call() -> None:
    stack = FakeStack()
    stack.fire("HighErrorRate", "orders")
    called = False

    async def record(inc_id: str) -> ScenarioOutcome:
        nonlocal called
        called = True
        return outcome("S1")

    with pytest.raises(DirtyStackError, match="HighErrorRate"):
        await drive(stack, S1, FakeSource(), record)
    assert stack.calls == [] and not called
    stack.clear()
    stack.prom_alerts.append(
        {"state": "pending", "labels": {"alertname": "HighErrorRate", "service": "orders"}}
    )
    with pytest.raises(DirtyStackError):
        await drive(stack, S1, FakeSource(), record)
    assert stack.calls == []


async def test_incident_timeout_is_typed_and_teardown_runs() -> None:
    stack = FakeStack()

    async def record(inc_id: str) -> ScenarioOutcome:
        raise AssertionError("must not record")

    with pytest.raises(IncidentTimeoutError, match="HighErrorRate"):
        await drive(stack, S1, FakeSource(), record)
    assert paths(stack)[-3:] == RESET


async def test_incident_must_match_and_be_new() -> None:
    stack = FakeStack()
    src = FakeSource(
        [
            incident("inc_old", ("HighErrorRate", "orders"), after=-60),  # before apply
            incident("inc_other", ("ServiceDown", "inventory")),  # wrong alert
            incident("inc_wrong_svc", ("HighErrorRate", "inventory")),  # wrong service
            incident("inc_ok", ("DBPoolSaturated", "orders"), ("HighErrorRate", "orders")),
        ]
    )
    seen: list[str] = []

    async def record(inc_id: str) -> ScenarioOutcome:
        seen.append(inc_id)
        return outcome("S1")

    await drive(stack, S1, src, record)
    assert seen == ["inc_ok"]


@pytest.mark.parametrize(
    "exc", [RuntimeError("boom"), KeyboardInterrupt(), asyncio.CancelledError(), LLMError("x")]
)
async def test_teardown_runs_on_every_failure_path(exc: BaseException) -> None:
    stack = FakeStack()
    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])

    async def record(inc_id: str) -> ScenarioOutcome:
        raise exc

    with pytest.raises(type(exc)):
        await drive(stack, S1, src, record)
    assert paths(stack)[-3:] == RESET


async def test_apply_failure_still_tears_down() -> None:
    stack = FakeStack()
    stack.fail_paths = {"/admin/deploy"}

    async def record(inc_id: str) -> ScenarioOutcome:
        raise AssertionError("must not record")

    with pytest.raises(LiveDriveError):
        await drive(stack, S1, FakeSource(), record)
    assert sum(1 for _, p in paths(stack) if p == "/admin/chaos/reset") >= 2


async def test_failed_reset_blocks_the_next_scenario(caplog: pytest.LogCaptureFixture) -> None:
    stack = FakeStack()
    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    state = DriveState()

    async def record(inc_id: str) -> ScenarioOutcome:
        stack.fail_paths = {"/admin/chaos/reset"}  # the stack breaks before teardown
        return outcome("S1")

    with caplog.at_level(logging.ERROR):
        await drive(stack, S1, src, record, state)
    assert state.broken and "reset step" in caplog.text
    with pytest.raises(DirtyStackError, match="earlier teardown failed"):
        await drive(stack, S2, src, record, state)


async def test_alerts_that_never_clear_are_logged_and_block_next(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stack = FakeStack()
    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    state = DriveState()

    async def record(inc_id: str) -> ScenarioOutcome:
        stack.fire("HighErrorRate", "orders")  # appears and never goes away
        return outcome("S1")

    with caplog.at_level(logging.ERROR):
        await drive(stack, S1, src, record, state)
    assert "still active" in caplog.text
    assert state.problems
    with pytest.raises(DirtyStackError):
        await drive(stack, S1, src, record, state)


async def test_clean_wait_succeeds_once_alerts_clear() -> None:
    stack = FakeStack()
    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    state = DriveState()

    async def record(inc_id: str) -> ScenarioOutcome:
        stack.fire("HighErrorRate", "orders")
        asyncio.get_running_loop().call_later(0.05, stack.clear)
        return outcome("S1")

    await drive(stack, S1, src, record, state)
    assert state.problems == []


async def test_admin_token_only_on_admin_client() -> None:
    tools = ToolSettings(shopfloor_admin_token="s3cret")  # type: ignore[arg-type]
    stack = FakeStack()
    async with make_admin_client(tools) as admin:
        admin._transport = httpx.MockTransport(stack.handler)  # type: ignore[attr-defined]
        await admin.post("http://orders/admin/chaos/reset")
    assert stack.auth == {"Bearer s3cret"}
    assert "s3cret" not in repr(tools)


async def test_load_generator_bounds_concurrency_and_counts_errors() -> None:
    stack = FakeStack()
    async with stack.client() as c:
        gen = LoadGenerator(c, "http://gw", rps=1000, cart_size=3, max_concurrency=2)
        assert gen.workers == 2
        await gen.prepare()
        gen.start()
        await asyncio.sleep(0.05)
        await gen.stop()
    assert gen.http_errors > 0  # POSTs 500 in the fake: counted, not raised


async def test_load_generator_surfaces_crashed_workers() -> None:
    first = True

    def bad(request: httpx.Request) -> httpx.Response:
        nonlocal first
        if first:
            first = False
            return httpx.Response(200, json=[{"id": 1}])
        raise ValueError("worker bug")

    async with httpx.AsyncClient(transport=httpx.MockTransport(bad)) as c:
        gen = LoadGenerator(c, "http://gw", rps=10, max_concurrency=1)
        await gen.prepare()
        gen.start()
        await asyncio.sleep(0.05)
        with pytest.raises(LiveDriveError, match="unhealthy"):
            await gen.stop()


def test_config_from_settings() -> None:
    cfg = DriveConfig.from_settings(ToolSettings(), DriveSettings())
    assert cfg.incident_timeout_s == 600 and cfg.orders_url.endswith(":8101")


# --- per-scenario errors ------------------------------------------------------------------


async def test_batch_continues_after_a_scenario_errors() -> None:
    async def one(sc: Scenario) -> ScenarioOutcome:
        if sc.id == "S1":
            raise LLMError("context overflow")
        return outcome(sc.id)

    out = await record_batch([S1, S2], one)
    assert [o.scenario_id for o in out.outcomes] == ["S2"]
    assert len(out.errors) == 1
    assert out.errors[0][0] == "S1" and "context overflow" in out.errors[0][1]


@pytest.mark.parametrize(
    "exc", [SecretInRecordingError("leak"), asyncio.CancelledError(), KeyboardInterrupt()]
)
async def test_secret_failure_and_cancellation_abort_the_batch(exc: BaseException) -> None:
    async def one(sc: Scenario) -> ScenarioOutcome:
        raise exc

    with pytest.raises(type(exc)):
        await record_batch([S1, S2], one)


async def test_timeouts_and_eval_errors_are_per_scenario() -> None:
    async def one(sc: Scenario) -> ScenarioOutcome:
        raise IncidentTimeoutError("none") if sc.id == "S1" else EvalRunError("not found")

    out = await record_batch([S1, S2], one)
    assert [e[0] for e in out.errors] == ["S1", "S2"]


# --- CLI ----------------------------------------------------------------------------------


def test_drive_and_incident_are_exclusive(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as ei:
        main(["run", "--mode", "record", "--drive", "--incident", "S1=inc_1"])
    assert ei.value.code == 2
    assert "not allowed with" in capsys.readouterr().err


def test_drive_needs_record_mode(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", "--mode", "replay", "--drive"]) == 1
    assert "--drive only applies" in capsys.readouterr().err


def test_drive_still_needs_the_live_flag(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("FIREBREAK_EVAL_LIVE", raising=False)
    assert main(["run", "--mode", "record", "--drive"]) == 1
    assert "FIREBREAK_EVAL_LIVE=1" in capsys.readouterr().err


def test_cli_exit_code_nonzero_when_a_scenario_errored(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from firebreak.evals import __main__ as cli

    async def fake_record(*a: object, **k: object) -> RunOutcome:
        return RunOutcome(errors=[("S1", "LLMError: overflow")])

    async def noop(*a: object, **k: object) -> None:
        return None

    monkeypatch.setenv("FIREBREAK_EVAL_LIVE", "1")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    monkeypatch.setattr(cli, "_record", fake_record)
    monkeypatch.setattr(cli, "ensure_eval_database", noop)
    code = main(
        [
            "--database-url",
            "postgresql+asyncpg://x:x@127.0.0.1:1/x_eval",
            "run",
            "--mode",
            "record",
            "--drive",
            "--scenarios",
            "s01",
        ]
    )
    assert code == 1
    assert "S1: error: LLMError: overflow" in capsys.readouterr().out


async def test_unrelated_alert_blocks_the_preflight() -> None:
    stack = FakeStack()
    stack.fire("DiskFull", "inventory")  # not one of S1's alerts

    async def record(inc_id: str) -> ScenarioOutcome:
        raise AssertionError("must not record")

    with pytest.raises(DirtyStackError, match="DiskFull"):
        await drive(stack, S1, FakeSource(), record)
    assert stack.calls == []


async def test_clear_timeout_is_sticky_even_if_the_stack_clears_later() -> None:
    stack = FakeStack()
    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    state = DriveState()

    async def stuck(inc_id: str) -> ScenarioOutcome:
        stack.fire("HighErrorRate", "orders")
        return outcome("S1")

    async def fine(inc_id: str) -> ScenarioOutcome:
        return outcome("S1")

    await drive(stack, S1, src, stuck, state)
    assert len(state.clear_timeouts) == 1
    stack.clear()
    await drive(stack, S1, src, fine, state)  # clean again: the next scenario ran
    assert state.uncleared == set()
    assert any("still active" in p for p in state.problems)  # but the run still fails


async def test_clean_wait_also_waits_for_unrelated_alerts() -> None:
    stack = FakeStack()
    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    state = DriveState()

    async def record(inc_id: str) -> ScenarioOutcome:
        stack.fire("SomethingElse", "gateway")
        return outcome("S1")

    await drive(stack, S1, src, record, state)
    assert state.clear_timeouts


async def test_cancel_during_slow_reset_still_finishes_the_reset() -> None:
    stack = FakeStack()
    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    resets = 0
    slow = False
    in_record = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal resets
        if slow and request.url.host in ("orders", "inv"):
            await asyncio.sleep(0.05)
            resets += 1
        return stack.handler(request)

    async def record(inc_id: str) -> ScenarioOutcome:
        nonlocal slow
        slow = True
        in_record.set()
        await asyncio.sleep(10)
        return outcome("S1")

    async def run() -> ScenarioOutcome:
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handler)) as admin,
            stack.client() as plain,
        ):
            return await drive_scenario(
                S1, record, cfg=CFG, admin=admin, plain=plain, incidents=src, state=DriveState()
            )

    task = asyncio.create_task(run())
    await in_record.wait()
    task.cancel()  # lands in record(); teardown starts its slow reset
    await asyncio.sleep(0.02)
    task.cancel()  # lands inside the reset
    with pytest.raises(asyncio.CancelledError):
        await task
    assert resets == 3  # chaos x2 and the clean deploy all completed


async def test_load_worker_exceptions_are_counted_and_workers_restart() -> None:
    first = True

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal first
        if first:
            first = False
            return httpx.Response(200, json=[{"id": 1}])
        raise httpx.DecodingError("bad body")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        gen = LoadGenerator(c, "http://gw", rps=100, max_concurrency=1)
        await gen.prepare()
        gen.start()
        await asyncio.sleep(0.3)
        assert gen.crashes >= 2  # the worker kept going after the first crash
        with pytest.raises(LiveDriveError, match="unhealthy") as ei:
            await gen.stop()
    assert "DecodingError" in str(ei.value)


async def test_unhealthy_load_becomes_the_scenario_error() -> None:
    stack = FakeStack()
    base = stack.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "gw" and request.url.path != "/api/products":
            raise httpx.DecodingError("bad body")
        return base(request)

    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])

    async def record(inc_id: str) -> ScenarioOutcome:
        await asyncio.sleep(0.2)
        return outcome("S1")

    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(handler)) as plain,
        stack.client() as admin,
    ):
        with pytest.raises(LiveDriveError, match="unhealthy"):
            await drive_scenario(S1, record, cfg=CFG, admin=admin, plain=plain, incidents=src)
    assert paths(stack)[-3:] == RESET


# ---- T-052: hold, RSS guard ---------------------------------------------------------------

MB = 1024 * 1024


def held(sc: Scenario, **load: Any) -> Scenario:
    data = sc.model_dump(mode="json")
    data["load"].update(load)
    return Scenario.model_validate(data)


async def never_record(_: str) -> ScenarioOutcome:
    await asyncio.sleep(30)
    raise AssertionError("unreachable")


async def test_hold_duration_stops_load_after_duration_s() -> None:
    stack = FakeStack()
    sc = held(S1, hold="duration", duration_s=1)
    hits: list[int] = []

    async def record(_: str) -> ScenarioOutcome:
        await asyncio.sleep(0.5)
        hits.append(stack.gateway_hits)
        await asyncio.sleep(1.0)  # crosses the 1s hold
        hits.append(stack.gateway_hits)
        await asyncio.sleep(0.3)
        hits.append(stack.gateway_hits)
        return outcome("S1")

    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    await drive(stack, sc, src, record)
    assert hits[1] > hits[0]  # load ran until the hold elapsed
    assert hits[2] == hits[1]  # and sent nothing after it


async def test_hold_until_teardown_keeps_load_running() -> None:
    stack = FakeStack()
    assert S1.load.hold == "until_teardown"
    hits: list[int] = []

    async def record(_: str) -> ScenarioOutcome:
        await asyncio.sleep(0.1)
        hits.append(stack.gateway_hits)
        await asyncio.sleep(0.3)
        hits.append(stack.gateway_hits)
        return outcome("S1")

    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    await drive(stack, S1, src, record)
    assert hits[1] > hits[0]


async def test_rss_guard_trips_stops_load_and_tears_down() -> None:
    stack = FakeStack()
    stack.rss_bytes = 3000 * MB
    with pytest.raises(OrdersRssGuardError, match="exceeds the limit"):
        await drive(stack, S1, FakeSource(), never_record)
    assert paths(stack)[-3:] == RESET  # torn down


async def test_rss_guard_trips_mid_run() -> None:
    stack = FakeStack()

    async def record(_: str) -> ScenarioOutcome:
        stack.rss_bytes = 2600 * MB
        await asyncio.sleep(30)
        raise AssertionError("guard should have cancelled record")

    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    with pytest.raises(OrdersRssGuardError):
        await drive(stack, S1, src, record)
    assert paths(stack)[-3:] == RESET


@pytest.mark.parametrize("mode", ["http_error", "no_sample"])
async def test_rss_guard_fails_closed_when_prometheus_is_unusable(mode: str) -> None:
    stack = FakeStack()
    if mode == "http_error":
        stack.rss_status = 503
    else:
        stack.rss_empty = True
    with pytest.raises(OrdersRssGuardError, match="RSS guard"):
        await drive(stack, S1, FakeSource(), never_record)
    assert paths(stack)[-3:] == RESET


def graced(grace_s: float) -> DriveConfig:
    return dataclasses.replace(CFG, rss_blind_grace_s=grace_s)


async def test_rss_blind_gap_shorter_than_grace_is_survived(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stack = FakeStack()
    stack.rss_blind_script = lambda n: 2 <= n <= 6  # read 1 (start check) is good
    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])

    async def record(_: str) -> ScenarioOutcome:
        await asyncio.sleep(0.3)
        return outcome("S1")

    with caplog.at_level("WARNING"):
        await drive(stack, S1, src, record, cfg=graced(5.0))
    assert any("RSS guard blind" in r.message for r in caplog.records)


async def test_rss_blind_longer_than_grace_fails_and_tears_down() -> None:
    stack = FakeStack()
    stack.rss_blind_script = lambda n: n >= 2
    with pytest.raises(OrdersRssGuardError, match="RSS_BLIND_GRACE_S"):
        await drive(stack, S1, FakeSource(), never_record, cfg=graced(0.1))
    assert paths(stack)[-3:] == RESET


async def test_rss_good_read_resets_blind_clock() -> None:
    stack = FakeStack()
    # blind 4 polls, good, blind 4 polls, good...: each gap < grace, total blind time > grace
    stack.rss_blind_script = lambda n: n >= 2 and (n - 2) % 5 != 4

    async def record(_: str) -> ScenarioOutcome:
        await asyncio.sleep(0.6)
        return outcome("S1")

    src = FakeSource([incident("inc_1", ("HighErrorRate", "orders"))])
    await drive(stack, S1, src, record, cfg=graced(0.08))


async def test_rss_blind_at_start_fails_closed_without_starting_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started_rps: list[float] = []
    real_start = LoadGenerator.start

    def spy(self: LoadGenerator) -> None:
        started_rps.append(self._interval_s)
        real_start(self)

    monkeypatch.setattr(LoadGenerator, "start", spy)
    stack = FakeStack()
    stack.rss_empty = True
    with pytest.raises(OrdersRssGuardError, match="no process_resident_memory_bytes"):
        await drive(stack, S1, FakeSource(), never_record, cfg=graced(999.0))
    assert 2 / S1.load.rps not in started_rps  # scenario load never started (teardown refill only)
    assert paths(stack)[-3:] == RESET


def test_drive_settings_default_rss_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FIREBREAK_EVAL_DRIVE_MAX_ORDERS_RSS_MB", raising=False)
    assert DriveSettings(_env_file=None).max_orders_rss_mb == 2500  # type: ignore[call-arg]
    monkeypatch.setenv("FIREBREAK_EVAL_DRIVE_MAX_ORDERS_RSS_MB", "900")
    assert DriveSettings(_env_file=None).max_orders_rss_mb == 900  # type: ignore[call-arg]


def test_drive_settings_rss_blind_grace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FIREBREAK_EVAL_DRIVE_RSS_BLIND_GRACE_S", raising=False)
    assert DriveSettings(_env_file=None).rss_blind_grace_s == 180  # type: ignore[call-arg]
    monkeypatch.setenv("FIREBREAK_EVAL_DRIVE_RSS_BLIND_GRACE_S", "30")
    cfg = DriveConfig.from_settings(ToolSettings(), DriveSettings(_env_file=None))  # type: ignore[call-arg]
    assert cfg.rss_blind_grace_s == 30

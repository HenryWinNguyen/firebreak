"""T-051 live record driver. No network, no Docker: MockTransport and fakes."""

import asyncio
import logging
from collections.abc import Sequence
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

    def handler(self, request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        if host == "gw":
            self.gateway_hits += 1
            if path == "/api/products":
                return httpx.Response(200, json=[{"id": 1}, {"id": 2}])
            return httpx.Response(500)
        if host == "am":
            return httpx.Response(200, json=self.am_alerts)
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
) -> ScenarioOutcome:
    async with stack.client() as plain, stack.client() as admin:
        return await drive_scenario(
            sc, record, cfg=CFG, admin=admin, plain=plain, incidents=source, state=state
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
        with pytest.raises(LiveDriveError, match="crashed"):
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

"""Live record driver (T-051): apply a scenario, wait for its incident, record, tear down.

Used only by the eval CLI (``run --mode record --drive``). The Shopfloor admin API is reached
here and nowhere an agent can get to; nothing in this module is registered as an agent tool.

Per scenario (``drive_scenario``):

1. refuse unless the stack is clean (none of the scenario's expected alertnames is active in
   Alertmanager or pending/firing in Prometheus, and no earlier teardown failed);
2. apply the setup via the admin API in the documented order
   (reset -> prior_deploy -> chaos flags -> deploy);
3. start the scenario's load (bounded concurrency) and keep it running until teardown, so the
   fault stays visible to the agent while it investigates;
4. poll the live DB, read-only, for an incident opened after step 2 that carries any of
   ``expect.alerts``;
5. hand the incident id to ``record`` (the existing ``record_scenario``);
6. always (``finally``): stop the load, reset chaos and the orders deploy, and wait until the
   expected alerts stop firing. A teardown that fails or times out is logged, remembered in
   ``DriveState`` and makes the next scenario refuse to start.

The admin token is read from ``ToolSettings`` and only ever placed in the admin client's
``Authorization`` header; it is never logged or put in an error message.
"""

import asyncio
import logging
import math
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

import httpx
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import select

from firebreak.db.repositories import SessionFactory
from firebreak.db.tables import AlertRow, IncidentAlertRow, IncidentRow
from firebreak.evals.runner import EvalRunError, ScenarioOutcome
from firebreak.evals.scenario import Scenario
from firebreak.models.common import utcnow
from firebreak.tools.settings import ToolSettings

logger = logging.getLogger(__name__)

CLEAN_VERSION = "v1.0.0"
PRODUCT_EVERY = 5  # 1 request in 5 is GET /api/products (the M1 generator's mix)
ALERT_KEY = tuple[str, str]  # (alertname, service)


class LiveDriveError(EvalRunError):
    """The live driver could not do what it was asked."""


class DirtyStackError(LiveDriveError):
    """Refused to start: expected alerts are already active, or an earlier teardown failed."""


class IncidentTimeoutError(LiveDriveError, TimeoutError):
    """No matching incident appeared in time."""


class DriveSettings(BaseSettings):
    """``FIREBREAK_EVAL_DRIVE_*``: driver knobs. Service URLs and the admin token come from
    ``ToolSettings`` (one source of truth)."""

    model_config = SettingsConfigDict(
        env_prefix="FIREBREAK_EVAL_DRIVE_", env_file=".env", extra="ignore"
    )

    alertmanager_url: str = "http://localhost:9093"
    incident_timeout_s: float = Field(default=600.0, gt=0)
    clean_timeout_s: float = Field(default=600.0, gt=0)
    poll_interval_s: float = Field(default=5.0, gt=0)
    clean_polls: int = Field(default=3, ge=1)
    max_concurrency: int = Field(default=50, ge=1)
    http_timeout_s: float = Field(default=15.0, gt=0)


@dataclass(frozen=True)
class DriveConfig:
    gateway_url: str
    orders_url: str
    inventory_url: str
    alertmanager_url: str
    prometheus_url: str
    incident_timeout_s: float = 600.0
    clean_timeout_s: float = 600.0
    poll_interval_s: float = 5.0
    clean_polls: int = 3
    max_concurrency: int = 50

    def service_url(self, service: str) -> str:
        urls = {"orders": self.orders_url, "inventory": self.inventory_url}
        if service not in urls:
            raise LiveDriveError(
                f"no admin endpoint for service {service!r}; known: {sorted(urls)}"
            )
        return urls[service]

    @classmethod
    def from_settings(
        cls, tools: ToolSettings | None = None, drive: DriveSettings | None = None
    ) -> "DriveConfig":
        tools = tools or ToolSettings()
        drive = drive or DriveSettings()
        urls = tools.shopfloor_urls
        missing = [k for k in ("gateway", "orders", "inventory") if k not in urls]
        if missing:
            raise LiveDriveError(f"shopfloor_urls is missing {missing}")
        return cls(
            gateway_url=urls["gateway"],
            orders_url=urls["orders"],
            inventory_url=urls["inventory"],
            alertmanager_url=drive.alertmanager_url,
            prometheus_url=tools.prometheus_url,
            incident_timeout_s=drive.incident_timeout_s,
            clean_timeout_s=drive.clean_timeout_s,
            poll_interval_s=drive.poll_interval_s,
            clean_polls=drive.clean_polls,
            max_concurrency=drive.max_concurrency,
        )


def make_admin_client(tools: ToolSettings, *, timeout_s: float = 15.0) -> httpx.AsyncClient:
    """Client for Shopfloor ``/admin/*``. The only place the admin token is used."""
    token = tools.shopfloor_admin_token.get_secret_value()
    return httpx.AsyncClient(headers={"Authorization": f"Bearer {token}"}, timeout=timeout_s)


@dataclass
class DriveState:
    """Carried across scenarios so a dirty teardown blocks the next start."""

    #: Alerts that did not clear after the last teardown; watched by the next preflight.
    uncleared: set[ALERT_KEY] = field(default_factory=set)
    #: A teardown step (reset, load stop) failed: the stack is in an unknown state.
    broken: list[str] = field(default_factory=list)

    @property
    def problems(self) -> list[str]:
        out = list(self.broken)
        if self.uncleared:
            out.append(f"alerts still active after teardown: {sorted(self.uncleared)}")
        return out


@dataclass(frozen=True)
class LiveIncident:
    id: str
    opened_at: datetime
    alerts: tuple[ALERT_KEY, ...]


class IncidentSource(Protocol):
    async def incidents_since(self, since: datetime) -> Sequence[LiveIncident]:
        """Incidents opened strictly after ``since``, oldest first, with their alerts."""
        ...


class DbIncidentSource:
    """Read-only view of the live DB (selects only)."""

    def __init__(self, sessions: SessionFactory) -> None:
        self._sessions = sessions

    async def incidents_since(self, since: datetime) -> Sequence[LiveIncident]:
        async with self._sessions() as session:
            incidents = (
                await session.execute(
                    select(IncidentRow.id, IncidentRow.opened_at)
                    .where(IncidentRow.opened_at > since)
                    .order_by(IncidentRow.opened_at, IncidentRow.id)
                )
            ).all()
            out: list[LiveIncident] = []
            for inc_id, opened_at in incidents:
                pairs = (
                    await session.execute(
                        select(AlertRow.alertname, AlertRow.service)
                        .join(IncidentAlertRow, IncidentAlertRow.alert_id == AlertRow.id)
                        .where(IncidentAlertRow.incident_id == inc_id)
                    )
                ).all()
                out.append(LiveIncident(inc_id, opened_at, tuple((a, s) for a, s in pairs)))
        return out


# --- monitoring ---------------------------------------------------------------------------


async def _get_json(client: httpx.AsyncClient, url: str, **params: str) -> Any:
    resp = await client.get(url, params=params or None)
    resp.raise_for_status()
    return resp.json()


async def active_alerts(client: httpx.AsyncClient, cfg: DriveConfig) -> set[ALERT_KEY]:
    """(alertname, service) active in Alertmanager or pending/firing in Prometheus."""
    am = await _get_json(client, f"{cfg.alertmanager_url}/api/v2/alerts", active="true")
    prom = await _get_json(client, f"{cfg.prometheus_url}/api/v1/alerts")
    out: set[ALERT_KEY] = set()
    for a in am:
        if a.get("status", {}).get("state") == "active":
            out.add((a["labels"].get("alertname", ""), a["labels"].get("service", "")))
    for a in prom["data"]["alerts"]:
        if a.get("state") in ("pending", "firing"):
            out.add((a["labels"].get("alertname", ""), a["labels"].get("service", "")))
    return out


def expected_alerts(scenario: Scenario) -> set[ALERT_KEY]:
    return {(a.alertname, a.service) for a in scenario.expect.alerts}


def _watch(active: set[ALERT_KEY], names: set[str]) -> set[ALERT_KEY]:
    return {k for k in active if k[0] in names}


# --- admin API ----------------------------------------------------------------------------


async def _admin_post(
    admin: httpx.AsyncClient, url: str, body: dict[str, Any] | None = None
) -> None:
    resp = await admin.post(url, json=body)
    resp.raise_for_status()


async def reset_stack(admin: httpx.AsyncClient, cfg: DriveConfig) -> list[str]:
    """Reset chaos on both services and record a clean orders deploy (as M1 does).

    Every step is attempted; returns the failures (empty = fully reset).
    """
    failures: list[str] = []
    steps: list[tuple[str, str, dict[str, Any] | None]] = [
        ("orders chaos reset", f"{cfg.orders_url}/admin/chaos/reset", None),
        ("inventory chaos reset", f"{cfg.inventory_url}/admin/chaos/reset", None),
        (
            "orders clean deploy",
            f"{cfg.orders_url}/admin/deploy",
            {"version": CLEAN_VERSION, "message": "reset"},
        ),
    ]
    for label, url, body in steps:
        try:
            await _admin_post(admin, url, body)
        except httpx.HTTPError as exc:
            logger.error("reset step %r failed: %s", label, _describe(exc))
            failures.append(f"{label}: {_describe(exc)}")
    return failures


def _describe(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code} from {exc.request.url}"
    return f"{type(exc).__name__}: {exc}"


async def apply_setup(admin: httpx.AsyncClient, scenario: Scenario, cfg: DriveConfig) -> None:
    """reset -> prior_deploy -> chaos flags -> deploy. Raises on the first failure."""
    failures = await reset_stack(admin, cfg)
    if failures:
        raise LiveDriveError(f"scenario {scenario.id}: reset before apply failed: {failures}")
    setup = scenario.setup
    try:
        if setup.prior_deploy is not None:
            d = setup.prior_deploy
            await _admin_post(
                admin,
                f"{cfg.service_url(d.service)}/admin/deploy",
                {"version": d.version, "message": f"{scenario.id} prior"},
            )
        for flag, value in setup.chaos.items():
            service = flag.partition(".")[0]
            await _admin_post(
                admin, f"{cfg.service_url(service)}/admin/chaos", {"flag": flag, "value": value}
            )
        if setup.deploy is not None:
            d = setup.deploy
            await _admin_post(
                admin,
                f"{cfg.service_url(d.service)}/admin/deploy",
                {"version": d.version, "message": scenario.id},
            )
    except httpx.HTTPError as exc:
        raise LiveDriveError(
            f"scenario {scenario.id}: applying setup failed: {_describe(exc)}"
        ) from exc


# --- load ---------------------------------------------------------------------------------


class LoadGenerator:
    """Async load against the gateway: ``rps`` requests per second in total, 1 in 5 of them
    ``GET /api/products`` and the rest ``POST /api/orders`` with ``quantity=cart_size``.

    Concurrency is bounded by ``max_concurrency`` workers; each paces itself, so a slow gateway
    lowers the achieved rate instead of piling up requests. 4xx/5xx and transport errors are
    expected during a fault and are only counted; any other worker exception is raised by
    ``stop``. Product ids are looked up once in ``prepare``, while the stack is still healthy.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        gateway_url: str,
        *,
        rps: float,
        cart_size: int = 1,
        max_concurrency: int = 50,
    ) -> None:
        self._client = client
        self._base = gateway_url.rstrip("/")
        self.workers = max(1, min(max_concurrency, math.ceil(rps)))
        self._interval_s = self.workers / rps
        self._cart_size = cart_size
        self._product_ids: list[int] = []
        self._tasks: list[asyncio.Task[None]] = []
        self.ok = 0
        self.http_errors = 0
        self.transport_errors = 0

    async def prepare(self) -> None:
        try:
            resp = await self._client.get(f"{self._base}/api/products")
            resp.raise_for_status()
            self._product_ids = [int(p["id"]) for p in resp.json()]
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            raise LiveDriveError(f"cannot list products for the load generator: {exc}") from exc
        if not self._product_ids:
            raise LiveDriveError("gateway returned no products")

    async def _worker(self, n: int) -> None:
        i = n
        while True:
            started = time.monotonic()
            try:
                if i % PRODUCT_EVERY == 0:
                    resp = await self._client.get(f"{self._base}/api/products")
                else:
                    pid = self._product_ids[i % len(self._product_ids)]
                    resp = await self._client.post(
                        f"{self._base}/api/orders",
                        json={"product_id": pid, "quantity": self._cart_size},
                    )
                if resp.status_code < 400:
                    self.ok += 1
                else:
                    self.http_errors += 1
            except httpx.TransportError:
                self.transport_errors += 1
            i += self.workers
            await asyncio.sleep(max(0.0, self._interval_s - (time.monotonic() - started)))

    def start(self) -> None:
        if not self._product_ids:
            raise LiveDriveError("LoadGenerator.start() before prepare()")
        self._tasks = [asyncio.create_task(self._worker(n)) for n in range(self.workers)]

    async def stop(self) -> None:
        """Cancel workers; raise if any of them died from something but cancellation."""
        crashed = [
            exc
            for t in self._tasks
            if t.done() and not t.cancelled() and (exc := t.exception()) is not None
        ]
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        if crashed:
            raise LiveDriveError(f"{len(crashed)} load worker(s) crashed: {crashed[0]!r}") from (
                crashed[0]
            )

    def summary(self) -> str:
        return (
            f"ok={self.ok} http_errors={self.http_errors} transport_errors={self.transport_errors}"
        )


# --- waiting ------------------------------------------------------------------------------


async def wait_for_incident(
    scenario: Scenario,
    source: IncidentSource,
    *,
    since: datetime,
    timeout_s: float,
    poll_interval_s: float,
) -> str:
    """Id of the oldest incident opened after ``since`` with any of ``expect.alerts``."""
    wanted = expected_alerts(scenario)
    deadline = time.monotonic() + timeout_s
    while True:
        for inc in await source.incidents_since(since):
            if wanted & set(inc.alerts):
                logger.info("scenario %s: incident %s", scenario.id, inc.id)
                return inc.id
        if time.monotonic() >= deadline:
            raise IncidentTimeoutError(
                f"scenario {scenario.id}: no incident with any of {sorted(wanted)} "
                f"opened within {timeout_s:g}s"
            )
        await asyncio.sleep(poll_interval_s)


async def wait_until_clean(
    plain: httpx.AsyncClient, cfg: DriveConfig, watch: set[ALERT_KEY]
) -> set[ALERT_KEY]:
    """Poll until none of ``watch`` (by alertname) is active for ``clean_polls`` polls in a row.

    Returns the alerts still active at the deadline (empty = clean). Polls that fail to reach
    Alertmanager/Prometheus are logged and count as not clean.
    """
    names = {a for a, _ in watch}
    deadline = time.monotonic() + cfg.clean_timeout_s
    clean = 0
    still: set[ALERT_KEY] = set(watch)
    while True:
        try:
            still = _watch(await active_alerts(plain, cfg), names)
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            logger.warning("clean-wait poll failed: %s", _describe(exc))
            clean = 0
            still = set(watch)
        else:
            clean = 0 if still else clean + 1
            if clean >= cfg.clean_polls:
                return set()
        if time.monotonic() >= deadline:
            return still
        await asyncio.sleep(cfg.poll_interval_s)


# --- the driver ---------------------------------------------------------------------------


async def _teardown(
    scenario: Scenario,
    *,
    cfg: DriveConfig,
    admin: httpx.AsyncClient,
    plain: httpx.AsyncClient,
    gen: LoadGenerator | None,
    state: DriveState,
) -> None:
    """Never raises (it runs in ``finally`` and must not mask the original error); every
    failure is logged and stored in ``state`` so the batch fails and the next start refuses."""
    sid = scenario.id
    if gen is not None:
        try:
            await gen.stop()
            logger.info("scenario %s: load done (%s)", sid, gen.summary())
        except LiveDriveError as exc:
            logger.error("scenario %s: %s", sid, exc)
            state.broken.append(f"{sid}: {exc}")
    failures = await reset_stack(admin, cfg)
    if failures:
        state.broken.extend(f"{sid}: reset failed: {f}" for f in failures)
        return  # waiting for alerts to clear is pointless while the fault may still be on
    # Light healthy traffic refills the alert rules' rate windows so the alerts can resolve.
    keepalive: LoadGenerator | None = LoadGenerator(
        plain, cfg.gateway_url, rps=5, max_concurrency=3
    )
    assert keepalive is not None
    try:
        await keepalive.prepare()
        keepalive.start()
    except LiveDriveError as exc:
        logger.warning("scenario %s: no keep-alive traffic while waiting to clear: %s", sid, exc)
        keepalive = None
    try:
        still = await wait_until_clean(plain, cfg, expected_alerts(scenario))
    finally:
        if keepalive is not None:
            try:
                await keepalive.stop()
            except LiveDriveError as exc:
                logger.error("scenario %s: keep-alive traffic: %s", sid, exc)
    if still:
        logger.error(
            "scenario %s: alerts still active %gs after reset: %s",
            sid,
            cfg.clean_timeout_s,
            sorted(still),
        )
        state.uncleared |= still
    else:
        state.uncleared.clear()
        logger.info("scenario %s: stack clean again", sid)


async def drive_scenario(
    scenario: Scenario,
    record: Callable[[str], Awaitable[ScenarioOutcome]],
    *,
    cfg: DriveConfig,
    admin: httpx.AsyncClient,
    plain: httpx.AsyncClient,
    incidents: IncidentSource,
    state: DriveState | None = None,
) -> ScenarioOutcome:
    """Apply ``scenario`` to the live stack, wait for its incident, ``record`` it, tear down.

    ``admin`` carries the admin token (``make_admin_client``); ``plain`` is used for the gateway,
    Alertmanager and Prometheus. Raises ``DirtyStackError``, ``IncidentTimeoutError`` or
    ``LiveDriveError`` (all ``EvalRunError``); ``record``'s own errors propagate. Teardown runs
    on every path, including ``KeyboardInterrupt`` and cancellation.
    """
    state = state if state is not None else DriveState()
    sid = scenario.id
    expected = expected_alerts(scenario)
    names = {a for a, _ in expected}
    # 1. refuse on a dirty stack
    if state.broken:
        raise DirtyStackError(
            f"scenario {sid}: refusing to start, an earlier teardown failed: {state.broken}"
        )
    try:
        active = await active_alerts(plain, cfg)
    except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
        raise LiveDriveError(f"scenario {sid}: cannot read alert state: {_describe(exc)}") from exc
    busy = _watch(active, names | {a for a, _ in state.uncleared})
    if busy:
        raise DirtyStackError(
            f"scenario {sid}: refusing to start, alerts already active: {sorted(busy)}"
        )
    state.uncleared.clear()

    gen = LoadGenerator(
        plain,
        cfg.gateway_url,
        rps=scenario.load.rps,
        cart_size=scenario.load.cart_size,
        max_concurrency=cfg.max_concurrency,
    )
    started = False
    try:
        await gen.prepare()  # while healthy: a faulted gateway may not list products
        since = utcnow()
        # 2. apply
        started = True
        await apply_setup(admin, scenario, cfg)
        logger.info("scenario %s: applied; waiting for an incident", sid)
        # 3. load
        gen.start()
        # 4. incident
        incident_id = await wait_for_incident(
            scenario,
            incidents,
            since=since,
            timeout_s=cfg.incident_timeout_s,
            poll_interval_s=cfg.poll_interval_s,
        )
        # 5. record
        return await record(incident_id)
    finally:
        # 6. teardown (anything applied or started is undone; a failed prepare() applied nothing)
        if started:
            await _teardown(scenario, cfg=cfg, admin=admin, plain=plain, gen=gen, state=state)

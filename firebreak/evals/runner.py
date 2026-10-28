"""Eval runner (ARCH §9): replay (or record) the baseline over scenarios, grade, store.

Replay is fully offline. It uses the T-044 cassette adapters and replay LLM client inside
``deterministic(...)`` (all done by ``replay_run``), hands the adapters to ``investigate`` through
its ``registry=`` hook, and never builds the real adapters or an LLM client. The only I/O is
Postgres.

A recording is the T-044 directory plus ``run.json`` (this module): the incident and alerts
the agent was shown and the live latency. Replay recreates that incident under its recorded ids
(the task text, and so the LLM request hash, contains them), runs the agent, and grades.

Statuses: no recording -> "not recorded" (reported, not a failure); a recording that no
longer matches the code -> ``StaleRecordingError``, collected for every scenario and raised as
one ``EvalRunError`` naming them, with nothing stored.
"""

import logging
import os
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from alembic import command
from alembic.config import Config
from pydantic import BaseModel, ConfigDict, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import text
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

import firebreak.db
from firebreak.agents.baseline import BaselineResult, investigate
from firebreak.config import Settings
from firebreak.db.repositories import (
    AlertRepository,
    EvalRepository,
    EvalResult,
    EvalRun,
    IncidentRepository,
    PurgeRefusedError,
    SessionFactory,
)
from firebreak.evals.cassette import (
    META_FILE,
    SecretInRecordingError,
    StaleRecordingError,
    default_secrets,
    find_secrets,
    record_run,
    recording_dir,
    replay_run,
)
from firebreak.evals.grader import grade
from firebreak.evals.scenario import Scenario
from firebreak.models.alert import Alert
from firebreak.models.common import new_id, utcnow
from firebreak.models.evidence import EvidenceKind
from firebreak.models.incident import Incident
from firebreak.models.llm import LLMClient
from firebreak.tools.read_tools import ReadAdapters, build_read_registry
from firebreak.tools.settings import ToolSettings

logger = logging.getLogger(__name__)

RUN_FILE = "run.json"
RUN_FORMAT_VERSION = 1
LIVE_FLAG = "FIREBREAK_EVAL_LIVE"
API_KEY_VAR = "ANTHROPIC_API_KEY"


EVAL_DB_SUFFIXES = ("_eval", "_test")
DEFAULT_EVAL_DB = "firebreak_eval"
_DB_DIR = Path(firebreak.db.__file__).parent


class EvalRunError(Exception):
    """The eval run cannot complete (stale or unreadable recordings, bad arguments)."""


class EvalSettings(BaseSettings):
    """``FIREBREAK_EVAL_*``. Evals never use ``FIREBREAK_DATABASE_URL`` for writes."""

    model_config = SettingsConfigDict(env_prefix="FIREBREAK_EVAL_", env_file=".env", extra="ignore")

    #: Default: the dev server, database ``firebreak_eval``.
    database_url: str | None = None

    def resolved_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        dev = make_url(Settings().database_url)
        return dev.set(database=DEFAULT_EVAL_DB).render_as_string(hide_password=False)


def require_eval_database_name(name: str | None) -> None:
    """Replay deletes and recreates incidents by id: only ever in a disposable database."""
    if not name or not name.endswith(EVAL_DB_SUFFIXES):
        raise EvalRunError(
            f"refusing to run evals against database {name!r}: its name must end with "
            f"{' or '.join(EVAL_DB_SUFFIXES)} (set FIREBREAK_EVAL_DATABASE_URL)"
        )


def _sessions_db_name(sessions: SessionFactory) -> str | None:
    bind = sessions.kw.get("bind")
    return None if bind is None else bind.url.database


async def ensure_eval_database(url: str) -> None:
    """Create the eval database if missing (via the server's ``postgres`` db) and migrate it."""
    parsed = make_url(url)
    require_eval_database_name(parsed.database)
    name = str(parsed.database)
    admin = create_async_engine(
        parsed.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    try:
        async with admin.connect() as conn:
            found = (
                await conn.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": name}
                )
            ).first()
            if found is None:
                logger.info("creating eval database %s", name)
                quoted = '"' + name.replace('"', '""') + '"'
                await conn.execute(text(f"CREATE DATABASE {quoted}"))
    except DBAPIError as exc:
        raise EvalRunError(
            f"could not create eval database {name!r}: {exc.orig or exc}. Create it yourself "
            f"(CREATE DATABASE {name}) or grant the role CREATEDB"
        ) from exc
    finally:
        await admin.dispose()
    eng = create_async_engine(url, poolclass=NullPool)
    try:
        async with eng.begin() as conn:
            await conn.run_sync(_upgrade_head)
    finally:
        await eng.dispose()


def _upgrade_head(sync_conn: Connection) -> None:
    cfg = Config(str(_DB_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(_DB_DIR / "migrations"))
    cfg.attributes["connection"] = sync_conn
    command.upgrade(cfg, "head")


class LiveModeRefusedError(EvalRunError):
    """Record mode spends money; it needs an explicit opt-in."""


class RecordedRunInfo(BaseModel):
    """``run.json``: what the agent was shown, plus how long the live run took."""

    model_config = ConfigDict(extra="forbid")

    format_version: int = RUN_FORMAT_VERSION
    incident: Incident
    alerts: list[Alert]
    latency_s: float


@dataclass
class ScenarioOutcome:
    scenario_id: str
    result: EvalResult | None  # None -> not recorded
    prompt_sha256: str | None = None


@dataclass
class RunOutcome:
    outcomes: list[ScenarioOutcome] = field(default_factory=list)

    @property
    def graded(self) -> list[ScenarioOutcome]:
        return [o for o in self.outcomes if o.result is not None]

    @property
    def not_recorded(self) -> list[str]:
        return [o.scenario_id for o in self.outcomes if o.result is None]


def scenario_key(scenario_id: str) -> str:
    """Recording directory name: ``S1`` -> ``s01`` (matches the scenario file stem)."""
    digits = scenario_id[1:]
    if scenario_id[:1] in ("S", "s") and digits.isdigit():
        return f"s{int(digits):02d}"
    return scenario_id.lower()


def select_scenarios(all_scenarios: Mapping[str, Scenario], names: Sequence[str]) -> list[Scenario]:
    """Scenarios in key order; ``names`` (``s01`` or ``S1``) restricts them. Unknown names raise."""
    by_key = {scenario_key(sid): s for sid, s in all_scenarios.items()}
    wanted = {scenario_key(n) for n in names} if names else set(by_key)
    unknown = sorted(wanted - set(by_key))
    if unknown:
        raise EvalRunError(f"unknown scenarios {unknown}; known: {sorted(by_key)}")
    return [by_key[k] for k in sorted(wanted)]


def load_run_info(directory: Path, scenario: str) -> RecordedRunInfo:
    path = directory / RUN_FILE
    try:
        info = RecordedRunInfo.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValidationError) as exc:
        raise EvalRunError(
            f"scenario {scenario}: recording {directory} has no usable {RUN_FILE} ({exc}); "
            "re-record it with `python -m firebreak.evals run --mode record`"
        ) from exc
    if info.format_version != RUN_FORMAT_VERSION:
        raise EvalRunError(
            f"scenario {scenario}: {RUN_FILE} format {info.format_version}, "
            f"this code reads {RUN_FORMAT_VERSION}"
        )
    return info


def _to_result(
    scenario: Scenario,
    result: BaselineResult,
    evidence_kinds: Mapping[str, EvidenceKind],
    latency_s: float,
) -> EvalResult:
    g = grade(
        scenario,
        result.report,
        evidence_kinds=evidence_kinds,
        usage_usd=result.usage.cost_usd,
        latency_s=latency_s,
        error=result.error,
    )
    return EvalResult(
        scenario_id=scenario.id,
        top1=g.top1,
        top3=g.top3,
        cited_kinds_ok=g.cited_kinds_ok,
        report_valid=g.report_valid,
        cost_usd=g.cost_usd,
        latency_s=g.latency_s,
        steps=result.steps,
        error=g.error,
        report=None if result.report is None else result.report.model_dump(mode="json"),
    )


async def _install_incident(
    repo: EvalRepository, sessions: SessionFactory, info: RecordedRunInfo
) -> None:
    """Recreate the recorded incident and alerts under their recorded ids, replacing any copy."""
    try:
        await repo.purge_incident(info.incident.id, [a.id for a in info.alerts])
    except PurgeRefusedError as exc:
        raise EvalRunError(f"eval database not safe to replay into: {exc}") from exc
    alerts = AlertRepository(sessions)
    for a in info.alerts:
        stored_id, inserted = await alerts.insert_or_get(a)
        if not inserted:
            raise EvalRunError(
                f"alert {a.id} collides with stored alert {stored_id} (same idempotency key); "
                "refusing to replay over it"
            )
    await IncidentRepository(sessions).create(info.incident)


async def replay_scenario(
    scenario: Scenario,
    *,
    agent: str,
    root: Path,
    sessions: SessionFactory,
    tools_settings: ToolSettings,
) -> ScenarioOutcome:
    """Replay one scenario offline and grade it. No recording -> ``result=None``.

    Raises ``StaleRecordingError`` (naming the scenario) if the recording no longer matches.
    """
    require_eval_database_name(_sessions_db_name(sessions))
    key = scenario_key(scenario.id)
    directory = recording_dir(agent, key, root)
    if not (directory / META_FILE).exists():
        logger.info("scenario %s: not recorded (no %s)", scenario.id, directory / META_FILE)
        return ScenarioOutcome(scenario.id, None)
    info = load_run_info(directory, scenario.id)
    repo = EvalRepository(sessions)
    with replay_run(directory) as rp:
        await _install_incident(repo, sessions, info)
        registry = build_read_registry(tools_settings, rp.adapters)
        result = await investigate(
            info.incident.id,
            llm=rp.llm,
            sessions=sessions,
            tools_settings=tools_settings,
            registry=registry,
        )
        prompt_sha = rp.meta.prompt_sha256
    kinds = await repo.evidence_kinds(info.incident.id)
    # Replay speed says nothing about the agent; report the recorded live latency.
    return ScenarioOutcome(
        scenario.id, _to_result(scenario, result, kinds, info.latency_s), prompt_sha
    )


def require_live_env(env: Mapping[str, str] | None = None) -> None:
    """Record mode spends money: refuse unless FIREBREAK_EVAL_LIVE=1 and an API key are set."""
    env = os.environ if env is None else env
    if env.get(LIVE_FLAG) != "1":
        raise LiveModeRefusedError(f"record mode spends money; set {LIVE_FLAG}=1 to allow it")
    if not env.get(API_KEY_VAR):
        raise LiveModeRefusedError(f"record mode needs {API_KEY_VAR} to be set")


async def record_scenario(
    scenario: Scenario,
    incident_id: str,
    *,
    agent: str,
    root: Path,
    sessions: SessionFactory,
    live_sessions: SessionFactory,
    tools_settings: ToolSettings,
    llm: LLMClient,
    adapters: ReadAdapters,
    overwrite: bool = False,
    env: Mapping[str, str] | None = None,
) -> ScenarioOutcome:
    """Record one live run on an existing incident (T-042 drives this). Spends money.

    The incident is read from ``live_sessions`` (read-only: ``get`` and ``list_alerts``); a copy
    is installed in the eval database (``sessions``), which is the only database written.
    """
    require_live_env(env)
    require_eval_database_name(_sessions_db_name(sessions))
    key = scenario_key(scenario.id)
    directory = recording_dir(agent, key, root)
    live = IncidentRepository(live_sessions)
    incident = await live.get(incident_id)
    if incident is None:
        raise EvalRunError(f"scenario {scenario.id}: incident {incident_id} not found")
    alerts = await live.list_alerts(incident_id)
    await _install_incident(
        EvalRepository(sessions),
        sessions,
        RecordedRunInfo(incident=incident, alerts=alerts, latency_s=0.0),
    )
    with record_run(
        directory,
        agent=agent,
        scenario=key,
        at=utcnow(),
        adapters=adapters,
        llm=llm,
        overwrite=overwrite,
    ) as rec:
        result = await investigate(
            incident_id,
            llm=rec.llm,
            sessions=sessions,
            tools_settings=tools_settings,
            registry=build_read_registry(tools_settings, rec.adapters),
        )
    info = RecordedRunInfo(incident=incident, alerts=alerts, latency_s=result.latency_s)
    (directory / RUN_FILE).write_text(info.model_dump_json(indent=2) + "\n", encoding="utf-8")
    leaked = find_secrets(directory, default_secrets())
    if leaked:
        (directory / RUN_FILE).unlink()
        raise SecretInRecordingError(
            f"scenario {scenario.id}: a secret appears in {[p.name for p in leaked]}; "
            f"{RUN_FILE} was removed"
        )
    kinds = await EvalRepository(sessions).evidence_kinds(incident_id)
    return ScenarioOutcome(scenario.id, _to_result(scenario, result, kinds, result.latency_s))


def current_git_sha() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, timeout=10
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("could not read the git sha: %s", exc)
        return None
    return out.stdout.strip() or None


async def replay_all(
    scenarios: Sequence[Scenario],
    *,
    agent: str,
    root: Path,
    sessions: SessionFactory,
    tools_settings: ToolSettings,
) -> RunOutcome:
    """Replay every scenario; collect stale recordings and raise them together at the end."""
    out = RunOutcome()
    stale: list[StaleRecordingError] = []
    for scenario in scenarios:
        try:
            out.outcomes.append(
                await replay_scenario(
                    scenario,
                    agent=agent,
                    root=root,
                    sessions=sessions,
                    tools_settings=tools_settings,
                )
            )
        except StaleRecordingError as exc:
            logger.error("scenario %s: stale recording: %s", scenario.id, exc)
            stale.append(exc)
    if stale:
        raise EvalRunError(
            "stale recordings, nothing stored; re-record them:\n"
            + "\n".join(f"  - {e}" for e in stale)
        )
    return out


async def store_run(
    outcome: RunOutcome,
    *,
    mode: str,
    agent: str,
    sessions: SessionFactory,
    started_at: datetime | None = None,
) -> EvalRun:
    graded = outcome.graded
    if not graded:
        raise EvalRunError("no scenario was graded; nothing to store")
    shas = sorted({o.prompt_sha256 for o in graded if o.prompt_sha256})
    run = EvalRun(
        id=new_id("evr"),
        started_at=started_at or utcnow(),
        mode=mode,
        agent=agent,
        git_sha=current_git_sha(),
        prompt_sha256=",".join(shas) or None,
    )
    results = [o.result for o in graded if o.result is not None]
    await EvalRepository(sessions).save_run(run, results)
    return run

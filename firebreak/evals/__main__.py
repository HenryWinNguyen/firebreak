"""``python -m firebreak.evals run|report`` (see ``make eval``)."""

import argparse
import asyncio
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

import httpx

from firebreak.config import Settings
from firebreak.db.engine import make_engine, make_session_factory
from firebreak.db.repositories import EvalRepository, SessionFactory
from firebreak.evals.cassette import DEFAULT_ROOT, SecretInRecordingError
from firebreak.evals.live import (
    DbIncidentSource,
    DriveConfig,
    DriveSettings,
    DriveState,
    drive_scenario,
    make_admin_client,
)
from firebreak.evals.report import DEFAULT_REPORT, NoRunsError, render_report, write_report
from firebreak.evals.runner import (
    EvalRunError,
    EvalSettings,
    RunOutcome,
    ScenarioOutcome,
    agent_config_path,
    ensure_eval_database,
    record_batch,
    record_scenario,
    replay_all,
    require_eval_session,
    require_eval_url,
    require_live_env,
    scenario_key,
    select_scenarios,
    store_run,
)
from firebreak.evals.scenario import Scenario, ScenarioError, load_scenarios
from firebreak.tools.settings import ToolSettings

SCENARIOS_DIR = Path("evals/scenarios")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m firebreak.evals")
    p.add_argument(
        "--database-url",
        default=None,
        help="eval database (name must end _eval or _test); default FIREBREAK_EVAL_DATABASE_URL, "
        "else the dev server's firebreak_eval",
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run the agent over recorded scenarios and store results")
    run.add_argument("--mode", choices=["replay", "record"], required=True)
    run.add_argument("--agent", default="baseline")
    run.add_argument("--scenarios", default="", help="comma-separated, e.g. s01,s02 (default: all)")
    run.add_argument("--scenarios-dir", type=Path, default=SCENARIOS_DIR)
    run.add_argument("--recordings-dir", type=Path, default=DEFAULT_ROOT)
    src = run.add_mutually_exclusive_group()
    src.add_argument(
        "--drive",
        action="store_true",
        help="record mode: apply each scenario to the live stack, wait for its incident, record "
        "it, and tear down (T-051); exclusive with --incident",
    )
    src.add_argument(
        "--incident",
        action="append",
        default=[],
        metavar="SCENARIO=INCIDENT_ID",
        help="record mode: the live incident to investigate for each scenario (repeatable)",
    )
    run.add_argument("--overwrite", action="store_true", help="record mode: replace a recording")
    rep = sub.add_parser("report", help="write evals/REPORT.md from the stored results")
    rep.add_argument("--scenarios-dir", type=Path, default=SCENARIOS_DIR)
    rep.add_argument("--out", type=Path, default=DEFAULT_REPORT)
    return p


def _names(raw: str) -> list[str]:
    return [n.strip() for n in raw.split(",") if n.strip()]


def _eval_url(args: argparse.Namespace) -> str:
    url: str = args.database_url or EvalSettings().resolved_database_url()
    require_eval_url(url)
    return url


async def _run(args: argparse.Namespace) -> int:
    agent_config_path(args.agent)  # unknown agent: fail before touching anything
    if args.drive and args.mode != "record":
        raise EvalRunError("--drive only applies to --mode record")
    if args.mode == "record":
        require_live_env(agent=args.agent)  # fail before touching anything
    scenarios = select_scenarios(load_scenarios(args.scenarios_dir), _names(args.scenarios))
    url = _eval_url(args)
    await ensure_eval_database(url)
    engine = make_engine(url)
    sessions = make_session_factory(engine)
    tools_settings = ToolSettings()
    try:
        if args.mode == "replay":
            outcome = await replay_all(
                scenarios,
                agent=args.agent,
                root=args.recordings_dir,
                sessions=sessions,
                tools_settings=tools_settings,
            )
        else:
            outcome = await _record(args, scenarios, sessions, tools_settings)
        for sid in outcome.not_recorded:
            print(f"{sid}: not recorded")
        for o in outcome.graded:
            r = o.result
            if r is None:  # graded() only yields scored outcomes
                raise AssertionError("graded outcome without a result")
            print(
                f"{o.scenario_id}: top1={r.top1} top3={r.top3} cost=${r.cost_usd:.4f} "
                f"latency={r.latency_s:.1f}s steps={r.steps}"
                + (f" ERROR: {r.error}" if r.error else "")
            )
        for label, message in outcome.errors:
            print(f"{label}: error: {message}")
        if not outcome.graded and outcome.errors:
            return 1  # nothing to store; the errors above are the result
        if not outcome.graded:
            raise EvalRunError(
                f"no recordings found under {args.recordings_dir / args.agent}; "
                "record them first (T-042)"
            )
        run = await store_run(outcome, mode=args.mode, agent=args.agent, sessions=sessions)
        print(f"stored eval run {run.id} ({len(outcome.graded)} graded)")
        if outcome.errors:
            print(f"{len(outcome.errors)} error(s); exiting non-zero", file=sys.stderr)
            return 1
        return 0
    finally:
        await engine.dispose()


async def _record(
    args: argparse.Namespace,
    scenarios: Sequence[Scenario],
    sessions: SessionFactory,
    tools_settings: ToolSettings,
) -> RunOutcome:
    # Imported here so replay mode can never reach the real adapters or the live LLM client.
    from firebreak.llm.routing import RoutingLLMClient
    from firebreak.llm.settings import LLMSettings
    from firebreak.tools.read_tools import build_default_adapters

    incidents: dict[str, str] = {}
    if not args.drive:
        for item in args.incident:
            sid, sep, inc = item.partition("=")
            if not sep or not inc:
                raise EvalRunError(f"--incident expects SCENARIO=INCIDENT_ID, got {item!r}")
            incidents[scenario_key(sid.strip())] = inc.strip()
        missing = [k for k in (scenario_key(s.id) for s in scenarios) if k not in incidents]
        if missing:
            raise EvalRunError(
                f"record mode needs --incident for {missing} (SCENARIO=INCIDENT_ID), or --drive"
            )
    adapters = build_default_adapters(tools_settings)
    live_engine = make_engine(Settings().database_url)  # read-only: incident + alerts
    live_sessions = make_session_factory(live_engine)
    http_clients: list[httpx.AsyncClient] = []
    try:
        llm = RoutingLLMClient(LLMSettings())  # per-model provider; clients built lazily

        async def record_with(sc: Scenario, incident_id: str) -> ScenarioOutcome:
            return await record_scenario(
                sc,
                incident_id,
                agent=args.agent,
                root=args.recordings_dir,
                sessions=sessions,
                live_sessions=live_sessions,
                tools_settings=tools_settings,
                llm=llm,
                adapters=adapters,
                overwrite=args.overwrite,
            )

        if not args.drive:
            return await record_batch(
                scenarios, lambda sc: record_with(sc, incidents[scenario_key(sc.id)])
            )
        cfg = DriveConfig.from_settings(tools_settings)
        admin = make_admin_client(tools_settings)
        plain = httpx.AsyncClient(timeout=DriveSettings().http_timeout_s)
        http_clients += [admin, plain]
        source = DbIncidentSource(live_sessions)
        state = DriveState()
        outcome = await record_batch(
            scenarios,
            lambda sc: drive_scenario(
                sc,
                lambda inc_id: record_with(sc, inc_id),
                cfg=cfg,
                admin=admin,
                plain=plain,
                incidents=source,
                state=state,
            ),
        )
        outcome.errors.extend(("drive", p) for p in state.problems)
        return outcome
    finally:
        for c in http_clients:
            await c.aclose()
        await adapters.aclose()
        await live_engine.dispose()


async def _report(args: argparse.Namespace) -> int:
    scenarios = load_scenarios(args.scenarios_dir)
    engine = make_engine(_eval_url(args))
    try:
        sessions = make_session_factory(engine)
        await require_eval_session(sessions)
        runs = await EvalRepository(sessions).latest_run_per_agent()
    finally:
        await engine.dispose()
    path = write_report(render_report(runs, scenarios), args.out)
    print(f"wrote {path}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        return asyncio.run(_run(args) if args.cmd == "run" else _report(args))
    except (EvalRunError, ScenarioError, NoRunsError, SecretInRecordingError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

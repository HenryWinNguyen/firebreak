"""Single-agent baseline (SCOPE M3): one agent, every read tool, no orchestration.

``investigate`` loads an incident and its alerts, runs the ``baseline`` agent through the
shared runtime and returns the ``InvestigationReport`` with usage, steps and latency. Every
later multi-agent result is compared against this.

Run it: ``uv run python -m firebreak.agents.baseline <incident_id>`` (needs the stack up and a
real ``ANTHROPIC_API_KEY`` unless ``FIREBREAK_LLM_MODE=replay``). Spend gate: an agent that routes
to ``anthropic`` is refused (exit 2, before any client is built) unless ``FIREBREAK_EVAL_LIVE=1``;
``--agent baseline-local`` needs no Anthropic key; replay needs no opt-in.

Failure model: an incident that doesn't exist raises ``IncidentNotFoundError``. A run that
ends without a valid report (budget, max steps, refusal, invalid output) is a *result*, not
an exception: ``BaselineResult.report`` is ``None`` and ``error`` says why. Infrastructure
failures (LLM client, database) propagate.
"""

import argparse
import asyncio
import logging
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from firebreak.agents.config import CONFIGS_DIR, load_agent_config
from firebreak.agents.runtime import run_agent
from firebreak.db.repositories import (
    IncidentNotFoundError,
    IncidentRepository,
    PostgresEvidenceSink,
    SessionFactory,
)
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.models.agents import AgentRunResult, ToolCallRecord
from firebreak.models.alert import Alert
from firebreak.models.incident import Incident
from firebreak.models.llm import LLMClient, Usage
from firebreak.models.reasoning import InvestigationReport
from firebreak.tools.context import ToolContext
from firebreak.tools.gateway import ToolGateway
from firebreak.tools.read_tools import ReadAdapters, build_default_adapters, build_read_registry
from firebreak.tools.registry import ToolRegistry
from firebreak.tools.settings import ToolSettings
from firebreak.tools.untrusted import wrap_untrusted

logger = logging.getLogger(__name__)

BASELINE_CONFIG = CONFIGS_DIR / "baseline.yaml"
#: Cap on one alert's untrusted text (summary + labels) in the task.
MAX_ALERT_TEXT_CHARS = 1500


@dataclass
class BaselineResult:
    incident_id: str
    report: InvestigationReport | None
    usage: Usage
    steps: int
    latency_s: float
    stopped_reason: str
    error: str | None = None
    tool_calls: list[ToolCallRecord] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.report is not None and self.error is None


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_task(incident: Incident, alerts: Sequence[Alert]) -> str:
    """The user message: incident facts as plain text, alert free text wrapped as untrusted."""
    lines = [
        f"Investigate incident {incident.id}.",
        f"Severity: {incident.severity}. Services involved: {', '.join(incident.services)}. "
        f"Opened at {_iso(incident.opened_at)}.",
        f"The incident groups {len(alerts)} alert(s):",
    ]
    for i, a in enumerate(alerts, 1):
        ended = f", ended {_iso(a.ended_at)}" if a.ended_at else ""
        lines.append(
            f"{i}. alertname={a.alertname} service={a.service} severity={a.severity} "
            f"status={a.status} started {_iso(a.started_at)}{ended}"
        )
        text = f"summary: {a.summary}\nlabels: {sorted(a.labels.items())}"
        lines.append(
            wrap_untrusted(text, source="alert_annotations", max_chars=MAX_ALERT_TEXT_CHARS)
        )
    lines.append("Find the root cause and reply with the InvestigationReport.")
    return "\n".join(lines)


def _summarize(run: AgentRunResult) -> tuple[InvestigationReport | None, str | None]:
    """The validated report, or why there isn't one. Never silent."""
    if run.stopped_reason != "final_output":
        return None, run.error or f"run stopped without a report: {run.stopped_reason}"
    try:
        return InvestigationReport.model_validate(run.output), None
    except ValidationError as e:  # the runtime already validated; this guards schema drift
        logger.exception("baseline output failed InvestigationReport validation")
        return None, f"output is not a valid InvestigationReport: {e}"


async def investigate(
    incident_id: str,
    *,
    llm: LLMClient,
    sessions: SessionFactory,
    tools_settings: ToolSettings,
    registry: ToolRegistry | None = None,
    config_path: Path = BASELINE_CONFIG,
) -> BaselineResult:
    """Run the baseline agent on one incident.

    ``config_path`` picks the agent config (default: baseline).
    ``registry`` is for tests. When omitted, the real read tools are built and their HTTP
    clients are closed before returning.
    """
    incident = await IncidentRepository(sessions).get(incident_id)
    if incident is None:
        raise IncidentNotFoundError(incident_id)
    alerts = await IncidentRepository(sessions).list_alerts(incident.id)

    adapters: ReadAdapters | None = None
    started = time.perf_counter()
    try:
        if registry is None:
            # build_default_adapters creates the HTTP clients first and loads the graph last.
            # Load the graph up front so a failure there can't leak already-built clients.
            ServiceGraph.load()
            adapters = build_default_adapters(tools_settings)
            registry = build_read_registry(tools_settings, adapters)
        cfg = load_agent_config(config_path, registry)
        ctx = ToolContext(incident_id=incident.id, evidence=PostgresEvidenceSink(sessions))
        run = await run_agent(cfg, build_task(incident, alerts), ctx, llm, ToolGateway(registry))
    finally:
        if adapters is not None:
            try:
                await adapters.aclose()
            except Exception:
                # Never mask the exception (or result) of the run itself.
                logger.exception("closing read adapters failed incident=%s", incident.id)
    latency_s = time.perf_counter() - started

    report, error = _summarize(run)
    return BaselineResult(
        incident_id=incident.id,
        report=report,
        usage=run.usage,
        steps=run.steps,
        latency_s=latency_s,
        stopped_reason=run.stopped_reason,
        error=error,
        tool_calls=run.tool_calls,
    )


def summary_line(result: BaselineResult) -> str:
    top = "none"
    if result.report is not None:
        best = min(result.report.hypotheses, key=lambda h: h.rank)
        top = f"{best.root_cause_category} ({best.confidence:.2f})"
    status = "ok" if result.ok else f"ERROR: {result.error}"
    return (
        f"incident={result.incident_id} top1={top} cost=${result.usage.cost_usd:.4f} "
        f"latency={result.latency_s:.1f}s steps={result.steps} tools={len(result.tool_calls)} "
        f"{status}"
    )


async def _amain(incident_id: str, config_path: Path) -> int:
    from firebreak.db.engine import make_engine, make_session_factory
    from firebreak.llm.client import build_llm_client

    engine = make_engine()
    try:
        result = await investigate(
            incident_id,
            llm=build_llm_client(),
            sessions=make_session_factory(engine),
            tools_settings=ToolSettings(),
            config_path=config_path,
        )
    except IncidentNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    finally:
        await engine.dispose()
    if result.report is not None:
        print(result.report.model_dump_json(indent=2))
    if result.error is not None:
        print(f"error: {result.error}", file=sys.stderr)
    print(summary_line(result))
    return 0 if result.ok else 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the single-agent baseline on an incident.")
    parser.add_argument("incident_id")
    parser.add_argument("--agent", default="baseline", help="agent config name (default: baseline)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    # Imported here: the runner imports this module. The gate runs before any client is built.
    from firebreak.evals.runner import EvalRunError, agent_config_path, require_live_env
    from firebreak.llm.settings import LLMMode, LLMSettings

    try:
        config_path = agent_config_path(args.agent)
        if LLMSettings().mode is not LLMMode.REPLAY:
            require_live_env(agent=args.agent)
    except EvalRunError as e:  # includes LiveModeRefusedError
        print(f"error: refusing to run, nothing was spent: {e}", file=sys.stderr)
        return 2
    return asyncio.run(_amain(args.incident_id, config_path))


if __name__ == "__main__":
    sys.exit(main())

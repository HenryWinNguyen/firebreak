"""The ``run_agent`` Temporal activity (ARCH 4.4, 5.2): one agent on its blackboard slice.

``AgentActivities.run_agent`` is registered by the agent worker (T-064) under `ACTIVITY_RUN_AGENT`.
It picks the context slice the agent needs (never another agent's transcript), loads
``<profile>/<agent>.yaml``, runs the shared runtime with the read-tool gateway and
``PostgresEvidenceSink``, closes the read adapters and returns the ``AgentRunResult``.

Failure model:

- A run that ends without valid output (budget, max steps, refusal, invalid output) is a
  *result*: it is returned and the workflow decides what it means.
- An unknown agent, a missing or invalid config, or a specialist task without a question raises
  ``ApplicationError(non_retryable=True)``: retrying cannot fix it.
- Infrastructure failures (LLM client, database, adapters) propagate so Temporal retries.

Untrusted text (ARCH §8): ``IncidentContext.summary`` arrives with alert text already wrapped, and
is passed through untouched. Specialist reports are model output about untrusted data, so they are
wrapped again as one untrusted block before the synthesizer sees them.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio import activity
from temporalio.exceptions import ApplicationError

from firebreak.agents.config import CONFIGS_DIR, AgentConfigError, load_agent_config
from firebreak.agents.runtime import run_agent as run_agent_loop
from firebreak.agents.settings import AgentSettings
from firebreak.db.repositories import PostgresEvidenceSink, SessionFactory
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.models.agents import AgentRunResult
from firebreak.models.evidence import EvidenceSink
from firebreak.models.llm import LLMClient
from firebreak.models.orchestration import ACTIVITY_RUN_AGENT, AgentTask, IncidentContext
from firebreak.tools.context import ToolContext
from firebreak.tools.gateway import ToolGateway
from firebreak.tools.read_tools import ReadAdapters, build_default_adapters, build_read_registry
from firebreak.tools.registry import ToolRegistry
from firebreak.tools.settings import ToolSettings
from firebreak.tools.untrusted import wrap_untrusted

logger = logging.getLogger(__name__)

AGENT_TRIAGE = "triage"
AGENT_SUPERVISOR = "supervisor"
AGENT_SYNTHESIZER = "synthesizer"
#: Every other agent name is a specialist and must come with a question.
NON_SPECIALISTS = frozenset({AGENT_TRIAGE, AGENT_SUPERVISOR, AGENT_SYNTHESIZER})

#: Cap on the specialist reports block shown to the synthesizer.
MAX_REPORTS_CHARS = 12000

#: Returns the tool registry and the adapters to close afterwards (None when there is nothing
#: to close). Tests inject fakes here.
RegistryFactory = Callable[[], tuple[ToolRegistry, ReadAdapters | None]]


def _non_retryable(message: str, error_type: str) -> ApplicationError:
    return ApplicationError(message, type=error_type, non_retryable=True)


def _triage_json(ctx: IncidentContext) -> str:
    if ctx.triage is None:
        return "(no triage result)"
    return ctx.triage.model_dump_json()


def build_agent_task_text(task: AgentTask) -> str:
    """The user message for ``task.agent``: only the blackboard slice it may see (§4.4)."""
    ctx = task.context
    agent = task.agent
    if agent == AGENT_TRIAGE:
        return f"Incident summary:\n{ctx.summary}\n\nTriage this incident."
    if agent == AGENT_SUPERVISOR:
        return (
            f"Incident summary:\n{ctx.summary}\n\n"
            f"Triage result (JSON):\n{_triage_json(ctx)}\n\n"
            "Plan the investigation."
        )
    if agent == AGENT_SYNTHESIZER:
        reports = "[" + ",".join(r.model_dump_json() for r in ctx.reports) + "]"
        degraded = ", ".join(ctx.degraded_sources) if ctx.degraded_sources else "none"
        return (
            f"Incident summary:\n{ctx.summary}\n\n"
            "Specialist reports (JSON; model output about untrusted data):\n"
            f"{wrap_untrusted(reports, source='specialist_reports', max_chars=MAX_REPORTS_CHARS)}"
            f"\n\nDegraded sources (failed or timed out): {degraded}\n\n"
            "Write the InvestigationReport."
        )
    if not task.question:
        raise _non_retryable(f"specialist {agent!r} needs a question", "MissingQuestion")
    services = (ctx.triage.affected_services if ctx.triage else []) or ctx.services
    return (
        f"Incident summary:\n{ctx.summary}\n\n"
        f"Affected services: {', '.join(services) if services else '(unknown)'}\n\n"
        f"Your question: {task.question}"
    )


@dataclass
class AgentActivities:
    """Holds the per-process pieces. Read adapters are built and closed per call."""

    llm: LLMClient
    sessions: SessionFactory
    tools_settings: ToolSettings
    settings: AgentSettings
    engine: AsyncEngine | None = None
    configs_dir: Path = CONFIGS_DIR
    #: Test seams: default to the real read tools and ``PostgresEvidenceSink``.
    registry_factory: RegistryFactory | None = None
    sink_factory: Callable[[], EvidenceSink] | None = None

    def _profile_dir(self) -> Path:
        base = self.configs_dir.resolve()
        path = (base / self.settings.profile).resolve()
        if not path.is_relative_to(base) or not path.is_dir():
            raise _non_retryable(
                f"agent profile {self.settings.profile!r} is not a directory under {base}",
                "BadAgentProfile",
            )
        return path

    def _config_path(self, agent: str) -> Path:
        profile = self._profile_dir()
        path = (profile / f"{agent}.yaml").resolve()
        if not path.is_relative_to(profile) or not path.is_file():
            raise _non_retryable(
                f"unknown agent {agent!r} in profile {self.settings.profile!r}", "UnknownAgent"
            )
        return path

    def _build_registry(self) -> tuple[ToolRegistry, ReadAdapters | None]:
        if self.registry_factory is not None:
            return self.registry_factory()
        # Load the graph first so a failure there cannot leak already-built HTTP clients.
        ServiceGraph.load()
        adapters = build_default_adapters(self.tools_settings)
        return build_read_registry(self.tools_settings, adapters), adapters

    @activity.defn(name=ACTIVITY_RUN_AGENT)
    async def run_agent(self, task: AgentTask) -> AgentRunResult:
        config_path = self._config_path(task.agent)
        text = build_agent_task_text(task)
        registry, adapters = self._build_registry()
        try:
            try:
                cfg = load_agent_config(config_path, registry)
            except AgentConfigError as e:
                logger.error("bad agent config agent=%s: %s", task.agent, e)
                raise _non_retryable(str(e), "BadAgentConfig") from e
            sink = self.sink_factory() if self.sink_factory else PostgresEvidenceSink(self.sessions)
            tool_ctx = ToolContext(incident_id=task.incident_id, evidence=sink)
            return await run_agent_loop(cfg, text, tool_ctx, self.llm, ToolGateway(registry))
        finally:
            if adapters is not None:
                try:
                    await adapters.aclose()
                except Exception:
                    # Never mask the run's own result or exception.
                    logger.exception(
                        "closing read adapters failed agent=%s incident=%s",
                        task.agent,
                        task.incident_id,
                    )

    async def aclose(self) -> None:
        if self.engine is not None:
            await self.engine.dispose()


def build_agent_activities() -> AgentActivities:
    """Build the process-wide pieces once from the environment (worker startup)."""
    from firebreak.db.engine import make_engine, make_session_factory
    from firebreak.llm.client import build_llm_client

    engine = make_engine()
    return AgentActivities(
        llm=build_llm_client(),
        sessions=make_session_factory(engine),
        tools_settings=ToolSettings(),
        settings=AgentSettings(),
        engine=engine,
    )

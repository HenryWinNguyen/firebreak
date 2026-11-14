"""``IncidentWorkflow``: triage -> plan -> parallel specialists -> synthesize (ARCH §4, T-064).

Workflow code holds decisions only. Every database, LLM or tool call is an activity, and
nothing here reads the clock, random numbers or the network. M5 ends at ``REPORT_ONLY``
(critic is M6, remediation M8).

Re-execution safety: after a worker crash Temporal re-runs the in-flight activity from the
start. For ``run_agent`` that is acceptable because it only reads (tools are read-only) and
evidence ids are minted per tool call, so a re-run records new evidence rows instead of
corrupting old ones (orphaned evidence from the dead attempt is harmless).
``set_incident_state`` is "set to X", so it is idempotent.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from pydantic import BaseModel, ValidationError

    from firebreak.models.agents import AgentRunResult
    from firebreak.models.evidence import SpecialistReport
    from firebreak.models.incident import IncidentState
    from firebreak.models.llm import Usage
    from firebreak.models.orchestration import (
        ACTIVITY_LOAD_CONTEXT,
        ACTIVITY_RUN_AGENT,
        ACTIVITY_SET_INCIDENT_STATE,
        AGENT_TASK_QUEUE,
        AgentTask,
        IncidentContext,
        Plan,
        SpecialistTask,
        StateUpdate,
        TriageResult,
        TriageVerdict,
        WorkflowOutcome,
    )
    from firebreak.models.reasoning import InvestigationReport

MAX_ATTEMPTS = 3
DB_TIMEOUT = timedelta(seconds=30)
TRIAGE_TIMEOUT = timedelta(seconds=300)
SUPERVISOR_TIMEOUT = timedelta(seconds=300)
SYNTHESIZER_TIMEOUT = timedelta(seconds=600)
#: ``ApplicationError(non_retryable=True)`` from an activity is never retried by Temporal.
RETRY = RetryPolicy(maximum_attempts=MAX_ATTEMPTS)

WORKFLOW_ID_PREFIX = "incident-"


def workflow_id(incident_id: str) -> str:
    return f"{WORKFLOW_ID_PREFIX}{incident_id}"


class _Invalid(Exception):
    """An agent step produced no usable output; the message becomes ``WorkflowOutcome.error``."""


def _describe(e: ActivityError) -> str:
    """Root cause of a failed activity (the ``ActivityError`` wrapper only says it failed)."""
    root: BaseException = e
    while root.__cause__ is not None:
        root = root.__cause__
    return f"{type(root).__name__}: {root}"


@workflow.defn(name="IncidentWorkflow")
class IncidentWorkflow:
    def __init__(self) -> None:
        self._runs: list[AgentRunResult] = []
        self._usage = Usage()

    @workflow.run
    async def run(self, incident_id: str) -> WorkflowOutcome:
        ctx: IncidentContext = await workflow.execute_activity(
            ACTIVITY_LOAD_CONTEXT,
            incident_id,
            result_type=IncidentContext,
            task_queue=AGENT_TASK_QUEUE,
            start_to_close_timeout=DB_TIMEOUT,
            retry_policy=RETRY,
        )

        # TRIAGE
        await self._set_state(incident_id, IncidentState.TRIAGE)
        try:
            triage = await self._step("triage", ctx, TriageResult, limit=TRIAGE_TIMEOUT)
        except _Invalid as e:
            return await self._finish(incident_id, IncidentState.ESCALATED, ctx, error=str(e))
        ctx = ctx.model_copy(update={"triage": triage})
        if triage.verdict in (TriageVerdict.NOISE, TriageVerdict.DUPLICATE):
            return await self._finish(incident_id, IncidentState.DISMISSED, ctx)

        # PLANNING
        await self._set_state(incident_id, IncidentState.PLANNING)
        try:
            plan = await self._step("supervisor", ctx, Plan, limit=SUPERVISOR_TIMEOUT)
        except _Invalid as e:
            return await self._finish(incident_id, IncidentState.ESCALATED, ctx, error=str(e))
        ctx = ctx.model_copy(update={"plan": plan})

        # INVESTIGATING
        await self._set_state(incident_id, IncidentState.INVESTIGATING)
        reports, degraded = await self._fan_out(ctx, plan.tasks)
        ctx = ctx.model_copy(update={"reports": reports, "degraded_sources": degraded})
        if not reports:
            return await self._finish(
                incident_id,
                IncidentState.ESCALATED,
                ctx,
                error=f"all specialists failed: {', '.join(degraded)}",
            )

        # SYNTHESIZING
        await self._set_state(incident_id, IncidentState.SYNTHESIZING)
        try:
            report = await self._step(
                "synthesizer", ctx, InvestigationReport, limit=SYNTHESIZER_TIMEOUT
            )
        except _Invalid as e:
            return await self._finish(incident_id, IncidentState.ESCALATED, ctx, error=str(e))
        return await self._finish(incident_id, IncidentState.REPORT_ONLY, ctx, report=report)

    # -- helpers -------------------------------------------------------------------------

    async def _set_state(self, incident_id: str, state: IncidentState) -> None:
        await workflow.execute_activity(
            ACTIVITY_SET_INCIDENT_STATE,
            StateUpdate(incident_id=incident_id, state=state),
            task_queue=AGENT_TASK_QUEUE,
            start_to_close_timeout=DB_TIMEOUT,
            retry_policy=RETRY,
        )

    async def _finish(
        self,
        incident_id: str,
        state: IncidentState,
        ctx: IncidentContext,
        *,
        report: InvestigationReport | None = None,
        error: str | None = None,
    ) -> WorkflowOutcome:
        # The terminal state is recorded before returning so the DB matches the outcome.
        await self._set_state(incident_id, state)
        return WorkflowOutcome(
            incident_id=incident_id,
            final_state=state,
            report=report,
            degraded_sources=list(ctx.degraded_sources),
            agent_runs=list(self._runs),
            usage=self._usage,
            error=error,
        )

    async def _call_agent(
        self,
        agent: str,
        ctx: IncidentContext,
        limit: timedelta,
        question: str | None,
    ) -> AgentRunResult:
        run: AgentRunResult = await workflow.execute_activity(
            ACTIVITY_RUN_AGENT,
            AgentTask(agent=agent, incident_id=ctx.incident_id, question=question, context=ctx),
            result_type=AgentRunResult,
            task_queue=AGENT_TASK_QUEUE,
            start_to_close_timeout=limit,
            retry_policy=RETRY,
        )
        return run

    def _record(self, run: AgentRunResult) -> None:
        self._runs.append(run)
        self._usage = self._usage + run.usage

    @staticmethod
    def _validate[M: BaseModel](agent: str, run: AgentRunResult, schema: type[M]) -> M:
        if run.stopped_reason != "final_output":
            raise _Invalid(
                f"{agent} ended without output: {run.stopped_reason}"
                + (f" ({run.error})" if run.error else "")
            )
        try:
            return schema.model_validate(run.output)
        except ValidationError as e:
            raise _Invalid(f"{agent} output is not a valid {schema.__name__}: {e}") from e

    async def _step[M: BaseModel](
        self,
        agent: str,
        ctx: IncidentContext,
        schema: type[M],
        *,
        limit: timedelta,
    ) -> M:
        """Run one non-specialist agent. Failure of any kind raises ``_Invalid`` (no defaults)."""
        try:
            run = await self._call_agent(agent, ctx, limit, None)
        except ActivityError as e:
            raise _Invalid(f"{agent} activity failed: {_describe(e)}") from e
        self._record(run)
        return self._validate(agent, run, schema)

    async def _fan_out(
        self, ctx: IncidentContext, tasks: list[SpecialistTask]
    ) -> tuple[list[SpecialistReport], list[str]]:
        """Run all specialists in parallel; failures degrade instead of aborting (§4.3)."""
        results = await asyncio.gather(
            *(
                self._call_agent(t.agent, ctx, timedelta(seconds=t.timeout_s), t.question)
                for t in tasks
            ),
            return_exceptions=True,
        )
        reports: list[SpecialistReport] = []
        degraded: list[str] = []
        for task, result in zip(tasks, results, strict=True):
            if isinstance(result, ActivityError):
                workflow.logger.warning("specialist %s failed: %s", task.agent, _describe(result))
                degraded.append(task.agent)
                continue
            if isinstance(result, BaseException):
                raise result  # cancellation or a workflow bug: never swallow
            self._record(result)
            try:
                report = self._validate(task.agent, result, SpecialistReport)
                if report.agent != task.agent:
                    raise _Invalid(f"{task.agent} returned a report for agent {report.agent!r}")
            except _Invalid as e:
                workflow.logger.warning("specialist %s invalid: %s", task.agent, e)
                degraded.append(task.agent)
                continue
            reports.append(report)
        return reports, list(dict.fromkeys(degraded))

"""``IncidentWorkflow``: triage, plan, parallel specialists, synthesize (ARCH §4).

Workflow code is deterministic and does no I/O: every LLM or database call is an activity,
referenced by name (CONTRACTS v1.6), so this module imports only contract models.

Failure model (§4.3). A specialist that raises, times out, stops without ``final_output`` or
returns something that is not a valid ``SpecialistReport`` for its own agent goes into
``degraded_sources``; the others continue. Triage, supervisor or synthesizer without valid output
(or all specialists failing) ends in ``ESCALATED`` with ``WorkflowOutcome.error``. There are no
silent defaults and no invented plans.

Retry and re-execution: every activity has 3 attempts; ``ApplicationError(non_retryable=True)``
is not retried. Re-running ``run_agent`` after a worker death is acceptable because evidence ids
are per tool call (a re-run records new evidence rows rather than clashing with the first
attempt's) and nothing is persisted from agent output in M5, so a second run simply produces a
second complete result.
"""

import asyncio
from datetime import timedelta
from typing import TypeVar

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

_RETRY = RetryPolicy(maximum_attempts=MAX_ATTEMPTS)

M = TypeVar("M", bound=BaseModel)


def _describe(exc: BaseException) -> str:
    """Root cause of an activity failure (``ActivityError`` wraps the real one)."""
    cause = exc.__cause__
    return f"{type(cause).__name__}: {cause}" if cause is not None else str(exc)


class _Escalate(Exception):
    """Internal: a mandatory step had no usable output."""


class _NoOutput(Exception):
    """Internal: an agent run ended without ``final_output``."""


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
            retry_policy=_RETRY,
        )
        try:
            return await self._investigate(ctx)
        except _Escalate as e:
            workflow.logger.error("incident %s escalated: %s", incident_id, e)
            return await self._finish(ctx, IncidentState.ESCALATED, error=str(e))

    async def _investigate(self, ctx: IncidentContext) -> WorkflowOutcome:
        await self._set_state(ctx.incident_id, IncidentState.TRIAGE)
        triage = await self._mandatory(ctx, "triage", TriageResult, TRIAGE_TIMEOUT)
        ctx = ctx.model_copy(update={"triage": triage})
        if triage.verdict in (TriageVerdict.NOISE, TriageVerdict.DUPLICATE):
            return await self._finish(ctx, IncidentState.DISMISSED)

        await self._set_state(ctx.incident_id, IncidentState.PLANNING)
        plan = await self._mandatory(ctx, "supervisor", Plan, SUPERVISOR_TIMEOUT)
        ctx = ctx.model_copy(update={"plan": plan})

        await self._set_state(ctx.incident_id, IncidentState.INVESTIGATING)
        outcomes = await asyncio.gather(*(self._specialist(ctx, t) for t in plan.tasks))
        reports = [r for r in outcomes if r is not None]
        degraded = list(
            dict.fromkeys(t.agent for t, r in zip(plan.tasks, outcomes, strict=True) if r is None)
        )
        ctx = ctx.model_copy(update={"reports": reports, "degraded_sources": degraded})
        if not reports:
            raise _Escalate(f"all specialists failed: {', '.join(degraded)}")

        await self._set_state(ctx.incident_id, IncidentState.SYNTHESIZING)
        report = await self._mandatory(ctx, "synthesizer", InvestigationReport, SYNTHESIZER_TIMEOUT)
        return await self._finish(ctx, IncidentState.REPORT_ONLY, report=report)

    async def _set_state(self, incident_id: str, state: IncidentState) -> None:
        await workflow.execute_activity(
            ACTIVITY_SET_INCIDENT_STATE,
            StateUpdate(incident_id=incident_id, state=state),
            task_queue=AGENT_TASK_QUEUE,
            start_to_close_timeout=DB_TIMEOUT,
            retry_policy=_RETRY,
        )

    async def _finish(
        self,
        ctx: IncidentContext,
        state: IncidentState,
        *,
        report: InvestigationReport | None = None,
        error: str | None = None,
    ) -> WorkflowOutcome:
        await self._set_state(ctx.incident_id, state)
        return WorkflowOutcome(
            incident_id=ctx.incident_id,
            final_state=state,
            report=report,
            degraded_sources=ctx.degraded_sources,
            agent_runs=self._runs,
            usage=self._usage,
            error=error,
        )

    async def _call(
        self, ctx: IncidentContext, agent: str, question: str | None, limit: timedelta
    ) -> dict[str, object]:
        """Run one agent and return its output. Raises on any failure; never swallows it."""
        result: AgentRunResult = await workflow.execute_activity(
            ACTIVITY_RUN_AGENT,
            AgentTask(agent=agent, incident_id=ctx.incident_id, question=question, context=ctx),
            result_type=AgentRunResult,
            task_queue=AGENT_TASK_QUEUE,
            start_to_close_timeout=limit,
            retry_policy=_RETRY,
        )
        self._runs.append(result)
        self._usage = self._usage + result.usage
        if result.stopped_reason != "final_output":
            raise _NoOutput(
                f"{agent} stopped without output ({result.stopped_reason}): {result.error}"
            )
        return result.output

    async def _mandatory(
        self, ctx: IncidentContext, agent: str, schema: type[M], limit: timedelta
    ) -> M:
        try:
            return schema.model_validate(await self._call(ctx, agent, None, limit))
        except (ActivityError, _NoOutput, ValidationError) as e:
            raise _Escalate(f"{agent} produced no valid output: {_describe(e)}") from e

    async def _specialist(
        self, ctx: IncidentContext, task: SpecialistTask
    ) -> SpecialistReport | None:
        try:
            output = await self._call(
                ctx, task.agent, task.question, timedelta(seconds=task.timeout_s)
            )
            report = SpecialistReport.model_validate(output)
        except (ActivityError, _NoOutput, ValidationError) as e:
            workflow.logger.warning("specialist %s degraded: %s", task.agent, _describe(e))
            return None
        if report.agent != task.agent:
            workflow.logger.warning(
                "specialist %s degraded: report names agent %r", task.agent, report.agent
            )
            return None
        return report

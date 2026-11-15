"""Record/replay for the multi-agent ``IncidentWorkflow`` (T-067, ADR-005).

The baseline cassette (``cassette.py``) records one sequential agent: one deterministic id
stream, one tool stream in call-start order, one LLM key log. The workflow runs specialists in
parallel, so none of those single streams is deterministic any more: the specialists' tool calls
interleave differently on every live run, and evidence ids handed out from one global
``deterministic(...)`` counter would differ between record and replay, which changes the LLM
request hashes. So every ``run_agent`` activity call gets its own small recording.

Design (how record and replay stay identical with parallel specialists):

- **Per-call key.** Each ``run_agent`` call is named ``<agent>-<k>`` (``triage-0``,
  ``synthesizer-0``) or, for a specialist, ``<agent>-<sha256(question)[:8]>-<k>``
  (``metrics-1a2b3c4d-0``). ``k`` is an occurrence index for the same (agent, question): stages
  run in a fixed order, so repeats across stages count up deterministically. When the supervisor
  plans the *same* agent and question twice in one fan-out, the identical tasks are told apart by
  their Temporal activity id (the workflow schedules them in plan order, so the id order is the
  same on every run): each waits until all of its identical siblings have started, then takes the
  index of its activity id among them. Keys depend on the plan only, never on completion order.
- **Per-call determinism.** The wrapper enters ``deterministic(seed, at)`` *inside* the activity
  (Temporal activity tasks don't inherit the caller's contextvars) with
  ``seed = "<scenario>/<call key>@<at ISO>"`` and the recording's ``at`` (read-tool query windows
  use ``utcnow``). Evidence and tool-call ids therefore depend only on the call's own key and its
  own call order, whichever specialist finishes first.
- **Per-call streams.** Each call is an ordinary T-044 recording in ``calls/<key>/``: its own
  ``tools.jsonl`` (call-start order within that agent run, ``_InOrder``), its own LLM fixtures
  keyed by request hash and its own ``meta.json`` (seed, at, LLM step keys in that call's order).
  Replay builds one ``StrictReplayLLMClient`` per call with that call's keys, so a miss names the
  scenario, the call and the step (e.g. ``scenario 's01/metrics-1a2b3c4d-0' at llm step 2``).
- **Retries and timeouts in record.** Local models are slow, so specialists do time out and model
  servers do drop connections; degraded runs (ARCH §4.3) are part of what the evals measure.
  Temporal's retry policy applies unchanged. Each attempt is stopped ``DEADLINE_MARGIN_S`` before
  its start_to_close (``ActivityTimeout``, retryable; the timer starts after any cancel-wait and
  lock), and a result that still arrives after start_to_close is recorded as a failure, so an
  attempt Temporal timed out is never kept as a success. Temporal doesn't cancel
  an in-process attempt itself (no heartbeats), so attempt ``n > 1`` also cancels any abandoned
  earlier attempt and waits for its cleanup, then re-records its call from scratch with its own seed
  ``<scenario>/<key>#a<n>@<at>``, so its evidence ids never collide with a dead attempt's rows
  (``EvidenceConflictError``). Only the attempt that returned is kept; ``replay_run`` reads each
  call's seed from its ``meta.json``.
- **Terminal failures.** A call whose last attempt failed (retries exhausted, non-retryable error,
  final timeout) is kept as ``calls/<key>/failed.json`` (``FailedCall``) plus that attempt's first
  LLM request (``first_request.json``, its fixture key in ``FailedCall.llm_steps``). Replay runs
  the agent up to that first request, checks it against the recording (prompt, tool schemas,
  model and task: a difference is ``StaleRecordingError`` naming the call), then raises the
  recorded failure as a non-retryable ``ApplicationError``. The call must end with exactly that
  failure (or, if it failed before any LLM request, an error of the recorded type raised before
  one); any other error or a result is drift. Only the first LLM request is checked, so drift
  that would show only from step 2 on isn't detected for failed calls. The workflow degrades (or
  escalates) exactly as it did live: same ``degraded_sources``, same synthesizer prompt, same
  report. The scenario ``prompt_sha256`` covers these first requests too. Temporal's own error
  text for a timed-out non-specialist can't be reproduced from an activity, so the runner stores
  the recorded outcome's ``error`` in ``run.json`` and uses it when the same agent escalated.
- **Consistency.** After a recorded workflow, the completed calls per agent must equal the
  workflow's ``agent_runs`` per agent, and every degraded agent must have a failed or completed
  call; otherwise the recording could not replay and is refused.
- **Recording problems** (a secret in a recording, a failed write, stable keys impossible) are
  not agent failures: they abort the workflow at once (no more model time is spent) and nothing
  is kept.
- **Drift.** A ``StaleRecordingError`` inside an activity would otherwise become a degraded
  specialist; the wrapper keeps it, fails the activity as non-retryable, aborts the workflow and
  ``replay_multi`` re-raises it. A recorded call that was never made in replay, or a call that was
  never recorded, is stale too. A call that failed in replay for another reason (database down)
  is a ``MultiReplayError``, not drift. Replay never falls back to a live call.

Layout::

    evals/recordings/multi-local/<scenario>/
        meta.json              scenario-level RecordingMeta (the Makefile counts */meta.json):
                               seed, at, models, prompt sha256 of every completed call, their LLM
                               keys concatenated in call-key order, total tool calls
        run.json               incident, alerts, live latency, final state, degraded sources and
                               outcome error (runner.py)
        calls/<call key>/      one T-044 recording per completed run_agent call
            meta.json
            tools.jsonl
            llm/<key>.json
        calls/<call key>/      or, for a call that failed terminally while recording
            failed.json
            first_request.json     (absent if it failed before its first LLM request)

Record uses the local Temporal dev server (``start_local``: real time, so real LLM latency
meets the real activity timeouts); replay uses the time-skipping test server. Both are started
in-process by ``temporalio.testing.WorkflowEnvironment`` (the binary is downloaded and cached on
first use).
"""

import asyncio
import contextlib
import dataclasses
import hashlib
import logging
import os
import secrets as _secrets
import shutil
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NoReturn, Protocol

from pydantic import BaseModel, ConfigDict, Field
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from firebreak.activities.agents import NON_SPECIALISTS, AgentActivities
from firebreak.evals.cassette import (
    META_FILE,
    Cassette,
    CassetteMode,
    RecordingMeta,
    SecretInRecordingError,
    StaleRecordingError,
    _atomic_write,
    _load_meta,
    _prompt_sha256,
    _swap_in,
    default_secrets,
    find_secrets,
    record_run,
    replay_run,
)
from firebreak.ingest.service_graph import ServiceGraph
from firebreak.llm.client import KeyLoggingRecorder
from firebreak.llm.fixtures import first_difference, fixture_key
from firebreak.models.agents import AgentRunResult
from firebreak.models.common import deterministic
from firebreak.models.llm import LLMClient, LLMRequest, LLMResponse
from firebreak.models.orchestration import (
    ACTIVITY_RUN_AGENT,
    AGENT_TASK_QUEUE,
    ORCHESTRATOR_TASK_QUEUE,
    AgentTask,
    IncidentContext,
    StateUpdate,
    WorkflowOutcome,
)
from firebreak.tools.read_tools import ReadAdapters, build_read_registry
from firebreak.tools.registry import ToolRegistry
from firebreak.workflows.incident import IncidentWorkflow, workflow_id

__all__ = [
    "CALLS_DIR",
    "FAILED_FILE",
    "FailedCall",
    "IncidentActivitiesLike",
    "MultiRecordError",
    "MultiReplayError",
    "MultiRun",
    "UnsetLLM",
    "call_key",
    "record_multi",
    "replay_multi",
    "run_incident_workflow",
]

logger = logging.getLogger(__name__)

CALLS_DIR = "calls"
FAILED_FILE = "failed.json"
FIRST_REQUEST_FILE = "first_request.json"
FAILED_FORMAT_VERSION = 1
#: How long identical sibling tasks wait for each other to start (they are scheduled together).
DEFAULT_GROUP_TIMEOUT_S = 60.0
#: ``FailedCall.error_type`` for an attempt that was cancelled (workflow end, shutdown).
CANCELLED = "ActivityCancelled"
#: Record stops an attempt this long before its start_to_close (``LOCAL_TIMEOUT``): an attempt
#: that finished just after Temporal timed it out would otherwise be kept as a success while the
#: workflow saw a timeout.
DEADLINE_MARGIN_S = 1.0
LOCAL_TIMEOUT = "ActivityTimeout"


class MultiRecordError(Exception):
    """A multi-agent record run cannot be kept (a recording problem, not an agent failure)."""


class MultiReplayError(Exception):
    """A replayed call failed for a reason that is not drift (e.g. the database is down)."""


class FailedCall(BaseModel):
    """``calls/<key>/failed.json``: a call whose last attempt failed while recording."""

    model_config = ConfigDict(extra="forbid")

    format_version: int = FAILED_FORMAT_VERSION
    agent: str
    key: str
    error_type: str
    message: str
    attempts: int
    #: Fixture key of the final attempt's first LLM request (empty if it failed before one).
    #: The request itself is ``first_request.json`` next to this file; replay checks it.
    llm_steps: list[str] = Field(default_factory=list)


class UnsetLLM:
    """Placeholder ``llm`` for the base ``AgentActivities``: every call gets its own client."""

    async def complete(self, request: LLMRequest) -> LLMResponse:
        raise RuntimeError("UnsetLLM: the eval wrapper replaces the LLM client for every call")


class IncidentActivitiesLike(Protocol):
    """``IncidentActivities`` (or a test stand-in): the two non-agent activities."""

    async def load_context(self, incident_id: str) -> IncidentContext: ...

    async def set_incident_state(self, update: StateUpdate) -> None: ...


def call_key(agent: str, question: str | None, index: int) -> str:
    """Per-call recording name; stable across record and replay (module docstring)."""
    if question is None:
        return f"{agent}-{index}"
    qhash = hashlib.sha256(question.encode("utf-8")).hexdigest()[:8]
    return f"{agent}-{qhash}-{index}"


def _activity_order(activity_id: str) -> tuple[int, str]:
    # Temporal's default activity ids are "1", "2", ...: order numerically, not as text.
    return (len(activity_id), activity_id)


def _stale_in(exc: BaseException) -> StaleRecordingError | None:
    """The ``StaleRecordingError`` behind ``exc``: causes, contexts and exception groups."""
    seen: set[int] = set()
    todo: list[BaseException] = [exc]
    while todo:
        cur = todo.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        if isinstance(cur, StaleRecordingError):
            return cur
        if isinstance(cur, BaseExceptionGroup):
            todo.extend(cur.exceptions)
        todo.extend(e for e in (cur.__cause__, cur.__context__) if e is not None)
    return None


def _in_chain(exc: BaseException, target: BaseException) -> bool:
    """Whether ``target`` is ``exc`` or behind it (causes, contexts, exception groups)."""
    seen: set[int] = set()
    todo: list[BaseException] = [exc]
    while todo:
        cur = todo.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        if cur is target:
            return True
        if isinstance(cur, BaseExceptionGroup):
            todo.extend(cur.exceptions)
        todo.extend(e for e in (cur.__cause__, cur.__context__) if e is not None)
    return False


def _remaining_s(info: activity.Info, margin_s: float) -> float | None:
    """Seconds until ``margin_s`` before this attempt's start_to_close (None: no timeout)."""
    limit = info.start_to_close_timeout
    if not limit:
        return None
    elapsed = (datetime.now(UTC) - info.started_time).total_seconds()
    return limit.total_seconds() - margin_s - elapsed


class _FirstRequest:
    """Passes calls through and keeps the attempt's first request, logged when it is *sent*
    (a timed-out attempt may never get its answer)."""

    def __init__(self, inner: LLMClient) -> None:
        self._inner = inner
        self.first: LLMRequest | None = None

    async def complete(self, request: LLMRequest) -> LLMResponse:
        if self.first is None:
            self.first = request.model_copy(deep=True)
        return await self._inner.complete(request)


class _FailedCallLLM:
    """Replay of a call that failed while recording: step 1 must match the recorded request
    (prompt, tools, model, task), then the recorded failure is raised where the recording ends.
    """

    def __init__(self, failed: FailedCall, recorded: LLMRequest | None, scenario: str) -> None:
        self._failed = failed
        self._recorded = recorded
        self._scenario = scenario
        self.called = False
        #: The recorded failure this stub raised (only that exact error ends the call cleanly).
        self.raised: ApplicationError | None = None

    def failure(self) -> ApplicationError:
        f = self._failed
        return ApplicationError(
            f"recorded failure after {f.attempts} attempt(s): {f.message}",
            type=f.error_type,
            non_retryable=True,
        )

    async def complete(self, request: LLMRequest) -> LLMResponse:
        key = fixture_key(request)
        if self.called:  # the recorded failure was raised at step 1; nothing comes after it
            raise StaleRecordingError(
                scenario=self._scenario,
                step="llm step 2",
                detail="the call failed at step 1 while recording; replay went on",
                key=key,
            )
        self.called = True
        if self._recorded is None or not self._failed.llm_steps:
            raise StaleRecordingError(
                scenario=self._scenario,
                step="llm step 1",
                detail="the call failed before its first LLM request while recording; replay "
                "made one",
                key=key,
            )
        if key != self._failed.llm_steps[0]:
            raise StaleRecordingError(
                scenario=self._scenario,
                step="llm step 1",
                detail=first_difference(self._recorded, request)
                + " (this call failed while recording)",
                key=key,
            )
        self.raised = self.failure()
        raise self.raised


def _describe(e: BaseException) -> tuple[str, str]:
    if isinstance(e, asyncio.CancelledError):
        return CANCELLED, "attempt cancelled (start_to_close timeout, workflow end or shutdown)"
    if isinstance(e, ApplicationError) and e.type:
        return e.type, e.message
    return type(e).__name__, str(e)


@dataclass
class _Group:
    """Identical (agent, question) tasks of one fan-out."""

    size: int
    base: int
    ids: list[str] = field(default_factory=list)
    full: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class _Status:
    """Outcome of a call's latest attempt (record mode)."""

    agent: str
    attempt: int
    ok: bool = False
    error_type: str = ""
    message: str = ""
    first_request: LLMRequest | None = None


class MultiRun:
    """One ``IncidentWorkflow`` run being recorded or replayed, one sub-recording per call.

    ``run_agent`` is the activity to register under ``ACTIVITY_RUN_AGENT`` in place of
    ``AgentActivities.run_agent``. It runs the real activity (``dataclasses.replace`` of ``base``
    with the call's LLM client and cassette-backed read registry), so prompts, config loading,
    the gateway and the evidence sink are all the production path.
    """

    def __init__(
        self,
        mode: CassetteMode,
        directory: Path,
        *,
        agent: str,
        scenario: str,
        at: datetime,
        base: AgentActivities,
        adapters: ReadAdapters | None = None,
        llm: LLMClient | None = None,
        graph: ServiceGraph | None = None,
        secrets: Sequence[str] | None = None,
        recorded_calls: Sequence[str] = (),
        failed_calls: dict[str, FailedCall] | None = None,
        first_requests: dict[str, LLMRequest] | None = None,
        meta: RecordingMeta | None = None,
        group_timeout_s: float = DEFAULT_GROUP_TIMEOUT_S,
    ) -> None:
        if mode is CassetteMode.RECORD and (adapters is None or llm is None):
            raise ValueError("MultiRun record mode needs the real adapters and LLM client")
        self.mode = mode
        self.directory = directory
        self.agent = agent
        self.scenario = scenario
        self.at = at
        self.meta = meta
        self.recorded_calls = list(recorded_calls)
        #: Replay: failures loaded from ``failed.json``. Record: filled by ``finish_record``.
        self.failed_calls: dict[str, FailedCall] = dict(failed_calls or {})
        self._base = base
        self._adapters = adapters
        self._llm = llm
        self._graph = graph
        self._secrets = secrets
        self._group_timeout_s = group_timeout_s
        self._keys: dict[str, str] = {}  # activity id -> call key (retries reuse it)
        self._seen: dict[tuple[str, str | None], int] = {}
        self._groups: dict[tuple[str, str | None], _Group] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._running: dict[str, asyncio.Task[object]] = {}
        #: Replay: each failed call's recorded first request (``first_request.json``).
        self._first_requests: dict[str, LLMRequest] = dict(first_requests or {})
        self._status: dict[str, _Status] = {}
        self._recorders: dict[str, KeyLoggingRecorder] = {}
        self._last_error: dict[str, str] = {}
        #: Call keys that returned an ``AgentRunResult`` (record: on their latest attempt).
        self.completed: list[str] = []
        #: Replay: keys whose recorded failure was raised again.
        self.replayed_failures: list[str] = []
        #: Record mode: why the recording can't be kept. Raised as ``MultiRecordError``.
        self.problems: list[str] = []
        #: Replay mode: drift found inside activities, re-raised by ``replay_multi``.
        self.stale: list[StaleRecordingError] = []
        #: Set on the first problem or drift: ``run_incident_workflow`` terminates the workflow.
        self.abort = asyncio.Event()

    # --- per-call keys -----------------------------------------------------------------

    @staticmethod
    def _identical_in_plan(task: AgentTask) -> int:
        plan = task.context.plan
        if task.agent in NON_SPECIALISTS or plan is None:
            return 1
        n = sum(1 for t in plan.tasks if (t.agent, t.question) == (task.agent, task.question))
        return max(1, n)

    async def _key_for(self, task: AgentTask, activity_id: str) -> str:
        known = self._keys.get(activity_id)
        if known is not None:
            return known
        pair = (task.agent, task.question)
        size = self._identical_in_plan(task)
        if size == 1:
            index = self._seen.get(pair, 0)
            self._seen[pair] = index + 1
        else:
            group = self._groups.get(pair)
            if group is None or len(group.ids) >= group.size:
                group = _Group(size=size, base=self._seen.get(pair, 0))
                self._seen[pair] = group.base + size
                self._groups[pair] = group
            group.ids.append(activity_id)
            if len(group.ids) == group.size:
                group.full.set()
            try:
                async with asyncio.timeout(self._group_timeout_s):
                    await group.full.wait()
            except TimeoutError as e:
                raise RuntimeError(
                    f"only {len(group.ids)} of {group.size} identical {task.agent!r} tasks "
                    f"started within {self._group_timeout_s}s; cannot assign stable call keys"
                ) from e
            index = group.base + sorted(group.ids, key=_activity_order).index(activity_id)
        key = call_key(task.agent, task.question, index)
        self._keys[activity_id] = key
        return key

    # --- problems ------------------------------------------------------------------------

    def _problem(self, message: str, cause: BaseException | None = None) -> NoReturn:
        self.problems.append(message)
        logger.error("multi record scenario=%s: %s", self.scenario, message)
        self.abort.set()
        raise ApplicationError(message, type="RecordingProblem", non_retryable=True) from cause

    def _drift(self, stale: StaleRecordingError, cause: BaseException, key: str) -> NoReturn:
        self.stale.append(stale)
        logger.error("multi replay scenario=%s call=%s: %s", self.scenario, key, stale)
        self.abort.set()
        raise ApplicationError(str(stale), type="StaleRecording", non_retryable=True) from cause

    def replay_failure(self) -> "MultiReplayError | None":
        """A call that failed in replay for a non-drift reason and never completed.

        It comes first: once a call fails (database down), every later prompt differs, so the
        drift found downstream is a consequence, not the cause.
        """
        done = set(self.completed) | set(self.replayed_failures)
        failed = [k for k in self._last_error if k not in done]
        if not failed:
            return None
        return MultiReplayError(
            f"scenario {self.scenario}: call {failed[0]} failed in replay, not because the "
            f"recording is stale: {self._last_error[failed[0]]}"
        )

    def raise_abort(self) -> NoReturn:
        """The reason the run was aborted: replay failure, drift, then recording problems."""
        failure = self.replay_failure()
        if failure is not None:
            raise failure from (self.stale[0] if self.stale else None)
        if self.stale:
            raise self.stale[0]
        raise MultiRecordError(
            f"scenario {self.scenario}: recording not kept: " + "; ".join(self.problems)
        )

    # --- the activity ------------------------------------------------------------------

    def _inner(self, llm: LLMClient, adapters: ReadAdapters) -> AgentActivities:
        settings = self._base.tools_settings

        def registry() -> tuple[ToolRegistry, ReadAdapters | None]:
            # None: nothing to close per call (record shares the real adapters across calls).
            return build_read_registry(settings, adapters), None

        return dataclasses.replace(self._base, llm=llm, registry_factory=registry)

    @activity.defn(name=ACTIVITY_RUN_AGENT)
    async def run_agent(self, task: AgentTask) -> AgentRunResult:
        info = activity.info()
        try:
            key = await self._key_for(task, info.activity_id)
        except RuntimeError as e:
            if self.mode is CassetteMode.RECORD:
                self._problem(f"{task.agent}: {e}", e)
            raise MultiReplayError(f"{task.agent}: {e}") from e
        if self.mode is CassetteMode.RECORD:
            return await self._record_attempt(key, task, info)
        return await self._replay_attempt(key, task, info.attempt)

    def _call_scenario(self, key: str, attempt: int) -> str:
        base = f"{self.scenario}/{key}"
        return base if attempt == 1 else f"{base}#a{attempt}"

    async def _record_attempt(
        self, key: str, task: AgentTask, info: activity.Info
    ) -> AgentRunResult:
        if self._adapters is None or self._llm is None:  # guarded in __init__
            raise RuntimeError("record mode without adapters or LLM client")
        attempt = info.attempt
        latest = self._status.get(key)
        if latest is None or attempt >= latest.attempt:
            self._status[key] = _Status(agent=task.agent, attempt=attempt)
            # An earlier attempt's late success must not count once this attempt has started.
            self._recorders.pop(key, None)
            self.completed = [k for k in self.completed if k != key]
        # Temporal does not cancel an in-process attempt whose start_to_close expired (no
        # heartbeats), so the dead attempt may still be running: cancel it (it stops spending
        # model time) and let it finish its cleanup before this attempt records.
        previous = self._running.get(key)
        current = asyncio.current_task()
        if previous is not None and previous is not current and not previous.done():
            logger.warning(
                "multi record scenario=%s call=%s: cancelling the abandoned attempt before "
                "attempt %d",
                self.scenario,
                key,
                attempt,
            )
            previous.cancel()
            await asyncio.wait({previous})
        if current is not None:
            self._running[key] = current
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:  # belt and braces: one attempt per key records at a time
            agent_error: BaseException | None = None
            recorder: KeyLoggingRecorder | None = None
            first: _FirstRequest | None = None
            # Started only now: the cancel-wait and the lock above must not eat the margin.
            remaining = _remaining_s(info, DEADLINE_MARGIN_S)
            loop = asyncio.get_running_loop()
            timer = asyncio.timeout_at(
                None if remaining is None else loop.time() + max(0.0, remaining)
            )
            try:
                with record_run(
                    self.directory / CALLS_DIR / key,
                    agent=self.agent,
                    scenario=self._call_scenario(key, attempt),
                    at=self.at,
                    adapters=self._adapters,
                    llm=self._llm,
                    secrets=self._secrets,
                    overwrite=attempt > 1,
                ) as rec:
                    recorder = rec.llm
                    first = _FirstRequest(rec.llm)
                    try:
                        async with timer:
                            result = await self._inner(first, rec.adapters).run_agent(task)
                    except BaseException as e:
                        agent_error = e
                        raise
            except BaseException as e:
                if e is not agent_error and not isinstance(e, asyncio.CancelledError):
                    # The recording itself failed (secret found, write error): not the agent.
                    self._problem(
                        f"call {key} attempt {attempt}: recording failed: {type(e).__name__}: {e}",
                        e,
                    )
                first_request = first.first if first is not None else None
                if timer.expired():
                    stopped = ApplicationError(
                        f"attempt {attempt} stopped {DEADLINE_MARGIN_S:.0f}s before its "
                        "start_to_close timeout",
                        type=LOCAL_TIMEOUT,
                    )
                    self._attempt_failed(key, attempt, stopped, first_request)
                    raise stopped from e  # retryable: Temporal's policy decides
                self._attempt_failed(key, attempt, e, first_request)
                raise  # Temporal's retry policy decides; attempt n+1 re-records from scratch
        late = _remaining_s(info, 0.0)
        if late is not None and late <= 0:
            # Finished after start_to_close: Temporal already timed this attempt out and will
            # drop the result, so the workflow sees a failure. Record it as one.
            stopped = ApplicationError(
                f"attempt {attempt} finished after its start_to_close timeout",
                type=LOCAL_TIMEOUT,
            )
            self._attempt_failed(key, attempt, stopped, first.first if first else None)
            raise stopped
        self._attempt_ok(key, attempt, recorder)
        return result

    def _attempt_failed(
        self,
        key: str,
        attempt: int,
        e: BaseException,
        first_request: LLMRequest | None = None,
    ) -> None:
        error_type, message = _describe(e)
        status = self._status[key]
        logger.warning(
            "multi record scenario=%s call=%s attempt=%d failed: %s: %s",
            self.scenario,
            key,
            attempt,
            error_type,
            message,
        )
        if attempt >= status.attempt:
            status.ok, status.error_type, status.message = False, error_type, message
            status.first_request = first_request

    def _attempt_ok(self, key: str, attempt: int, recorder: KeyLoggingRecorder | None) -> None:
        status = self._status[key]
        if attempt < status.attempt or recorder is None:
            return  # a newer attempt is running and will replace this sub-recording
        status.ok = True
        self._recorders[key] = recorder
        self.completed.append(key)

    async def _replay_attempt(self, key: str, task: AgentTask, attempt: int) -> AgentRunResult:
        directory = self.directory / CALLS_DIR / key
        if key not in self.recorded_calls:
            stale = StaleRecordingError(
                scenario=self.scenario,
                step=f"call {key} ({task.agent})",
                detail=f"this run_agent call is not in the recording (recorded calls: "
                f"{', '.join(self.recorded_calls) or 'none'})",
                path=str(directory),
            )
            self._drift(stale, stale, key)
        failed = self.failed_calls.get(key)
        if failed is not None:
            return await self._replay_failed(key, task, failed)
        try:
            with replay_run(directory, graph=self._graph) as rp:
                result = await self._inner(rp.llm, rp.adapters).run_agent(task)
        except asyncio.CancelledError:
            self._last_error[key] = "cancelled"
            raise
        except Exception as e:
            found = _stale_in(e)
            if found is not None:
                self._drift(found, e, key)
            self._last_error[key] = f"{type(e).__name__}: {e}"
            logger.error(
                "multi replay scenario=%s call=%s attempt=%d failed (Temporal may retry): %s",
                self.scenario,
                key,
                attempt,
                self._last_error[key],
            )
            raise
        self.completed.append(key)
        return result

    async def _replay_failed(self, key: str, task: AgentTask, failed: FailedCall) -> NoReturn:
        """Run the agent against the recorded first request; the call must end with exactly the
        recorded failure, anything else is drift.

        With a recorded step 1, only the failure ``_FailedCallLLM`` raises after a matching step 1
        counts. With none (it failed before any LLM request), only an error of the recorded type
        raised before any LLM request counts. A returned result is drift too.
        """
        scenario = f"{self.scenario}/{key}"
        directory = self.directory / CALLS_DIR / key
        recorded = self._first_requests.get(key) if failed.llm_steps else None
        llm = _FailedCallLLM(failed, recorded, scenario)
        # No tool results exist for a failed call: any tool call is drift.
        adapters = Cassette(
            CassetteMode.REPLAY, directory, scenario=scenario, graph=self._graph
        ).adapters
        try:
            with deterministic(f"{scenario}#failed@{self.at.isoformat()}", at=self.at):
                await self._inner(llm, adapters).run_agent(task)
        except asyncio.CancelledError:
            self._last_error[key] = "cancelled"
            raise
        except Exception as e:
            found = _stale_in(e)
            if found is not None:
                self._drift(found, e, key)
            if llm.raised is not None and _in_chain(e, llm.raised):
                accepted = True
            elif recorded is None and not llm.called:
                accepted = _describe(e)[0] == failed.error_type
            else:
                accepted = False
            if not accepted:
                error_type, message = _describe(e)
                stale = StaleRecordingError(
                    scenario=scenario,
                    step="end of call",
                    detail=f"failed differently in replay: {error_type}: {message} (recorded: "
                    f"{failed.error_type}: {failed.message})",
                )
                self._drift(stale, e, key)
            if key not in self.replayed_failures:
                self.replayed_failures.append(key)
            raise llm.failure() from e
        stale = StaleRecordingError(
            scenario=scenario,
            step="end of call",
            detail="the call failed while recording but returned a result in replay",
        )
        self._drift(stale, stale, key)

    # --- end of run --------------------------------------------------------------------

    def raise_if_stale(self) -> None:
        if self.stale:
            raise self.stale[0]

    def check_replay_complete(self) -> None:
        """Every recorded call must have been replayed (completed, or its failure re-raised)."""
        done = set(self.completed) | set(self.replayed_failures)
        missing = [k for k in self.recorded_calls if k not in done]
        if not missing:
            return
        failure = self.replay_failure()
        if failure is not None:
            raise failure
        raise StaleRecordingError(
            scenario=self.scenario,
            step=f"call {missing[0]}",
            detail=f"recorded call(s) {', '.join(missing)} were never made in replay",
            path=str(self.directory / CALLS_DIR / missing[0]),
        )

    def check_record(self, outcome: WorkflowOutcome) -> None:
        """A recording must replay: what each call did must match what the workflow saw.

        Every completed call returned a result the workflow recorded in ``agent_runs`` (valid or
        not), so the completed keys per agent must equal the runs per agent. A mismatch (e.g. a
        success Temporal had already timed out) is a recording problem.
        """
        completed: dict[str, int] = {}
        for key in self.completed:
            agent = self._status[key].agent
            completed[agent] = completed.get(agent, 0) + 1
        runs: dict[str, int] = {}
        for r in outcome.agent_runs:
            runs[r.agent] = runs.get(r.agent, 0) + 1
        if completed != runs:
            self.problems.append(
                f"recorded calls per agent {dict(sorted(completed.items()))} do not match the "
                f"workflow's agent runs {dict(sorted(runs.items()))}; this recording could not "
                "replay"
            )
        failed_agents = {s.agent for s in self._status.values() if not s.ok}
        for agent in outcome.degraded_sources:
            if agent not in failed_agents and agent not in completed:
                self.problems.append(
                    f"{agent} is degraded but has neither a failed nor a completed call"
                )

    def finish_record(self) -> None:
        """Write ``failed.json`` for every call whose last attempt failed (record mode)."""
        for key, status in sorted(self._status.items()):
            if status.ok:
                continue
            first = status.first_request
            failed = FailedCall(
                agent=status.agent,
                key=key,
                error_type=status.error_type,
                message=status.message,
                attempts=status.attempt,
                llm_steps=[] if first is None else [fixture_key(first)],
            )
            directory = self.directory / CALLS_DIR / key
            if directory.exists():  # an earlier attempt's sub-recording, superseded
                shutil.rmtree(directory)
            directory.mkdir(parents=True)
            if first is not None:
                _atomic_write(
                    directory / FIRST_REQUEST_FILE, first.model_dump_json(indent=2) + "\n"
                )
                self._first_requests[key] = first
            _atomic_write(directory / FAILED_FILE, failed.model_dump_json(indent=2) + "\n")
            self.failed_calls[key] = failed

    def scenario_meta(self, recorded_on: date | None = None) -> RecordingMeta:
        """The scenario-level ``meta.json`` from the completed calls (record mode)."""
        calls = sorted(k for k in self._recorders if self._status[k].ok)
        metas = [_load_meta(self.directory / CALLS_DIR / k) for k in calls]
        requests = [r for k in calls for r in self._recorders[k].requests]
        requests += [self._first_requests[k] for k in sorted(self._first_requests)]
        return RecordingMeta(
            agent=self.agent,
            scenario=self.scenario,
            seed=f"{self.scenario}@{self.at.isoformat()}",
            at=self.at,
            models=sorted({m for meta in metas for m in meta.models}),
            prompt_sha256=_prompt_sha256(requests),
            llm_steps=[s for meta in metas for s in meta.llm_steps],
            tool_calls=sum(meta.tool_calls for meta in metas),
            recorded_on=recorded_on or datetime.now(UTC).date(),
        )


@contextmanager
def record_multi(
    directory: Path,
    *,
    agent: str,
    scenario: str,
    at: datetime,
    base: AgentActivities,
    adapters: ReadAdapters,
    llm: LLMClient,
    secrets: Sequence[str] | None = None,
    overwrite: bool = False,
    recorded_on: date | None = None,
) -> Iterator[MultiRun]:
    """Record one workflow run into ``directory``; the body runs the workflow with ``run_agent``.

    Like ``record_run``: everything goes to a staging directory, and only a body that finishes
    with no recording problem *and* passes the secret scan replaces ``directory``. Agent failures
    (timeouts, retries, degraded specialists) are part of the recording. Otherwise nothing is
    kept and the error propagates (``MultiRecordError`` for recording problems).
    """
    if directory.exists() and not overwrite:
        raise FileExistsError(f"recording {directory} exists; pass overwrite=True to re-record")
    tag = f"{os.getpid()}.{_secrets.token_hex(4)}"
    staging = directory.with_name(f".{directory.name}.recording.{tag}")
    staging.mkdir(parents=True)
    try:
        run = MultiRun(
            CassetteMode.RECORD,
            staging,
            agent=agent,
            scenario=scenario,
            at=at,
            base=base,
            adapters=adapters,
            llm=llm,
            secrets=secrets,
        )
        yield run
        if run.problems:
            run.raise_abort()
        run.finish_record()
        if not run.completed and not run.failed_calls:
            raise MultiRecordError(f"scenario {scenario}: no run_agent call was made")
        meta = run.scenario_meta(recorded_on)
        _atomic_write(staging / META_FILE, meta.model_dump_json(indent=2) + "\n")
        leaked = find_secrets(staging, default_secrets() if secrets is None else secrets)
        if leaked:
            names = ", ".join(str(p.relative_to(staging)) for p in leaked)
            raise SecretInRecordingError(
                f"recording for scenario {scenario!r} contains a secret in: {names}; "
                "nothing was kept"
            )
        _swap_in(staging, directory, tag)
    except BaseException:
        try:
            shutil.rmtree(staging)
        except OSError:
            logger.exception("could not remove the staging recording %s", staging)
        raise
    logger.info(
        "recorded multi scenario=%s agent=%s calls=%d failed=%d llm_steps=%d tool_calls=%d dir=%s",
        scenario,
        agent,
        len(run.completed),
        len(run.failed_calls),
        len(meta.llm_steps),
        meta.tool_calls,
        directory,
    )


def _load_failed(path: Path, scenario: str) -> FailedCall:
    try:
        failed = FailedCall.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise StaleRecordingError(
            scenario=scenario, step="setup", detail=f"{path} is not a valid {FAILED_FILE} ({e})"
        ) from e
    if failed.format_version != FAILED_FORMAT_VERSION:
        raise StaleRecordingError(
            scenario=scenario,
            step="setup",
            detail=f"{path}: format {failed.format_version}, this code reads "
            f"{FAILED_FORMAT_VERSION}",
        )
    return failed


@contextmanager
def replay_multi(
    directory: Path, *, base: AgentActivities, graph: ServiceGraph | None = None
) -> Iterator[MultiRun]:
    """Replay the workflow run recorded in ``directory``. No LLM client, no adapters.

    On exit, drift found inside any activity is raised as ``StaleRecordingError`` (even if the
    body itself failed because of it), then every recorded call must have been replayed.
    """
    meta = _load_meta(directory)
    calls_dir = directory / CALLS_DIR
    recorded = (
        sorted(p.name for p in calls_dir.iterdir() if p.is_dir() and not p.name.startswith("."))
        if calls_dir.is_dir()
        else []
    )
    failed = {
        k: _load_failed(calls_dir / k / FAILED_FILE, meta.scenario)
        for k in recorded
        if (calls_dir / k / FAILED_FILE).exists()
    }
    first_requests: dict[str, LLMRequest] = {}
    for k, f in failed.items():
        path = calls_dir / k / FIRST_REQUEST_FILE
        if f.llm_steps:
            try:
                first_requests[k] = LLMRequest.model_validate_json(path.read_text("utf-8"))
            except (OSError, ValueError) as e:
                raise StaleRecordingError(
                    scenario=meta.scenario, step="setup", detail=f"{path} is unreadable ({e})"
                ) from e
    run = MultiRun(
        CassetteMode.REPLAY,
        directory,
        agent=meta.agent,
        scenario=meta.scenario,
        at=meta.at,
        base=base,
        graph=graph or ServiceGraph.load(),
        recorded_calls=recorded,
        failed_calls=failed,
        first_requests=first_requests,
        meta=meta,
    )
    try:
        yield run
    except (StaleRecordingError, MultiReplayError):
        raise
    except BaseException as e:
        failure = run.replay_failure()
        if failure is not None:
            raise failure from e
        if run.stale:
            raise run.stale[0] from e
        raise
    failure = run.replay_failure()
    if failure is not None:
        raise failure from (run.stale[0] if run.stale else None)
    run.raise_if_stale()
    run.check_replay_complete()


async def run_incident_workflow(
    incident_id: str,
    *,
    run: MultiRun,
    incident_activities: IncidentActivitiesLike,
    time_skipping: bool,
) -> tuple[WorkflowOutcome, float]:
    """Run ``IncidentWorkflow`` in-process (both workers here) and return it with its latency.

    The latency is the workflow's wall time, excluding the test server's startup. A recording
    problem or drift (``run.abort``) terminates the workflow at once and raises it.
    """
    start = (
        WorkflowEnvironment.start_time_skipping
        if time_skipping
        else WorkflowEnvironment.start_local
    )
    async with await start(data_converter=pydantic_data_converter) as env:
        workflow_worker = Worker(
            env.client, task_queue=ORCHESTRATOR_TASK_QUEUE, workflows=[IncidentWorkflow]
        )
        activity_worker = Worker(
            env.client,
            task_queue=AGENT_TASK_QUEUE,
            activities=[
                incident_activities.load_context,
                incident_activities.set_incident_state,
                run.run_agent,
            ],
        )
        async with workflow_worker, activity_worker:
            started = time.monotonic()
            handle = await env.client.start_workflow(
                IncidentWorkflow.run,
                incident_id,
                id=workflow_id(incident_id),
                task_queue=ORCHESTRATOR_TASK_QUEUE,
            )
            result = asyncio.ensure_future(handle.result())
            aborted = asyncio.ensure_future(run.abort.wait())
            try:
                await asyncio.wait({result, aborted}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                aborted.cancel()
            if not result.done() and run.abort.is_set():
                result.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await result
                await handle.terminate(reason="eval run aborted (recording problem or drift)")
                run.raise_abort()
            outcome: WorkflowOutcome = await result
            latency = time.monotonic() - started
            if run.mode is CassetteMode.RECORD:
                run.check_record(outcome)
            return outcome, latency

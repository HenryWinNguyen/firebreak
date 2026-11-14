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
- **Retries.** Record fails on any ``run_agent`` attempt that raises or on ``attempt > 1``: a
  failed attempt leaves tool entries and LLM keys behind, a same-seed retry would collide in the
  evidence sink (``EvidenceConflictError``) and a live retry may see different data. Nothing is
  kept; re-record. Replay may retry (a database blip): the retry reuses the call key (cached by
  activity id), reopens the call's streams from the start and reproduces the same evidence ids,
  which ``PostgresEvidenceSink`` accepts as an identical retry.
- **Drift.** A ``StaleRecordingError`` inside an activity would otherwise become a degraded
  specialist; the wrapper keeps it, fails the activity as non-retryable and ``replay_multi``
  re-raises it after the workflow. A recorded call that did not complete in replay, or a call
  that was never recorded, is stale too. Replay never falls back to a live call.

Layout::

    evals/recordings/multi-local/<scenario>/
        meta.json              scenario-level RecordingMeta (the Makefile counts */meta.json):
                               seed, at, models, prompt sha256 of every call, the calls' LLM
                               keys concatenated in call-key order, total tool calls
        run.json               incident, alerts and live latency (runner.py)
        calls/<call key>/      one T-044 recording per run_agent call
            meta.json
            tools.jsonl
            llm/<key>.json

Record uses the local Temporal dev server (``start_local``: real time, so real LLM latency
meets the real activity timeouts); replay uses the time-skipping test server. Both are started
in-process by ``temporalio.testing.WorkflowEnvironment`` (the binary is downloaded and cached on
first use).
"""

import asyncio
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
from typing import Protocol

from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from firebreak.activities.agents import NON_SPECIALISTS, AgentActivities
from firebreak.evals.cassette import (
    META_FILE,
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
from firebreak.models.agents import AgentRunResult
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
    "IncidentActivitiesLike",
    "MultiRecordError",
    "MultiRun",
    "UnsetLLM",
    "call_key",
    "record_multi",
    "replay_multi",
    "run_incident_workflow",
]

logger = logging.getLogger(__name__)

CALLS_DIR = "calls"
#: How long identical sibling tasks wait for each other to start (they are scheduled together).
DEFAULT_GROUP_TIMEOUT_S = 60.0


class MultiRecordError(Exception):
    """A multi-agent record run cannot be kept (a call failed, was retried or cancelled)."""


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
    """The ``StaleRecordingError`` behind ``exc``, if any (activities wrap errors in causes)."""
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, StaleRecordingError):
            return cur
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    return None


@dataclass
class _Group:
    """Identical (agent, question) tasks of one fan-out."""

    size: int
    base: int
    ids: list[str] = field(default_factory=list)
    full: asyncio.Event = field(default_factory=asyncio.Event)


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
        self._base = base
        self._adapters = adapters
        self._llm = llm
        self._graph = graph
        self._secrets = secrets
        self._group_timeout_s = group_timeout_s
        self._keys: dict[str, str] = {}  # activity id -> call key (a replay retry reuses it)
        self._seen: dict[tuple[str, str | None], int] = {}
        self._groups: dict[tuple[str, str | None], _Group] = {}
        self._recorders: dict[str, KeyLoggingRecorder] = {}
        self._last_error: dict[str, str] = {}
        #: Call keys that returned an ``AgentRunResult``, in completion order (diagnostics only).
        self.completed: list[str] = []
        #: Record mode: why the recording can't be kept. Raised as ``MultiRecordError`` on exit.
        self.problems: list[str] = []
        #: Replay mode: drift found inside activities, re-raised by ``replay_multi``.
        self.stale: list[StaleRecordingError] = []

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
        if self.mode is CassetteMode.RECORD and info.attempt > 1:
            msg = (
                f"{task.agent}: run_agent attempt {info.attempt} (activity {info.activity_id}); "
                "a retried call can't be recorded deterministically"
            )
            self.problems.append(msg)
            logger.error("multi record scenario=%s: %s", self.scenario, msg)
            raise ApplicationError(msg, type="RetryDuringRecord", non_retryable=True)
        key = task.agent
        try:
            key = await self._key_for(task, info.activity_id)
            if self.mode is CassetteMode.RECORD:
                result = await self._record_call(key, task)
            else:
                result = await self._replay_call(key, task)
        except asyncio.CancelledError:
            if self.mode is CassetteMode.RECORD:
                self.problems.append(f"call {key} was cancelled (activity timeout or shutdown)")
            self._last_error[key] = "cancelled"
            raise
        except Exception as e:
            stale = _stale_in(e)
            if stale is not None:
                self.stale.append(stale)
                logger.error("multi replay scenario=%s call=%s: %s", self.scenario, key, stale)
                raise ApplicationError(str(stale), type="StaleRecording", non_retryable=True) from e
            self._last_error[key] = f"{type(e).__name__}: {e}"
            if self.mode is CassetteMode.RECORD:
                msg = f"call {key} failed: {type(e).__name__}: {e}"
                self.problems.append(msg)
                logger.error("multi record scenario=%s: %s", self.scenario, msg)
                raise ApplicationError(msg, type="RecordCallFailed", non_retryable=True) from e
            logger.error(
                "multi replay scenario=%s call=%s attempt=%d failed: %s",
                self.scenario,
                key,
                info.attempt,
                self._last_error[key],
            )
            raise
        self.completed.append(key)
        return result

    async def _record_call(self, key: str, task: AgentTask) -> AgentRunResult:
        if self._adapters is None or self._llm is None:  # guarded in __init__
            raise RuntimeError("record mode without adapters or LLM client")
        with record_run(
            self.directory / CALLS_DIR / key,
            agent=self.agent,
            scenario=f"{self.scenario}/{key}",
            at=self.at,
            adapters=self._adapters,
            llm=self._llm,
            secrets=self._secrets,
        ) as rec:
            self._recorders[key] = rec.llm
            return await self._inner(rec.llm, rec.adapters).run_agent(task)

    async def _replay_call(self, key: str, task: AgentTask) -> AgentRunResult:
        directory = self.directory / CALLS_DIR / key
        if key not in self.recorded_calls:
            raise StaleRecordingError(
                scenario=self.scenario,
                step=f"call {key} ({task.agent})",
                detail=f"this run_agent call is not in the recording (recorded calls: "
                f"{', '.join(self.recorded_calls) or 'none'})",
                path=str(directory),
            )
        with replay_run(directory, graph=self._graph) as rp:
            return await self._inner(rp.llm, rp.adapters).run_agent(task)

    # --- end of run --------------------------------------------------------------------

    def raise_if_stale(self) -> None:
        if self.stale:
            raise self.stale[0]

    def check_replay_complete(self) -> None:
        """Every recorded call must have completed in replay."""
        done = set(self.completed)
        missing = [k for k in self.recorded_calls if k not in done]
        if missing:
            first = missing[0]
            why = self._last_error.get(first, "it was never called")
            raise StaleRecordingError(
                scenario=self.scenario,
                step=f"call {first}",
                detail=f"recorded call(s) {', '.join(missing)} did not complete in replay "
                f"({first}: {why})",
                path=str(self.directory / CALLS_DIR / first),
            )

    def scenario_meta(self, recorded_on: date | None = None) -> RecordingMeta:
        """The scenario-level ``meta.json`` from the calls recorded so far (record mode)."""
        calls = sorted(self._recorders)
        metas = [_load_meta(self.directory / CALLS_DIR / k) for k in calls]
        requests = [r for k in calls for r in self._recorders[k].requests]
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
    with no failed, retried or cancelled call *and* passes the secret scan replaces
    ``directory``. Otherwise nothing is kept and the error propagates (``MultiRecordError`` for
    call problems).
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
            raise MultiRecordError(
                f"scenario {scenario}: recording not kept: " + "; ".join(run.problems)
            )
        if not run.completed:
            raise MultiRecordError(f"scenario {scenario}: no run_agent call completed")
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
        "recorded multi scenario=%s agent=%s calls=%d llm_steps=%d tool_calls=%d dir=%s",
        scenario,
        agent,
        len(run.completed),
        len(meta.llm_steps),
        meta.tool_calls,
        directory,
    )


@contextmanager
def replay_multi(
    directory: Path, *, base: AgentActivities, graph: ServiceGraph | None = None
) -> Iterator[MultiRun]:
    """Replay the workflow run recorded in ``directory``. No LLM client, no adapters.

    On exit, drift found inside any activity is raised as ``StaleRecordingError`` (even if the
    body itself failed because of it), then every recorded call must have completed.
    """
    meta = _load_meta(directory)
    calls_dir = directory / CALLS_DIR
    recorded = (
        sorted(p.name for p in calls_dir.iterdir() if p.is_dir() and not p.name.startswith("."))
        if calls_dir.is_dir()
        else []
    )
    run = MultiRun(
        CassetteMode.REPLAY,
        directory,
        agent=meta.agent,
        scenario=meta.scenario,
        at=meta.at,
        base=base,
        graph=graph or ServiceGraph.load(),
        recorded_calls=recorded,
        meta=meta,
    )
    try:
        yield run
    except BaseException as e:
        if run.stale and not isinstance(e, StaleRecordingError):
            raise run.stale[0] from e
        raise
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

    The latency is the workflow's wall time, excluding the test server's startup.
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
            outcome: WorkflowOutcome = await env.client.execute_workflow(
                IncidentWorkflow.run,
                incident_id,
                id=workflow_id(incident_id),
                task_queue=ORCHESTRATOR_TASK_QUEUE,
            )
            return outcome, time.monotonic() - started

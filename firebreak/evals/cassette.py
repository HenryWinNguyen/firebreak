"""Record/replay cassettes for agent runs (ARCH §9 "Replay" mode).

A recording makes one agent run on one scenario reproducible offline, for $0:

- ``record_run``: the live run, inside ``deterministic(seed=<scenario>@<start ISO>, at=<start>)``.
  Every LLM response goes to a fixture keyed by the request hash, and every read-adapter result
  goes to ``tools.jsonl``.
- ``replay_run``: the identical run with no LLM client and no adapters behind it. The
  gateway, defanging and evidence path still run for real; only the three read adapters
  (metrics, logs, deploys) and the LLM are served from the recording.

A replay that leaves the recording (a prompt, tool definition, model or tool argument
changed) raises ``StaleRecordingError`` naming the scenario and the first differing step.
It never falls back to a live call.

Layout (``recording_dir``)::

    evals/recordings/<agent>/<scenario>/
        llm/<key>.json   one per LLM step: {"key", "request", "response"}
        tools.jsonl      one line per adapter call, in call-start order
        meta.json        seed, at, model ids, prompt sha256, LLM step keys, recording date

Ordering: parallel tool calls finish in any order on a live stack, but the gateway hands
out evidence ids as calls finish, and those ids reach the model. So both modes release
adapter results strictly in call-start order (``_InOrder``), and both yield once before
touching the adapter, which makes evidence ids identical between record and replay.
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets as _secrets
import shutil
import tempfile
import urllib.parse
from collections import deque
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict

from firebreak.ingest.service_graph import ServiceGraph
from firebreak.llm.client import KeyLoggingRecorder, StrictReplayLLMClient
from firebreak.llm.fixtures import FixtureStore, StaleRecordingError
from firebreak.llm.settings import LLMSettings
from firebreak.models.common import deterministic
from firebreak.models.llm import LLMClient, LLMRequest
from firebreak.tools.adapters.base import (
    BackendError,
    BackendTimeout,
    BackendUnavailable,
    ForbiddenRequest,
)
from firebreak.tools.adapters.loki import LogResult, LogsAdapter, LogStream
from firebreak.tools.adapters.prometheus import MetricResult, MetricsAdapter, Series
from firebreak.tools.adapters.shopfloor import Deploy, DeploysAdapter
from firebreak.tools.errors import InvalidToolArgs, ToolError, ToolHandlerFailed
from firebreak.tools.read_tools import ReadAdapters
from firebreak.tools.settings import ToolSettings

__all__ = [
    "DEFAULT_ROOT",
    "Cassette",
    "CassetteMode",
    "RecordedRun",
    "RecordingMeta",
    "ReplayedRun",
    "SecretInRecordingError",
    "StaleRecordingError",
    "default_secrets",
    "find_secrets",
    "record_run",
    "recording_dir",
    "replay_run",
    "tool_key",
]

logger = logging.getLogger(__name__)

FORMAT_VERSION = 1
DEFAULT_ROOT = Path("evals/recordings")
TOOLS_FILE = "tools.jsonl"
META_FILE = "meta.json"
LLM_DIR = "llm"
#: Shorter "secrets" (``test``, ``x``) would match by accident; real keys/tokens are longer.
MIN_SECRET_CHARS = 8

_METRICS_INSTANT = "metrics.instant"
_METRICS_RANGE = "metrics.range"
_METRICS_NAMES = "metrics.metric_names"
_LOGS_RANGE = "logs.query_range"
_DEPLOYS = "deploys.recent_deploys"

#: Adapter errors that replay re-raises as the same type (the gateway maps them by type).
_TOOL_ERRORS: dict[str, type[ToolError]] = {
    c.__name__: c
    for c in (
        BackendError,
        BackendTimeout,
        BackendUnavailable,
        ForbiddenRequest,
        InvalidToolArgs,
        ToolHandlerFailed,
    )
}


class CassetteMode(StrEnum):
    RECORD = "record"
    REPLAY = "replay"


class SecretInRecordingError(Exception):
    """A recording contained a secret (API key, admin token). Nothing was kept."""


def recording_dir(agent: str, scenario: str, root: Path = DEFAULT_ROOT) -> Path:
    for part in (agent, scenario):
        if not part or "/" in part or "\\" in part or part.startswith("."):
            raise ValueError(f"invalid recording path component {part!r}")
    return root / agent / scenario


# --- keys ------------------------------------------------------------------------------


def _dt(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("cassette: adapter datetimes must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def tool_key(method: str, args: dict[str, Any]) -> str:
    """sha256 of the canonical JSON of ``[method, normalized args]``."""
    return hashlib.sha256(_canonical([method, args]).encode("utf-8")).hexdigest()


# --- result encoding -------------------------------------------------------------------


def _encode_metric(r: MetricResult) -> dict[str, Any]:
    return {
        "result_type": r.result_type,
        "series": [{"labels": s.labels, "points": [[t, v] for t, v in s.points]} for s in r.series],
        "raw": r.raw,
    }


def _decode_metric(d: Any) -> MetricResult:
    return MetricResult(
        result_type=str(d["result_type"]),
        series=[
            Series(dict(s["labels"]), [(float(t), str(v)) for t, v in s["points"]])
            for s in d["series"]
        ],
        raw=d["raw"],
    )


def _encode_logs(r: LogResult) -> dict[str, Any]:
    return {
        "result_type": r.result_type,
        "streams": [
            {"labels": s.labels, "entries": [[ns, line] for ns, line in s.entries]}
            for s in r.streams
        ],
        "raw": r.raw,
    }


def _decode_logs(d: Any) -> LogResult:
    return LogResult(
        result_type=str(d["result_type"]),
        streams=[
            LogStream(dict(s["labels"]), [(int(ns), str(line)) for ns, line in s["entries"]])
            for s in d["streams"]
        ],
        raw=d["raw"],
    )


def _encode_names(names: list[str]) -> list[str]:
    return list(names)


def _decode_names(d: Any) -> list[str]:
    return [str(n) for n in d]


def _encode_deploys(deploys: list[Deploy]) -> list[dict[str, Any]]:
    return [d.model_dump(mode="json") for d in deploys]


def _decode_deploys(d: Any) -> list[Deploy]:
    return [Deploy.model_validate(x) for x in d]


_CODECS: dict[str, tuple[Callable[[Any], Any], Callable[[Any], Any]]] = {
    _METRICS_INSTANT: (_encode_metric, _decode_metric),
    _METRICS_RANGE: (_encode_metric, _decode_metric),
    _METRICS_NAMES: (_encode_names, _decode_names),
    _LOGS_RANGE: (_encode_logs, _decode_logs),
    _DEPLOYS: (_encode_deploys, _decode_deploys),
}


def _encode_error(e: Exception) -> dict[str, Any]:
    if isinstance(e, ToolError):
        return {
            "type": type(e).__name__,
            "message": e.message,
            "untrusted_detail": e.untrusted_detail,
            "tool_error": True,
        }
    return {"type": type(e).__name__, "message": str(e), "tool_error": False}


def _decode_error(d: dict[str, Any]) -> Exception:
    name = str(d["type"])
    if d.get("tool_error"):
        cls = _TOOL_ERRORS.get(name, ToolHandlerFailed)
        return cls(str(d["message"]), untrusted_detail=d.get("untrusted_detail"))
    # Same class name and text as the live failure, so the gateway shows the model the
    # identical "failed (<Name>)" error. A plain Exception subclass: nothing special-cased.
    replayed: type[Exception] = type(name, (Exception,), {"__module__": __name__})
    return replayed(str(d["message"]))


# --- the cassette ----------------------------------------------------------------------


@dataclass
class _Entry:
    seq: int
    method: str
    args: dict[str, Any]
    key: str
    result: Any = None
    error: dict[str, Any] | None = None
    cancelled: bool = False
    #: Record mode only: the live exception, re-raised unchanged (never serialized).
    live_error: Exception | None = None

    def to_json(self) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "seq": self.seq,
            "method": self.method,
            "args": self.args,
            "key": self.key,
        }
        if self.cancelled:
            doc["cancelled"] = True
        elif self.error is not None:
            doc["error"] = self.error
        else:
            doc["result"] = self.result
        return doc

    @classmethod
    def from_json(cls, doc: dict[str, Any]) -> "_Entry":
        return cls(
            seq=int(doc["seq"]),
            method=str(doc["method"]),
            args=dict(doc["args"]),
            key=str(doc["key"]),
            result=doc.get("result"),
            error=doc.get("error"),
            cancelled=bool(doc.get("cancelled", False)),
        )


class _InOrder:
    """Releases results in ticket (call-start) order, whatever order they complete in."""

    def __init__(self) -> None:
        self._last: asyncio.Event | None = None

    def ticket(self) -> tuple[asyncio.Event | None, asyncio.Event]:
        prev, mine = self._last, asyncio.Event()
        self._last = mine
        return prev, mine


class Cassette:
    """Wraps the three read adapters behind their Protocols, recording or replaying them.

    Record: the real adapter runs and ``(method, normalized args) -> result`` is kept in
    call-start order; ``save()`` writes ``tools.jsonl``. Replay: results come from
    ``tools.jsonl`` by key (repeated keys are served in recorded order) and nothing else is
    reachable; a key not in the cassette raises ``StaleRecordingError``. The gateway turns
    an adapter exception into a tool error, so every stale error is also kept in ``stale``
    and re-raised by ``raise_if_stale()`` (the replay LLM client calls it before each step).
    """

    def __init__(
        self,
        mode: CassetteMode,
        directory: Path,
        *,
        scenario: str,
        inner: ReadAdapters | None = None,
        recorded: Sequence[_Entry] = (),
        graph: ServiceGraph | None = None,
    ) -> None:
        self.mode = mode
        self.directory = directory
        self.scenario = scenario
        self._inner = inner
        self._entries: list[_Entry] = []
        self._by_key: dict[str, deque[_Entry]] = {}
        for e in recorded:
            self._by_key.setdefault(e.key, deque()).append(e)
        self._recorded = list(recorded)
        self._seq = 0
        self._order = _InOrder()
        self.stale: list[StaleRecordingError] = []
        if mode is CassetteMode.RECORD:
            if inner is None:
                raise ValueError("Cassette.record needs the real adapters")
            graph = inner.graph
        elif graph is None:
            graph = ServiceGraph.load()
        self.adapters = ReadAdapters(
            metrics=_Metrics(self), logs=_Logs(self), deploys=_Deploys(self), graph=graph
        )

    @classmethod
    def record(cls, directory: Path, adapters: ReadAdapters, *, scenario: str) -> "Cassette":
        return cls(CassetteMode.RECORD, directory, scenario=scenario, inner=adapters)

    @classmethod
    def replay(
        cls, directory: Path, *, scenario: str, graph: ServiceGraph | None = None
    ) -> "Cassette":
        path = directory / TOOLS_FILE
        if not path.exists():
            raise StaleRecordingError(
                scenario=scenario, step="setup", detail=f"no {TOOLS_FILE} at {path}"
            )
        entries: list[_Entry] = []
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                entries.append(_Entry.from_json(json.loads(line)))
            except (ValueError, KeyError, TypeError) as e:
                raise StaleRecordingError(
                    scenario=scenario,
                    step="setup",
                    detail=f"{path}:{n} is not a valid cassette entry ({e})",
                ) from e
        return cls(CassetteMode.REPLAY, directory, scenario=scenario, recorded=entries, graph=graph)

    @property
    def entries(self) -> list[_Entry]:
        """Calls made through this cassette so far (record mode), in call-start order."""
        return sorted(self._entries, key=lambda e: e.seq)

    @property
    def unconsumed(self) -> int:
        """Recorded tool calls the replay has not used (always 0 in record mode)."""
        return sum(len(q) for q in self._by_key.values())

    @property
    def calls(self) -> int:
        return self._seq

    def save(self) -> Path:
        """Write ``tools.jsonl`` (record mode only), atomically."""
        if self.mode is not CassetteMode.RECORD:
            raise RuntimeError("Cassette.save() is only for record mode")
        # Not key-sorted: a result's ``raw`` keeps the backend's key order, because the read
        # tools store ``json.dumps(raw)`` as evidence and replay must reproduce it byte for byte.
        lines = [
            json.dumps(e.to_json(), separators=(",", ":"), ensure_ascii=False) for e in self.entries
        ]
        return _atomic_write(self.directory / TOOLS_FILE, "".join(f"{x}\n" for x in lines))

    def real(self) -> ReadAdapters:
        """The wrapped adapters. Replay has none, by construction."""
        if self._inner is None:
            raise RuntimeError("replay cassette has no real adapters; nothing may reach a backend")
        return self._inner

    def raise_if_stale(self) -> None:
        if self.stale:
            raise self.stale[0]

    async def aclose(self) -> None:
        if self._inner is not None:
            await self._inner.aclose()

    # --- the call path -------------------------------------------------------------------

    async def call(
        self, method: str, args: dict[str, Any], run: Callable[[], Awaitable[Any]]
    ) -> Any:
        seq = self._seq
        self._seq += 1
        key = tool_key(method, args)
        prev, mine = self._order.ticket()
        recording: _Entry | None = None
        if self.mode is CassetteMode.RECORD:
            recording = _Entry(seq=seq, method=method, args=args, key=key)
            self._entries.append(recording)
        try:
            # Both modes yield once first, so tasks interleave identically (module docstring).
            await asyncio.sleep(0)
            if recording is not None:
                entry = await self._record(recording, run)
            else:
                entry = self._lookup(seq, method, args, key)
            if prev is not None:
                await prev.wait()
        except asyncio.CancelledError:
            # Cut off (the gateway's deadline, or the run) at any point, even after the live
            # result arrived: the gateway saw no result, so replay must not return one either.
            if recording is not None:
                recording.cancelled = True
            raise
        finally:
            mine.set()
        if entry.cancelled:
            # The live call was cut off by the gateway's deadline. Hang the same way so the
            # gateway's timeout fires again (the replay then takes that long).
            await asyncio.Event().wait()
        if entry.live_error is not None:
            raise entry.live_error
        if entry.error is not None:
            raise _decode_error(entry.error)
        return _CODECS[method][1](entry.result)

    async def _record(self, entry: _Entry, run: Callable[[], Awaitable[Any]]) -> _Entry:
        try:
            result = await run()
        except Exception as e:  # noqa: BLE001 - not swallowed: re-raised by call() in order
            # Recorded, then replayed as the same failure; the gateway reports it either way.
            entry.error = _encode_error(e)
            entry.live_error = e
            logger.info(
                "cassette recorded a failed call scenario=%s seq=%d method=%s: %s",
                self.scenario,
                entry.seq,
                entry.method,
                type(e).__name__,
            )
            return entry
        entry.result = _CODECS[entry.method][0](result)
        return entry

    def _lookup(self, seq: int, method: str, args: dict[str, Any], key: str) -> _Entry:
        queue = self._by_key.get(key)
        if queue:
            return queue.popleft()
        err = StaleRecordingError(
            scenario=self.scenario,
            step=f"tool call {seq + 1} ({method})",
            detail=self._what_changed(method, args, key),
            key=key,
            path=str(self.directory / TOOLS_FILE),
        )
        self.stale.append(err)
        logger.error("%s", err)
        raise err

    def _what_changed(self, method: str, args: dict[str, Any], key: str) -> str:
        if any(e.key == key for e in self._recorded):
            n = sum(e.key == key for e in self._recorded)
            return f"{method}{_canonical(args)} was recorded {n} time(s) and is called again"
        same = [e for e in self._recorded if e.method == method]
        if not same:
            return f"{method} was never called in the recording (args {_canonical(args)})"

        def diff(e: _Entry) -> list[str]:
            names = sorted(set(e.args) | set(args))
            return [n for n in names if e.args.get(n) != args.get(n)]

        best = min(same, key=lambda e: len(diff(e)))
        parts = [
            f"{n}={_canonical(args.get(n))} (recorded {_canonical(best.args.get(n))})"
            for n in diff(best)
        ]
        return f"{method} args differ from the closest recorded call: " + ", ".join(parts)


class _Metrics:
    def __init__(self, c: Cassette) -> None:
        self._c = c

    def _inner(self) -> MetricsAdapter:
        return self._c.real().metrics

    async def instant(self, query: str, at: datetime) -> MetricResult:
        result: MetricResult = await self._c.call(
            _METRICS_INSTANT,
            {"query": query, "at": _dt(at)},
            lambda: self._inner().instant(query, at),
        )
        return result

    async def range(self, query: str, start: datetime, end: datetime, step: str) -> MetricResult:
        result: MetricResult = await self._c.call(
            _METRICS_RANGE,
            {"query": query, "start": _dt(start), "end": _dt(end), "step": step},
            lambda: self._inner().range(query, start, end, step),
        )
        return result

    async def metric_names(self) -> list[str]:
        names: list[str] = await self._c.call(
            _METRICS_NAMES, {}, lambda: self._inner().metric_names()
        )
        return names


class _Logs:
    def __init__(self, c: Cassette) -> None:
        self._c = c

    def _inner(self) -> LogsAdapter:
        return self._c.real().logs

    async def query_range(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> LogResult:
        result: LogResult = await self._c.call(
            _LOGS_RANGE,
            {"query": query, "start": _dt(start), "end": _dt(end), "limit": limit},
            lambda: self._inner().query_range(query, start, end, limit),
        )
        return result


class _Deploys:
    def __init__(self, c: Cassette) -> None:
        self._c = c

    def _inner(self) -> DeploysAdapter:
        return self._c.real().deploys

    async def recent_deploys(self, service: str, limit: int) -> list[Deploy]:
        deploys: list[Deploy] = await self._c.call(
            _DEPLOYS,
            {"service": service, "limit": limit},
            lambda: self._inner().recent_deploys(service, limit),
        )
        return deploys


# --- meta, secrets, files --------------------------------------------------------------


class RecordingMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format_version: int = FORMAT_VERSION
    agent: str
    scenario: str
    seed: str
    at: AwareDatetime
    #: Models requested and models that answered, sorted.
    models: list[str]
    #: sha256 of the system prompt(s) sent, so a prompt change is visible without a replay.
    prompt_sha256: str
    #: Fixture keys in LLM call order (used to name the first differing step on a miss).
    llm_steps: list[str]
    tool_calls: int
    recorded_on: date


def default_secrets() -> list[str]:
    """Values that must never appear in a recording: provider keys and the admin token."""
    openai_key = LLMSettings().openai_api_key
    found = [
        os.environ.get("ANTHROPIC_API_KEY", ""),
        openai_key.get_secret_value() if openai_key is not None else "",
        ToolSettings().shopfloor_admin_token.get_secret_value(),
    ]
    return [s for s in found if s]


def _secret_needles(secrets: Sequence[str]) -> list[bytes]:
    """Byte forms to look for: raw, JSON-escaped, base64 (both alphabets), URL-encoded."""
    needles: set[bytes] = set()
    for i, s in enumerate(secrets):
        if len(s) < MIN_SECRET_CHARS:
            # Never log the value itself.
            logger.warning(
                "secret #%d is only %d chars (< %d): too short to scan for safely, so it is "
                "NOT checked in recordings",
                i,
                len(s),
                MIN_SECRET_CHARS,
            )
            continue
        raw = s.encode("utf-8")
        forms = {
            s,
            json.dumps(s, ensure_ascii=True)[1:-1],
            urllib.parse.quote(s, safe=""),
            urllib.parse.quote_plus(s),
            base64.b64encode(raw).decode("ascii"),
            base64.urlsafe_b64encode(raw).decode("ascii"),
        }
        # Padding depends on what follows in the encoded text, so match the unpadded form too.
        forms |= {f.rstrip("=") for f in forms}
        needles |= {f.encode("utf-8") for f in forms if f}
    return sorted(needles)


def find_secrets(directory: Path, secrets: Sequence[str]) -> list[Path]:
    """Every file under ``directory`` containing a secret (raw, JSON-escaped, base64, URL-encoded).

    Secrets shorter than ``MIN_SECRET_CHARS`` can't be scanned safely; each one is logged
    as a warning (without its value) and skipped.
    """
    needles = _secret_needles(secrets)
    hits: list[Path] = []
    for path in sorted(p for p in directory.rglob("*") if p.is_file()):
        data = path.read_bytes()
        if any(n in data for n in needles):
            hits.append(path)
    return hits


def _atomic_write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return path


def _prompt_sha256(requests: Sequence[LLMRequest]) -> str:
    systems = sorted({r.system for r in requests})
    return hashlib.sha256("\n\x00\n".join(systems).encode("utf-8")).hexdigest()


def _load_meta(directory: Path) -> RecordingMeta:
    path = directory / META_FILE
    if not path.exists():
        raise StaleRecordingError(
            scenario=directory.name,
            step="setup",
            detail=f"no {META_FILE} at {path}; record the scenario first",
        )
    meta = RecordingMeta.model_validate_json(path.read_text(encoding="utf-8"))
    if meta.format_version != FORMAT_VERSION:
        raise StaleRecordingError(
            scenario=meta.scenario,
            step="setup",
            detail=f"recording format {meta.format_version}, this code reads {FORMAT_VERSION}",
        )
    return meta


# --- runs ------------------------------------------------------------------------------


@dataclass
class RecordedRun:
    """What a record-mode run uses: wrap the run in ``record_run`` and pass these in."""

    llm: KeyLoggingRecorder
    adapters: ReadAdapters
    cassette: Cassette
    directory: Path


@dataclass
class ReplayedRun:
    llm: StrictReplayLLMClient
    adapters: ReadAdapters
    cassette: Cassette
    meta: RecordingMeta


def _swap_in(staging: Path, directory: Path, tag: str) -> None:
    """Put ``staging`` at ``directory``: old aside, new in, then old deleted.

    A failure before the new recording is in place restores the old one untouched.
    """
    aside: Path | None = None
    if directory.exists():
        aside = directory.with_name(f".{directory.name}.old.{tag}")
        directory.rename(aside)
    try:
        staging.rename(directory)
    except BaseException:
        if aside is not None:
            aside.rename(directory)
        raise
    if aside is not None:
        try:
            shutil.rmtree(aside)
        except OSError:
            # The new recording is already in place; a leftover copy is harmless but not silent.
            logger.exception("could not remove the replaced recording %s", aside)


@contextmanager
def record_run(
    directory: Path,
    *,
    agent: str,
    scenario: str,
    at: datetime,
    adapters: ReadAdapters,
    llm: LLMClient,
    secrets: Sequence[str] | None = None,
    overwrite: bool = False,
    recorded_on: date | None = None,
) -> Iterator[RecordedRun]:
    """Record one run into ``directory`` (see ``recording_dir``).

    The body runs inside ``deterministic(seed, at=at)`` with
    ``seed = f"{scenario}@{at.isoformat()}"`` (e.g.
    ``s01@2026-10-29T18:04:05.123456+00:00``), also stored as ``meta.seed``. The seed
    is unique per recording so a re-record never reuses the ``ev_...`` ids an earlier one left
    in the eval DB. That uniqueness relies on callers passing a distinct ``at`` per recording
    (runner.py passes ``utcnow()``); there is deliberately no random component, so the seed
    is reproducible from meta. Files go to a staging
    directory first; only a body that finishes cleanly *and* passes the secret scan
    replaces ``directory``. On any failure nothing is kept and the error propagates.
    """
    if directory.exists() and not overwrite:
        raise FileExistsError(f"recording {directory} exists; pass overwrite=True to re-record")
    # Unique per run, so concurrent or crashed runs never share or clobber a staging dir.
    tag = f"{os.getpid()}.{_secrets.token_hex(4)}"
    staging = directory.with_name(f".{directory.name}.recording.{tag}")
    staging.mkdir(parents=True)
    seed = f"{scenario}@{at.isoformat()}"
    try:
        cassette = Cassette.record(staging, adapters, scenario=scenario)
        recorder = KeyLoggingRecorder(llm, FixtureStore(staging / LLM_DIR))
        with deterministic(seed, at=at):
            yield RecordedRun(
                llm=recorder, adapters=cassette.adapters, cassette=cassette, directory=directory
            )
        cassette.save()
        models = {r.model for r in recorder.requests} | {r.model for r in recorder.responses}
        meta = RecordingMeta(
            agent=agent,
            scenario=scenario,
            seed=seed,
            at=at,
            models=sorted(models),
            prompt_sha256=_prompt_sha256(recorder.requests),
            llm_steps=recorder.keys,
            tool_calls=cassette.calls,
            # The real date on purpose: when this was recorded, not part of the run.
            recorded_on=recorded_on or datetime.now(UTC).date(),
        )
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
        "recorded scenario=%s agent=%s llm_steps=%d tool_calls=%d dir=%s",
        scenario,
        agent,
        len(meta.llm_steps),
        meta.tool_calls,
        directory,
    )


@contextmanager
def replay_run(directory: Path, *, graph: ServiceGraph | None = None) -> Iterator[ReplayedRun]:
    """Replay the run recorded in ``directory``. No LLM client, no adapters, no network.

    The body runs inside the recording's ``deterministic(seed, at)``. A miss anywhere
    raises ``StaleRecordingError``; a body that finishes with a stale tool call or fewer
    LLM steps than recorded raises it on exit.
    """
    meta = _load_meta(directory)
    cassette = Cassette.replay(directory, scenario=meta.scenario, graph=graph)
    llm = StrictReplayLLMClient(
        FixtureStore(directory / LLM_DIR),
        scenario=meta.scenario,
        recorded_keys=meta.llm_steps,
        check=cassette.raise_if_stale,
    )
    with deterministic(meta.seed, at=meta.at):
        yield ReplayedRun(llm=llm, adapters=cassette.adapters, cassette=cassette, meta=meta)
    cassette.raise_if_stale()
    if cassette.unconsumed:
        raise StaleRecordingError(
            scenario=meta.scenario,
            step=f"tool call {cassette.calls + 1}",
            detail=f"the run ended after {cassette.calls} tool calls; the recording has "
            f"{cassette.calls + cassette.unconsumed}",
            path=str(directory / TOOLS_FILE),
        )
    if llm.steps != len(meta.llm_steps):
        raise StaleRecordingError(
            scenario=meta.scenario,
            step=f"llm step {llm.steps}",
            detail=f"the run ended after {llm.steps} LLM steps; the recording has "
            f"{len(meta.llm_steps)}",
        )

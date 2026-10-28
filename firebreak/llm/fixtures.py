"""Fixture keys and storage for record/replay."""

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from firebreak.llm.errors import FixtureMissing, LLMError
from firebreak.models.llm import LLMRequest, LLMResponse

# The untrusted-data wrapper (tools/untrusted.py) puts a fresh random nonce on every block,
# so the same run never produces byte-identical messages twice. The nonce is a security
# boundary, not content: the key replaces each distinct nonce with its order of first
# appearance, so the key still changes when anything else in the conversation does.
#
# Only the wrapper's own tags match: ``<untrusted-data ... nonce="X">`` and
# ``</untrusted-data nonce="X">``. Nonce-shaped text inside content is content, and is hashed.
_NONCE = re.compile(
    r'(<untrusted-data\b[^<>]*?\snonce="|</untrusted-data\snonce=")([0-9a-f]{16})(")'
)


class StaleRecordingError(FixtureMissing):
    """Replay found no recording for this step: a prompt, tool, model or tool arg changed.

    Replay never falls back to a live call or a live backend. Re-record the scenario.
    ``scenario`` and ``step`` say where the run first left the recording; ``detail`` says
    what differs, when that is known.
    """

    def __init__(
        self,
        *,
        scenario: str,
        step: str,
        detail: str,
        key: str = "",
        path: str = "",
    ) -> None:
        message = f"stale recording for scenario {scenario!r} at {step}: {detail}"
        if key:
            message += f" (key {key}"
            message += f", looked at {path})" if path else ")"
        message += "; re-record the scenario"
        LLMError.__init__(self, message)
        self.key = key
        self.path = path
        self.scenario = scenario
        self.step = step
        self.detail = detail


def _normalize_nonces(value: Any, seen: dict[str, str]) -> Any:
    if isinstance(value, str):
        return _NONCE.sub(lambda m: f"{m[1]}{seen.setdefault(m[2], f'n{len(seen)}')}{m[3]}", value)
    if isinstance(value, list):
        return [_normalize_nonces(v, seen) for v in value]
    if isinstance(value, dict):
        # Sorted, so the numbering doesn't depend on dict insertion order.
        return {k: _normalize_nonces(value[k], seen) for k in sorted(value)}
    return value


def key_payload(request: LLMRequest) -> list[Any]:
    """What the key hashes: (model, system, messages, tools), nonces normalized."""
    seen: dict[str, str] = {}
    return [
        request.model,
        _normalize_nonces(request.system, seen),
        _normalize_nonces(request.messages, seen),
        [t.model_dump(mode="json") for t in request.tools],
    ]


def fixture_key(request: LLMRequest) -> str:
    """sha256 of the canonical JSON of (model, system, messages, tools). max_tokens is excluded.

    Untrusted-data nonces are normalized (see ``_NONCE``); everything else is hashed as is.
    """
    canonical = json.dumps(
        key_payload(request), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class FixtureStore:
    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def path_for(self, key: str) -> Path:
        return self.directory / f"{key}.json"

    def save(self, request: LLMRequest, response: LLMResponse) -> str:
        key = fixture_key(request)
        self.directory.mkdir(parents=True, exist_ok=True)
        doc = {
            "key": key,
            "request": request.model_dump(mode="json"),
            "response": response.model_dump(mode="json"),
        }
        text = json.dumps(doc, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        # Atomic: a crash mid-write must never leave a truncated fixture under the final name.
        fd, tmp = tempfile.mkstemp(dir=self.directory, prefix=f".{key}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp, self.path_for(key))
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        return key

    def load(self, request: LLMRequest) -> LLMResponse:
        key = fixture_key(request)
        path = self.path_for(key)
        if not path.exists():
            raise FixtureMissing(key, str(path))
        return LLMResponse.model_validate(json.loads(path.read_text())["response"])

    def load_request(self, key: str) -> LLMRequest | None:
        """The request recorded under ``key``, or None if there is no such fixture."""
        path = self.path_for(key)
        if not path.exists():
            return None
        return LLMRequest.model_validate(json.loads(path.read_text())["request"])


def first_difference(recorded: LLMRequest, current: LLMRequest) -> str:
    """Where ``current`` first departs from ``recorded`` (nonces ignored), for error text."""
    old, new = key_payload(recorded), key_payload(current)
    if old[0] != new[0]:
        return f"model changed ({recorded.model!r} recorded, {current.model!r} now)"
    if old[1] != new[1]:
        return (
            "system prompt changed (sha256 "
            f"{_sha(recorded.system)[:12]} recorded, {_sha(current.system)[:12]} now)"
        )
    if old[3] != new[3]:
        was = [t.name for t in recorded.tools]
        now = [t.name for t in current.tools]
        if was != now:
            return f"tool list changed ({was} recorded, {now} now)"
        for a, b in zip(recorded.tools, current.tools, strict=True):
            if a != b:
                return f"tool definition {a.name!r} changed"
    msgs_old, msgs_new = old[2], new[2]
    for i, (a, b) in enumerate(zip(msgs_old, msgs_new, strict=False)):
        if a != b:
            return f"messages[{i}] ({b.get('role', '?')}) differs from the recording"
    if len(msgs_old) != len(msgs_new):
        return f"{len(msgs_new)} messages now, {len(msgs_old)} recorded"
    return "request differs from the recording"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

"""Fixture keys and storage for record/replay."""

import hashlib
import json
from pathlib import Path

from firebreak.llm.errors import FixtureMissing
from firebreak.models.llm import LLMRequest, LLMResponse


def fixture_key(request: LLMRequest) -> str:
    """sha256 of the canonical JSON of (model, system, messages, tools). max_tokens is excluded."""
    payload = [
        request.model,
        request.system,
        request.messages,
        [t.model_dump(mode="json") for t in request.tools],
    ]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
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
        self.path_for(key).write_text(text)
        return key

    def load(self, request: LLMRequest) -> LLMResponse:
        key = fixture_key(request)
        path = self.path_for(key)
        if not path.exists():
            raise FixtureMissing(key, str(path))
        return LLMResponse.model_validate(json.loads(path.read_text())["response"])

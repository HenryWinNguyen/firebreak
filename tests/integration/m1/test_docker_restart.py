"""Unit tests for docker_restart with a fake subprocess runner (no docker needed)."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from tests.integration.m1.docker_restart import RestartError, restart_service


class FakeDocker:
    def __init__(self, ps_out: str, started: list[str], restart_rc: int = 0) -> None:
        self.ps_out = ps_out
        self.started = list(started)
        self.restart_rc = restart_rc
        self.calls: list[list[str]] = []

    async def __call__(self, argv: Sequence[str]) -> tuple[int, str]:
        self.calls.append(list(argv))
        if argv[1] == "ps":
            return 0, self.ps_out
        if argv[1] == "inspect":
            return 0, self.started.pop(0) + "\n"
        if argv[1] == "restart":
            return self.restart_rc, "" if self.restart_rc == 0 else "boom"
        raise AssertionError(argv)


async def test_restarts_the_single_labelled_container() -> None:
    fake = FakeDocker("abc123\n", ["t1", "t2"])
    assert await restart_service("orders", fake) == "abc123"
    assert fake.calls[0][-2:] == ["--filter", "label=com.docker.compose.service=orders"]
    assert ["docker", "restart", "abc123"] in fake.calls


async def test_no_container_is_an_error() -> None:
    with pytest.raises(RestartError, match="found 0"):
        await restart_service("orders", FakeDocker("", []))


async def test_ambiguous_match_is_an_error() -> None:
    fake = FakeDocker("a\nb\n", [])
    with pytest.raises(RestartError, match="found 2"):
        await restart_service("orders", fake)
    assert not any(c[1] == "restart" for c in fake.calls)


async def test_unchanged_started_at_is_an_error() -> None:
    with pytest.raises(RestartError, match="StartedAt unchanged"):
        await restart_service("orders", FakeDocker("abc\n", ["t1", "t1"]))


async def test_restart_failure_is_an_error() -> None:
    with pytest.raises(RestartError, match="exited 1"):
        await restart_service("orders", FakeDocker("abc\n", ["t1"], restart_rc=1))


async def test_compose_project_name_adds_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "proj")
    fake = FakeDocker("abc\n", ["t1", "t2"])
    await restart_service("orders", fake)
    assert "label=com.docker.compose.project=proj" in fake.calls[0]

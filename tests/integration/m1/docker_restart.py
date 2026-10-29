"""Restart a compose service's container by label and prove the restart took effect.

`docker compose restart <svc>` resolves the project from the current directory name, so from a
different checkout it can find no container and still exit 0. Looking the container up by its
compose labels works from any checkout; a miss or an ambiguous match is a loud error.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Sequence

# (argv) -> (returncode, combined output)
Runner = Callable[[Sequence[str]], Awaitable[tuple[int, str]]]


class RestartError(RuntimeError):
    """The container could not be found, restarted, or did not actually restart."""


async def subprocess_runner(argv: Sequence[str]) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode()


async def _run_ok(run: Runner, argv: Sequence[str]) -> str:
    code, out = await run(argv)
    if code != 0:
        raise RestartError(f"{' '.join(argv)} exited {code}: {out.strip()}")
    return out.strip()


async def restart_service(service: str, run: Runner = subprocess_runner) -> str:
    """Restart the one container for compose service `service`; return its id.

    Honours COMPOSE_PROJECT_NAME when set. Raises RestartError unless exactly one container
    matches and its `State.StartedAt` changed after the restart."""
    argv = ["docker", "ps", "-q", "--filter", f"label=com.docker.compose.service={service}"]
    project = os.environ.get("COMPOSE_PROJECT_NAME")
    if project:
        argv += ["--filter", f"label=com.docker.compose.project={project}"]
    ids = (await _run_ok(run, argv)).split()
    if len(ids) != 1:
        raise RestartError(
            f"expected exactly 1 running container for compose service {service!r}, "
            f"found {len(ids)}: {ids} (set COMPOSE_PROJECT_NAME to disambiguate)"
        )
    cid = ids[0]
    inspect = ["docker", "inspect", "--format", "{{.State.StartedAt}}", cid]
    before = await _run_ok(run, inspect)
    await _run_ok(run, ["docker", "restart", cid])
    after = await _run_ok(run, inspect)
    if after == before:
        raise RestartError(
            f"container {cid} ({service}) StartedAt unchanged ({before}) after restart"
        )
    return cid

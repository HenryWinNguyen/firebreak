"""Merge gate G2: a builder's diff may only touch its task's owned_paths.

Usage: check_owned_paths.py [--task T-021] [--base origin/main]
Task id defaults to the one encoded in the branch name (t-021-some-slug).
Branches without a task id are lead branches and are not checked.
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

ALWAYS_ALLOWED = ("docs/PROGRESS.md",)
BRANCH_RE = re.compile(r"^t-(\d{3})(?:-|$)", re.IGNORECASE)


def task_id_from_branch(branch: str) -> str | None:
    m = BRANCH_RE.match(branch)
    return f"T-{m.group(1)}" if m else None


def load_owned_paths(task_id: str, tasks_dir: Path) -> list[str]:
    spec = tasks_dir / f"{task_id}.md"
    text = spec.read_text()
    parts = text.split("---")
    if len(parts) < 3:
        raise ValueError(f"{spec} has no front matter")
    meta = yaml.safe_load(parts[1])
    owned = meta.get("owned_paths") or []
    if not owned:
        raise ValueError(f"{spec} declares no owned_paths")
    return [str(p) for p in owned]


def is_allowed(path: str, owned: list[str], task_id: str) -> bool:
    if path in ALWAYS_ALLOWED or path == f"docs/tasks/{task_id}.md":
        return True
    for o in owned:
        if o.endswith("/") and path.startswith(o):
            return True
        if path == o:
            return True
    return False


def violations(changed: list[str], owned: list[str], task_id: str) -> list[str]:
    return [p for p in changed if not is_allowed(p, owned, task_id)]


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--task")
    ap.add_argument("--base", default="origin/main")
    args = ap.parse_args()

    branch = os.environ.get("GITHUB_HEAD_REF") or _git("rev-parse", "--abbrev-ref", "HEAD").strip()
    task_id = args.task or task_id_from_branch(branch)
    if task_id is None:
        print(f"branch {branch!r} has no task id: lead branch, ownership not checked")
        return 0

    owned = load_owned_paths(task_id, Path("docs/tasks"))
    changed = [p for p in _git("diff", "--name-only", f"{args.base}...HEAD").splitlines() if p]
    bad = violations(changed, owned, task_id)
    if bad:
        print(f"{task_id}: {len(bad)} file(s) outside owned_paths {owned}:")
        for p in bad:
            print(f"  {p}")
        return 1
    print(f"{task_id}: {len(changed)} changed file(s), all within owned_paths")
    return 0


if __name__ == "__main__":
    sys.exit(main())

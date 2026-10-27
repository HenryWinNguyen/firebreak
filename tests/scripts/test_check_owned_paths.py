from pathlib import Path

import pytest

from scripts.check_owned_paths import (
    load_owned_paths,
    task_id_from_branch,
    violations,
)


@pytest.mark.parametrize(
    ("branch", "expected"),
    [("t-021-ingest", "T-021"), ("T-011", "T-011"), ("m0-skeleton", None), ("main", None)],
)
def test_task_id_from_branch(branch: str, expected: str | None) -> None:
    assert task_id_from_branch(branch) == expected


def test_violations_allow_dirs_files_progress_and_own_spec() -> None:
    owned = ["firebreak/ingest/", "tests/ingest/", "firebreak/api/app.py"]
    changed = [
        "firebreak/ingest/grouping.py",
        "tests/ingest/test_grouping.py",
        "firebreak/api/app.py",
        "docs/PROGRESS.md",
        "docs/tasks/T-023.md",
        "firebreak/models/alert.py",
        "docs/tasks/T-021.md",
        "firebreak/ingestion/x.py",
    ]
    assert violations(changed, owned, "T-023") == [
        "firebreak/models/alert.py",
        "docs/tasks/T-021.md",
        "firebreak/ingestion/x.py",
    ]


def test_load_owned_paths(tmp_path: Path) -> None:
    (tmp_path / "T-099.md").write_text("---\nid: T-099\nowned_paths:\n  - a/\n  - b.py\n---\n# x\n")
    assert load_owned_paths("T-099", tmp_path) == ["a/", "b.py"]


def test_load_owned_paths_requires_paths(tmp_path: Path) -> None:
    (tmp_path / "T-098.md").write_text("---\nid: T-098\n---\n")
    with pytest.raises(ValueError, match="no owned_paths"):
        load_owned_paths("T-098", tmp_path)

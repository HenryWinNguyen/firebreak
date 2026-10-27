# CLAUDE.md: Firebreak

Multi-agent incident-response system. Purpose and milestones are in
[SCOPE.md](./SCOPE.md), design in [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md),
and status in [docs/PROGRESS.md](./docs/PROGRESS.md). Don't duplicate them here.

## How this repo is built: multiple Claude sessions

Read [docs/AGENT_WORKFLOW.md](./docs/AGENT_WORKFLOW.md). The short version:

- You are either the **lead** (main checkout) or a **builder** (a worktree
  started with `claude -w <track> --name <track>`). Your session name tells you which.
- Builders work from one task spec in `docs/tasks/` and **edit only that spec's
  owned paths**. If you need something outside them, message `@lead`.
- `firebreak/models/` and `docs/CONTRACTS.md` are **lead-only**. To request a
  contract change, message `@lead` with the proposed diff.
- Decisions go in files (specs, ADRs, PROGRESS), not only in chat or
  cross-session messages.
- Before you open a PR: run the spec's verify command, then have the `reviewer`
  subagent compare the diff against the spec, then message `@lead` with the PR
  link and the verify result.
- **Builders** open PRs and never merge. The **lead** merges a PR on its own once merge
  gates G1–G6 pass (AGENT_WORKFLOW.md §5).
- **Stop and wait for Henry only at major checkpoints** (CP0 after M0, CP1 after M4,
  CP2 after M8, CP3 before anything public/deployed/paid) and on the always-stop
  triggers in AGENT_WORKFLOW.md §6.2. Every other milestone end is an async
  summary in PROGRESS.md, then keep going.

## Conventions

- **Python 3.12 + uv.** `make install` once per worktree (copy `.env.example` → `.env` if
  `.worktreeinclude` didn't bring it).
- **Checks:** `make check` (ruff lint + format check, mypy --strict on `firebreak/` and
  `scripts/`, pytest). `make test-<dir>` runs `tests/<dir>`. `make owned` runs merge gate G2.
- **Stack:** `make up` / `make down` (Postgres + API, plus whatever `shopfloor/compose.yml` and
  `ops/compose.yml` define). Ports are in `docs/CONTRACTS.md` §3.3.
- **Tests:** mirror the package path (`firebreak/ingest/x.py` → `tests/ingest/test_x.py`). Tests
  that need Postgres are marked `db` and read `FIREBREAK_TEST_DATABASE_URL`. Full-stack tests
  are marked `integration` and are skipped unless `FIREBREAK_INTEGRATION=1`.
- **Errors:** no bare `except`, no swallowed exceptions. Raise typed errors, or log with context
  and re-raise.
- **Contracts:** import types from `firebreak.models`. Never redefine them locally.
- **Branches:** builders use `t-<id>-<slug>` (e.g. `t-021-ingest`). The PR title starts with the task id.
- **Shopfloor** is its own uv project under `shopfloor/` (excluded from the root mypy run).

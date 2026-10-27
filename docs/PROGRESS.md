# Progress Log

Reverse-chronological log of milestones. See [SCOPE.md](../SCOPE.md) for the
milestone plan (M0–M13) and [ARCHITECTURE.md](./ARCHITECTURE.md) for design.

## Current status

**M0 complete. ⏸ Waiting on Henry at CP0** (see block below).
Repo: https://github.com/HenryWinNguyen/firebreak (public).

## ⏸ Waiting on Henry: CP0

Review before anything runs in parallel:
1. `docs/CONTRACTS.md` + `firebreak/models/`: are the seams right?
2. `docs/tasks/` (T-011…T-032) + `BOARD.md`: is the split and the first wave right?
3. Say "go" and the lead spawns the first wave: **T-011, T-022, T-032**.

## Log

### 2026-10-27: M0 skeleton + contracts v1
- uv project (Python 3.12), flat `firebreak/` package layout, ruff + mypy --strict + pytest.
  `make check` is green (15 tests).
- Contracts v1: `firebreak/models/` (alert, incident, evidence, reasoning, llm, agents) +
  `docs/CONTRACTS.md` (Shopfloor API, chaos flags, metrics, log format, alert rules, webhook,
  ports, file ownership).
- `scripts/check_owned_paths.py` (merge gate G2) + tests. GitHub Actions CI: check job with a
  Postgres service + owned-paths job on PRs.
- Docker: API image + root compose (Postgres + API, includes stubs for `shopfloor/` and `ops/`).
  `make up` → `/healthz` OK.
- Task specs for M1 (T-011–014), M2 (T-021–024), runtime (T-031, T-032) + BOARD.
- Decision: in M2 the grouping window is DB-backed. M5 moves it into a Temporal timer (T-023 notes).

### 2026-10-27: Autonomous operating model
- Team: 1 lead (Opus) + max 3 builder subagents (Sonnet, one task each, own worktree).
  Henry runs one terminal.
- Task specs with owned paths and dependencies, a lead-maintained BOARD, merge
  gates G1–G6 so the lead can merge on its own, and 4 major checkpoints
  (after M0, after M4, after M8, before M12). Everything else is an async summary.
- Added `.claude/agents/builder.md`. Reviewer now checks for silent failures and
  duplicated code. The task template gained front matter, mandatory reading and
  patterns to mirror.
- Project permission allowlist drafted at `docs/setup/claude-settings.proposed.json`.
  The auto-mode classifier blocked Claude from writing `.claude/settings.json`
  directly, so Henry installs it.

### 2026-10-27: Multi-session build workflow
- Researched Claude Code's options for running several agents at once
  (subagents, worktree sessions, cross-session messaging, agent teams, agent
  view, /batch) against the official docs, CLI v2.1.311.
- Wrote `docs/AGENT_WORKFLOW.md`: lead/builder/reviewer roles, contracts
  before parallel work, task-spec loop, parallel plan per milestone, where
  agent teams fit (debate, red-team, review only), and efficiency rules.
- Added `CLAUDE.md`, `.claude/agents/reviewer.md` (read-only reviewer), and
  `docs/tasks/_TEMPLATE.md`.

### 2026-10-27: Project kickoff
- Picked the incident response copilot from the portfolio brainstorm. The
  goal is a project centered on agent orchestration that pairs with
  Sentry Load.
- Wrote `SCOPE.md` (purpose, safety line, success metrics, 10 fault
  scenarios, M0–M13) and `docs/ARCHITECTURE.md` (Temporal workflow state
  machine, agent runtime, tool gateway, policy/approval model,
  prompt-injection defenses, eval harness, ADRs 001–005).
- Open: hosting capacity on the Oracle VM, demo-mode abuse limits, final name.

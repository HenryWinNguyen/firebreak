# Progress Log

Reverse-chronological log of milestones. See [SCOPE.md](../SCOPE.md) for the
milestone plan (M0–M13) and [ARCHITECTURE.md](./ARCHITECTURE.md) for design.

## ⏸ Waiting on Henry: paid live run for M3 (T-042)

**Question:** OK to run the single-agent baseline against live scenarios S1–S7 with real Claude API calls?

- **Needs from you:** `ANTHROPIC_API_KEY=` in `.env` (currently empty), and an OK on spend.
- **Spend:** each investigation is hard-capped at $0.50 (`budget_usd`), so a full S1–S7 run is ≤ $3.50; ≤ 1 re-run planned (≤ $7 total).
- **Options:** (a) go as specced (Sonnet 5.5, Haiku 5.5 fallback); (b) a cheaper smoke run first (S1 only, ≤ $0.50); (c) hold.
- **Recommendation:** (b) then (a): one scenario confirms the wiring end to end before spending on all seven.
- **Not blocked meanwhile:** M4 eval-harness work that runs on replay fixtures (no spend) continues.

## Current status

**M2 done. M3 code done; live acceptance (T-042) waits on Henry.** M1 acceptance (T-014) in rework. CP0 passed 2026-10-27. Board: `docs/tasks/BOARD.md`.
Repo: https://github.com/HenryWinNguyen/firebreak (public).

## Log

### 2026-10-28: M2 complete, M3 baseline built
- **T-026** (#21, 1 rework): grouper uses only repository `*_in` methods; out-of-order guard (firing replay
  after resolved opens nothing); deterministic row-lock test that fails without `FOR UPDATE` (mutation-checked).
- **T-027** (#23): resolved notifications update the stored alert via `mark_resolved`; stored alert id in
  responses; 503 on DB-unreachable; engine disposal on shutdown.
- **T-033** (#25): fallback only via `LLMRequest.fallback_model`; Retry-After; atomic fixtures; NaN cost trips budget.
- **Contracts v1.3** (#27): `InvestigationReport` (validated `f<index>` refs, ranks 1..n) and the read-tool list
  (CONTRACTS §3.4). `/admin/chaos` is deliberately not exposed: it would leak eval ground truth.
- **T-034** (#28) + **T-035** (#31, 1 rework, Opus reviews): six read tools behind a GET-only, path- and
  host-pinned transport; full JSON stored as evidence, compact view shown; whole-second windows;
  4 MB response cap; non-finite JSON rejected; only a real backend timeout reports `timeout`.
- **T-041** (#32, 1 rework): single-agent baseline (Sonnet 5.5, 6 read tools, 16 steps, $0.50 cap) returning an
  `InvestigationReport`; CLI `python -m firebreak.agents.baseline <incident_id>`. Prompt checked for
  scenario leakage (no IDs, flags, service names or `-bad` versions).
- Backlog (M10 hardening): stacked `Content-Encoding` decompression bomb from a compromised backend;
  `make check` doesn't type-check `tests/`.

### 2026-10-27: Runtime, ingest, chaos and persistence hardening merged
- **T-031** (#7): Claude API client with retries, usage/cost accounting from `config/pricing.yaml`
  (official pricing page, 2026-10-27), record/replay fixtures. Review minors → **T-033**.
- **T-013** (#10): Prometheus v3.5.0, Alertmanager v0.28.1, Loki/Promtail 3.5.0, Grafana 12.0.2
  (all pinned), alert rules per CONTRACTS §3, promtool tests. Review minors → **T-015**.
- **T-023** (#11): incident grouper (per-cluster advisory lock, window, service-graph clusters).
- **T-021** (#13, 1 rework round): Alertmanager + generic webhooks, normalization to aware UTC
  (naive → 422), idempotency key dedupe, firing alerts always reach the grouper so replay recovers
  an inserted-but-ungrouped alert. Re-review minors → **T-027**.
- **T-012** (#12, 1 rework round): chaos flags S1–S6. S3 now costs 0.25 s per item (5-item cart > 1 s),
  quantity capped at 20, compensating stock release on error/cancel, Redis timeouts.
- **T-025** (#18): `_test` DB-name guard, shared fixtures in `tests/conftest.py`, session-taking
  `*_in` repository methods, `mark_resolved`, `EvidenceConflictError`, migration drift test.
- **T-024** (#19, 1 rework round): real `PostgresIncidentGrouper` wired into the API; S10 storm test
  (15 alerts → 1 incident, re-post → all duplicates) and replay-after-grouper-failure test.
  Rework caught a copied fixture that bypassed the `_test` guard before `DROP SCHEMA`.
  Lead still to run the `make up` + curl reproduction once the compose stack is free.
- **T-032** (#16 replacing #8, 2 rework rounds, Opus): bounded agent loop + tool gateway. Highlights
  the reviews forced: never invent tool_use ids, exactly one tool_result per tool_use on every path,
  TaskGroup cancellation, handler error codes never trusted, `max_tokens` tool turns not executed,
  untrusted-content defang on a folded copy so the model sees the original text, size cap applied last.
- **T-014** (#20): all four M1 scenarios fired the expected alert on the real stack
  (S1 155 s, S2 281 s, S3 270 s, S4 150 s). In rework for test robustness (reset in `finally`,
  load generators can't fail silently, no stale-alert passes).
- Contracts: CONTRACTS §2.1 now documents inventory `POST /release` and the quantity cap.
- New follow-up specs: **T-015** (ops minors), **T-033** (LLM client + runtime minors).
- Builder process note: two builders hit non-fast-forward pushes after rebasing; both pushed a
  merge or a new branch instead of force-pushing, as the rules require. Builders now merge
  `origin/main` instead of rebasing.

### 2026-10-27: First wave merges (T-022, T-011)
- **T-022** (#4): persistence layer. Tables, `0001` migration, Alert/Incident repositories,
  `PostgresEvidenceSink`, with real-Postgres tests (duplicate, 10-way concurrent duplicate,
  window boundary). Review: 0 blocking, 6 minor → **T-025**.
- **T-011** (#5): Shopfloor gateway/orders/inventory with the CONTRACTS §2 metrics, JSON logs,
  trace propagation, Redis cache, runtime-sized DB pool. Review: 0 blocking, 6 minor → folded into **T-012**.
- Process fixes: builders don't touch PROGRESS.md (the lead copies entries from the PR body), the lead
  runs reviews, separate Postgres ports per builder, a `shopfloor` CI job, and `main` protection now
  **enforced for admins** with `check` + `shopfloor` + `owned-paths` required. #5 had merged right
  after a branch update, before its new checks started. `main` CI was green afterwards, but the bypass is closed now.

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

# Progress Log

Reverse-chronological log of milestones. See [SCOPE.md](../SCOPE.md) for the
milestone plan (M0–M13) and [ARCHITECTURE.md](./ARCHITECTURE.md) for design.

## Current status

**M4 eval harness done. T-049 (local $0 baseline) in progress: first live pass graded S1–S4 (3/4 top-1); fixes T-054/T-055/T-056 before the final re-record. Paid Claude run optional (T-042).** M1–M3 done. CP0 passed 2026-10-27. Board: `docs/tasks/BOARD.md`.
Repo: https://github.com/HenryWinNguyen/firebreak (public).

## Log

### 2026-10-29: T-049 first live pass, fixes before the final recording
- Ollama running (`qwen3:8b`, 32k context). First live record pass, baseline-local, $0:
  S1 ✅ top-1 (3 steps), S2 ✅ top-1 (5), S3 ❌ (answered resource_exhaustion; expected slow_query), S4 ✅ top-1 (5).
  S6 errored: `OrdersRssGuardError`, no RSS sample. Orders was stalled on Redis, 2 scrapes timed out (RSS ~755 MiB, fine).
  The first S4/S6 attempt hit `LLMConnectionRefused` because the Ollama server had stopped; rerun after restart.
- Findings: (1) the guard fails on any blind poll → **T-054** grace window. (2) the model wrote `level=error` as LogQL
  in S4 and S6 (Loki 400, no logs): the tool description doesn't name the labels → **T-055**. (3) S3 and S2 look the same
  in every orders metric → **T-056** `db_queries_total` (CONTRACTS §2.4 v1.1, additive).
- T-055 changes the tool list, which is part of every LLM step key, so all recordings get re-recorded after these merge.
  The first-pass recordings are not committed.

### 2026-10-28: M4 eval harness
- **T-016** (#61, 1 rework): live validation of S5–S7 (no LLM). S5 HighMemory/orders fires at 216 s (peak 1143 MiB);
  S6 fires at 321 s with its window widened to 600 s (orders only records latency when slow requests finish); **S7
  never fired** in 5 runs (the single-process Python load generator tops out at ~120–440 rps; orders p95 peaked at
  0.74 s) → xfail via `known_unfired`, follow-up T-053. Rework: the S5 restart looked the container up by directory
  name and could silently restart nothing from another checkout; it now finds it by compose labels (exactly one
  match) and verifies `State.StartedAt` changed.
- **T-052** (#63, 0 blocking): S5 is safe to drive. Orders' chaos reset frees the leak buffer; `load.hold: duration`
  (S5 stops load after 300 s, ~1.6–1.9 GB); an orders RSS guard (`process_resident_memory_bytes{job="orders"}`,
  default 2500 MB, fails closed) cancels the investigation and tears down. Needs the orders image rebuilt before a
  live S5 run. Backlog: guard task created before `gen.start()`; narrow `uncancel()` window; ~25% guard margin if a
  faster load generator lands.
- **T-051** (#59, Opus review 0 blocking + 4 pre-merge fixes): `run --mode record --agent NAME --drive` applies each
  scenario's faults via the Shopfloor admin API, runs bounded closed-loop load, waits for the matching incident
  (opened after apply; alertname + service), records, and always resets (shielded from cancellation). Any firing or
  pending alert counts as a dirty stack; a clear timeout fails the run (sticky); crashed load workers restart and an
  unhealthy load fails the scenario. One scenario's error no longer aborts the batch; exit code 1 on any error.
  Lead confirmed against Ollama: history with malformed tool args echoed as `{}` → 200, raw invalid JSON → 400
  (the T-047 review's blocking finding was real). Backlog: reset an inventory deploy too (no scenario uses one yet).
- **T-050** (#56): Ollama silently truncates prompts beyond `OLLAMA_CONTEXT_LENGTH` (default 4096). The
  OpenAI-compatible client now refuses a prompt that can't fit (`FIREBREAK_LLM_OPENAI_CONTEXT_TOKENS`, default 32768,
  must match the server) and raises `ContextOverflowError` when reply usage shows near-full context; never retried,
  never falls back. Lead added a warning when a reply carries no usage. Henry installed Ollama; `qwen3:8b` pulled.
  Note: an LLM error aborts the whole record run (not just the scenario), so local runs go one scenario at a time.
  Backlog: record a per-scenario error and continue instead.
- **T-048** (#53): spend gate. `python -m firebreak.agents.baseline [--agent NAME]` refuses (exit 2, before any client,
  engine or adapter is built) unless `FIREBREAK_EVAL_LIVE=1`, plus `ANTHROPIC_API_KEY` when the config routes to
  Anthropic; replay is exempt. The OpenAI-compatible key is now in the recording secret scan. No remaining path
  reaches a paid provider without the opt-in.
  Backlog: an invalid `FIREBREAK_LLM_MODE` prints a traceback (fails closed) instead of exit 2; tests for record mode
  without opt-in and an invalid mode; the refusal text says "record mode" for CLI runs too.
- **T-047** (#49, 1 rework, Opus review): hybrid model plan. `OpenAICompatLLMClient` (Ollama / any OpenAI-compatible
  server) translates the Anthropic-shaped contract internally; `RoutingLLMClient` picks the provider per model from
  `pricing.yaml` (`provider` required; unknown model = typed error, never a default to Anthropic). `baseline-local`
  (Qwen3 8B, same prompt/tools) runs via `python -m firebreak.evals run --agent baseline-local`; it needs only
  `FIREBREAK_EVAL_LIVE=1`, never an Anthropic key. Malformed tool args reach the gateway as `invalid_args` and are
  echoed to the server as `{}`. `investigate()` takes `config_path` (lead grant #50).
  Backlog: the baseline CLI (`python -m firebreak.agents.baseline`) can reach Anthropic without `FIREBREAK_EVAL_LIVE`
  (pre-existing; add a live gate); add `openai_api_key` to the recording secret scan; confirm against a real Ollama
  that malformed history is accepted (run the `FIREBREAK_OLLAMA=1` integration test once installed).
- **T-015** (#46): Alertmanager webhook token file is 0600 (verified in the pinned image); `honor_labels: true` removes the
  spurious `exported_service` label (Shopfloor exports no `job`/`instance`, so routing, grouping and ingest are
  unchanged); promtool tests moved to `ops/prometheus/tests/` with below-threshold and `for:`-pending cases per rule.
  Backlog: pair each pending case with a later `eval_time` that asserts the fire.
- **T-043** (#37, 1 rework): scenarios S1–S7 (`evals/scenarios/`) + deterministic grader (top-1/top-3, cited evidence kinds, report validity).
- **T-044** (#40, Opus, 0 blocking): record/replay cassettes for LLM + tool calls. Replay has no live fallback; stale
  recordings raise `StaleRecordingError` naming the first difference. 20× round trip + 20× random-latency runs identical.
  Lead decision: untrusted-block nonces stay random in all modes (seeding them would make the injection boundary
  predictable during live recording); `fixture_key` normalizes them instead.
- **T-046** (#42, Opus, 0 blocking): nonce normalization anchored to real wrapper tags; secret scan adds base64/URL forms
  and warns on short secrets; crash-safe overwrite; unconsumed recorded tool calls are stale; the gateway assigns
  evidence ids at call start.
- **T-045** (#43, 2 rework rounds, Opus re-reviews): `python -m firebreak.evals run|report`, `make eval`, `eval_runs`/
  `eval_results` (migration 0002), `evals/REPORT.md`. Review caught a data-loss bug: replay purged the recorded
  incident in the **dev** DB. Fixed: evals use only a dedicated `firebreak_eval` DB, and every write path checks
  `current_database()` after connecting (a `?database=` URL override bypassed the name-only check in round 1); the purge
  touches only rows listed in the recording and aborts on extra/shared links; record mode refuses eval DB == live DB.
  `make eval` exits 1 ("no recordings found") until T-042 records.
- Lead fix (#44): the test fixture's `_test` guard (it runs `DROP SCHEMA`) had the same `?database=` bypass; it now
  rejects the parameter and re-checks `current_database()` before dropping.
- Backlog (M10 hardening): secret scan misses a secret embedded inside a larger base64 blob (e.g. Basic auth) and
  `quote()`-with-`/`/lowercase-`%2b` forms; a hard kill mid-overwrite leaves the old recording in a hidden dir with no
  recovery, and crashed-run staging dirs accumulate; partial-consumption replay test; `run.json` written outside the
  `record_run` staging dir; same-DB detection misses socket-vs-TCP to one server (compare `system_identifier`);
  `?options=` raises a raw `TypeError`; fixed test DB names collide across concurrent local runs.

### 2026-10-28: M1 acceptance + live M2 reproduction
- **T-014** (#20, 1 rework): S1–S4 each fire the expected alert on the live stack and clear after reset
  (fire: S1 171 s, S2 150 s, S3 210 s, S4 153 s). Reset always runs, broken load generators fail the test,
  stale alerts can't pass (3 clean polls, Prometheus pending checked, `startsAt` after the fault). **M1 done.**
- Lead ran T-024's `make up` + curl reproduction and found the API container never applied migrations
  (`relation "alerts" does not exist`, 500); every test builds its own schema, so nothing caught it. Fixed:
  the API image runs `alembic upgrade head` before serving; `make migrate` added. Rerun: 15-alert storm → 1
  incident with 15 alerts; replay → all duplicates. **M2 done end to end.**
- Backlog: a CI smoke job that builds the API image against a fresh Postgres and posts one webhook.

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

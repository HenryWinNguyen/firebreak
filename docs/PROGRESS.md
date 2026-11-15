# Progress Log

Reverse-chronological log of milestones. See [SCOPE.md](../SCOPE.md) for the
milestone plan (M0–M13) and [ARCHITECTURE.md](./ARCHITECTURE.md) for design.

## Current status

**M5 in progress: multi-agent path records and replays; first `multi-local` scenario (S1) recorded and it missed (the supervisor never planned `deploys`). Next: supervisor planning fix, then S2–S6. CP1 passed 2026-11-14.** M1–M4 done (S7 waits on T-053). CP0 passed 2026-10-27. Paid Claude run optional (T-042). Board: `docs/tasks/BOARD.md`.
Repo: https://github.com/HenryWinNguyen/firebreak (public).

## Log

### 2026-11-15: T-067, T-068 merged; first multi-agent recording (S1)
- **T-067 (#90, Opus, high risk)**: `multi-local` records and replays through `IncidentWorkflow`, with one recording
  per agent call. Three Opus re-review rounds, each with a reproduced blocker:
  - a timeout or retry discarded the whole recording;
  - a call that failed during record replayed with no drift check;
  - a failed call was accepted if it failed in any way, not only as recorded.
  Final state: retried calls are re-recorded with per-attempt seeds; terminal failures are stored and must fail
  identically on replay; the recorded outcome (state, degraded, dropped) is checked; and a recording that could not
  replay is refused at record time.
- **T-068 (#94)**: unknown plan agents are dropped into `dropped_tasks`; timeouts are 900/1200/1800 s (specialists
  1800 s); agent retries back off 10 s / 20 s.
- **First live `multi-local` recording, S1 (bad deploy), $0**: qwen3:8b with a 16k context (32k ran a 16 GiB Mac out of
  memory; Claude Code killed the first attempt, and the driver's reset was run by hand). It took 402 s, 10 LLM steps
  (baseline: 448 s, 7 steps), and replays offline identically.
  **Result: miss (top-1 and top-3), where the single-agent baseline hit.** The cause is visible in the recording: the
  supervisor planned `metrics`, `logs` and the *tool* `service_graph` again (dropped by T-068, despite the prompt
  fix) and never planned `deploys`, so no agent looked at the deploy.
  Takeaway: with a small local model, the supervisor's plan is the weak link; better specialists don't help if
  the right one is never asked.
- Open: an empty `question` is allowed in `SpecialistTask` (a reviewer-found path to a guaranteed failure).

### 2026-11-14 (evening): T-066 merged, first live end-to-end run, contracts v1.8
- **T-066 merged (#89):** ingest signal-with-starts `incident-<id>`; the workflow waits in `GROUPING` for the window
  (or a critical alert) and reloads the context before triage. Review blockers fixed in rework: stale context after
  the wait, `None`-window semantics, and a concurrent connect that could block past its time bound.
- **First live end-to-end run** (compose stack, qwen3:8b on one Ollama, $0): a warning webhook created
  `inc_11276771…`, the workflow waited 60 s, then triage, supervisor, metrics, deploys and synthesizer all ran, and it
  finished `REPORT_ONLY` in ~11 min. The alert was synthetic (orders was healthy), so the report's "unknown, 0.5"
  is honest, and its one finding cites a real evidence id. Flaws found: the supervisor planned the *tool*
  `service_graph` as an agent (it degraded harmlessly), and serial Ollama makes 300 s timeouts too short.
- **#91 (lead):** 8 failed workflows on the live Temporal came from host test runs (start identity `<pid>@<host>`),
  bisected to `tests/integration` posting webhooks through the real signaller. A root autouse fixture now points
  every test at a dead Temporal address; the full suite adds 0 live workflows (was 4).
- **Contracts v1.8 (#92, additive):** `SPECIALIST_AGENTS`, `WorkflowOutcome.dropped_tasks`; `SpecialistTask.timeout_s`
  default 300→1800, max 900→3600. Spec **T-068** (Sonnet): drop unknown agents, local-model timeouts, LLM-down
  backoff, supervisor prompt fix. The supervisor prompt is in the request hash, so `multi-local` is recorded after it.
- **T-067 (#90) in rework:** the Opus review confirmed replay is deterministic (a shared-stream probe makes the
  skewed-completion test fail, so it is not blind), but reproduced one blocker: any timeout or retry during record
  discarded the whole recording. The fix records retried calls fresh and stores terminal failures so replay degrades
  the same way.
- Ollama now runs as a background process the lead starts (log in the session scratchpad), not in a terminal tab.

### 2026-11-14 (later): contracts v1.7, specs T-066/T-067
- Contracts **v1.7** (additive): `IncidentContext.window_closes_at`, `AlertAdded`, `SIGNAL_ALERT_ADDED`; grouping rule in
  CONTRACTS §1 (ingest signal-with-start; the workflow waits for the window as a timer; Postgres grouper keeps
  membership, a deliberate simplification of ARCH §3). Compose `api` gets `FIREBREAK_TEMPORAL_ADDRESS`.
- Specs: **T-066** ingest → workflow (Sonnet), **T-067** `multi-local` eval record/replay through the workflow (Opus,
  high risk: determinism with parallel specialists).

### 2026-11-14: M5 core merged: workflow, agent roster, worker live; S7 load generator
- **T-064** (#86, build B of two): `IncidentWorkflow` (load_context → triage → supervisor plan → parallel specialists →
  synthesizer → `REPORT_ONLY`; noise/duplicate → `DISMISSED`; failures → `ESCALATED` with `error`), `load_context` /
  `set_incident_state` activities, `IncidentRepository.set_state`, worker (`python -m firebreak.workflows.worker`),
  `firebreak/workflows/client.py` (`connect`, `start_incident_workflow`, id `incident-<id>`, reject duplicates).
  Kill-the-worker test restarts both workers on Temporal's dev server; the dead attempt times out and the run finishes.
  Polish round after selection (sandbox import, client module, worker tests, root-cause error text).
- **T-064 same-task comparison (AGENT_WORKFLOW §8), 2026-11-14.** Two Sonnet builders implemented the same spec
  independently (A #84, B #86). Both passed CI and the verify command (A 47 tests, B 50; ~10–12 min each).
  An independent reviewer compared them; **B kept**, A closed.
  - A had a real bug that its tests missed: on `ESCALATED` the outcome dropped `degraded_sources` (the updated context
    never left `_investigate`). Its kill-the-worker test stopped only the activity worker, so the cancelled activity
    retried in ~1 s: it proved retry, not crash recovery.
  - B: no blockers; the kill test restarts both workers and lets the dead attempt time out (10 s), so it is the real
    acceptance. Ported from A: the `connect()` client helper and clean Ctrl-C handling.
  - Both builders independently hit the same Temporal limit: the time-skipping test server doesn't advance time while a
    killed attempt is in flight (B) or stalled in CI (A), so both moved the kill test to `start_local` (real dev server).
  - Takeaway: same spec, similar size (~890 vs ~1000 lines), but test strength differed more than code shape. A spec item
    phrased as an outcome ("degraded sources are listed") needs an explicit assertion in the DoD, or a builder can pass
    without it. Two builds cost ~2× builder time for one bug caught; worth it once for a high-risk core task, not as a
    habit.
- **T-065** (#82, 1 rework): `multi-local` roster on `qwen3:8b` (triage, supervisor, metrics, logs, deploys,
  synthesizer) and the `run_agent` activity: per-agent blackboard slices (§4.4), synthesizer input capped at 30 findings
  round-robin with an explicit "N omitted" note, adapters per call (none for tool-less agents), non-retryable
  `ApplicationError` for bad config and permanent LLM errors, transient errors retried.
- **T-053** (#83, 1 rework): `Load.processes` (1–8) and a multi-process load generator (spawn children, per-child
  counters, mid-run child death logged and fails at stop, no orphans). Local floor ~2400 rps with 4 processes vs ~1300
  with 1. S7 set to 4 processes, 1200 rps total (4 × 300); live validation pending (needs a driven run).
- Lead: `worker` service in compose (Temporal at `temporal:7233`, tools on the compose network, Ollama via
  `host.docker.internal`). **Live smoke** (Ollama off): started `incident-inc_cab1556b…` → worker picked it up, triage
  retried 3× (~4 s), outcome `ESCALATED` with "triage activity failed: …ConnectError…", DB state `escalated`.
  Follow-up: an Ollama blip escalates in ~4 s; LLM connection errors want a longer backoff.
- Review minors left open: T-064 worker test doesn't build a real `Worker` (activity name/signature acceptance) and
  `aclose()` failure handling; T-065 no end-to-end test of the synthesizer `{"error": ...}` path, `affected_services`
  filter drops all names when `ctx.services` is empty; T-053 watcher task created outside `start()`'s try, Ctrl-C may log
  one benign "died mid-run", worst-case halt ~15 s.
- Next for M5: start workflows from ingest when the grouping window closes; `multi-local` eval path (record/replay
  through the workflow) and S1–S6 recordings vs baseline (needs Ollama); live kill-the-worker acceptance.

### 2026-11-14: CP1 passed; M5 started
- **CP1 (Henry, 2026-11-14): go on M5.** Baseline reviewed: `baseline-local` S1–S6 top-1 2/6, top-3 3/6, required
  evidence cited 0/6, S5/S6 invalid (empty support; invented evidence ids). S7 stays a gap until T-053.
  Also decided: T-053 tries a multi-process load generator first, time-boxed, fallback a scenario-scoped orders capacity
  cap; OK to stop log services or shrink the Ollama context if memory runs out.
- Contracts **v1.6** (additive): `firebreak/models/orchestration.py` (`TriageResult`, `Plan`, `SpecialistTask`,
  `IncidentContext`, `AgentTask`, `StateUpdate`, `WorkflowOutcome`, task queues, activity names) and the workflow ↔
  activity rule in CONTRACTS §1.
- Temporal in compose: the CLI dev server (`temporalio/temporal:1.9.1`, SQLite on a volume, ~85 MiB), instead of the
  server plus its own Postgres from ARCHITECTURE §1. Checked: history survives a container restart. Ports 7233/8233.
- ADR-001 (Temporal) already accepted, so no new ADR. Specs: **T-064** workflow skeleton + worker (two builders,
  AGENT_WORKFLOW §8 comparison), **T-065** local multi-agent roster + `run_agent` activity, **T-053** S7 load.

### 2026-11-01: T-060, T-061, T-062 merged (review minors); `replay` is a required check
- **T-060** (#78): `make eval AGENT=all` replays only agent dirs that hold recordings (`skip <dir>: no recordings`
  otherwise); the `replay` job has `timeout-minutes: 15` and a comment that matches the runner (fails on stale
  recording, bad run info, unknown agent, setup error; graded misses and invalid answers exit 0).
- **T-061** (#77): `db_queries_total` is counted before each `await`, so failed round trips count; tests for a raising
  connection and the lifespan wiring. Live after rebuilding orders: 3 orders → 6.
- **T-062** (#79): the pre-load RSS check refuses to start load when orders is already over the limit; docstrings give
  the real blind bound (≈ grace + poll + HTTP timeout); the two timing-sensitive guard tests use a fake clock; a golden
  old-format recording (`tests/evals/fixtures/old_format/`) replays unpatched. Minors left: the "load never started"
  assertion is indirect (worker-count constant); the golden fixture goes stale on a prompt change like any recording.
- `replay` added to main's required status checks (with `check`, `shopfloor`, `owned-paths`; strict unchanged).
- Housekeeping: 34 merged builder worktrees and their local branches removed.

### 2026-10-31: T-058, T-059 merged: S5 memory really frees, CI replays recordings
- **T-058** (#74): S5 leak buffers are anonymous mmaps; `/admin/chaos/reset` closes them. Live check after rebuilding
  orders: 71 MiB → 371 MiB (300 leaked requests × 1 MiB) → reset → 71 MiB, no restart. A failed close returns 500,
  which the driver records as a broken teardown (next scenario refuses to start).
- **T-059** (#73): `make eval AGENT=<dir>|all` (default `baseline-local`); new CI job `replay` (Postgres `firebreak_eval`,
  no keys, ~40 s) replays every committed recording. Replay exits non-zero only on a stale recording or setup error;
  graded misses and invalid answers are results. Proven red with a missing LLM fixture. Note: editing a hash in
  `meta.json` `llm_steps` does not fail replay (hashes are diagnostic; only the step count is checked). The spec's
  red recipe was wrong. Open minors: the ci.yml comment overstates ("any error"); no job `timeout-minutes`; a stray dir
  under `evals/recordings/` turns CI red. A regression from a good answer to a worse grade still passes CI.
- `replay` is not yet a required check in branch protection (needs Henry's OK).

### 2026-10-30: T-049 local baseline recorded (S1–S6), $0 (recordings span 10-30 16:41 – 10-31 03:57 UTC)
- T-057 (#71): each recording seeds ids with `<scenario>@<at ISO>`, so re-records no longer collide in the eval DB
  (0 conflicts across all passes). Review minors left open: the disjoint-ids test uses the in-memory sink, and the
  old-format replay test patches `deterministic` during replay too (no golden old-format recording).
- Recorded live with `qwen3:8b` (Ollama, 32k ctx), committed under `evals/recordings/baseline-local/`, and replayed
  offline with identical results. `evals/REPORT.md`:

  | | S1 bad_deploy | S2 resource_exhaustion | S3 slow_query | S4 dependency_failure | S5 memory_leak | S6 misconfiguration |
  |---|---|---|---|---|---|---|
  | top-1 / top-3 | ✅ / ✅ | ❌ / ✅ | ❌ / ❌ | ✅ / ✅ | ❌ / ❌ (invalid) | ❌ / ❌ (invalid) |

  **Top-1 33%, top-3 50%, 2 of 6 invalid final answers, required evidence kinds cited in 0 of 6.** S6 cited invented ids
  (`alert_1`, `deploy_0`), S5 had a hypothesis with no supporting finding, S3 still answers without querying
  `db_queries_total`. Latency p50 180 s / p95 448 s on a laptop.
- Operational: Ollama stopped twice mid-run (cause unknown, no crash/jetsam log). After S5, orders kept ~1.5 GiB and
  `HighMemory` kept firing; the T-052 reset doesn't release it, so the lead restarted orders → **T-058**. S5 was
  recorded alone with `FIREBREAK_EVAL_DRIVE_MAX_ORDERS_RSS_MB=2000` (the 1500 attempt tripped the guard at 1502 MiB).
- S7 still waits on T-053 (load generator).

### 2026-10-29: T-049 re-record lost to evidence-id collisions → T-057
- Stack rebuilt from main (it had been running T-016's worktree code, `0c6d7ac`) under the same compose project, so the
  Postgres volume kept earlier evidence. Grafana not started (host port 3000 in use); evals don't need it.
- Live check: `db_queries_total` = 6 after 3 orders (T-056 lifespan wiring works). T-054 grace seen working in S6.
- Re-record S1–S6: recording seeds ids with the scenario id only, so each tool result collided with the first pass's
  evidence (`EvidenceConflictError`, 18×) and was discarded; S1–S4 ended with no citable findings. S5 (first time
  recorded) graded, wrong. S6: Ollama down again (`LLMConnectionRefused`). → **T-057**: per-recording seed. Not committed.

### 2026-10-29: T-049 first live pass, fixes before the final recording
- Ollama running (`qwen3:8b`, 32k context). First live record pass, baseline-local, $0:
  S1 ✅ top-1 (3 steps), S2 ✅ top-1 (5), S3 ❌ (answered resource_exhaustion; expected slow_query), S4 ✅ top-1 (5).
  S6 errored: `OrdersRssGuardError`, no RSS sample. Orders was stalled on Redis, 2 scrapes timed out (RSS ~755 MiB, fine).
  The first S4/S6 attempt hit `LLMConnectionRefused` because the Ollama server had stopped; rerun after restart.
- Findings: (1) the guard fails on any blind poll → **T-054** grace window. (2) the model wrote `level=error` as LogQL
  in S4 and S6 (Loki 400, no logs): the tool description doesn't name the labels → **T-055**. (3) S3 and S2 look the same
  in every orders metric → **T-056** `db_queries_total` (CONTRACTS §2.4 v1.1, additive).
- **T-054** (#68): the RSS guard rides out a Prometheus blind window (grace 180 s,
  `FIREBREAK_EVAL_DRIVE_RSS_BLIND_GRACE_S`) and still fails closed if blind at start (new sync check before load) or past
  the grace. Review minors left open: the start check doesn't compare against the limit (the first guard poll does); the
  real blind bound is ≈ grace + poll + HTTP timeout; two test assertions are timing-tight / indirect.
- **T-055** (#66, 1 rework: said `warn`, but Shopfloor logs `warning`): `logql_query` description names the stream
  selector, the labels `service`/`level` and two examples. Test guards both descriptions.
- **T-056** (#67): orders exports `db_queries_total{service}` (2 per order, quantity+1 under N+1). Review minors left
  open: failed round trips aren't counted (counted after the await); the lifespan wiring has no test (checked live on rebuild).
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

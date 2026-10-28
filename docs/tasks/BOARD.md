# Task Board

**Only the lead edits status** (AGENT_WORKFLOW §3.2). At most 3 in progress.

| Task | Title | Track | Model | Depends on | Status | PR |
|---|---|---|---|---|---|---|
| T-011 | Shopfloor services | target | sonnet | — | merged | #5 |
| T-012 | Chaos flags + faults S1–S6 (+ T-011 review follow-ups) | target | sonnet | T-011 | merged | #12 |
| T-013 | Observability stack + alert rules | target | sonnet | — | merged | #10 |
| T-014 | M1 acceptance: S1–S4 alerts | target | sonnet | T-012, T-013 | merged | #20 |
| T-015 | Observability follow-ups (T-013 minors) | target | sonnet | T-013, T-014 | merged | #46 |
| T-016 | Live validation of S5–S7 (tune scenario files) | target | sonnet | T-043 | in-progress |  |
| T-022 | Persistence layer | core | sonnet | — | merged | #4 |
| T-025 | Persistence hardening (T-022 review follow-ups) | core | sonnet | — | merged | #18 |
| T-026 | Grouper onto shared repo methods + tests | core | sonnet | T-025 | merged | #21 |
| T-021 | Alert ingest + dedupe | core | sonnet | T-022 | merged | #13 |
| T-023 | Incident grouper | core | sonnet | T-022 | merged | #11 |
| T-027 | Ingest follow-ups (resolved updates, fixtures) | core | sonnet | T-021, T-024, T-025 | merged | #23 |
| T-024 | M2 acceptance: storm + replay | core | sonnet | T-021, T-023 | merged | #19 |
| T-031 | LLM client + record/replay | agents | sonnet | — | merged | #7 |
| T-032 | Agent runtime + tool gateway | agents | **opus** (high risk) | — | merged | #16 |
| T-033 | LLM client + runtime follow-ups (T-031/T-032 minors) | agents | sonnet | T-031, T-032 | merged | #25 |
| T-034 | Read tools (Prometheus, Loki, deploys, service graph) | agents | sonnet | T-032 | merged | #28 |
| T-035 | Read-tool hardening (T-034 minors) | agents | sonnet | T-034 | merged | #31 |
| T-041 | Single-agent baseline (config, prompt, runner, CLI) | agents | sonnet | T-033, T-034 | merged | #32 |
| T-042 | M3 acceptance on Claude (paid, **optional**: Henry deciding) | evals | sonnet | T-014, T-041, T-044, T-045 | deferred (optional) |  |
| T-043 | Eval scenarios S1–S7 + deterministic grader | evals | sonnet | T-041 | merged | #37 |
| T-044 | Record/replay cassettes (tools + LLM, deterministic) | evals | **opus** (high risk) | T-041 | merged | #40 |
| T-045 | Eval runner, results tables, `make eval`, REPORT.md | evals | sonnet | T-043, T-044 | merged | #43 |
| T-046 | Record/replay hardening + evidence ids at call start | evals | sonnet | T-044 | merged | #42 |
| T-047 | OpenAI-compatible client (local Ollama) + baseline-local (hybrid) | agents | sonnet | T-045, T-046 | merged | #49 |
| T-048 | Baseline CLI spend gate + OpenAI key in secret scan | agents | sonnet | T-047 | merged | #53 |
| T-049 | Local baseline run S1–S7 (`baseline-local`, $0) for CP1 | evals | lead | T-047, T-016 | waiting on Henry (install Ollama) | |

```
T-011 ─► T-012 ─┐
T-013 ──────────┴─► T-014            (M1 done)
T-022 ─┬─► T-021 ─┐
       ├─► T-023 ─┴─► T-024           (M2 done)
       └─► T-025 ─► T-026
T-031, T-032 ─► T-033 ─┐
       T-032 ─► T-034 ─┴─► T-041 ─► T-042 (M3 done; T-042 needs Henry: API key + spend)
```

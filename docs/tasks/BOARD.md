# Task Board

**Only the lead edits status** (AGENT_WORKFLOW §3.2). At most 3 in progress.

| Task | Title | Track | Model | Depends on | Status | PR |
|---|---|---|---|---|---|---|
| T-011 | Shopfloor services | target | sonnet | — | merged | #5 |
| T-012 | Chaos flags + faults S1–S6 (+ T-011 review follow-ups) | target | sonnet | T-011 | merged | #12 |
| T-013 | Observability stack + alert rules | target | sonnet | — | merged | #10 |
| T-014 | M1 acceptance: S1–S4 alerts | target | sonnet | T-012, T-013 | rework | #20 |
| T-015 | Observability follow-ups (T-013 minors) | target | sonnet | T-013, T-014 | ready | |
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
| T-035 | Read-tool hardening (T-034 minors) | agents | sonnet | T-034 | rework | #31 |
| T-041 | Single-agent baseline (config, prompt, runner, CLI) | agents | sonnet | T-033, T-034 | rework | #32 |
| T-042 | M3 acceptance: live S1–S7 (**paid, needs Henry**) | evals | sonnet | T-014, T-041 | pending | |

```
T-011 ─► T-012 ─┐
T-013 ──────────┴─► T-014            (M1 done)
T-022 ─┬─► T-021 ─┐
       ├─► T-023 ─┴─► T-024           (M2 done)
       └─► T-025 ─► T-026
T-031, T-032 ─► T-033 ─┐
       T-032 ─► T-034 ─┴─► T-041 ─► T-042 (M3 done; T-042 needs Henry: API key + spend)
```

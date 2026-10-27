# Task Board

**Only the lead edits status** (AGENT_WORKFLOW §3.2). At most 3 in progress.

| Task | Title | Track | Model | Depends on | Status | PR |
|---|---|---|---|---|---|---|
| T-011 | Shopfloor services | target | sonnet | — | ready | |
| T-012 | Chaos flags + faults S1–S6 | target | sonnet | T-011 | pending | |
| T-013 | Observability stack + alert rules | target | sonnet | — | ready | |
| T-014 | M1 acceptance: S1–S4 alerts | target | sonnet | T-012, T-013 | pending | |
| T-022 | Persistence layer | core | sonnet | — | ready | |
| T-021 | Alert ingest + dedupe | core | sonnet | T-022 | pending | |
| T-023 | Incident grouper | core | sonnet | T-022 | pending | |
| T-024 | M2 acceptance: storm + replay | core | sonnet | T-021, T-023 | pending | |
| T-031 | LLM client + record/replay | agents | sonnet | — | ready | |
| T-032 | Agent runtime + tool gateway | agents | **opus** (high risk) | — | ready | |

**First wave (after CP0):** T-011, T-022, T-032. These are the critical paths
for M1, M2 and the runtime. Next when a slot frees up: T-013, T-031.

```
T-011 ─► T-012 ─┐
T-013 ──────────┴─► T-014            (M1 done)
T-022 ─┬─► T-021 ─┐
       └─► T-023 ─┴─► T-024           (M2 done)
T-031, T-032 (independent)            (runtime ready for M3)
```

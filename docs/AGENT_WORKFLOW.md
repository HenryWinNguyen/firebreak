# Building Firebreak with Multiple Claude Agents

How this project gets built by a **lead** Claude session that plans the work
and hands it to **parallel builder agents**. It runs on its own between a
small number of **major checkpoints** where Henry decides.

Firebreak *is* a multi-agent system, and it's also *built by* one. The same
principles apply to both: contracts before parallelism, evidence (tests)
over claims, and a human at the approval gates.

Sources (Oct 2026, CLI v2.1.311):
[Run agents in parallel](https://code.claude.com/docs/en/agents.md) ·
[Subagents](https://code.claude.com/docs/en/sub-agents.md) ·
[Agent teams](https://code.claude.com/docs/en/agent-teams) ·
[Cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging.md) ·
[Worktrees](https://code.claude.com/docs/en/worktrees.md) ·
[Best practices](https://code.claude.com/docs/en/best-practices). Also borrows
ideas from the "Shards" worktree-orchestrator build series (plans with
mandatory reading, refactor before parallelizing, review for silent failures).

---

## 1. Team size: 1 lead + up to 3 builders

| | Count | Model | Why |
|---|---|---|---|
| **Lead** | 1 (Henry's one open terminal) | Opus | Planning, contracts, gating merges. These are the high-leverage decisions |
| **Builders** | **max 3 in flight** | Sonnet (Opus for flagged hard tasks) | The first parallel phase (M1 ∥ M2 ∥ runtime) has exactly 3 independent seams. More than 3 hits usage limits faster and adds merge churn for little gain |
| **Reviewer** | 1 per task, short-lived | Sonnet | A fresh read-only check before every merge |

Phases with fewer seams use fewer builders. M0 and M5 run with one each,
on purpose.

---

## 2. How it runs: one terminal, lead-spawned builders

Henry opens **one** session. The lead does the rest:

```bash
cd ~/Desktop/Projects/firebreak
caffeinate -i claude --name lead        # caffeinate keeps the Mac awake for long runs
```

```
 Henry ──(major checkpoints only)──► LEAD (Opus, main checkout)
                                       │  reads BOARD, picks ready tasks
                ┌──────────────────────┼───────────────────────┐
                ▼                      ▼                       ▼
     builder "b-T011"        builder "b-T021"         builder "b-T031"      ← background subagents,
     own worktree+branch     own worktree+branch      own worktree+branch     isolation: worktree,
     one task, fresh ctx     one task, fresh ctx      one task, fresh ctx     agent type: builder
                │                      │                       │
                └── PR + "done" ──► LEAD runs merge gates (§5) ─► merge ─► broadcast ─► next task
```

- **One builder = one task = one fresh context.** The lead spawns a
  `builder` subagent (`.claude/agents/builder.md`) per task, with
  `isolation: worktree` and a name like `b-T021`. It runs in the
  background, gets its own branch, and is cleaned up after merge.
- **No terminals to juggle.** Builders show up in the lead's agent panel.
  Henry can select one (↑/↓, Enter) to watch it or message it directly.
- **Builders are named, so they can talk:** builder ↔ lead and builder ↔
  builder over `SendMessage`.
- **Fallback:** if Henry wants to drive one track by hand, `claude -w core
  --name core` in a second terminal still works. It talks to `@lead` the
  same way.

Agent **teams** stay off for building. They're turned on only for the
debate and red-team sessions in §8.

---

## 3. How tasks are assigned

### 3.1 Task specs (lead writes, builders execute)
Every unit of work is `docs/tasks/T-<milestone><nn>.md` (template:
`_TEMPLATE.md`), e.g. `T-021` = M2, task 1. Every spec has:

- **Front matter:** `track`, `model`, `depends_on`, `owned_paths`,
  `status`, `risk`
- **Mandatory reading:** the files the builder must read first
- **Patterns to mirror:** existing code to copy conventions from
- **Definition of done:** observable checkboxes
- **Verify command:** what "done" means mechanically
- **Out of scope**

### 3.2 Board
`docs/tasks/BOARD.md` is one table: task, track, deps, status, PR. **Only
the lead changes status.** Having a single writer avoids the "task status
lags" problem the agent-teams docs warn about.

```
pending → ready (deps merged) → in-progress (builder spawned) → in-review (PR open)
        → merged | rework (back to builder, max 2 rounds) | escalated (checkpoint)
```

### 3.3 Scheduling rules (what the lead runs on its own)
1. A task is **ready** when every `depends_on` task is merged to `main`.
2. Never have two in-flight tasks with **overlapping `owned_paths`**.
3. Max **3 in flight**. Pick in order: critical path first, then milestone
   order, then lowest number.
4. Size: **one PR, roughly 30–120 min of agent work, about 400 lines of
   diff or less.** Split anything bigger.
5. `risk: high` tasks (workflow core, policy engine, executor) get **Opus**
   and a second reviewer pass.

### 3.4 Tracks (owned directories)
| Track | Owns | Active in |
|---|---|---|
| `target` | `shopfloor/`, `ops/` | M1, M10 scenarios |
| `core` | `firebreak/{api,ingest,workflows,activities,policy}/` | M2, M5, M7, M8 |
| `agents` | `firebreak/{llm,agents,tools}/` | runtime, M3, M6 |
| `evals` | `evals/` | M4 onward |
| `dashboard` | `dashboard/` | M9 |
| **lead-only** | `firebreak/models/`, `docs/`, `CLAUDE.md`, `.claude/`, CI config | always |

---

## 4. How builders work together

1. **Contracts land first.** Shared schemas and Protocols
   (`firebreak/models/`, `docs/CONTRACTS.md`) are merged to `main`, with
   stubs and tests, *before* the tasks that use them are spawned. That's
   what makes parallel work safe.
2. **Builders never edit contracts.** If one needs a change, it messages
   `@lead` with the proposed diff and keeps working on other parts of its
   task if it can.
   - **Additive change** (new optional field or new type): the lead lands
     it, broadcasts "rebase on main", and nothing stops.
   - **Breaking change** (rename or remove, a type change): **checkpoint**.
     Henry decides.
3. **Direct dependencies talk directly.** For example `b-T031` messages
   `b-T011`: "is `PrometheusAdapter.range()` returning UTC?" Anything else
   goes through the lead.
4. **After every merge** the lead messages in-flight builders whose task
   touches the merged area: "#N merged, rebase on main."
5. **Decisions live in files.** A message can trigger work, but the result
   always lands in a spec, an ADR, CONTRACTS.md or PROGRESS.md.

---

## 5. Merge gates (lead merges without asking Henry)

A PR merges on its own when **all** of these pass:

| Gate | Check |
|---|---|
| G1 CI | ruff + mypy --strict + pytest (+ replay evals once M4 exists) green |
| G2 Ownership | `scripts/check_owned_paths.py <task>`: the diff touches only `owned_paths` |
| G3 Spec | Every definition-of-done box is checked, and the verify command passes in the lead's own run |
| G4 Review | `reviewer` subagent reports **zero blocking** items (it explicitly looks for silent failures, swallowed exceptions, duplication, contract drift) |
| G5 Log | PR adds a PROGRESS.md entry |
| G6 Risk | `risk: high` → a second review with Opus and `/code-review`, zero blocking |

If a gate fails, the lead sends the findings back to the same named
builder (rework). **After 2 failed rounds**, the task is **escalated**,
which is a checkpoint.

The lead merges with `gh pr merge --squash`. Builders only push their own
branch and open a PR. They never merge, and they never push to `main`.

---

## 6. Checkpoints: the only times Henry is needed

### 6.1 Major checkpoints (blocking, the lead stops and waits)
| CP | When | Henry decides |
|---|---|---|
| **CP0** | After M0: skeleton, contracts v1, M1/M2 task specs | Is the foundation right before anything runs in parallel |
| **CP1** | After M4: baseline + eval harness | Baseline scores look sane. Go/no-go on the multi-agent design |
| **CP2** | After M8: policy engine + approval + executor | Sign off on the safety model before the system can act |
| **CP3** | Before M12: anything public, deployed, or costing money | Hosting, demo limits, repo visibility |

### 6.2 Always-stop triggers (any time)
- Breaking contract change, or a change in scope or architecture vs. SCOPE/ARCHITECTURE
- Anything outward-facing or irreversible: making the repo public, deploying,
  new accounts, paid services, secrets, force-push, deleting branches that
  have unmerged work
- A task escalated after 2 failed rework rounds
- Usage or rate limits blocking progress for more than 30 min
- A builder needs to edit lead-only paths for reasons the lead can't resolve

### 6.3 Everything else is async
At the end of each milestone that isn't a CP, the lead writes a summary
to PROGRESS.md (what shipped, numbers, anything odd) and **continues** to
the next milestone. Henry reads it whenever he likes.

At every stop, the lead sends a **push notification** (if enabled) and
writes a `## ⏸ Waiting on Henry` block at the top of PROGRESS.md: the
question, the options, and a recommendation.

---

## 7. The lead's loop

```
for milestone in plan:
    write/refresh task specs + BOARD (plan mode reasoning, no code)
    land the contracts this milestone needs (lead-only paths) → CI green
    while milestone has unmerged tasks:
        spawn ready tasks up to 3 in flight (builder subagents, worktrees)
        on builder "done": run gates G1–G6 → merge | rework | escalate
        on builder question: answer / additive contract change / checkpoint
        on merge: update BOARD, broadcast rebase to affected builders
    milestone summary → PROGRESS.md (+ metrics table §9)
    if milestone ends at a CP: stop, notify, wait
```

The lead itself writes almost no feature code. If it notices it's
implementing instead of coordinating, it hands that work off as a task.

---

## 8. Where agent teams fit (debate and red-team only)

Teams are turned on just for the session that needs one
(`CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` in `.claude/settings.local.json`),
and turned off again afterwards.

1. **ADR debates.** For example: "3 teammates: Temporal advocate,
   LangGraph advocate, devil's advocate. Try to disprove each other. Write
   the consensus to docs/adr/001."
2. **Red-team the safety model before M8/M10.** "3 teammates find ways an
   injected log line could cause an unapproved action. Challenge each
   other's attacks." The output becomes eval scenarios.
3. **One same-task comparison (M5):** two builders implement the same
   workflow spec in separate worktrees, and the lead keeps the better one.
   This is a one-time experiment and the result gets written up.

---

## 9. Measuring the process (for the website)

The lead adds one row per milestone to PROGRESS.md:

| Milestone | Tasks | Builders peak | PRs | First-pass merge % | Rework rounds | Escalations | Contract changes | Wall-clock |
|---|---|---|---|---|---|---|---|---|

---

## 10. Efficiency rules

1. Parallelize only across a real seam. Same file or a sequential
   dependency means the work stays in one task.
2. Specs before code. A builder starts with a clean context and a written
   definition of done.
3. Right-size models: Opus for lead and `risk: high` tasks, Sonnet for
   builders, Haiku for search subagents.
4. The reviewer looks for correctness and skips style, so code doesn't get
   pushed into over-abstraction.
5. Safe commands are pre-approved in `.claude/settings.json`, so builders
   don't stall on permission prompts.
6. `.worktreeinclude` covers `.env`, and install steps are in `CLAUDE.md`.
7. Check `/usage` weekly and cut agent types that don't pay off.

## 11. Anti-patterns

- Two tasks in flight on the same paths.
- Builders editing `firebreak/models/`.
- Decisions that only exist inside messages.
- Agent teams used for implementation (they share a checkout and overwrite each other).
- The lead writing features instead of coordinating.
- Spawning a builder for a 5-minute change. The lead just does those.

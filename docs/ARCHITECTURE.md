# Firebreak — Architecture

See [SCOPE.md](../SCOPE.md) for purpose, milestones and success criteria.
This doc covers **how** the system works. Sections are numbered so other
docs and code comments can cite them (e.g. "see ARCH §6.3").

---

## 1. System overview

```
 ┌──────────────────────── TARGET (Shopfloor) ─────────────────────────┐
 │  gateway ──► orders ──► inventory      Postgres   Redis               │
 │     ▲  chaos flags (/admin/chaos)        │ OTel / metrics / logs     │
 └─────┼────────────────────────────────────┼──────────────────────────┘
       │ traffic                              ▼
  Sentry Load          Prometheus · Alertmanager · Loki · Tempo · Grafana
  (load gen)                    │ webhook             ▲ read-only queries
                                ▼                      │
 ┌──────────────────────────── FIREBREAK ──────────────┼────────────────┐
 │                                                     │                │
 │  Ingest API (FastAPI) ─► Dedupe/Grouper ─► Postgres (incidents)       │
 │                                │ start workflow                      │
 │                                ▼                                     │
 │                    ┌──── Temporal Server ────┐                       │
 │                    │  IncidentWorkflow (§4)  │◄── signals: approve/   │
 │                    └──────────┬──────────────┘    reject/cancel       │
 │                               │ activities                           │
 │          ┌─────────────┬──────┴──────┬─────────────┬────────────┐    │
 │          ▼             ▼             ▼             ▼            ▼    │
 │       Triage       Specialists    Synthesizer    Critic     Remediator│
 │       agent      (metrics/logs/    agent         agent       agent   │
 │                   traces/deploy/                                     │
 │                   runbook)  ──── Tool Gateway (§6) ─────────────┘    │
 │                                   │ allowlist · policy · audit       │
 │                                   ▼                                  │
 │                 Adapters: PromQL · LogQL · TraceQL · GitHub ·        │
 │                           pgvector runbooks · Shopfloor Actions API  │
 │                                                                      │
 │  Evidence Store (Postgres) ◄── every tool result                     │
 │  Event bus (Postgres LISTEN/NOTIFY) ─► SSE ─► Next.js dashboard       │
 │  OTel traces + Prometheus metrics for the agents themselves (§10)    │
 └──────────────────────────────────────────────────────────────────────┘
```

**Processes:**

| Process | Role | Scales |
|---|---|---|
| `api` | FastAPI: webhooks, REST, SSE, approval endpoints | Stateless, N replicas |
| `orchestrator-worker` | Temporal worker that runs workflow code | N replicas, Temporal shards the work |
| `agent-worker` | Temporal worker that runs activities (LLM and tool calls) | N replicas, separate task queue so the slow LLM calls don't starve workflow tasks |
| `temporal` | Temporal server (+ its Postgres) | 1 in dev |
| `postgres` | App DB + pgvector | 1 |
| `dashboard` | Next.js | Vercel |
| `shopfloor-*` | Target services | Compose |

---

## 2. Domain model

```
Alert ──(n:1)──► Incident ──(1:n)──► Investigation ──(1:n)──► AgentRun ──(1:n)──► ToolCall
                                          │                                       │
                                          ├──► Hypothesis ◄──cites── Evidence ◄───┘
                                          ├──► ProposedAction ──► Approval ──► Execution
                                          └──► Postmortem
```

Core Pydantic schemas (abbreviated):

```python
class Alert(BaseModel):
    source: Literal["alertmanager", "sentry", "generic"]
    fingerprint: str              # stable per alert rule + labels
    service: str
    severity: Literal["info", "warning", "critical"]
    summary: str
    labels: dict[str, str]
    started_at: datetime

class Evidence(BaseModel):
    id: str                       # "ev_01H..." — what agents cite
    tool_call_id: str
    kind: Literal["metric_series", "log_lines", "trace", "deploy", "runbook", "config"]
    query: str                    # exact query run, reproducible
    summary: str                  # tool-produced, size-capped
    raw_ref: str                  # pointer to full payload in blob table
    untrusted: bool = True        # anything containing target-produced text

class Finding(BaseModel):         # specialist output
    claim: str
    evidence_ids: list[str]       # MUST be non-empty, MUST exist (validated)
    confidence: float             # 0–1, self-reported, calibrated by evals
    agent: str

class Hypothesis(BaseModel):      # synthesizer output
    root_cause_category: RootCauseCategory   # enum, gradeable
    explanation: str
    supporting_findings: list[str]
    contradicting_findings: list[str]
    confidence: float
    rank: int

class ProposedAction(BaseModel):
    action: ActionType            # enum: rollback, restart, scale, toggle_flag, revert_config, none
    target: str
    params: dict
    risk_tier: Literal["read", "reversible", "irreversible"]  # set by POLICY, not the model
    rationale: str
    hypothesis_id: str
    idempotency_key: str
```

**Design rule:** the categories the agents produce (`RootCauseCategory`,
`ActionType`) are **enums**, so evals can grade them deterministically. Free
text goes in the `explanation` fields and only an LLM judge grades it.

---

## 3. Ingest, dedupe, and grouping

1. `POST /webhooks/alertmanager` (and `/sentry`, `/generic`). Auth uses an
   HMAC signature or shared bearer token per source.
2. Normalize to `Alert` and persist. An **idempotency key** of
   `(source, fingerprint, started_at)` means a replayed webhook is a no-op.
3. **Grouping** (alert storms, scenario S10):
   - Open a *grouping window* of 60s, configurable, per
     `(environment, service-dependency-cluster)`.
   - Alerts on services that are linked in the service dependency graph
     (a static YAML for v1) join the same open incident.
   - The window is a Temporal **timer** inside the workflow. The first
     alert starts it, and later alerts arrive as `add_alert` **signals**
     to the existing workflow (signal-with-start). Grouping survives
     restarts for free.
4. Investigations start when the window closes, or right away for
   `critical`.

**Admission control:** the cap on concurrent investigations (default 3) is
set through Temporal task-queue rate limits. When it's full, incidents
queue by severity. Each incident has a cost budget (default $1), and the
workflow ends in `BUDGET_EXHAUSTED` with partial findings instead of
continuing to spend.

---

## 4. The orchestration: `IncidentWorkflow`

### 4.1 State machine

```
 GROUPING ─► TRIAGE ─► PLANNING ─► INVESTIGATING ─► SYNTHESIZING ─► CRITIQUING
                │                       ▲                               │
                │ (noise/dup)           └──── need_more_evidence ───────┤ (max 2 loops)
                ▼                                                       ▼
            DISMISSED                                             PROPOSING
                                                                       │
                                     ┌─────────── action == none ──────┤
                                     ▼                                 ▼
                              REPORT_ONLY                     AWAITING_APPROVAL ──timeout──► ESCALATED
                                                                 │        │
                                                          approve│        │reject
                                                                 ▼        ▼
                                                            EXECUTING   REPORT_ONLY
                                                                 │
                                                                 ▼
                                                        VERIFYING_RECOVERY ──fail──► PROPOSING (once)
                                                                 │ ok
                                                                 ▼
                                                            POSTMORTEM ─► RESOLVED
```

Each state transition emits an event (Postgres `NOTIFY`, which feeds SSE)
and is visible in Temporal's own history. That means two independent
audit trails.

### 4.2 Workflow pseudocode

```python
@workflow.defn
class IncidentWorkflow:
    @workflow.run
    async def run(self, incident_id: str) -> Outcome:
        await self._group_alerts()                                # timer + add_alert signals

        triage = await run_agent("triage", self.ctx, timeout=20s)
        if triage.verdict == "noise":
            return Outcome.DISMISSED

        plan = await run_agent("supervisor", self.ctx, triage)    # which specialists, which questions
        for round in range(MAX_ROUNDS):                           # MAX_ROUNDS = 3
            findings = await self._fan_out(plan.tasks)            # §4.3
            hyps = await run_agent("synthesizer", self.ctx, findings)
            review = await run_agent("critic", self.ctx, hyps)    # §5.4
            if review.verdict == "accept":
                break
            plan = review.follow_up_plan                          # targeted re-investigation

        action = await run_agent("remediator", self.ctx, hyps.top)
        decision = policy.evaluate(action, self.ctx)              # deterministic, NOT an LLM (§7)
        if decision.requires_approval:
            approved = await self._await_approval(timeout=15m)    # signal or timer
            if not approved:
                return await self._report_only()
        await execute(action)                                     # idempotent activity
        recovered = await self._verify_recovery(window=5m)        # poll the alert state + SLO metric
        ...
        await run_agent("postmortem", self.ctx)
```

`run_agent` is a thin wrapper that calls the `agent_step` **activity**
(§5). Temporal workflow code has to be deterministic, so every LLM or
network call happens inside activities and the workflow only holds the
decisions.

### 4.3 Parallel fan-out with partial failure

```python
async def _fan_out(self, tasks):
    handles = [
        workflow.start_activity(run_specialist, t,
            start_to_close_timeout=t.timeout,            # per-specialist, 30–90s
            retry_policy=RetryPolicy(maximum_attempts=3,
                non_retryable_error_types=["BudgetExceeded", "PolicyViolation"]))
        for t in tasks
    ]
    results = await asyncio.gather(*handles, return_exceptions=True)
    ok   = [r for r in results if not isinstance(r, Exception)]
    failed = [t.agent for t, r in zip(tasks, results) if isinstance(r, Exception)]
    self.ctx.degraded_sources = failed                    # surfaced in UI + lowers confidence
    if not ok:
        raise ApplicationError("all specialists failed")  # → ESCALATED
    return flatten(ok)
```

**Outcome:** if the logs backend is down (scenario M7), the metrics,
deploy and trace specialists still report. The synthesizer is told which
sources were unavailable, and the final report is marked **degraded**.
It doesn't fail silently and it doesn't hang.

### 4.4 Why the context passes between agents as a "blackboard" and not as transcripts

Specialists never see each other's raw transcripts. The workflow keeps a
compact **IncidentContext** (alert summary, triage result, service graph
slice, findings list with evidence IDs). Each agent gets only the slice it
needs. This keeps token cost roughly linear in the number of agents, and it
stops one agent's hallucination from becoming another agent's "fact".
Evidence is the only shared currency between agents.

---

## 5. Agent runtime

### 5.1 One loop, many configs

Every agent is data:

```yaml
# agents/metrics.yaml
name: metrics
model: claude-sonnet-5-5
system_prompt: prompts/metrics.md
tools: [promql_query, promql_range, list_metrics, get_service_slo]
max_steps: 8
max_output_tokens: 4096
budget_usd: 0.15
output_schema: SpecialistReport
```

The runtime (`firebreak/agents/runtime.py`) is a bounded tool-use loop:

```
build messages (system + task + ctx slice)
loop up to max_steps:
    response = llm.call(messages, tools=allowed_tools)    # retries/fallback in client
    record tokens + cost; abort if budget exceeded
    if response has tool_use blocks:
        for each (in parallel): result = tool_gateway.invoke(agent, tool, args)   # §6
        append wrapped results (§8)
    else:
        parse final output → output_schema (Pydantic); on validation error,
        feed error back once ("repair turn"), else fail
validate citations: every evidence_id exists AND belongs to this incident
return typed output
```

### 5.2 Agent roster

| Agent | Model tier | Tools | Output |
|---|---|---|---|
| **Triage** | Haiku | `get_alert`, `recent_incidents` | severity, verdict (`investigate`/`noise`/`duplicate`), affected services |
| **Supervisor** | Sonnet | `service_graph`, `list_specialists` | `Plan{tasks: [ {agent, question, timeout} ]}` |
| **Metrics** | Sonnet | PromQL instant/range, SLO lookup | `SpecialistReport` (findings) |
| **Logs** | Sonnet | LogQL query, pattern/cluster summary | `SpecialistReport` |
| **Traces** | Sonnet | TraceQL search, span breakdown | `SpecialistReport` |
| **Deploys** | Sonnet | recent deploys, diff summary, config history | `SpecialistReport` |
| **Runbook** | Haiku | pgvector search over runbooks + past postmortems | `SpecialistReport` |
| **Synthesizer** | Opus | none (reasoning only, reads findings) | ranked `Hypothesis[]` |
| **Critic** | Opus | `get_evidence` (re-read raw evidence) | `accept` / `reject` + follow-up plan |
| **Remediator** | Sonnet | `list_actions`, `action_preconditions` | `ProposedAction` |
| **Postmortem** | Sonnet | none | Markdown postmortem |

### 5.3 LLM client

- One wrapper around the Anthropic SDK that handles retries with jittered
  backoff on 429/5xx/overloaded, a fallback model per tier (Opus → Sonnet),
  prompt caching for the static system prompt and tool definitions, and
  token and cost accounting per call.
- A global token-bucket rate limiter (Redis or a Postgres advisory lock)
  stops concurrent investigations from tripping provider rate limits.
- **Record/replay mode** (`FIREBREAK_LLM_MODE=record|replay|live`):
  responses are keyed by a hash of (model, messages, tools) and stored as
  fixtures. Evals and CI run deterministically and for free in replay
  mode (§9).

### 5.4 Critic: the anti-hallucination step

The critic gets the hypotheses **and** can re-fetch the raw evidence they
cite. It checks that:
1. Every claim cites evidence that actually supports it (it re-reads the
   raw evidence, not the specialist's summary of it).
2. Timing makes sense (the cause starts before the symptom).
3. Contradicting findings are accounted for.

If the check fails, the critic returns a targeted follow-up plan
("deploy agent: check whether v1.4.2 touched `orders/db.py`"). This is
capped at 2 critique loops so it can't spin forever.

---

## 6. Tool Gateway

Every tool call goes through one chokepoint:

```
agent → ToolGateway.invoke(agent, tool, args)
          1. allowlist check      (agent config)          → PolicyViolation
          2. schema validation    (Pydantic args)         → repair turn
          3. classification       read | write            → writes are NEVER callable by agents;
                                                             only the Executor activity can run them
          4. rate limit + timeout per tool
          5. execute adapter
          6. size-cap + summarize result; store full payload as Evidence
          7. wrap output as untrusted (§8)
          8. audit log row + OTel span
```

Adapters implement a small interface so the observability backend can be
swapped later (Datadog, CloudWatch) without touching the agents:

```python
class MetricsAdapter(Protocol):
    async def instant(self, query: str, at: datetime) -> MetricResult: ...
    async def range(self, query: str, start: datetime, end: datetime, step: str) -> MetricResult: ...
```

**Agents never get a generic "run shell" or "HTTP request" tool.** Every
tool is narrow and typed.

---

## 7. Safety model: policy engine, approvals, execution

### 7.1 Policy engine (deterministic, no LLM)

`policies.yaml` is evaluated in plain Python:

```yaml
- action: restart
  risk_tier: reversible
  approval: required          # v1: everything requires approval
  preconditions: [service_in_incident_scope]
- action: rollback
  risk_tier: reversible
  approval: required
  preconditions: [service_in_incident_scope, previous_version_exists, no_pending_migration]
- action: scale
  risk_tier: reversible
  approval: required
  limits: {max_replicas: 5}
- action: "*"
  target_not_in_incident_scope: deny     # injection defense (S9)
```

The **policy engine sets the risk tier**, not the model. If the model
claims an action is "low risk", that has no effect.

### 7.2 Approval as a durable signal

- The workflow waits on `workflow.wait_condition(lambda: self.decision, timeout=15m)`.
- Approve or reject comes in through `POST /incidents/{id}/approval` (the
  dashboard, later Slack). The API authenticates the user and sends a
  Temporal **signal** with `{approver, decision, reason}`.
- On timeout the incident becomes **ESCALATED** (notify, then report only).
  Nothing ever runs by default.
- Because it's a Temporal signal, an approval can arrive hours later,
  after deploys and restarts, and the workflow continues where it left
  off. This is the main thing durable execution gives the project.

### 7.3 Executor

- A separate activity using **a different credential** from the read
  adapters, so agents physically can't reach it.
- It's idempotent: Shopfloor's Actions API takes an `idempotency_key`, so
  Temporal retries can't roll back twice.
- After execution, `VERIFYING_RECOVERY` polls the alert state and the SLO
  metric for 5 minutes. If it hasn't recovered, the workflow goes back to
  PROPOSING once with the new evidence. If that fails too, the incident is
  ESCALATED.

---

## 8. Prompt-injection defense

Threat: log lines, error messages, commit messages and runbook text are
written by the target system or by users. An attacker who can write a log
line can try to control the agents (scenario S9).

Defenses are layered and none depends on the model behaving:
1. **Structural:** agents have no write tools (§6.3). The worst an
   injected agent can do is say something wrong.
2. **Policy:** actions are limited to the incident's service scope and to
   enum action types, and every action needs human approval (§7).
3. **Wrapping:** tool output is delivered inside a clearly delimited
   untrusted-data block, and system prompts state that content inside it
   is data and never instructions.
4. **Detection:** a cheap classifier pass (Haiku or regex heuristics) flags
   suspicious content in evidence. It shows up as a red badge in the UI
   and the critic receives it.
5. **Eval:** S9-style red-team scenarios run in CI. The metric is
   "policy bypasses = 0", plus "investigation still reaches the right root
   cause" as a secondary metric.

---

## 9. Evaluation harness

The eval harness is what turns this from a demo into a measurable system.

```
evals/scenarios/s01_bad_deploy.yaml
  setup:    { chaos: {orders.bug_rate: 0.1}, deploy: orders@v1.4.2-bad }
  load:     { sentry_load_preset: "steady-50rps", duration: 5m }
  expect:
    alert: OrdersHighErrorRate
    root_cause_category: bad_deploy
    must_cite_kinds: [deploy, metric_series]
    action: rollback
    action_target: orders
  teardown: { chaos: reset }
```

**Two modes:**

| Mode | What runs | Cost | When |
|---|---|---|---|
| **Live** | Real Shopfloor, real fault, real load, real LLM | $ and ~5 min per scenario | Nightly, before releases |
| **Replay** | Recorded tool outputs + recorded LLM responses | $0, seconds | Every PR in CI |

Changing a prompt invalidates the replay fixtures for that agent. The
harness flags them as stale and they need a live re-record. That way prompt
changes can't pass CI on old recordings without anyone noticing.

**Graders:**
- Deterministic: root-cause category (top-1, top-3), action type and
  target, citations exist, policy violations = 0, latency, cost.
- LLM-as-judge (Opus, with a rubric): explanation quality and
  postmortem quality. This is reported separately and never gates CI on
  its own.

**Comparisons reported in `evals/REPORT.md` (generated):**
- Single-agent baseline (M3) vs. multi-agent.
- With and without the critic.
- Model-tier ablations (all-Sonnet vs. tiered).

These tables go on the website.

---

## 10. Observability of Firebreak itself

- **Traces:** OTel span per workflow → agent run → LLM call / tool call,
  exported to the same Tempo instance. One trace shows the whole
  investigation tree.
- **Metrics (Prometheus):** `firebreak_investigation_duration_seconds`,
  `firebreak_llm_tokens_total{agent,model,type}`,
  `firebreak_llm_cost_usd_total{agent}`,
  `firebreak_tool_calls_total{tool,status}`,
  `firebreak_specialist_failures_total{agent,reason}`,
  `firebreak_approvals_total{decision}`.
- **Grafana dashboard** committed as JSON (same pattern as Sentry Load's
  `ops/grafana-dashboard.json`).
- Optional: Langfuse for prompt and response inspection. OTel is the
  source of truth.

---

## 11. Live dashboard (Next.js)

- **Incident list** with state and severity.
- **Investigation view:**
  - An agent graph (React Flow) where nodes light up as agents start,
    finish or fail, and edges show the flow of evidence.
  - A timeline of events streamed over SSE.
  - Hypotheses with confidence bars. Clicking a citation opens the raw
    evidence (query + chart/log lines).
  - The degraded-source and injection-flag badges.
- **Approval panel:** shows the proposed action, the policy decision,
  preconditions, and approve or reject buttons with a reason.
- **Demo mode (M12):** a "Break something" button with a scenario picker.
  It requires GitHub login and is rate-limited.

---

## 12. API surface

| Method | Path | Purpose |
|---|---|---|
| POST | `/webhooks/{source}` | Alert ingest (HMAC) |
| GET | `/incidents` | List |
| GET | `/incidents/{id}` | Full state: hypotheses, evidence, actions |
| GET | `/incidents/{id}/events` | SSE stream |
| POST | `/incidents/{id}/approval` | Approve or reject (authenticated) |
| POST | `/incidents/{id}/cancel` | Cancel workflow |
| GET | `/evidence/{id}` | Raw evidence payload |
| POST | `/demo/scenarios/{name}` | Trigger a chaos scenario (demo mode) |
| GET | `/evals/runs`, `/evals/runs/{id}` | Eval results |

---

## 13. Data storage

Postgres tables: `alerts`, `incidents`, `investigations`, `agent_runs`,
`tool_calls`, `evidence`, `evidence_blobs`, `findings`, `hypotheses`,
`proposed_actions`, `approvals`, `executions`, `postmortems`,
`runbook_chunks` (pgvector), `eval_runs`, `eval_results`, `audit_log`
(append-only and never updated, enforced with a trigger).

Migrations use Alembic.

---

## 14. Repo layout

```
firebreak/
├── SCOPE.md
├── docs/            ARCHITECTURE.md · PROGRESS.md · adr/
├── firebreak/
│   ├── api/         FastAPI app, webhooks, SSE, approvals
│   ├── ingest/      normalize, dedupe, grouping
│   ├── workflows/   IncidentWorkflow + child workflows (deterministic code only)
│   ├── activities/  agent_step, run_specialist, execute_action, verify_recovery
│   ├── agents/      runtime.py, configs/*.yaml, prompts/*.md
│   ├── llm/         client, rate limiter, cost table, record/replay
│   ├── tools/       gateway.py, registry, adapters/{prometheus,loki,tempo,github,runbooks,shopfloor}
│   ├── policy/      engine.py, policies.yaml
│   ├── models/      Pydantic schemas + SQLAlchemy models
│   └── telemetry/   OTel + Prometheus setup
├── shopfloor/       target services (gateway, orders, inventory) + chaos admin API
├── evals/           scenarios/*.yaml, harness, graders, fixtures/, REPORT.md
├── dashboard/       Next.js
├── ops/             prometheus.yml, alert rules, loki/tempo config, grafana json
├── docker-compose.yml
└── Makefile         up · down · test · eval · eval-live · chaos S=<id>
```

---

## 15. Deployment

- **Dev:** `docker compose up` brings up everything (Temporal dev server,
  Postgres, the observability stack, Shopfloor, api and workers).
- **Prod (M12):**
  - Backend, Temporal and Shopfloor run on a VM. The Oracle Always Free
    ARM VM is the candidate, but it may be too small for everything (see
    SCOPE open question 1). The fallback is to split the observability
    stack onto a second free VM, or use Grafana Cloud's free tier for
    Prometheus, Loki and Tempo the way Sentry Load uses it.
  - The dashboard runs on Vercel.
  - Secrets come from env and are never committed. The executor's
    credential is separate from the read credentials.
- **CI (GitHub Actions):** ruff, mypy, pytest, replay evals with a
  results diff posted as a PR comment, and the dashboard build.

---

## 16. Architecture decision records

**ADR-001: Temporal over LangGraph and a hand-rolled engine.**
- *LangGraph:* good graph ergonomics and checkpointing, but durability,
  timers, signals and worker scaling are thinner, and the skill is
  framework-specific.
- *Hand-rolled on Postgres:* best for learning, but rebuilding retries,
  timers, signals and replay is a project in itself.
- *Temporal:* gives industry-standard durable execution (used at Uber,
  Netflix and Stripe, and by OpenAI for Codex). It provides signals for
  human approval, replay for debugging, and a reason to explain
  workflow-vs-activity determinism in interviews.
- *Cost:* operational weight (one more server) and determinism rules in
  workflow code.
- *Accepted.*

**ADR-002: Hand-written agent loop, no agent framework.** The loop is
small, and owning it makes the budget, citation and gateway enforcement
explicit and testable. A framework would hide exactly the parts this
project is supposed to show.

**ADR-003: Evidence-cited findings as the only data shared between agents.**
This stops hallucinations from compounding, makes the critic possible,
and makes the UI explainable. The cost is extra tokens for evidence
summaries and stricter schemas.

**ADR-004: The policy engine is code, not an LLM.** Safety decisions have
to be deterministic and testable, and immune to prompt injection.

**ADR-005: Evals before multi-agent.** The single-agent baseline and the
harness come first (M3/M4), so every architecture claim is measured.

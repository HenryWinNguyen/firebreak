# Contracts (v1.8)

The seams between tracks. **Lead-only.** Builders read this and must not edit
it or `firebreak/models/`. To change something, message `lead` with the
proposed diff (AGENT_WORKFLOW §4). Additive changes land fast. Breaking ones
are a checkpoint.

| Version | Date | Change |
|---|---|---|
| v1 | 2026-10-27 | Initial: models, Shopfloor API/metrics/logs/alerts, webhook, ports |
| v1.2 | 2026-10-27 | `GroupingDecision.incident_id` is `str \| None` (T-021 request); resolved-alert write path (T-025/T-027) |
| v1.5 | 2026-10-29 | Additive: Shopfloor metric `db_queries_total{service}` (T-056) |
| v1.1 | 2026-10-27 | Additive (T-032 requests): `AgentRunResult.error`, `LLMRequest.fallback_model`; `Evidence.raw_ref` ownership rule |
| v1.6 | 2026-11-14 | Additive (M5): `orchestration` module (workflow ↔ activity types, task queues, activity names); Temporal at 7233 |
| v1.7 | 2026-11-14 | Additive (M5, T-066): `IncidentContext.window_closes_at`, `AlertAdded`, signal `SIGNAL_ALERT_ADDED` |
| v1.8 | 2026-11-14 | `SPECIALIST_AGENTS`, `WorkflowOutcome.dropped_tasks` (additive); `SpecialistTask.timeout_s` default 300→1800, max 900→3600 (local model, serial Ollama) |

---

## 1. Python contracts (`firebreak/models/`)

| Module | Types | Producer → consumer |
|---|---|---|
| `common` | `new_id`, `utcnow`, `Severity` | everyone |
| `alert` | `Alert`, `AlertSource`, `AlertStatus`, `AlertmanagerWebhook`, `AlertmanagerAlert` | ingest (T-021) → grouper (T-023), DB (T-022) |
| `incident` | `Incident`, `IncidentState`, `TERMINAL_STATES`, `GroupingDecision`, `IncidentGrouper` (Protocol) | grouper (T-023) ← ingest (T-021) |
| `evidence` | `Evidence`, `EvidenceKind`, `Finding`, `SpecialistReport`, `EvidenceSink` (Protocol) | tool gateway (T-032) → DB/core |
| `reasoning` | `Hypothesis`, `RootCauseCategory`, `InvestigationReport`, `finding_ref`, `ProposedAction`, `ActionType`, `RiskTier` | synthesizer/remediator → policy, evals |
| `llm` | `LLMClient` (Protocol), `LLMRequest`, `LLMResponse`, `ToolSpec`, `ToolAccess`, `Usage` | llm (T-031) → agent runtime (T-032) |
| `agents` | `AgentConfig`, `ToolCallRecord`, `AgentRunResult` | agent runtime (T-032) → workflows |
| `orchestration` (v1.6) | `TriageVerdict`, `TriageResult`, `SpecialistTask`, `Plan`, `IncidentContext`, `AgentTask`, `StateUpdate`, `WorkflowOutcome`, `AlertAdded` (v1.7), `SPECIALIST_AGENTS` (v1.8), task-queue, activity- and signal-name constants | workflow (core, T-064) ↔ activities (agents, T-065) |

Rules:
- Ids are prefixed (`al_`, `inc_`, `ev_`, `hy_`, `act_`) and made with `new_id`.
- All datetimes are timezone-aware UTC.
- `ProposedAction.risk_tier` is set only by the policy engine.
- `Evidence.raw_ref`: the tool gateway proposes the key (`evidence/<incident_id>/<evidence_id>`). The `EvidenceSink` may replace it, and the `Evidence` returned by `record` is authoritative.
- `LLMRequest.fallback_model`: the only fallback the client uses (v1.3: no built-in fallback map). Unset → no fallback. Each call is priced at the model that call requested (the fallback call requests the fallback model).
- `Finding.evidence_ids` must be non-empty. The runtime also checks they exist with `EvidenceSink.exists`.
- `common.deterministic(seed, at=...)` (v1.4): seeds `new_id` and freezes `utcnow` for the current context. Run-path code must get ids from `new_id` and time from `utcnow()` (never `uuid4`/`datetime.now` directly), or replay breaks.
- Orchestration (v1.6): `IncidentWorkflow` (workflow id `incident-<incident_id>`, queue `ORCHESTRATOR_TASK_QUEUE`) calls only the activities named by the `ACTIVITY_*` constants, on `AGENT_TASK_QUEUE`. `run_agent` returns `AgentRunResult`; the workflow validates `output` as the agent's schema (`TriageResult` for `triage`, `Plan` for `supervisor`, `SpecialistReport` for specialists, `InvestigationReport` for `synthesizer`). A run that ends without valid output is a result (`stopped_reason`/`error`), not an activity failure; infrastructure errors raise. Clients and workers use `pydantic_data_converter`. Address from `FIREBREAK_TEMPORAL_ADDRESS` (default `localhost:7233`), namespace `default`.
- Plans (v1.8): the workflow runs only tasks whose `agent` is in `SPECIALIST_AGENTS`; others go to `WorkflowOutcome.dropped_tasks` (logged), and a plan with none left is `ESCALATED`.
- Grouping (v1.7): ingest sends `SIGNAL_ALERT_ADDED` with signal-with-start for every firing, non-duplicate alert it groups (workflow id `incident-<incident_id>`). The workflow waits in `GROUPING` until `IncidentContext.window_closes_at` (a Temporal timer; `None` or past = no wait) or a `critical` `AlertAdded`, then reloads the context (`load_context` again, so alerts that joined during the window are seen) and triages. Ingest also re-signals duplicates (idempotent; repairs a failed signal on webhook retry). Alerts signalled after the window are logged and ignored (the grouper puts later alerts in a new incident). This replaces ARCH §3's "timer in the workflow is the window": the Postgres grouper stays the source of truth for membership, the workflow only waits.
- `InvestigationReport` (v1.3): `Hypothesis.supporting_findings`/`contradicting_findings` are refs `f<index>` into the report's own `findings`; ranks are exactly `1..n`, at most 3 hypotheses. The single-agent baseline (M3) returns it; the synthesizer (M5) will too.

## 2. Shopfloor (target system)

### 2.1 Services and ports
| Service | Port | Calls |
|---|---|---|
| `gateway` | 8100 | orders |
| `orders` | 8101 | inventory, postgres, redis |
| `inventory` | 8102 | postgres |

Business endpoints: `GET /api/products` and `POST /api/orders` on the gateway.
The gateway calls `POST /orders` on orders, which calls `POST /reserve` on inventory.
If the order insert fails or the request is cancelled after a reservation, orders calls
`POST /release` on inventory with the same `{product_id, quantity}` (compensating release).
`quantity` is 1–20 on every order and reserve endpoint; above 20 → 422.

### 2.2 Endpoints every service exposes
| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | `{"status":"ok"}` |
| GET | `/metrics` | Prometheus exposition |
| GET | `/admin/chaos` | Current flag values |
| POST | `/admin/chaos` | `{"flag": "<name>", "value": <json>}` → 200 with the new state; unknown flag → 422 |
| POST | `/admin/chaos/reset` | All flags back to default |
| GET | `/admin/deploys` | `[{"service","version","deployed_at","message"}]`, newest first |
| POST | `/admin/deploy` | `{"version","message"}`. Records a deploy. Version `*-bad` turns on that service's bug path |

The admin endpoints need the header `Authorization: Bearer $SHOPFLOOR_ADMIN_TOKEN`.

### 2.3 Chaos flags (scenario → flag)
| Scenario | Flag | Type | Default |
|---|---|---|---|
| S1 bad deploy | (deploy `orders` version ending `-bad`) → `orders.error_rate` = 0.1 | float 0–1 | 0.0 |
| S2 pool exhaustion | `orders.db_pool_size` | int | 20 |
| S3 N+1 query | `orders.n_plus_one` | bool | false |
| S4 dependency down | `inventory.unavailable` (all routes return 503) | bool | false |
| S5 memory leak | `orders.leak_kb_per_request` | int | 0 |
| S6 misconfig | `orders.redis_host` | str | `redis` |
| S7 traffic spike | (load only, no flag) | | |

### 2.4 Metrics (names are fixed, alert rules depend on them)
| Metric | Type | Labels |
|---|---|---|
| `http_requests_total` | counter | `service, route, method, status` |
| `http_request_duration_seconds` | histogram | `service, route` |
| `db_pool_in_use` | gauge | `service` |
| `db_pool_size` | gauge | `service` |
| `db_queries_total` | counter | `service` (one per DB round trip; v1.5, T-056) |
| `app_info` | gauge (=1) | `service, version` |
| `process_resident_memory_bytes` | gauge | (prometheus_client default) |

`route` is the route template (`/orders`), never the raw path.

### 2.5 Logs
One JSON object per line on stdout:
`{"ts","level","service","msg","route","status","latency_ms","trace_id","error"}`.
Fields that don't apply are omitted, never set to null.

### 2.6 Service graph
`config/service_graph.yaml` (lead-only) defines the clusters and edges used for grouping.

## 3. Observability → Firebreak

### 3.1 Alert rules (`ops/`)
Every rule sets the labels `alertname`, `service`, `severity` (`warning`|`critical`) and `env="demo"`, plus a `summary` annotation.

| alertname | Condition (sketch) | for | severity |
|---|---|---|---|
| `HighErrorRate` | 5xx ratio > 5% per service | 2m | critical |
| `HighLatencyP95` | p95 > 1s per service | 2m | warning |
| `ServiceDown` | `up == 0` | 1m | critical |
| `DBPoolSaturated` | in_use / size > 0.9 | 2m | warning |
| `HighMemory` | RSS > 400MB | 2m | warning |

### 3.2 Webhook
Alertmanager → `POST http://api:8000/webhooks/alertmanager` with header
`Authorization: Bearer $FIREBREAK_WEBHOOK_TOKEN` and the payload `AlertmanagerWebhook`.
`group_wait: 10s`, `group_interval: 30s`.

### 3.3 Ports
| Component | Port |
|---|---|
| Firebreak API | 8000 |
| Postgres | 5432 |
| Prometheus | 9090 |
| Alertmanager | 9093 |
| Loki | 3100 |
| Grafana | 3000 |
| Temporal gRPC / UI | 7233 / 8233 |

### 3.4 Read tools for agents (v1.3)
Registered by `firebreak/tools/read_tools.py`; adapters in `firebreak/tools/adapters/`. All are
`ToolAccess.READ`, all go through the gateway, and every result is recorded as `Evidence`.

| Tool | Args | Backend | EvidenceKind |
|---|---|---|---|
| `promql_query` | `query`, optional `at` (aware UTC) | Prometheus `/api/v1/query` | `metric_series` |
| `promql_range` | `query`, `minutes_back` (1–120), `step_seconds` (5–600) | Prometheus `/api/v1/query_range` | `metric_series` |
| `list_metrics` | optional `match` (metric-name prefix) | Prometheus `/api/v1/label/__name__/values` | `metric_series` |
| `logql_query` | `query`, `minutes_back` (1–120), `limit` (1–500) | Loki `/loki/api/v1/query_range` | `log_lines` |
| `recent_deploys` | `service` (gateway/orders/inventory), `limit` (1–20) | Shopfloor `GET /admin/deploys` | `deploy` |
| `service_graph` | none | `config/service_graph.yaml` | `config` |

**Never exposed to agents:** `/admin/chaos` (it would leak the injected fault, i.e. the eval
ground truth), any POST admin endpoint, Alertmanager silences. Adapter settings use the
`FIREBREAK_TOOLS_` prefix and live in `firebreak/tools/` (CONTRACTS §4).

## 4. Ownership of shared files
| Path | Owner |
|---|---|
| `firebreak/models/`, `firebreak/config.py`, `config/`, `docs/` (except PROGRESS entries), `CLAUDE.md`, `.claude/`, `.github/`, `pyproject.toml`, `uv.lock`, `docker-compose.yml`, `Makefile` | lead |
| Track-specific settings | each package's own `settings.py` (e.g. `firebreak/llm/settings.py`) |
| New dependency needed | message `lead`. The lead adds it to `pyproject.toml` |

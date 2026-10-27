# Contracts (v1)

The seams between tracks. **Lead-only.** Builders read this and must not edit
it or `firebreak/models/`. To change something, message `lead` with the
proposed diff (AGENT_WORKFLOW §4). Additive changes land fast. Breaking ones
are a checkpoint.

| Version | Date | Change |
|---|---|---|
| v1 | 2026-10-27 | Initial: models, Shopfloor API/metrics/logs/alerts, webhook, ports |
| v1.1 | 2026-10-27 | Additive (T-032 requests): `AgentRunResult.error`, `LLMRequest.fallback_model`; `Evidence.raw_ref` ownership rule |

---

## 1. Python contracts (`firebreak/models/`)

| Module | Types | Producer → consumer |
|---|---|---|
| `common` | `new_id`, `utcnow`, `Severity` | everyone |
| `alert` | `Alert`, `AlertSource`, `AlertStatus`, `AlertmanagerWebhook`, `AlertmanagerAlert` | ingest (T-021) → grouper (T-023), DB (T-022) |
| `incident` | `Incident`, `IncidentState`, `TERMINAL_STATES`, `GroupingDecision`, `IncidentGrouper` (Protocol) | grouper (T-023) ← ingest (T-021) |
| `evidence` | `Evidence`, `EvidenceKind`, `Finding`, `SpecialistReport`, `EvidenceSink` (Protocol) | tool gateway (T-032) → DB/core |
| `reasoning` | `Hypothesis`, `RootCauseCategory`, `ProposedAction`, `ActionType`, `RiskTier` | synthesizer/remediator → policy, evals |
| `llm` | `LLMClient` (Protocol), `LLMRequest`, `LLMResponse`, `ToolSpec`, `ToolAccess`, `Usage` | llm (T-031) → agent runtime (T-032) |
| `agents` | `AgentConfig`, `ToolCallRecord`, `AgentRunResult` | agent runtime (T-032) → workflows |

Rules:
- Ids are prefixed (`al_`, `inc_`, `ev_`, `hy_`, `act_`) and made with `new_id`.
- All datetimes are timezone-aware UTC.
- `ProposedAction.risk_tier` is set only by the policy engine.
- `Evidence.raw_ref`: the tool gateway proposes the key (`evidence/<incident_id>/<evidence_id>`). The `EvidenceSink` may replace it, and the `Evidence` returned by `record` is authoritative.
- `LLMRequest.fallback_model`: the client uses it, when set, in preference to any constructor-level fallback map.
- `Finding.evidence_ids` must be non-empty. The runtime also checks they exist with `EvidenceSink.exists`.

## 2. Shopfloor (target system)

### 2.1 Services and ports
| Service | Port | Calls |
|---|---|---|
| `gateway` | 8100 | orders |
| `orders` | 8101 | inventory, postgres, redis |
| `inventory` | 8102 | postgres |

Business endpoints: `GET /api/products` and `POST /api/orders` on the gateway.
The gateway calls `POST /orders` on orders, which calls `POST /reserve` on inventory.

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

## 4. Ownership of shared files
| Path | Owner |
|---|---|
| `firebreak/models/`, `firebreak/config.py`, `config/`, `docs/` (except PROGRESS entries), `CLAUDE.md`, `.claude/`, `.github/`, `pyproject.toml`, `uv.lock`, `docker-compose.yml`, `Makefile` | lead |
| Track-specific settings | each package's own `settings.py` (e.g. `firebreak/llm/settings.py`) |
| New dependency needed | message `lead`. The lead adds it to `pyproject.toml` |

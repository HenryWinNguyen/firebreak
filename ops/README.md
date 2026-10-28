# ops: observability stack (T-013)

Prometheus, Alertmanager, Loki (+ Promtail), and Grafana. Started by the root
`docker compose up -d` (or `make up`), which includes `ops/compose.yml`.

## UIs

| Component | URL | Notes |
|---|---|---|
| Prometheus | http://localhost:9090 | Status > Targets, Alerts |
| Alertmanager | http://localhost:9093 | |
| Loki | http://localhost:3100/ready | API only; query it from Grafana |
| Grafana | http://localhost:3000 | anonymous admin, no login. Dashboard: "Shopfloor overview" |

Prometheus scrapes `gateway:8100`, `orders:8101`, `inventory:8102`. They show as
down until the Shopfloor services (T-011) are running.

## Environment

`FIREBREAK_WEBHOOK_TOKEN` (root `.env`, passed through compose) is the bearer
token Alertmanager sends to `http://api:8000/webhooks/alertmanager`. Compose
fails to start if it is unset. Alertmanager can't expand env vars in its config,
so the container entrypoint writes it to `/tmp/webhook_token` and the config
uses `credentials_file`. The entrypoint runs `umask 077` first, so the token
file is mode 0600 (owner-only).

## Logs

Promtail reads Docker container stdout via the Docker socket and labels each
stream with `service` (the compose service name). In Grafana > Explore > Loki:
`{service="orders"}`.

Scrape jobs set `honor_labels: true` because the services already export a
`service` label; without it Prometheus renames it to `exported_service` and that
label leaks into alerts.

## Testing alert rules

Rules live in `prometheus/rules/shopfloor.yml`, tests in `prometheus/tests/`. From the repo root:

```bash
docker run --rm -v "$PWD/ops:/ops" --entrypoint promtool prom/prometheus:v3.5.0 check config /ops/prometheus/prometheus.yml
docker run --rm -v "$PWD/ops:/ops" --entrypoint promtool prom/prometheus:v3.5.0 test rules /ops/prometheus/tests/*.yml
```

To add a rule: edit the rules file, add a case with `input_series` and
`alert_rule_test` to `prometheus/tests/shopfloor_rules.yml`, and rerun the second command.

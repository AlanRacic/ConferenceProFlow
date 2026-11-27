# M4.3D1 — Prometheus + Grafana (local metrics backend)

## Purpose

This increment adds **only infrastructure and provisioning**. It does **not** overwrite any existing M4.3 C# metric instrumentation, Jaeger configuration, application Compose files or database volumes. Jaeger remains the tracing backend; Prometheus stores metrics received through its **OTLP/HTTP receiver**, and Grafana displays them.

## Architecture

```text
Registration API ─┐
Allocation Worker ├─ OTLP/HTTP POST ──> 127.0.0.1:9090/api/v1/otlp/v1/metrics
Analytics Worker ─┘                         │
                                           Prometheus ──(Docker network)──> Grafana :3000
Registration / Allocation / Analytics traces ───────────> Jaeger :4318 (independent)
```

All .NET services currently run on the Windows host, so their OTLP endpoint is `http://127.0.0.1:9090/api/v1/otlp/v1/metrics`. **Do not** use `http://prometheus:9090` in a Windows-hosted app: that DNS name works only from a container on the Compose network. Grafana provisioning deliberately uses `http://prometheus:9090` because Grafana is containerized.

## Prerequisites

- Docker Desktop in Linux container mode.
- Repository root: `C:\work\ConferenceProFlow`.
- Current branch: `feature/metrics-and-dashboards`.
- Port 9090 and port 3000 available on Windows loopback.
- No additional cloud accounts, paid services or package installation.

## Start (Visual Studio Developer PowerShell, one command per line)

```powershell
docker compose -f infrastructure/compose.metrics.yaml config
docker compose -f infrastructure/compose.metrics.yaml up -d
docker compose -f infrastructure/compose.metrics.yaml ps
```

First run downloads two pinned, official images (`prom/prometheus:v3.16.0`, `grafana/grafana:12.4.16`). Prometheus and Grafana have **separate Compose project and volumes** from the business infrastructure and Jaeger.

## Validate monitoring backend before any .NET app starts

1. Open `http://127.0.0.1:9090/-/ready` → expect **Prometheus Server is Ready.**
2. Open `http://127.0.0.1:9090/api/v1/query?query=up` → JSON should show `up{job="prometheus"}=1`. This confirms **self-scraping**, not application OTLP ingestion.
3. Open `http://127.0.0.1:3000` → Grafana login. On a *new* Grafana volume the default user/password are **admin/admin**; change the password when prompted. Existing volume means the previous password is retained. Never commit the chosen password.
4. Grafana **Connections → Data sources → Prometheus** should show the provisioned URL `http://prometheus:9090`; **Save & test** (if available) should succeed.
5. Grafana **Dashboards → ConferenceProFlow → ConferenceProFlow | M4.3 Metrics Overview** should exist automatically. Its Prometheus self-check shows **1**. Other panels can show **No data** until application metrics are enabled and generated.

If the Prometheus panel has **1** but business panels are empty at this stage, **this is expected**; M4.3D2 verifies applications and actual metric ingestion.

## Enable .NET application metric export (M4.3D2 validation)

The existing `RegistrationMetricsConfiguration`, `AllocationMetricsConfiguration`, and `AnalyticsMetricsConfiguration` already support **Development-only opt-in**, with these environment variables set *in each application process before startup*:

```powershell
$env:Observability__Metrics__Enabled='true'
$env:Observability__Metrics__Otlp__Enabled='true'
$env:Observability__Metrics__Otlp__Endpoint='http://127.0.0.1:9090/api/v1/otlp/v1/metrics'
```

Do **not** blindly start all three workers from one terminal without preserving the existing RabbitMQ/Kafka and SQL configuration. M4.3D2 will give the full verified startup sequence, including the already-tested end-to-end scenario. Existing Jaeger trace export remains independently controlled by `Observability__Otlp__Enabled`.

**Metric naming:** Prometheus' OTLP translation normally converts dots to underscores and adds conventional suffixes (e.g. Counter → `_total`, duration histogram → `_seconds_bucket`). Actual names will be checked against Prometheus in M4.3D2 before asserting that business dashboard queries are correct.

## Stop (without deleting data)

```powershell
docker compose -f infrastructure/compose.metrics.yaml stop
```

To remove just the monitoring containers and network while **retaining named volumes**, use:

```powershell
docker compose -f infrastructure/compose.metrics.yaml down
```

Do **not** run `down -v`, `docker system prune`, or manually delete named volumes unless you explicitly intend to erase stored monitoring data.

## Security and scope

- Both web interfaces and the **unauthenticated** OTLP receiver are bound to `127.0.0.1`; never publish this compose unchanged to an external server.
- Change Grafana's initial admin password on first login; the Compose file contains no credentials.
- No personally identifiable values or message IDs should be used in metric labels.
- Prometheus stores seven days of local samples; this is a reproducible **development/demo** setup, not a production deployment.
- The Grafana dashboard is provisioned from JSON in version control. Changes made through the UI are not the source of truth.
- No service/process instrumentation is changed in this D1 increment.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Compose YAML error | Verify you copied the whole `infrastructure/metrics/` directory; rerun `docker compose ... config`. |
| Prometheus not ready | `docker compose -f infrastructure/compose.metrics.yaml logs --tail 80 prometheus`. |
| Grafana unavailable | `docker compose -f infrastructure/compose.metrics.yaml logs --tail 80 grafana`. |
| Port conflict | `docker ps` and Windows port usage; do not overwrite running unrelated services. |
| Grafana datasource fails | Datasource URL must be `http://prometheus:9090` inside Docker, **not** `127.0.0.1:9090`. |
| Prometheus self-scrape works, business panels empty | Expected before applications enable opt-in export and emit measurements. |

## Verification scope

This package has been checked for YAML/JSON syntax and path consistency in the preparation environment. **Docker containers and live ingestion cannot be tested here**; validate them on your development machine. Integration tests stay in separately executed projects with xUnit `--parallel none`; no new integration-test work is required to validate infrastructure startup.

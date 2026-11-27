# M4.3D2 — Application metrics export and end-to-end verification

**Status:** Manual runbook — record actual results before marking as passed.  
**Branch:** `feature/metrics-and-dashboards`  
**Scope:** Registration API, Allocation Worker, Analytics Worker → OpenTelemetry Metrics → local Prometheus OTLP/HTTP receiver → Grafana.  
**Dependencies:** M4.3A–C3C metric instrumentation and M4.3D1 Prometheus/Grafana configuration.  
**Code changes in this increment:** None. No EF migrations, new packages or database resets.

## Purpose and invariants

Unlike Jaeger traces, metrics describe aggregate behavior. Verify both that individual .NET processes actually export metrics and that a fresh Registration → Allocation → Analytics business flow still completes. Operational counters are **not** an exactly-once business ledger; retries and process restarts can affect recorded values. The SQL databases are authoritative for business outcomes.

- Prometheus native OTLP/HTTP receiver: `http://127.0.0.1:9090/api/v1/otlp/v1/metrics` (Windows-hosted apps).
- Prometheus read API/UI: `http://127.0.0.1:9090`.
- Grafana: `http://127.0.0.1:3000` (Grafana datasource uses Docker network URL `http://prometheus:9090`).
- Metric exporters must be explicitly enabled **per application process**, in `Development` only. Tracing to Jaeger is independent.
- Published business metric names are translated in Prometheus: `.` → `_`, `s` → `_seconds`, and counters → `_total` (with the default Prometheus OTLP name translation).
- By default the .NET OTLP metrics exporter runs a periodic export approximately every **60 seconds**. After sending a request, leave applications running long enough for an export; rates/`increase()` and histogram quantiles need multiple samples and sufficient activity.
- Avoid screenshots containing User Secrets, `.env`, connection strings or real participant details. Use only synthetic example participants.

## Phase 1 — Verify services and start development infrastructure

In Visual Studio 2026, open **View → Terminal**, working directory `C:\work\ConferenceProFlow`. Run one command at a time:

```powershell
docker compose -f infrastructure/compose.metrics.yaml ps
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka ps
```

Expect the metrics stack to be running, and the business SQL Server, RabbitMQ and Kafka to become ready/healthy. These commands do **not** delete database volumes or reset Kafka offsets. If your main stack is already running, `up -d` is intended to reconcile it rather than destroy the data. Jaeger is optional for the metrics experiment; if you also want traces, use the separate `infrastructure/compose.observability.yaml` configuration and the previously validated tracing exporter settings.

Verify Prometheus **self-scrape** in `http://127.0.0.1:9090/api/v1/query?query=up`. `up=1` alone does **not** prove application metrics ingestion.

## Phase 2 — Start Registration API and prove basic OTLP ingestion

Open a **new Visual Studio Developer PowerShell terminal**. Do not copy passwords or change User Secrets. From repository root run this **single line** (existing launch profile and User Secrets still apply):

```powershell
$env:ASPNETCORE_ENVIRONMENT='Development'; $env:Observability__Metrics__Enabled='true'; $env:Observability__Metrics__Otlp__Enabled='true'; $env:Observability__Metrics__Otlp__Endpoint='http://127.0.0.1:9090/api/v1/otlp/v1/metrics'; $env:OutboxPublisher__Enabled='true'; $env:AllocationResultConsumer__Enabled='true'; $env:KafkaAnalyticsPublisher__Enabled='true'; $env:Kafka__BootstrapServers='127.0.0.1:9092'; dotnet run --project src/ConferenceProFlow.Registration.Api/ConferenceProFlow.Registration.Api.csproj
```

The API should use its existing local HTTP port (previously `http://localhost:5048`). Verify the actual listening address in the console output. Visit its `/health` endpoint; expect `Healthy`.

Leave the process running. After one or two exporter intervals, open `docs/M4_3_PROMETHEUS_QUERIES.http` in Visual Studio and send:

- `00` Prometheus self-scrape (`up`), expected `1`.
- `01` Registration HTTP duration histogram **count**, expected a value after `/health` requests.
- `02` `target_info`, expect at least one series with `job="conferenceproflow-registration-api"` (depending on Prometheus resource processing).
- `03` Registration outbox pending gauge, expect at least a series for `registration_request_rabbitmq` / `analytics_kafka` after the SQL sampler has produced a snapshot. Value `0` is **valid** for an empty outbox.

**Important:** Do not mistake the absence of `conferenceproflow_registration_finalized_total` at this stage for an exporter failure: that counter appears only after the actual async registration is finalized. If the HTTP histogram and backlog gauges are present, metric delivery is functioning.

## Phase 3 — Start Allocation and Analytics in separate terminals

The business stack and Registration API must stay running. Use **new terminal tabs**, one per Worker; variables are process/session scoped. Confirm the local SQL connection strings are still configured via the existing User Secrets. Do not paste them into chat or documentation.

**Terminal 2 — Allocation Worker (one line):**

```powershell
$env:DOTNET_ENVIRONMENT='Development'; $env:Observability__Metrics__Enabled='true'; $env:Observability__Metrics__Otlp__Enabled='true'; $env:Observability__Metrics__Otlp__Endpoint='http://127.0.0.1:9090/api/v1/otlp/v1/metrics'; $env:RabbitMqConsumer__Enabled='true'; $env:AllocationInboxProcessor__Enabled='true'; $env:AllocationOutboxPublisher__Enabled='true'; dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj
```

**Terminal 3 — Analytics Worker (one line; durable live mode, not preview/replay):**

```powershell
$env:DOTNET_ENVIRONMENT='Development'; $env:Observability__Metrics__Enabled='true'; $env:Observability__Metrics__Otlp__Enabled='true'; $env:Observability__Metrics__Otlp__Endpoint='http://127.0.0.1:9090/api/v1/otlp/v1/metrics'; $env:AnalyticsReplay__Enabled='false'; $env:AnalyticsKafkaConsumer__Enabled='true'; $env:AnalyticsKafkaConsumer__ObserveOnly='false'; $env:AnalyticsKafkaConsumer__BootstrapServers='127.0.0.1:9092'; $env:AnalyticsKafkaConsumer__GroupId='conferenceproflow.analytics.v1'; dotnet run --project src/ConferenceProFlow.Analytics.Worker/ConferenceProFlow.Analytics.Worker.csproj
```

**Do not** change the durable group ID, reset Kafka committed offsets, truncate SQL tables or enable Analytics Replay. If a Worker crashes at startup, inspect the specific configuration error and previously verified M4.2 E2E settings before modifying source code.

## Phase 4 — Generate real business measurements

Reuse our **existing** M4.2 validation files; no new API contract needs to be invented:

1. Open `docs/OBSERVABILITY_E2E_DEMO.http` in Visual Studio. Set its `@baseUrl` to the Registration API HTTP address if needed (previously `http://localhost:5048`).
2. Execute request **01** (create a fresh conference), then **02** (create a fresh session). Copy the **new** Session GUID from response 02.
3. In **View → SQL Server Object Explorer**, open `docs/OBSERVABILITY_E2E_SEED.sql`, replace its **single** `PASTE-FRESH-SESSION-GUID-HERE` with that Session ID, execute against the **local** Allocation database. Verify `Capacity=2`, `ReservedCount=0`.
4. Return to the `.http` file and execute POST requests **03–08**, in order, exactly once: create three synthetic participants and three registrations. Do not repeat POST requests merely to force a desired metric value.
5. Run GET requests **09–11** after asynchronous processing. Expected outcome is **2 Confirmed, 1 Rejected, 0 Pending**; the identity of the rejected participant is not guaranteed. If a named-response variable in the Visual Studio `.http` file shows `No response yet`, use an actual registration ID returned by its POST to run a plain GET by ID; do not create an additional registration.
6. In SQL Server Object Explorer run **read-only** `docs/OBSERVABILITY_E2E_CHECKS.sql` with the same new Session GUID. Expect three Registration rows, capacity-safe Allocation inventory (`ReservedCount=2`), three durable inbox/outbox results and Analytics projection (`ConfirmedCount=2`, `RejectedCount=1`). If eventual-consistency processing is still active, repeat only GET/SELECT checks later.
7. **Keep the three .NET processes running** through at least two metric-export intervals so the backend can receive samples.

If the registration business path fails, investigate its normal SQL/RabbitMQ/Kafka logs first. Metrics do not replace the proven M4.2 business verification.

## Phase 5 — Confirm the raw measurements in Prometheus

Open `docs/M4_3_PROMETHEUS_QUERIES.http` and send all relevant read-only requests, or paste the following PromQL expressions into `http://127.0.0.1:9090` → **Query**:

| PromQL (instant query) | Expected after successful fresh E2E workflow |
| --- | --- |
| `http_server_request_duration_seconds_count` | Non-empty series for HTTP requests in Registration API. |
| `conferenceproflow_registration_finalized_total` | At least `confirmed` and `rejected` series, counters positive if recorded while the process was running. Values include earlier activity within the same process, if any. |
| `conferenceproflow_outbox_backlog_pending` | Three pipeline series should exist after successful SQL sampler snapshots; `0` is valid after drain. |
| `conferenceproflow_outbox_backlog_oldest_age_seconds` | Gauge series; `0` is valid with no pending rows. |
| `conferenceproflow_outbox_publish_outcomes_total` | At least `published` series from active RabbitMQ/Kafka publisher pipelines. |
| `conferenceproflow_allocation_inbox_processing_attempts_total` | At least `confirmed`/`rejected` (or relevant actual processing result) after Allocation. |
| `conferenceproflow_analytics_kafka_handling_attempts_total` | `applied` or another factual disposition for durable Kafka processing. |
| `conferenceproflow_analytics_kafka_offset_commit_attempts_total` | `success` when the live consumer committed offsets. |

You can also enumerate existing names using the read-only Prometheus HTTP API `/api/v1/label/__name__/values` and look for `conferenceproflow_*`. **Do not assume** a counter has value exactly three: retries, prior traffic, restarts and exporter temporality can affect observed values. Do not substitute query `up` for application ingest evidence.

### Why some Grafana panels may display No data or 0

The dashboard contains `rate()`, `increase()` and `histogram_quantile()` expressions. With only three synthetic registrations and approximately 60-second export intervals, these require sufficient samples and may not immediately show a meaningful line, percentile or nonzero rate. **First check raw counters/gauges via instant PromQL**. Keep the processes running, expand Grafana's time window (e.g. **Last 30 minutes**) and refresh; if the raw series exist but Grafana still shows no useful line, treat that as a dashboard query/visualization task, not proof the exporter failed. Do not repeatedly generate artificial registrations just to make a chart look full.

A successful `GET /health` proves the Registration HTTP histogram is live, not that Allocation, Analytics or end-to-end finalization metrics are live. Likewise, Jaeger trace export and Prometheus metric export are separate features.

## Phase 6 — Evidence and next milestone step

Record the actual results (screenshots or concise text):

- Business status counts from the fresh E2E (2 Confirmed / 1 Rejected, if achieved).
- Prometheus metric names and actual `pipeline`, `outcome`, `result` label values — avoid real user data and secrets.
- Presence of the three expected operational service/pipeline groups.
- Grafana dashboard panels that have real data, and any remaining blank/misleading panels.
- Any exporter or worker warnings that persist (not just one transient warning at startup).

**Do not commit yet.** The next increment M4.3D3 will reconcile observed Prometheus names and Grafana query behavior, add any targeted E2E verification needed, then finalize regression tests and an issue-linked Pull Request.

## Troubleshooting (safe and non-destructive)

| Symptom | Next check |
| --- | --- |
| Prometheus `up=1`, no app metrics | Confirm `Development`, the three `Observability__Metrics__...` variables are set in the **same terminal used to start that process**, Prometheus receiver enabled; check after one or two export intervals. |
| API `/health` OK, no business counters | Produce fresh business events, check SQL finalization; the HTTP histogram and backlog gauges should appear without finalized registrations. |
| Registration metrics only | Confirm both Workers started in durable mode and had actual traffic; note the Worker metrics generally appear only after handling events, except Allocation backlog gauge. |
| Analytics `Kafka consumption is not implemented yet` | That is the disabled worker stub; confirm `AnalyticsKafkaConsumer__Enabled=true`, `ObserveOnly=false`, correct `GroupId`, and restart that worker with its own configured terminal. |
| Kafka connectivity warnings for `[::1]:9092` | Ensure Kafka `advertised.listeners` is the current IPv4 `127.0.0.1:9092` and host bootstrap address matches. |
| Backlog pending gauge stays `0` | Valid if publisher drains promptly. To test a **rising** backlog, use a dedicated later controlled failure scenario; don't delete or mutate SQL to manufacture values. |
| Grafana query `No data`, Prometheus raw metric exists | Inspect naming (`_total`, `_seconds`, `_bucket`), dashboard rate interval/time range and datasource provisioning. Do not change business code. |
| Prometheus receiver returns export errors | `docker compose -f infrastructure/compose.metrics.yaml logs --tail 80 prometheus`; keep ports on loopback. |
| Missing data after stopping .NET process | Metric export is periodic and best-effort; do not stop before successful export. |

### Safe shutdown

Stop each .NET app with **Ctrl+C** in its own terminal. To stop only the optional monitoring stack while retaining volumes: `docker compose -f infrastructure/compose.metrics.yaml stop`. To stop business infrastructure without deleting its volumes: `docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka stop`.

**Never** use `docker compose down -v`, delete Kafka topics, force-reset offsets or overwrite databases for this verification.

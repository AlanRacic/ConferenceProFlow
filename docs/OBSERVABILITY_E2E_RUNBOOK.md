# ConferenceProFlow — M4.2D3: End-to-End Distributed Tracing Validation

**Status:** Manual validation runbook. Do not mark as Passed until the real local checks below are completed and evidence is recorded.  
**Scope:** Registration API → RabbitMQ → Allocation Worker → Registration Reconciliation → Kafka → Analytics Worker, plus Jaeger (OTLP).  
**Environment:** Local Development only. Zero-cost Docker/.NET/Visual Studio; synthetic participants.  
**Git:** Continue on `feature/opentelemetry-distributed-tracing`. No migration or source-code change is required for this step.

## Why this test matters

The existing M4.2 implementation already has unit/integration coverage of individual tracing boundaries. This run proves, at runtime, that three separate .NET processes actually send spans to Jaeger **while the full business process completes**. A successful `/health` HTTP trace is necessary but insufficient: that checks only the Registration API exporter.

Trace contexts at asynchronous SQL Inbox/Outbox boundaries are **durably persisted**. Background operations can create independent trace roots with `ActivityLink` references to earlier contexts. **Do not assume that the full business flow appears as one trace ID or one waterfall.** The Jaeger UI's span-link rendering can vary; finding a span from each service plus checking stored W3C metadata is meaningful evidence, but missing displayed links requires a targeted review rather than a blanket success claim.

## Prerequisites: use CURRENT local setup, not the historical M2/M3 demo URLs

- Jaeger UI: `http://127.0.0.1:16686` (verified in M4.2D2).
- Registration API current startup URL: `http://localhost:5048` (confirmed previously). Update `@baseUrl` in the `.http` file if your terminal shows a different URL. `GET /health` should return `Healthy`.
- Docker SQL Server, RabbitMQ and Kafka: `docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka ps`; the Jaeger stack is separate.
- One instance each of Registration API, Allocation Worker, Analytics Worker. All in Development and with `Observability__Otlp__Enabled=true`.
- Registration: `OutboxPublisher:Enabled=true`, `AllocationResultConsumer:Enabled=true`, `KafkaAnalyticsPublisher:Enabled=true`, `Kafka:BootstrapServers=127.0.0.1:9092`.
- Allocation: `RabbitMqConsumer:Enabled=true`, `AllocationInboxProcessor:Enabled=true`, `AllocationOutboxPublisher:Enabled=true`.
- Analytics: `AnalyticsKafkaConsumer:Enabled=true`, `AnalyticsKafkaConsumer:ObserveOnly=false`, `AnalyticsKafkaConsumer:BootstrapServers=127.0.0.1:9092`, `AnalyticsKafkaConsumer:GroupId=conferenceproflow.analytics.v1`. Use its EXISTING durable group and live Analytics database, not the preview/replay group.

**If everything is already running and connected, do NOT restart services or rewrite settings unnecessarily.** If the Registration Kafka publisher is misconfigured or disabled, resolve that specific flag before continuing. Never paste passwords, `.env`, User Secrets or personal data into screenshots/reports.

## Phase A — fresh capacity-2 business event

1. In Visual Studio 2026 use **File → Open → File** to open `docs/OBSERVABILITY_E2E_DEMO.http`.
2. Use **Send Request** above HTTP request **01** (create conference). Expect HTTP 201 and a returned `id`.
3. Send request **02** (create session). Expect HTTP 201. Copy its response JSON `id`: **this is the sole GUID you need to paste manually, and only into the SQL files.** Do not reuse a session from earlier demos.
4. **STOP.** In Visual Studio choose **View → SQL Server Object Explorer → Databases → ConferenceProFlowAllocation → New Query**. Open `docs/OBSERVABILITY_E2E_SEED.sql`, replace `PASTE-FRESH-SESSION-GUID-HERE` with that new SessionId and execute against your **LOCAL** Allocation database. Expect `Capacity=2`, `ReservedCount=0`.
5. Return to the `.http` file and send requests **03,04,05,06,07,08** in that order. They create three unique synthetic participants and three registrations. Each successful POST should return HTTP **201**, with registration initially **Pending**. All IDs are passed automatically by Visual Studio named-request variables.
6. Send GET requests **09,10,11** after the background services have handled the events. Repeat GET requests only as needed. Expected aggregate **two Confirmed, one Rejected, zero Pending**. Which participant is rejected is not guaranteed. Never create duplicate POST requests or manually update business status just to get an expected result.

## Phase B — SQL persistence and Analytics verification

1. In SQL Server Object Explorer open **New Query** on a local database; use `docs/OBSERVABILITY_E2E_CHECKS.sql`. Replace its one SessionId placeholder with the SAME new SessionId.
2. Run the entire script (read-only). Key expected results:

| Check | Expected after successful delivery |
| --- | --- |
| Registration rows | 3 (`Pending=0`, `Confirmed=2`, `Rejected=1`) |
| Allocation inventory | `Capacity=2`, `ReservedCount=2` |
| Registration request Outbox | 3, all `PublishedAtUtc` non-null |
| Allocation Inbox | 3, all `ProcessedAtUtc` non-null |
| Allocation result Outbox | 3, all `PublishedAtUtc` non-null |
| Registration result Inbox | 3 |
| Analytics Outbox | 3, all `PublishedAtUtc` non-null |
| Analytics SQL projection | `ConfirmedCount=2`, `RejectedCount=1` |
| Analytics processed-event ledger | 3 unique records |
| `TraceParent` columns | Non-null for newly instrumented operations **when relevant tracing activity is sampled and active**; verify individual rows, don't fabricate values |

3. The application services use separate owned SQL databases; **cross-database SELECTs in this script are solely a human diagnostic tool**. Different stages may settle at different times due to eventual consistency. If results are not ready, re-run **SELECTs** later; do not reinsert or reseed.
4. To inspect Kafka durable-group progress without changing offsets (optional):

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group conferenceproflow.analytics.v1
```

## Phase C — Jaeger tracing evidence

1. Open `http://127.0.0.1:16686` in a browser. Under **Search**, select **Last 1h** (or a suitable interval if you perform a longer test). Click **Find Traces** and inspect the three service names, one at a time:
   - `conferenceproflow-registration-api`
   - `conferenceproflow-allocation-worker`
   - `conferenceproflow-analytics-worker`
2. Registration API: find the `POST /api/registrations` HTTP request(s), and the `rabbitmq.allocation-result.process` and `kafka.analytics.publish` activities (if produced). The `/health` trace alone does NOT prove the whole flow.
3. Allocation Worker: look for `rabbitmq.registration-request.process`, `allocation.inbox.process`, and `rabbitmq.allocation-result.publish`.
4. Analytics Worker: look for `kafka.analytics.process`, ideally one span corresponding to each received finalization. Details can show tags/disposition.
5. Open individual traces/spans. Examine operation names, service names, start timestamps, error status, parent span IDs and **Links / Span Links** (where exposed by your Jaeger version). At durable SQL boundaries, independent traces linked by `ActivityLink` are EXPECTED. Do not insist on one global `TraceId`.
6. Inspect `TraceParent` values in the SQL verification results. W3C format is `00-<32-char trace id>-<16-char span id>-<flags>`. Jaeger's **Lookup by Trace ID** can help find the originating context when that span was actually exported. A persisted `TraceParent` does NOT by itself prove the corresponding span was exported or link visualized.
7. Collect development-only evidence screenshots: (a) three final HTTP status results, (b) SQL summaries and TraceParent columns without unnecessary user data, (c) Jaeger traces for ALL THREE services. Note the trace relationships you ACTUALLY observed.

## Failure triage — never 'fix' by deleting data

| Symptom | First diagnostic |
| --- | --- |
| Registration stays Pending | Check RabbitMQ, five enabled Registration/Allocation services; inspect request/result Outbox and Inbox states. |
| Registration finalized, Analytics remains unchanged | Check `KafkaAnalyticsPublisher:Enabled`, Kafka 127.0.0.1:9092, Analytics durable worker group, Analytics Outbox publication and worker logs. |
| Registration trace visible, Workers missing | Check OTLP enablement/Development environment in EACH process, whether real business messages were consumed, and custom ActivitySource registration. |
| Trace IDs differ across steps | Expected at asynchronous boundaries when `ActivityLink` is used. Inspect links and persisted W3C context rather than assuming failure. |
| SQL `TraceParent` is NULL for NEW messages | Verify instrumented `ActivitySource`, sampling and exporter/listener setup of the producing process. Do not alter durable business records. |
| SQL/Jaeger disagreement | Check stage timestamps, pending retry, active worker modes, and tracing/exporter errors; don't claim success until verified. |

**Do not run `docker compose down -v`, delete topics, reset Kafka committed offsets, or replace existing databases** to make demo results match. Keep Jaeger local development only and recognize that the default in-memory backend can lose trace evidence on restart.

## Phase D — close the manual validation, not the milestone yet

After successful Phase A–C, stop applications with Ctrl+C, do **Build Solution** and run the existing unit/integration test suites in **Test Explorer**, with Docker Desktop running. Record actual passed/failed counts and any intermittent CI issue. In **Git Changes**, verify only the four new `docs/OBSERVABILITY_E2E_*` files were introduced by this package; previous M4.2 work remains intact. Do not create a PR yet: first record evidence and the final full-suite regression result, then prepare the professional M4.2 PR and merge with `Create a merge commit` after CI is green.

## Reference implementation / honest limitations

This runbook uses the established M2.6/M3.4 E2E contracts in `docs/ALLOCATION_E2E.http`, `docs/ALLOCATION_E2E_DEMO.md` and `docs/ANALYTICS_E2E_DEMO.md`, with the **current local Registration port and M4.2 tracing stages**. It is not a new fully automated cross-service integration test. Evidence comes from the user's own local run; do not mark any individual trace/row as confirmed until actually observed.

# ConferenceProFlow — M4.2 Distributed Tracing Validation Report

**Milestone:** M4.2 — Observability / distributed tracing  
**Validation date:** 2026-11-26 (local development, Zagreb)  
**Branch:** `feature/opentelemetry-distributed-tracing`  
**Result:** Manual end-to-end business flow and service tracing **reported successful**. Full Release regression, evidence capture, CI and Pull Request review remain **pending** until explicitly completed.  
**Scope:** Registration API → RabbitMQ → Allocation Worker → Registration Reconciliation → Kafka → Analytics Worker; Jaeger via OTLP/HTTP.

> Evidence integrity: This report distinguishes results reported during the guided local run from independently recorded evidence. Do not mark the entire milestone or CI as passed until the regression/review gates below complete. Avoid publishing local credentials, user secrets, private `.env`, or raw participant contact information.

## 1. Objective and architecture

Verify that a genuine conference registration is carried through the asynchronous, service-owned workflow **while** all three .NET processes export OpenTelemetry spans to local Jaeger. Trace context is carried in message headers and persisted in relevant SQL Inbox/Outbox records so that delayed execution and process restarts do not inherently sever causal correlation.

```
Registration API (HTTP)
  -> Registration SQL Outbox -> RabbitMQ -> Allocation Consumer
  -> Allocation SQL Inbox -> Allocation Inbox Processor
  -> Allocation result SQL Outbox -> RabbitMQ -> Registration reconciliation
  -> Analytics SQL Outbox -> Kafka -> Analytics durable consumer
  -> Analytics SQL projection
  [each process: OpenTelemetry -> OTLP/HTTP -> Jaeger]
```

**Trace topology:** A messaging consumer can follow the transmitted producer context as parent. A durable asynchronous operation can start a separate trace and reference the earlier context using `ActivityLink`. Therefore, one user operation is **not guaranteed** to appear as one Jaeger waterfall or share one `TraceId`. The presence of spans in each service is evidence of export; **proof of link correctness** additionally requires inspection of the span's links/parent context against stored W3C context.

**Delivery semantics unchanged:** At-least-once messaging, SQL Inbox deduplication, transactional outboxes, RabbitMQ confirmations/ACK, and Kafka offset commit after durable SQL handling. Tracing is diagnostic metadata and must not be required for business processing.

## 2. Validation environment

| Component | Local development setup |
| --- | --- |
| Registration API | .NET 10 / ASP.NET Core, Development, OTLP enabled |
| Allocation Worker | .NET 10 Generic Host, Development, OTLP enabled |
| Analytics Worker | .NET 10 Generic Host, durable Kafka live consumer, Development, OTLP enabled |
| Operational broker | RabbitMQ (Docker Compose) |
| Analytics broker | Kafka (Docker Compose profile `kafka`) |
| Databases | SQL Server; separate Registration, Allocation and Analytics DBs |
| Trace backend | Local Jaeger 2.x; localhost-only ports 16686 (UI), 4318 (OTLP/HTTP) |
| Source code | Local feature branch, not necessarily yet merged into public `master` |

**Known local start-up details:** The verified Registration `/health` endpoint used `http://localhost:5048/health` in this run; actual port is launch-profile dependent. Docker Compose reads credentials via `--env-file .env` (never commit this file). For the user's Windows Kafka client configuration, advertised host/bootstrap was changed to `127.0.0.1:9092`, while `kafka:19092` remains the internal listener. Analytics was started with durable mode enabled, `ObserveOnly=false`, and its existing consumer group `conferenceproflow.analytics.v1`.

## 3. Manual E2E results — reported in guided local run

Demonstration reference: `docs/OBSERVABILITY_E2E_RUNBOOK.md`; requests in `docs/OBSERVABILITY_E2E_DEMO.http`; read-only checks in `docs/OBSERVABILITY_E2E_CHECKS.sql`.

| Verification | Expected | Reported observation | Evidence status |
| --- | --- | --- | --- |
| Registration API `/health` | `Healthy` | Healthy | Confirmed in conversation + browser screenshot |
| Registration API OTLP export | HTTP trace in Jaeger | Found Registration API GET traces | Confirmed in conversation + Jaeger screenshot |
| New capacity-2 session | Inventory `Capacity=2`, `ReservedCount=2` after processing | Business SQL checks reported passed | User-reported; preserve SQL screenshot if needed |
| Three registration outcomes | 2 Confirmed / 1 Rejected / 0 Pending | SQL verification reported successful | User-reported |
| Sophie registration | Rejected due to filled capacity | `Rejected`, reason `Session capacity reached.`; SQL status 2 | Confirmed in browser and SQL screenshots |
| RabbitMQ Allocation processing | Durable receive, decision, result publication | E2E business flow succeeded; SQL checks reported passed | User-reported |
| Analytics Kafka processing | 3 events recorded, 2/1 aggregate | SQL checks reported passed | User-reported |
| Allocation Worker Jaeger spans | Receive, Inbox process, result publish | Jaeger verification reported successful | User-reported; save trace screenshots/IDs |
| Analytics Worker Jaeger spans | `kafka.analytics.process` | Jaeger verification reported successful | User-reported; save trace screenshots/IDs |
| Cross-trace causal links | Parent links or `ActivityLink` agree with persisted context | No per-link IDs/screenshots preserved in this report | **Not separately evidenced — optional focused verification** |

**Important diagnostic incident (not a business failure):** The final Visual Studio `.http` GET for Sophie displayed `No response yet` / an unavailable named-response ID. Direct HTTP GET by the SQL-confirmed RegistrationId returned the final rejected status. Do not rerun creation POSTs to fix this display issue. The business result is backed by API and SQL observations; an improvement to the `.http` helper can be made in a separate, reviewed change if warranted.

**Kafka connectivity incident resolved for this local run:** Initial `localhost:9092` connection attempts selected IPv6 `::1` while the host listener was IPv4. Kafka container was healthy and port 9092 available on `127.0.0.1`. Local advertised listener/bootstrap was aligned to `127.0.0.1:9092`, after which the user reported successful connection. This is a local Windows development configuration, not a general production-networking prescription.

## 4. Trace evidence capture (fill only with real values)

Do **not** invent trace IDs, timing data, or screenshots. These fields are intentionally blank unless manually extracted from Jaeger during the relevant run. In-memory Jaeger data may disappear after container restart.

| Service / operation | Trace ID or screenshot reference | Observation |
| --- | --- | --- |
| Registration API — `POST /api/registrations` | _Add only if recorded_ | _Add span name/status_ |
| Registration API — `rabbitmq.allocation-result.process` | _Add only if recorded_ | _Add span name/status_ |
| Registration API — `kafka.analytics.publish` | _Add only if recorded_ | _Add span name/status_ |
| Allocation — `rabbitmq.registration-request.process` | _Add only if recorded_ | _Add span name/status_ |
| Allocation — `allocation.inbox.process` | _Add only if recorded_ | _Add link when observed_ |
| Allocation — `rabbitmq.allocation-result.publish` | _Add only if recorded_ | _Add link when observed_ |
| Analytics — `kafka.analytics.process` | _Add only if recorded_ | _Add parent context when observed_ |

For a targeted link check, open a span in Jaeger, note `TraceId`/`ParentSpanId` and any `Links`, and compare those to the stored `TraceParent` in the relevant SQL row. The W3C format is `00-<trace-id>-<span-id>-<flags>`. An existing SQL context alone does **not** prove that the linked span was exported, sampled or visible in Jaeger.

## 5. Final local Release regression — PENDING

Before running these, stop all three terminal-hosted .NET applications with `Ctrl+C` **in their own terminals**. Keep Docker Desktop and broker/SQL containers available to Testcontainers. From `C:\work\ConferenceProFlow` in a fresh Visual Studio Developer PowerShell:

```powershell
dotnet restore ConferenceProFlow.slnx
```

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release --no-restore
```

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal
```

The full solution test run includes Registration UnitTests, Registration IntegrationTests, Allocation UnitTests, Allocation IntegrationTests and Analytics IntegrationTests. Save the **actual** outcome; do not treat historical green test runs as proof of this final combined state. Docker/Testcontainers integration tests can require Docker and network readiness.

| Gate | Actual outcome | Evidence |
| --- | --- | --- |
| Release restore | _Pending_ | _Terminal / log_ |
| Release build | _Pending_ | _0 errors; warnings review_ |
| Full-solution tests | _Pending_ | _Total/passed/failed/skipped counts_ |
| Git diff review | _Pending_ | _Git Changes screenshot / reviewed files_ |
| CI on Pull Request | _Pending_ | _GitHub Actions link/status_ |

If a test fails: capture the **exact** failing test name and error/stack trace, confirm infrastructure readiness, and fix or transparently track flakiness; don't simply mark the whole suite green. Rerun after correction and report any intermittent result.

## 6. Source control, documentation, and review checklist

- [ ] In **Visual Studio → Git Changes**, confirm branch `feature/opentelemetry-distributed-tracing`.
- [ ] Review all modified/new C# and `.csproj` files and all generated EF Core migrations/model snapshots. Check that tracing did not replace operational logic.
- [ ] Review `infrastructure/compose.yaml`: the Windows host Kafka listener change must not break container-internal clients (`kafka:19092`).
- [ ] Check `launchSettings.json` is valid JSON and includes only intended local development changes.
- [ ] Exclude local `.env`, passwords, User Secrets, private runtime data, `bin/`, `obj/`, Jaeger state and participant contact information from commits/screenshots.
- [ ] Ensure `docs/OBSERVABILITY_E2E_RUNBOOK.md`, Jaeger/OTLP documentation and this report are included where intended.
- [ ] Complete the final Release regression above.
- [ ] Create a meaningful local checkpoint commit (or review existing commits); preserve historical changes; do not reset or discard files.
- [ ] Prepare Pull Request toward `master`, link the existing M4.2 issue, wait for required CI checks, review diff, and merge using **Create a merge commit** only after green checks.
- [ ] After merge, delete merged branch on GitHub (if desired), switch to `master` in Visual Studio and Pull.

## 7. Limitations and production-readiness boundaries

- Demonstration runs on a single developer machine and does **not** establish production availability, high throughput, disaster recovery or full production security.
- Jaeger uses local in-memory development storage, with no production retention/SLO or access-control guarantee.
- Development-only OTLP exporter requires explicit enablement and an appropriate local endpoint. Missing Jaeger should affect trace export, **not** SQL commit, RabbitMQ ACK or Kafka offset handling.
- Sampling and exporter timing can make a particular span absent even when the business operation succeeded; therefore compare instrumentation, messaging metadata and SQL state before diagnosing a causal-link failure.
- The test assumes previously provisioned session inventory. Automated inventory synchronization remains outside the demonstrated scope.
- Some asynchronous spans are **linked**, not parent-child; do not promise one monolithic trace for an entire business registration.
- No production traffic, personal data or secrets were intentionally used in the test scenario.

## 8. Next action

Run the final Release regression and review Git Changes. After actual results are recorded, update Section 5 and proceed to a focused M4.2 Pull Request. Keep this report as evidence of the difference between *manually demonstrated behavior*, *automated regression*, and *CI verification*.

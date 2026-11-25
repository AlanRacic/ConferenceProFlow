# ConferenceProFlow — Observability & Performance Design

**Milestone:** M4 — Observability & Performance  
**Work item:** M4.1 — Architecture review and baseline definition  
**Status:** Design and initial signal inventory; telemetry instrumentation, dashboards and performance measurements are **not yet implemented or validated** by this document.  
**Target:** local .NET 10 + Docker Compose, zero paid services.

## 1. Purpose and boundaries

The goal is to show where accepted registration work goes, whether it completes, how long its distinct phases take and where a failure or throughput bottleneck occurs. M4.1 is an evidence-based design phase: it introduces **no application code, NuGet packages, SQL migrations, Docker containers or changes to business processing**.

Scope is the existing Registration API, Allocation Worker, Analytics Worker (including bounded replay), SQL Server, RabbitMQ, Apache Kafka and GitHub Actions. This design is not a claim that ConferenceProFlow currently has distributed traces, Prometheus metrics or measured performance targets.

### Questions the observability stack must answer

1. Did the HTTP API accept a registration, or reject the request before creating one?
2. How long did the synchronous HTTP request take? **This is not the same as finalization latency.**
3. For an accepted registration, how long from `CreatedAtUtc` to `Confirmed`/`Rejected`?
4. Is a delay attributable to an Outbox, RabbitMQ delivery, Allocation processing, result reconciliation, Kafka publication or Analytics consumption?
5. How many records are waiting, repeatedly failing, quarantined or being recognized as duplicates?
6. Does load testing preserve capacity and idempotency invariants?

## 2. Current-state audit (code reviewed at M4.1)

| Component | Verified existing entry points/signals | Gap / planned change |
| --- | --- | --- |
| Registration API | `Program.cs`: `AddHealthChecks`, `/health`, `MapOpenApi` in Development, `ILogger` within background services. `POST /api/registrations` and `GET /api/registrations/{registrationId}`. | No OpenTelemetry pipeline in inspected startup; `/health` is not a distributed business-readiness check. Add HTTP spans/metrics, later custom registration lifecycle spans. |
| Registration SQL | `Registrations` has `CreatedAtUtc`, `UpdatedAtUtc`, integer status 0/1/2. `OutboxMessages` and `AnalyticsOutboxMessages` have publish status/attempt timestamps. | Good for a retrospective **database-based approximation** of finalization and backlog. Not an HTTP latency or continuous processing latency measurement. |
| Allocation Worker | Generic Host; conditional RabbitMQ consumer, SQL inbox processor, result outbox publisher. Uses `ILogger`. | Instrument scoped processing, SQL and RabbitMQ hops; identify measurable queue/inbox wait without logging payloads. |
| Analytics Worker | Generic Host; live Kafka consumer performs durable SQL handling before `Commit(record)`. Replay has isolated SQL/consumer-group mode. Uses `ILogger`. | Instrument Kafka processing, SQL duration, duplicate/quarantine dispositions and live-consumer lag. Treat replay as separate mode, not as extra live traffic. |
| Docker Compose | SQL Server, RabbitMQ and optional Kafka; local ports bound to `127.0.0.1`; named volumes. | No dedicated telemetry backend in the inspected Compose file. Add optional local telemetry profile in M4.2–M4.3, not in M4.1. |
| CI | Restore, Release Build, Docker availability and `dotnet test` for the solution. | Extend with targeted telemetry tests after instrumentation; keep integration tests independent of local dashboards. |

**Audit limitation:** These findings are based on inspected repository files and prior completed M1–M3 demonstrations; they are not a runtime inspection of the user's private local settings or a fresh performance benchmark. If local code has diverged, recheck the referenced entry points before implementation.

### Source pointers

- [`docs/PROJECT_CHARTER.md`](PROJECT_CHARTER.md): FR-009 monitoring; NFR-003 measurable performance; local-only strategy.
- [`src/ConferenceProFlow.Registration.Api/Program.cs`](../src/ConferenceProFlow.Registration.Api/Program.cs)
- [`src/ConferenceProFlow.Allocation.Worker/Program.cs`](../src/ConferenceProFlow.Allocation.Worker/Program.cs)
- [`src/ConferenceProFlow.Analytics.Worker/Program.cs`](../src/ConferenceProFlow.Analytics.Worker/Program.cs)
- [`src/ConferenceProFlow.Analytics.Worker/Messaging/Consumers/KafkaAnalyticsConsumerService.cs`](../src/ConferenceProFlow.Analytics.Worker/Messaging/Consumers/KafkaAnalyticsConsumerService.cs)
- [`src/ConferenceProFlow.Registration.Api/Domain/Entities/Registration.cs`](../src/ConferenceProFlow.Registration.Api/Domain/Entities/Registration.cs)
- [`infrastructure/compose.yaml`](../infrastructure/compose.yaml)
- [`.github/workflows/dotnet-ci.yml`](../.github/workflows/dotnet-ci.yml)

## 3. Planned telemetry architecture

```text
Registration API  -- RabbitMQ --> Allocation Worker
      ^                               |
      +---- RabbitMQ result ----------+
      |
      +---- Kafka finalization ------> Analytics Worker

Each .NET process: ActivitySource + Meter + structured ILogger
        |                |                    |
        +-------- OpenTelemetry SDK (planned) +
                                |
                      OTLP / local collection
                                |
               +----------------+----------------+
               |                                 |
          Local tracing UI              Prometheus metrics
          (select in M4.2)             + Grafana (M4.3)
```

**Selection rules:** Begin with one minimal local trace exporter/backend, not a full production observability platform. The collector can be introduced if it simplifies one consistent export path. Prometheus/Grafana are deferred until M4.3. Keep telemetry tools behind an optional Docker Compose profile and bind UI endpoints to loopback. No mandatory cloud, API key or secret. Check tool image licensing/version and supported ports before introducing dependencies.

### Service identity

Use stable resource names: `conferenceproflow-registration-api`, `conferenceproflow-allocation-worker`, `conferenceproflow-analytics-worker`. Use attributes such as `service.version` when available and environment `development` / `test`; do not use personal or machine-identifying values as resource labels. Replay can be identified using a low-cardinality `processing.mode=replay` attribute where appropriate.

## 4. Distributed tracing and correlation plan

The following are **proposed** logical spans/operations, not code already present:

| Boundary | Example logical span(s) | Linkage / risk |
| --- | --- | --- |
| HTTP registration request | HTTP server span + `registration.accept` | The HTTP response only means a registration was accepted as Pending. |
| Registration Outbox | `registration.outbox.persist`, `rabbitmq.registration.publish` | Database commit and later publishing are separate lifetimes. |
| RabbitMQ request consumer | `rabbitmq.registration.process`, `allocation.decide` | Requires propagating producer trace context via AMQP headers, not by reusing process-local `Activity.Current`. |
| RabbitMQ result consumer | `rabbitmq.allocation-result.process`, `registration.finalize` | Correlate using stable business/message identities; distinguish duplicates/conflicts. |
| Kafka publication | `kafka.finalization.publish` | Producer context sent via Kafka record headers; publisher retries may create multiple delivery spans for one logical event. |
| Kafka live consumption | `kafka.finalization.process`, `analytics.sql.apply` | Processing may link to producer span; SQL handling precedes offset commit. |
| Replay | `kafka.replay.run`, `kafka.replay.partition.process` | Separate execution; links and `processing.mode=replay` distinguish replay from live counts. |

### Crucial async/outbox boundary

An HTTP request, SQL Outbox write and later message publication **do not share an active in-memory tracing scope**. For meaningful continuity the producer context must be **durably captured with the Outbox work** and later propagated in RabbitMQ/Kafka message headers. M4.2 should examine the smallest backward-compatible metadata storage approach (for example optional Outbox trace-context columns or an explicit transport envelope), test it and avoid changing business event identity or idempotency semantics.

The receiver should extract W3C `traceparent`/`tracestate` and create its processing span with an appropriate parent or `ActivityLink`. For asynchronous/event-driven work, links can more accurately represent causal relationships without creating an unrealistically long single trace. A fallback is correlation through `MessageId` and `RegistrationId` in **structured logs**, not metric labels; this is a weaker capability than end-to-end trace context propagation and must be described honestly.

**Do not claim an unbroken HTTP-to-Analytics trace before cross-process propagation is implemented and tested.** Do not place participant names, emails, arbitrary JSON payloads, secrets, full SQL statements or registration IDs in telemetry metric dimensions.

## 5. Metrics: definitions, sources, planned instruments

All initial target metrics are *proposals*. Do not report values until actually collected. Use bounded dimensions and document sampling/observation intervals.

| Metric / measurement | Unit | Definition and event boundary | Dimensions allowed (examples) | Proposed phase |
| --- | --- | --- | --- | --- |
| `http.server.request.duration` | seconds | Server HTTP request until HTTP response; report p50/p95/p99 for route+method. **Not finalization time.** | `http.route`, method, response status class | M4.2/M4.3 auto instrumentation |
| `conferenceproflow.registration.finalization.duration` | seconds | Final transition timestamp minus persisted `CreatedAtUtc` for registration. | `outcome=confirmed/rejected` | M4.2/M4.3 custom histogram |
| `conferenceproflow.registration.finalized` | registrations | Increase exactly once after committed Pending→final transition. | outcome | M4.3 counter |
| `conferenceproflow.messaging.publish.attempts` | attempts | One attempt to deliver to broker; success/failure tagged. | `system=rabbitmq/kafka`, message type (bounded), outcome | M4.3 counter |
| `conferenceproflow.messaging.processing.duration` | seconds | Time spent handling one received record, not total queueing time. | system, message type, disposition | M4.3 histogram |
| `conferenceproflow.outbox.pending` | messages | Count of unpublished, unquarantined records at a given observation time. Prefer bounded periodic sample; distinguish RabbitMQ and Kafka Outboxes. | component/outbox kind | M4.3 observable gauge |
| `conferenceproflow.analytics.records.handled` | records | Count SQL-applied, duplicate, quarantined or failed processing; no double count on retries unless explicitly documented. | disposition, mode=live/replay | M4.3 counter |
| Kafka consumer group lag | records | Broker high watermark minus committed offset per partition (not elapsed seconds). | group (fixed allowlist), topic, partition (bounded known topic) | M4.3 external / diagnostic measurement |
| Registration throughput | registrations/s | Accepted registrations and **separately** finalized registrations over a fixed time interval. | outcome, workload label (controlled) | M4.4 load testing |
| Analytics catch-up delay | seconds | Must explicitly choose event occurrence→SQL processing completion or committed Kafka offset lag; these are different measurements. | mode, disposition | M4.3/M4.4 |

No `RegistrationId`, `ParticipantId`, `SessionId`, `MessageId`, email, `TraceId` or arbitrary exception text as metric labels. Cardinality growth can overload a metrics system; these values can be used judiciously in bounded logs/traces with access controls.

**Percentile note:** Calculate p50/p95/p99 over an explicitly stated time window and sample count. Do not show p99 as meaningful for a three-registration demo. Latency goals and acceptable throughput remain **TBD until measured**; avoid made-up SLOs.

## 6. Baseline methodology (before and after telemetry)

### A. Immediate non-destructive SQL inspection (M4.1)

Run `docs/OBSERVABILITY_BASELINE_CHECKS.sql` in **Visual Studio SQL Server Object Explorer** against `ConferenceProFlowRegistration`. It uses only `SELECT` queries and does not change database state. For a newly created demonstration session, set `@SessionId` in the SQL script. For all records, leave it `NULL`.

The existing timestamps allow an **approximate historical finalization-duration measurement** (`UpdatedAtUtc - CreatedAtUtc`) for rows currently in final status. This includes waiting for Outbox and messaging and is not the HTTP request duration. It is not a precise runtime benchmark, is sensitive to old/demo records and is not evidence of causal latency breakdown. Compare it only with like-for-like workloads. The script also counts pending Kafka Analytics Outbox records and failure attempts.

Do not insert synthetic records directly into business tables to create measurements.

### B. Repeatable functional baseline (M4.1 documentation)

Existing verified scenario: `docs/ALLOCATION_E2E.http` + `docs/ALLOCATION_E2E_SEED.sql` (new session capacity 2; three registrations; expected 2 confirmed and 1 rejected). Capture at minimum: test session, date/time, services enabled, eventual final statuses and notable Outbox/consumer errors. Never assume which particular registration will be rejected.

This is a correctness demo, **not** a statistically meaningful load test. `POST` returns a Pending registration promptly; follow up using `GET /api/registrations/{id}` until final status (bounded polling and explicit timeout), then measure elapsed time from acceptance to finalization. Record both client-observed polling latency and SQL event timestamps separately.

### C. Controlled performance baseline (deferred to M4.4)

Create fresh session IDs and use separate development/test data; avoid reusing historical sessions. Use k6 against Registration API with deterministic request rate stages, bounded virtual users, unique test identities, a bounded status-polling interval and cleanup strategy **without deleting existing M3 data**. Track HTTP response codes (201, 409, unexpected failures), p50/p95/p99, accepted vs finalized rate, Pending backlog, resource usage, and capacity invariant `confirmed <= capacity`.

Define the workload before comparing runs: CPU/memory, .NET SDK, SQL, RabbitMQ/Kafka image versions, workers, feature flags, session capacity, duration, warm-up and number of records. Make no promises that the full multi-component chain will achieve a specific throughput until observed.

## 7. Logging, privacy and health

- Retain `ILogger` structured fields where present; add consistent event names/phase names, service name, exception type and retry state.
- Sample/limit noisy per-message informational logging under load; keep errors and operational warnings actionable.
- Never log raw participant JSON, credentials, connection strings or unredacted SQL parameters.
- `/health` currently shows basic ASP.NET Core health checks; this alone does **not** prove RabbitMQ, SQL, Kafka or downstream business readiness. Design targeted health/readiness checks later, with timeouts and no destructive writes.
- Local monitoring UIs should bind to `127.0.0.1` and should be optional; do not expose development telemetry unauthenticated to networks.

## 8. Delivery plan and measurable acceptance

| Phase | Minimal deliverable | Evidence required |
| --- | --- | --- |
| **M4.1 now** | This design, initial source audit, safe SQL baseline checks, named unknowns | Documentation review; script confirmed read-only; Git review |
| **M4.2** | Consistent OpenTelemetry bootstrap/SDK and distributed tracing, plus propagation through selected durable async hops | Unit/integration tests for headers and restarts, trace UI screenshot or captured export showing linked hops; existing regression tests |
| **M4.3** | Low-cardinality metrics, optional Prometheus/Grafana dashboard, backlog/latency visibility | Scrape succeeds; sample dashboard from real local traffic, names/units validated; no secrets/PII labels |
| **M4.4** | Controlled local k6 workload and documented baseline | Repeatable script, measurement environment, results with p50/p95/p99 and count/timeout limitations |
| **M4.5** | Only evidence-backed targeted optimizations | Before/after measured under same workload; correctness regression and query-plan evidence where applicable |
| **M4.6** | End-to-end validation, documentation, CI, PR | Build/test green; reproducible observability setup; explicit known limitations |

**Acceptance for this M4.1 document:** Current vs planned is explicit; latency definitions and units are unambiguous; the async propagation challenge is acknowledged; no ID-bearing metric dimensions; no paid services; no performance claims without measurements; actual measurement and export are scheduled rather than implied completed.

## 9. Risks and open questions before implementation

1. **Outbox trace context durability:** exact design (nullable fields vs envelope) needs code-level review and backward-compatibility tests. Avoid carrying transient `Activity.Current` across process restarts.
2. **.NET libraries:** pin compatible OpenTelemetry versions in M4.2 after checking current package compatibility; do not assume EF Core tracing is enabled automatically.
3. **Backend choice:** pick the lightest trace viewer/exporter available locally; Prometheus and Grafana are M4.3, not mandatory for first traces.
4. **Resource cost:** telemetry sampling, logging and SQL gauges can distort performance measurements; quantify overhead during M4.4.
5. **High-cardinality data:** unique IDs, topics supplied by users, session names and error strings must not become metric dimensions.
6. **Replay:** bounded replay is intentionally a different workload; do not merge replay counts/latencies into live business SLOs.
7. **SQL timestamp proxies:** `UpdatedAtUtc` reflects finalization for the current entity methods but is not a guaranteed universal performance event for future code paths.
8. **One-node local brokers:** results are not production throughput or HA/availability claims.

## 10. References

- Project Charter (`docs/PROJECT_CHARTER.md`, FR-009 / NFR-003).
- [Microsoft — .NET Observability with OpenTelemetry](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/observability-with-otel)
- [OpenTelemetry — Messaging span semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
- [OpenTelemetry — .NET metric cardinality best practices](https://opentelemetry.io/docs/languages/dotnet/metrics/best-practices/)
- [OpenTelemetry — .NET instrumentation libraries](https://opentelemetry.io/docs/languages/dotnet/libraries/)

**Next action:** review this design and the SQL inspection results. Implement no telemetry in M4.1 without defining the propagation/export/test boundary first.

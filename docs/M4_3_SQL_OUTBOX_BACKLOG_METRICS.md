# M4.3C3C — SQL Outbox backlog metrics

## Purpose

Show whether durable message publication is keeping up with incoming work. This is **not** a broker queue depth metric or a count of total produced events.

## Metrics and pipelines

| Instrument | Unit | Type | Meaning |
|---|---|---|---|
| `conferenceproflow.outbox.backlog.pending` | `1` | ObservableGauge<long> | Current SQL rows where `PublishedAtUtc IS NULL AND QuarantinedAtUtc IS NULL` |
| `conferenceproflow.outbox.backlog.oldest.age` | `s` | ObservableGauge<double> | Age of oldest pending row by `OccurredAtUtc` at last successful sample; zero when empty |

Fixed, bounded `pipeline` tags:
- `registration_request_rabbitmq` — RegistrationDbContext.OutboxMessages
- `analytics_kafka` — RegistrationDbContext.AnalyticsOutboxMessages
- `allocation_result_rabbitmq` — AllocationDbContext.AllocationOutboxMessages

Both **ready to publish** and **scheduled for a future retry** count as pending. Quarantined rows are excluded; their separate publication outcome is covered by M4.3C3A/B metrics. This metric is intentionally different from `NextAttemptAtUtc <= now` (ready-to-publish).

## Implementation and operational semantics

- Hosted `BackgroundService` samplers query existing SQL tables once on startup, then every 30 seconds; they use `AsNoTracking`, `CountAsync` and nullable `MinAsync` queries.
- `ObservableGauge` callbacks read **only cached immutable snapshots**; they never perform SQL or asynchronous work.
- The Registration sample is published atomically for both Registration SQL Outboxes after all queries succeed.
- Gauge values are **observations**, not durable event counters. The oldest age is approximate to the last sample (up to 30 seconds plus query time).
- No sample is published before first successful SQL query or when the latest snapshot becomes older than 2 minutes. If sampling fails, the worker logs a warning and retries; there is no misleading reset to zero.
- Samplers are enabled only when the existing `Observability:Metrics:Enabled=true` flag is active in **Development**. No extra DB permissions, broker changes, EF migration, or new NuGet dependency.
- The three pipelines emit the same metric names with different bounded `pipeline` tags and their existing `service.name` resources.
- **No identifiers, payloads, PII or secrets as metric tags.**

## What was validated / what remains

The new unit tests verify gauge instrument names, pending/age values, zero and negative-age behavior, and fixed tags via `MeterListener`; they do not prove correctness of actual SQL queries or OTLP/Prometheus delivery.

Next validation step: run an isolated Registration/Allocation SQL integration scenario with known pending/published/quarantined states; then verify the gauges with Prometheus and Grafana during M4.3D/E.

## Follow-up

If monitoring is enabled in production later, re-evaluate opt-in gating, polling intervals, database query plans, indexes and exporter topology. Do not treat gauge availability alone as a health check.

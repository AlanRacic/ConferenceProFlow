# M4.3C3A — Kafka Analytics / RabbitMQ Allocation Outbox publish metrics

## Scope

This increment adds operational metrics for **two already-existing durable Outbox publishers**:

- Registration API → Kafka Analytics event publisher (`analytics_kafka`).
- Allocation Worker → RabbitMQ allocation-result publisher (`allocation_result_rabbitmq`).

Registration API → RabbitMQ registration-request publisher and SQL Outbox backlog snapshots
are **not included** in this increment; they are scheduled for M4.3C3B / C3C.
The existing W3C tracing, retries, SQL writes, message IDs, payloads, and broker
acknowledgement logic are retained.

## Instruments

Both services emit the same instrument names under their own OpenTelemetry service resource:

| Instrument | Kind | Unit | Tags | Semantics |
| --- | --- | --- | --- | --- |
| `conferenceproflow.outbox.publish.outcomes` | Counter<long> | `1` | `pipeline`, `outcome` | Count of **SQL-persisted** Outbox state transitions, not unique events or broker requests. |
| `conferenceproflow.outbox.publish.duration` | Histogram<double> | `s` | `pipeline`, `outcome` | Seconds from starting a row's handling to successful persistence of its transition. |

Pipelines: `analytics_kafka`, `allocation_result_rabbitmq`.
Outcomes:

- `published`: broker acknowledgement/confirmation **and** SQL `PublishedAtUtc` committed.
- `retry_scheduled`: failed publish/validation attempt **and** updated SQL retry state committed.
- `quarantined`: invalid Allocation payload quarantined in SQL **before** contacting RabbitMQ.

Kafka Analytics currently retains validation failures in the pending/retry state; it does
**not** quarantine them automatically, so the Kafka publisher does not emit `quarantined`.
A SQL failure before the publication state is stored produces **no** outcome observation;
this is intentional, since no durable transition can be asserted. Because metrics are
best-effort, even a successful SQL state transition may go unreported after a process crash.
Consult SQL for authoritative publication state and retries.

**At-least-once:** Kafka/RabbitMQ may acknowledge a message but SQL may fail before
marking it published. A retry can publish the same business event again. The resulting
`published` observations are not a count of distinct business events.

No message IDs, exception strings, payloads, topic names provided by users, or participant
identifiers are metric tags. Outcomes are fixed, low-cardinality strings.

## Configuration

New Meter scopes are registered by adding `AddMeter(...)` inside existing **opt-in,
Development-only** metrics configuration classes. No configuration keys, packages,
EF migrations, Docker services or global application behavior are changed.

To export metrics later, the existing setting `Observability:Metrics:Enabled=true` and
`Observability:Metrics:Otlp:Enabled=true` is required as documented in M4.3A1.
M4.3D will provide Prometheus/Grafana setup.

## Verification

1. `dotnet build ConferenceProFlow.slnx --configuration Debug`
2. `dotnet test tests/ConferenceProFlow.Registration.UnitTests/ConferenceProFlow.Registration.UnitTests.csproj --configuration Debug`
3. `dotnet test tests/ConferenceProFlow.Allocation.UnitTests/ConferenceProFlow.Allocation.UnitTests.csproj --configuration Debug`

These MeterListener tests require no Docker. Later, run the Registration and Allocation
integration suites **separately** with `-- --parallel none`; do not launch the whole
solution's Testcontainers suite concurrently on a modest development workstation.

## Known limitations and follow-up

- M4.3C3B: instrument the Registration → RabbitMQ registration-request publisher.
- M4.3C3C: SQL Outbox backlog snapshots (pending/due/quarantined) and failure semantics.
- M4.3D: Prometheus/Grafana and true end-to-end export verification.
- M4.3E: focused integration regression, GitHub Actions and documentation review.

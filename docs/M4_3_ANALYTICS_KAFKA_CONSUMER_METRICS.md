# M4.3C2 — Analytics durable Kafka consumer metrics

## Scope

This change instruments **live durable Analytics Kafka consumption**, without changing Kafka subscription, SQL processing, quarantine, replay behavior, retries, or offset commit order. The existing M4.2 trace instrumentation remains in place.

The `AnalyticsKafkaConsumerMetrics` class emits best-effort operational measurements through `System.Diagnostics.Metrics` (`Meter`), using a dedicated Analytics meter.

| Instrument | Type | Unit | `result` values |
| --- | --- | --- | --- |
| `conferenceproflow.analytics.kafka.handling.attempts` | Counter<long> | `1` | `applied`, `duplicate_message`, `duplicate_business_event`, `quarantined`, `error` |
| `conferenceproflow.analytics.kafka.handling.duration` | Histogram<double> | `s` | Same as above |
| `conferenceproflow.analytics.kafka.offset.commit.attempts` | Counter<long> | `1` | `success`, `error` |

## Measurement boundaries

1. `KafkaAnalyticsRecordHandler.HandleAsync` performs SQL-durable business handling, duplicate recognition, or quarantine, **before** Kafka commit.
2. The handling stopwatch includes the scoped handler lifetime and its disposal; it **does not** include Kafka offset commit.
3. A returned disposition records one handling-attempt counter increment and one duration value. A handling exception records `error` and preserves the existing restart/replay semantics.
4. After durable handling, `consumer.Commit(record)` runs. A separate counter records whether the call returned normally or threw; a commit exception still forces consumer-session restart.
5. Host shutdown cancellation is excluded from error measurements.

**Important:** these are **attempt counters**. A failed Kafka commit can cause the record to be replayed and the SQL handler to return a duplicate result later. Neither handling-attempt nor offset-commit metrics equals the number of unique registrations or exactly-once deliveries. Database records are the source of truth.

`Quarantined` means the quarantine insert has been persisted by the handler. These instruments do not include replay-mode operations or the Kafka observe-only/preview consumer.

## Cardinality and safety

The only current metric label is the bounded `result` value. No Kafka offsets, topic names, partition IDs, message IDs, registration IDs, participant information, payloads, exception types or credentials are metric labels. Exporter availability does not decide business behavior.

## Configuration

Same opt-in switches as Registration and Allocation M4.3 metrics:

```powershell
$env:Observability__Metrics__Enabled='true'
```

The meter is then collected by the existing OpenTelemetry provider; it will produce measurements when the **durable live consumer is enabled** and records are processed.

When Prometheus OTLP ingestion is configured in a later M4.3 step:

```powershell
$env:Observability__Metrics__Otlp__Enabled='true'
$env:Observability__Metrics__Otlp__Endpoint='http://127.0.0.1:9090/api/v1/otlp/v1/metrics'
```

The OTLP exporter is not enabled by default. Config validation requires a development environment, an HTTP(S) endpoint with the expected path, and no credentials/query/fragment. The Prometheus OTLP receiver must be explicitly enabled and operational before end-to-end testing; no Prometheus/Grafana outcome is claimed by this increment.

Jaeger traces remain independently configured under `Observability:Otlp:*` (traces endpoint `:4318/v1/traces`).

## Local verification

Use the existing feature branch `feature/metrics-and-dashboards`, then run:

```powershell
dotnet build ConferenceProFlow.slnx --configuration Debug
```

Run only the fast test class (Docker is not required):

```powershell
dotnet test tests/ConferenceProFlow.Analytics.IntegrationTests/ConferenceProFlow.Analytics.IntegrationTests.csproj --configuration Debug -- --parallel none --filter-class "*AnalyticsKafkaConsumerMetricsTests"
```

After a successful result, the wider Analytics integration test suite may be executed **separately** later (not concurrently with other integration projects):

```powershell
dotnet test tests/ConferenceProFlow.Analytics.IntegrationTests/ConferenceProFlow.Analytics.IntegrationTests.csproj --configuration Debug -- --parallel none
```

In this increment the fast tests verify instrument names, fixed outcome labels, unit values and basic emission; they do **not** prove Kafka/SQL end-to-end measurement or OTLP/Prometheus ingestion. That verification belongs to the later M4.3 validation steps.

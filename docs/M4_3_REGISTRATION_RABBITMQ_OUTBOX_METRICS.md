# M4.3C3B — Registration → RabbitMQ Outbox publication metrics

## Scope

Extends the existing M4.3C3A `RegistrationOutboxPublishMetrics` Meter with the
`registration_request_rabbitmq` pipeline. No new Meter registration, new NuGet
packages, EF migrations, or Docker Compose changes are required.

## Instruments

- `conferenceproflow.outbox.publish.outcomes` (Counter<long>, unit `1`)
- `conferenceproflow.outbox.publish.duration` (Histogram<double>, unit `s`)

Fixed, low-cardinality labels: `pipeline=registration_request_rabbitmq` and
`outcome=published|retry_scheduled|quarantined`.

The existing `analytics_kafka` pipeline methods and labels remain available.

## Boundaries

- `published`: RabbitMQ broker acknowledgement **and** successful SQL SaveChanges
  of the Outbox's published state.
- `retry_scheduled`: failed/uncertain broker operation and successful SQL
  SaveChanges of retry state.
- `quarantined`: invalid payload detected before publishing, with quarantine
  state successfully committed to SQL (NOT a broker publish attempt).
- Normal application shutdown (`OperationCanceledException` with the host's
  cancellation token canceled) is not recorded as a publication failure.
- If SQL SaveChanges fails, no matching success/retry/quarantine metric is
  emitted. These metrics are best-effort, not a transactional audit log.
- Publishing may succeed even if saving the published state fails; this is an
  inherent at-least-once delivery window, and is **not counted as published**
  until the state is persisted.

`duration` measures a single Outbox message-processing attempt until the associated
state is committed; it includes validation, connection/publish/confirmation
and persistence work when applicable, but excludes poll wait and backlog delay.

## Verification

```powershell
dotnet build ConferenceProFlow.slnx --configuration Debug
dotnet test tests/ConferenceProFlow.Registration.UnitTests/ConferenceProFlow.Registration.UnitTests.csproj --configuration Debug
```

A full integration regression will be performed during M4.3E with integration
projects run separately (`--parallel none`) to avoid resource contention.

## Note on test coverage

The unit tests verify measurement names, values, units (via defined source), and
low-cardinality tags for both Kafka and RabbitMQ pipelines. They do not prove
that the hosted publisher reaches RabbitMQ or that Prometheus ingests metrics;
these are later integration/E2E checks.

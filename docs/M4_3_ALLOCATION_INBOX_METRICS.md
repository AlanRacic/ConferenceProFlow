# M4.3C1 — Allocation SQL Inbox Processing Metrics

## Scope and semantics

This increment adds **per-attempt** telemetry to the Allocation Worker SQL Inbox processor.
It does not modify SQL transactions, RabbitMQ acknowledgements, retry policy, capacity checks or business decisions.
The existing M4.2 `ActivityLink`-based distributed tracing is preserved.

| Metric | Instrument | Unit | `result` values |
| --- | --- | --- | --- |
| `conferenceproflow.allocation.inbox.processing.attempts` | Counter<long> | `1` | `confirmed`, `rejected`, `already_processed`, `existing_allocation`, `waiting_for_inventory`, `not_found`, `error`, `other` |
| `conferenceproflow.allocation.inbox.processing.duration` | Histogram<double> | `s` | Same bounded values |

- A measurement is recorded when `AllocationProcessingService.ProcessAsync` **returns** or throws an exception handled by the Inbox processor.
- The duration covers the actual `ProcessAsync` call, **not** RabbitMQ delivery, polling, inbox age or downstream publisher latency.
- A result of `waiting_for_inventory` or `not_found` represents one attempt and may appear again on a later retry.
- `already_processed` / `existing_allocation` represent attempts, **not additional business decisions**.
- Shutdown-triggered cancellation does not increment attempts or record a duration.
- Any SQL failure before the per-message processing loop (e.g. polling) is logged but not included in this per-message metric.
- Metrics are best-effort and in-memory before export; do **not** use these counts as the durable source of truth for allocations.
- Only one **bounded** tag (`result`) is used. Message IDs, participant IDs, session IDs, SQL exception text and other high-cardinality/sensitive details are excluded.

## OpenTelemetry configuration

`AllocationMetricsConfiguration` reuses M4.3A1's opt-in settings, with a separate worker meter:

```text
Observability__Metrics__Enabled=true
Observability__Metrics__Otlp__Enabled=false
```

When Prometheus's OTLP receiver is configured in a later M4.3 increment:

```text
Observability__Metrics__Otlp__Enabled=true
Observability__Metrics__Otlp__Endpoint=http://127.0.0.1:9090/api/v1/otlp/v1/metrics
```

Only enable OTLP when the receiver is available; metric collection is Development-only in this local implementation. Do not send metrics to Jaeger's trace ingestion endpoint.

## Implementation touch points

- New: `Observability/AllocationInboxMetrics.cs`
- New: `Observability/AllocationMetricsConfiguration.cs`
- Updated: `Program.cs` — opt-in metrics registration alongside existing tracing.
- Updated: `Processing/AllocationInboxProcessorService.cs` — timestamp and outcome measurement; unchanged processing logic.
- New: `tests/.../Observability/AllocationInboxMetricsTests.cs`

## Validation

```powershell
dotnet build ConferenceProFlow.slnx --configuration Debug
```

```powershell
dotnet test tests/ConferenceProFlow.Allocation.UnitTests/ConferenceProFlow.Allocation.UnitTests.csproj --configuration Debug
```

No Docker is required for these checks. Full integration tests are performed **project by project** with `--parallel none`; never invoke all Testcontainers SQL Server projects in parallel on a limited local test environment.

These tests verify instrument emission, labels and measurement units. They do **not** verify Prometheus export; that comes with the metrics pipeline and dashboards later in M4.3.

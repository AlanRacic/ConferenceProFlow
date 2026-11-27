# M4.3 — Metrics Foundation (A1 / Registration first increment)

## Context and scope

M4.2 added distributed traces to Registration, Allocation and Analytics. M4.3 adds **metrics**, starting with Registration. This is a staged implementation, not a claim that the full Prometheus/Grafana pipeline is already operating.

### Measurements implemented in this increment

| Instrument | Type | Unit | Source | Tags |
| --- | --- | --- | --- | --- |
| `http.server.request.duration` | ASP.NET Core histogram | seconds | Request to HTTP response, via ASP.NET Core instrumentation | framework HTTP tags |
| `conferenceproflow.registration.finalized` | Counter | 1 | Only after a successful SQL commit that finalizes a Pending registration | `outcome=confirmed/rejected` |
| `conferenceproflow.registration.finalization.duration` | Histogram | seconds | `UpdatedAtUtc - CreatedAtUtc`, recorded only after the above commit | `outcome=confirmed/rejected` |

**Important difference:** HTTP request duration measures accepting/rejecting the *HTTP request*. Finalization duration measures total time between persisted registration creation and final decision. The latter includes asynchronous waiting/processing; it is not the duration of a RabbitMQ handler.

## Correctness and tradeoffs

- Metric recording occurs **after** `SaveChangesAsync` commits the final registration state, result Inbox and Analytics Outbox.
- Duplicates, conflicts and failed SQL transactions do not emit a finalization counter or duration.
- Counters are **best-effort operational telemetry**, not an exactly-once business ledger. A crash between SQL commit and metric emission can omit a measurement; the database remains authoritative.
- No message ID, registration ID, participant ID, trace ID, email or exception text is used as a metric label. `outcome` has two allowed values.
- Histograms initially have no customized buckets; evaluate practical boundaries with controlled workloads before adjusting.
- The metric pipeline is disabled by default; no new DB migration, broker or external system is required for compilation/tests.

## Planned metrics export architecture

```text
Registration API + Allocation Worker + Analytics Worker
        OpenTelemetry SDK (traces and metrics)
                |                        |
       traces: OTLP/HTTP              metrics: OTLP/HTTP
                |                        |
          Jaeger :4318          Prometheus OTLP receiver :9090
                                         |
                                   Grafana :3000
```

This selects **direct OTLP/HTTP export to Prometheus** rather than introducing an OpenTelemetry Collector or an HTTP scrape endpoint in every Worker. Prometheus must use `--web.enable-otlp-receiver` (to be implemented/validated in M4.3D). Grafana will query Prometheus.

### Current configuration (Registration API only)

Set only in Development, using environment variables or User Secrets as appropriate:

| Setting | Default | Description |
| --- | --- | --- |
| `Observability__Metrics__Enabled` | false | Enable the local OTel metric provider and instruments. |
| `Observability__Metrics__Otlp__Enabled` | false | Enable export to the Prometheus OTLP receiver. Keep false until Prometheus is running. |
| `Observability__Metrics__Otlp__Endpoint` | `http://127.0.0.1:9090/api/v1/otlp/v1/metrics` | Local development OTLP/HTTP metrics destination. |

The existing Jaeger trace settings remain **unchanged** and separate. Metrics are disabled outside `Development` for this first local implementation.

## Next increments

1. Verify Registration build, focused unit test and the existing Registration tests without simultaneous heavy integration suites.
2. Add equivalent metric providers and instrumentation in Allocation and Analytics workers (M4.3C), with low-cardinality bounded dispositions and clear retry semantics.
3. Add a sampled SQL Outbox backlog gauge, avoiding database queries directly from synchronous ObservableGauge callbacks.
4. Provide optional Prometheus/Grafana Compose, validated dashboard and example metrics from a real local workflow (M4.3D–E).
5. Keep performance benchmarking and statistical p95/p99 claims for M4.4.

## References

- `docs/OBSERVABILITY_PERFORMANCE_DESIGN.md` — original metric names and phase definitions.
- OpenTelemetry .NET: https://opentelemetry.io/docs/languages/dotnet/metrics/getting-started-prometheus-grafana/
- ASP.NET Core metrics: https://learn.microsoft.com/aspnet/core/metrics/overview?view=aspnetcore-10.0

# M4.3 — Metrics & Dashboards: Validation Report

**Project:** ConferenceProFlow  
**Milestone:** M4 — Observability & Performance / M4.3 — Metrics & Dashboards  
**Prepared:** 2026-11-27  
**Branch under review:** `feature/metrics-and-dashboards`  
**State:** Local validation reported successful; final Git content review and Pull Request CI are **not yet recorded** in this report.

## 1. Scope and implementation

M4.3 extends M4.2 distributed tracing with **OpenTelemetry metrics** in three .NET services and a reproducible local metrics backend. The existing durable Inbox/Outbox architecture, SQL processing, RabbitMQ/Kafka delivery behavior, and Jaeger traces remain distinct from the monitoring system.

| Area | Instrumentation / component | Operational meaning |
| --- | --- | --- |
| Registration API | `http.server.request.duration` | HTTP response latency; **not** end-to-end business latency. |
| Registration API | `conferenceproflow.registration.finalized` | Committed finalizations, tagged `confirmed` / `rejected`. |
| Registration API | `conferenceproflow.registration.finalization.duration` | Creation-to-final-decision elapsed time, in seconds. |
| Allocation Worker | `conferenceproflow.allocation.inbox.processing.attempts` and `.duration` | SQL Inbox processing **attempts** and duration, tagged by bounded result. |
| Analytics Worker | `conferenceproflow.analytics.kafka.handling.attempts` and `.duration` | Durable Kafka record handling **attempts** and duration. |
| Analytics Worker | `conferenceproflow.analytics.kafka.offset.commit.attempts` | Separate Kafka offset commit success/error attempts. |
| Outbox publishers | `conferenceproflow.outbox.publish.outcomes` and `.duration` | Persisted publication outcomes for Registration RabbitMQ, Allocation Result RabbitMQ, and Analytics Kafka pipelines. |
| SQL Outboxes | `conferenceproflow.outbox.backlog.pending` and `.oldest.age` | Sampled pending row count and oldest pending row age (seconds). |
| Backend | Prometheus OTLP/HTTP receiver + Grafana | Local storage/querying and provisioned dashboard; Jaeger continues to handle traces. |

Pipeline labels use the bounded values `registration_request_rabbitmq`, `allocation_result_rabbitmq`, and `analytics_kafka`. Measurements avoid individual participant, registration, message and trace identifiers as metric labels.

## 2. Local verification status

The following results were **reported during the M4.3 development walkthrough**. Detailed CI run identifiers, terminal transcripts, test totals for four projects, timings, and exact warning counts have not been independently archived here.

| Validation | Reported outcome | Notes |
| --- | --- | --- |
| Registration Unit Tests (Debug) | **Passed** | Includes metrics instrumentation tests. |
| Allocation Unit Tests (Debug) | **Passed** | Includes Inbox, publication, and backlog instrumentation tests. |
| Registration Integration Tests (Debug) | **Passed** | Includes finalization metrics integration scenario. |
| Allocation Integration Tests (Debug) | **Passed** | Run separately to avoid competing Testcontainers SQL instances. |
| Analytics Integration Tests (Debug) | **Passed on final full-suite rerun** | The earlier 78-test run had 77 pass and 1 fail; see issue note below. |
| Full solution Release Build | **Passed** | Exact warning count not recorded in this report. |
| Local Registration → Allocation → Analytics E2E | **Reported successful** | Demo used a fresh session with capacity 2 and 3 synthetic registrations; validate business outcomes using SQL source-of-truth queries. |
| Prometheus metrics ingestion | **Reported successful** | HTTP and business/worker/Outbox metric series observed in local verification. |
| Grafana dashboard | **Reported successful** | Original dashboard plus low-traffic raw-counter verification panels. |
| Git diff review / inclusion / secrets check | **Pending evidence review** | Perform before staging and committing. |
| Pull Request GitHub Actions | **Pending** | Never describe the current M4.3 PR CI as passed before it actually runs. |

### Intermittent Kafka Replay test failure

An earlier isolated run of the Analytics integration project returned **77 passed / 1 failed (78 total)**. The failing test was `KafkaReplayRunnerIntegrationTests.MissingRetainedRecord_ResumeRefusesToDeclareCompleteOrSkipTheGap`. It expected `InvalidOperationException` but received `Confluent.Kafka.KafkaException: Broker: Not coordinator` while reading committed consumer-group offsets. The specific test **passed** when rerun alone; the user subsequently reported that the full Analytics suite and Release Build passed. No source changes were made to suppress, broaden, or skip that assertion.

**Follow-up:** If `Broker: Not coordinator` recurs in CI or later regression, investigate coordinator discovery and bounded retry for transient group-coordination errors. Do not declare the root cause proven or weaken the business-safety assertion just to achieve a green run.

## 3. End-to-end evidence and reproducibility

Use existing version-controlled runbooks rather than inventing new API contracts:

1. `docs/M4_3_APPLICATION_METRICS_E2E_VALIDATION.md` — configuration, services, expected outcomes and metric checks.
2. `docs/M4_3_PROMETHEUS_QUERIES.http` — raw Prometheus HTTP query examples.
3. `docs/OBSERVABILITY_E2E_DEMO.http` — create new conference/session, participants and registrations.
4. `docs/OBSERVABILITY_E2E_SEED.sql` — seed session capacity 2 in Allocation SQL.
5. `docs/OBSERVABILITY_E2E_CHECKS.sql` — query Registration, Allocation and Analytics SQL evidence.
6. `docs/M4_3_DASHBOARD_LOW_TRAFFIC_VALIDATION.md` — check base and additional raw-counter Grafana panels.
7. `docs/M4_3_PROMETHEUS_GRAFANA_LOCAL.md` — local Compose and provisioning guidance.

**Expected business invariant:** three unique registrations for a fresh capacity-two session result in two `Confirmed` and one `Rejected`, with reserved capacity **2**; SQL is authoritative. Metric counter totals may differ due to retries, earlier activity and process restarts. Do not infer exact business counts from operational counters alone.

## 4. Reliability and interpretation limits

- **Metrics are best-effort telemetry.** They can be lost between a durable SQL commit and in-memory metric emission/export; they are not an exactly-once audit trail.
- **Counters reflect defined events or attempts.** Publication metrics reflect persisted Outbox state transitions; Allocation and Kafka handling instruments include retry attempts, not only unique business operations.
- **Backlog gauges are sampled, not transactionally current.** The samplers query SQL approximately every 30 seconds; callbacks read cached values. If sampling stays stale beyond the configured threshold, no stale/false-zero sample is exported.
- **Histograms/rates need enough samples.** A three-registration demo can leave p95/rate panels sparse; the added raw counters help demonstrate low-volume activity. No benchmark or latency SLO is asserted by M4.3.
- **The metrics stack is a local development/demo configuration.** OTLP is opt-in in `Development`; the unauthenticated receiver must remain loopback-bound. Change initial Grafana credentials. Production monitoring, load test baselines and performance tuning are separate work.
- **Test orchestration:** Integration test projects should run **one at a time** locally using xUnit v3 `-- --parallel none`. This follows an earlier resource-contention incident from launching all Testcontainers-backed suites together; it does not limit application architecture or normal development.

## 5. Reproducible regression commands

Run in Visual Studio Developer PowerShell from `C:\work\ConferenceProFlow`. For integration tests leave Docker Desktop available and stop unnecessary development containers; do not delete Docker volumes.

```powershell
dotnet restore ConferenceProFlow.slnx
dotnet test tests/ConferenceProFlow.Registration.UnitTests/ConferenceProFlow.Registration.UnitTests.csproj --configuration Debug
dotnet test tests/ConferenceProFlow.Allocation.UnitTests/ConferenceProFlow.Allocation.UnitTests.csproj --configuration Debug
dotnet test tests/ConferenceProFlow.Registration.IntegrationTests/ConferenceProFlow.Registration.IntegrationTests.csproj --configuration Debug -- --parallel none
dotnet test tests/ConferenceProFlow.Allocation.IntegrationTests/ConferenceProFlow.Allocation.IntegrationTests.csproj --configuration Debug -- --parallel none
dotnet test tests/ConferenceProFlow.Analytics.IntegrationTests/ConferenceProFlow.Analytics.IntegrationTests.csproj --configuration Debug -- --parallel none
dotnet build ConferenceProFlow.slnx --configuration Release
```

Run commands sequentially; do not launch the whole solution's integration tests concurrently. A Release **build** is not a Release **test run**.

## 6. Pre-PR review and closeout

- [x] Local functional and metrics demonstration reported successful.
- [x] Registration Unit Tests passed.
- [x] Allocation Unit Tests passed.
- [x] Registration Integration Tests passed.
- [x] Allocation Integration Tests passed.
- [x] Analytics Integration Tests passed on subsequent full-suite rerun (after one transient failure in the earlier run).
- [x] Full solution Release Build reported successful.
- [ ] Inspect **all tracked and untracked files** and confirm M4.3 scope.
- [ ] Confirm `docs/M4_3_VALIDATION_REPORT.md` is included in Solution Items and version control.
- [ ] Ensure `.env`, User Secrets, passwords, local database files, binary outputs, `.trx`, and temporary diagnostics are not staged.
- [ ] Verify `git diff --check` and the staged diff.
- [ ] Commit implementation, monitoring configuration and docs in logical groups.
- [ ] Open Pull Request targeting `master` and link the actual M4.3 issue number using `Closes #<issue-number>`.
- [ ] Review GitHub Actions checks and PR diff; merge with **Create a merge commit** only after required checks pass.
- [ ] Verify issue closure and sync local `master` via Visual Studio Fetch → Pull.

**Completion criterion:** Treat M4.3 as closed only after the PR has been reviewed and merged and the linked issue is closed. M4.4 performance benchmarking is separate.

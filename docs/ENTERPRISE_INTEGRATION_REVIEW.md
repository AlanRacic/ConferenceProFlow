# ConferenceProFlow — Enterprise Integration Review

**Milestone:** M3 — Kafka & Event Analytics  
**Work item:** M3.6A — Architecture & Integration Audit (baseline)  
**Review date:** 2026-11-24  
**Status:** First-pass static audit; findings require disposition during M3.6B–F  
**Target branch:** `chore/enterprise-integration-hardening`

> **Scope and evidence boundary.** This review examines public ConferenceProFlow repository source and documentation, including the Registration and Allocation entry points and processing logic, Analytics entry point, the Kafka publishing and consumption implementations, replay code, Docker Compose and GitHub Actions workflow. Detailed portions of Kafka/Analytics source were also cross-checked against the previously supplied M3.3–M3.5 source packages. The author has **not cloned or run** the user's local solution as part of this review. Successful M3.5 local E2E, automated tests and CI were **reported by the developer**, not independently rerun for this report. The review is **not** a security certification, complete line-by-line audit or production-readiness declaration.

## 1. Executive assessment

The design demonstrates several sound distributed-systems choices: service-owned SQL data, transactional outbox/inbox, stable event identities, idempotent handlers, explicit message acknowledgments/offset commits following SQL persistence, concurrency protections and real-infrastructure integration tests. The implemented scope is appropriate for a **production-aware, locally reproducible .NET portfolio project**.

No confirmed critical loss-of-data defect was established in this first pass. A green test suite does not prove the absence of such a defect. The main remaining issues are **operational limitations, failure modes not yet fully qualified, and precision of documentation**. Prefer a few high-value tests or narrowly scoped corrections to adding frameworks or new services.

## 2. Architecture and invariants

| Boundary | Responsibility and inspected evidence | Important invariant | Assessment |
| --- | --- | --- | --- |
| Registration API + Registration SQL | HTTP registration lifecycle; Registration Outbox; Allocation Result Inbox and reconciliation | Finalization, receipt and Analytics Outbox event must be committed together | Architecture and reconciliation flow reviewed; concurrency/error paths still deserve targeted scenario review |
| RabbitMQ operational transport | Registration Requested → Allocation; Allocation Decided → Registration | Durable processing precedes broker ACK, repeat deliveries must be safe | Documented and tested; inspect publisher-return and parking-queue paths more deeply in M3.6B |
| Allocation Worker + Allocation SQL | Inventory, Inbox, Allocation decision and Result Outbox | Capacity is never exceeded; retry re-evaluates the decision | `AllocationProcessingService` shows bounded concurrency retries and `WaitingForInventory` state; positive |
| Kafka Publisher | Registration-owned Analytics Outbox → finalization topic | Preserve original event identity; mark published only after broker ACK | Confirmed in publisher implementation; known at-least-once ACK/SQL gap |
| Live Analytics Worker + Analytics SQL | Kafka event validation; processed ledger, aggregate counts and quarantine | SQL commit precedes Kafka offset commit; duplicates do not increment counts | Code paths inspected; targeted integration coverage reported |
| Replay Worker + isolated Replay SQL | Captured offset windows per partition; independent replay group and manifest | Never touch live group/database; do not claim success for incomplete windows | Safety checks and bounded runner inspected; isolated E2E and integration coverage reported |
| Local platform & CI | SQL Server, RabbitMQ, optional Kafka in Docker Compose; GitHub Actions build/test | Local, zero paid cloud; CI checks solution | Appropriate for local demo; not a production cluster |

## 3. Confirmed positive design choices

**P-01 — Service-owned persistence.** Registration, Allocation and Analytics use independently owned SQL databases and business workflows communicate through versioned events. There is no cross-service distributed SQL transaction.

**P-02 — Allocation consistency.** The Allocation processor returns `WaitingForInventory` when there is no provisioned inventory rather than falsely rejecting a registration. It retries a **whole decision** on recognized concurrency conflicts, rather than blindly replaying a stale `SaveChanges`.

**P-03 — Analytics SQL-first processing.** `AnalyticsEventProcessingService` writes the processed-event ledger and updates the session statistics within one SQL transaction. Transaction-scoped `sp_getapplock`, a SQL-atomic `ExecuteUpdateAsync` and uniqueness constraints protect the current single-active-instance design against important duplicate/concurrency cases.

**P-04 — Kafka offset discipline.** `KafkaAnalyticsConsumerService` has automatic offset commit/store disabled, and explicitly commits an offset only after the record handler succeeds with SQL processing, recognized duplicate or durable quarantine. If SQL or commit fails, it recreates the consumer and retries from broker-committed progress.

**P-05 — Publisher honesty.** `KafkaAnalyticsOutboxPublisherService` waits for `ProduceAsync` acknowledgement before saving `PublishedAtUtc` and preserves the original outbox payload/MessageId. It explicitly accepts possible duplicate delivery if the subsequent SQL update fails.

**P-06 — Replay fail-closed.** `AnalyticsReplaySafety` enforces distinct group naming, expected topic/database names and mutually exclusive live/replay modes. `KafkaAnalyticsReplayRunner` refuses unsafe resumes or false completion when retained Kafka records disappear. `ReplaySnapshotFile.SaveAsync` uses same-directory temporary write + move to reduce torn-manifest risk.

**P-07 — Local infrastructure boundaries.** Docker Compose binds the developer SQL Server, RabbitMQ and Kafka host ports to `127.0.0.1` and keeps Kafka optional under a profile. `.env` is excluded by `.gitignore`; `.env.example` is retained.

**P-08 — CI baseline.** GitHub Actions performs restore, Release build, Docker availability check and solution-wide automated tests on PRs and `master` pushes. Its success should be reported as a test gate, **not** a production deployment or full fault-tolerance proof.

## 4. Findings and decisions

Severity here means **priority for a local production-aware portfolio project**, not an assertion of an existing production incident. `Confirmed` denotes observed code/documentation behavior; `Risk` is a plausible consequence requiring evaluation.

| ID | Priority | Finding | Evidence | Disposition / proposed action |
| --- | --- | --- | --- | --- |
| M36-A01 | **P2** | **Replay manifest is local to OS temporary storage.** A cleanup/machine change can remove it and make an interrupted run impossible to resume, even though the replay SQL state and Kafka group survive. | `KafkaAnalyticsReplayRunner.GetManifestPath()` uses `Path.GetTempPath()` and stable hash of GroupId. | **Targeted hardening candidate:** evaluate configurable persistent *local* manifest directory, safe default and one test for missing manifest. No extra service required. Document recovery limitation until then. |
| M36-A02 | **P2** | **Poison Analytics Outbox records are retried, not automatically parked.** Invalid event type/payload enters the general publish-failure retry path; backoff is bounded, but no terminal quarantine or escalation threshold is implemented in the inspected publisher. | `KafkaAnalyticsOutboxPublisherService.PublishOneAsync()` catches validation/produce exceptions and calls `RecordPublishFailure()`, leaving the row pending. | **Targeted hardening candidate:** add a well-defined operator-visible failure state or escalation policy and test; do **not** silently drop or mark a failed event published. |
| M36-A03 | **P2** | **Outbox publishers are single-active-instance baselines.** Multiple replicas without claim/lease coordination can compete for pending rows and duplicate publish attempts. | Publisher class comments and query/update flow; documented limitations. | **Accept and document** as a conscious local topology limit. Do not build a full distributed claim/lease protocol unless M3.6 uncovers a real requirement. |
| M36-A04 | **P2 — documentation precision** | **Replay snapshot is bounded by partition offsets, not a globally atomic Kafka transaction.** Watermarks are queried sequentially per partition. A replay is complete for the captured per-partition windows, but not necessarily an exact global wall-clock instant. | `KafkaAnalyticsReplayRunner.CaptureSnapshot()` loops partitions and calls `QueryWatermarkOffsets()` separately. | **Document precisely**, retain existing behavior. Test post-snapshot event exclusion, already covered in M3.5E. |
| M36-A05 | **P2** | **Live Analytics multi-replica/rebalance behavior is not qualified.** The consumer is implemented and tested primarily as one active instance; rebalance boundaries and rolling updates deserve additional attention if later scaled. | `KafkaAnalyticsConsumerService` and known limitations in README. | **Accept current single-instance scope**; describe no multi-instance guarantee, possibly add one focused rebalance/failure test only if practical and relevant. |
| M36-A06 | **P2 / scope boundary** | **Session inventory provisioning still uses a guarded development SQL seed.** Newly created Registration sessions do not automatically become provisioned Allocation inventory. | `AllocationProcessingService` returns `WaitingForInventory`; README and E2E docs describe SQL seed. | **Document as a separate product gap**, not a defect introduced by M3.5. Do not hide it or silently enlarge M3.6 to implement cross-service provisioning. |
| M36-A07 | **P1 if publicly deployed; accepted local limitation** | **Public-service authentication/authorization and operational controls are incomplete.** | README explicitly states no authentication/authorization; Program.cs and local development documentation describe development posture. | **Do not expose this API as a production public endpoint.** Document risk clearly. Authentication is a separate feature, not an automatic M3.6 addition. |
| M36-A08 | **P3 / evidence to verify** | **Visible README status can lag behind current milestone.** One public rendered README snapshot still says “through M3.4,” while `LOCAL_DEVELOPMENT.md` and replay docs cover M3.5. GitHub-rendered/cached pages can differ. | Public README vs public local development/replay documents. | **Verify local `README.md` and current remote HEAD before editing.** If genuinely outdated, correct the milestone wording as a documentation-only change; do not overwrite newer user edits with an older copy. |
| M36-A09 | **P3 / CI improvement** | CI runs real tests but does not visibly define separate coverage, dependency-audit or reproducible full-stack smoke-test gates. | `.github/workflows/dotnet-ci.yml`: restore, build, docker info, solution test. | Existing gate is acceptable. Optionally add one low-maintenance check after identifying a concrete gap; avoid creating a brittle CI matrix for its own sake. |

### Priority recommendation

For M3.6B–E, prioritize **A01 and A02 only if we can implement and test them narrowly**, then clarify **A04** and **A08** in documentation. Treat A03, A05, A06 and A07 as **explicit known limits** unless new evidence raises their priority. Avoid a fourth service, Kubernetes, Kafka cluster HA or a broad security redesign in this milestone.

## 5. Failure matrix to carry into M3.6B

| Failure window | Current expected behavior | Existing evidence | Additional check worth considering |
| --- | --- | --- | --- |
| Registration DB commit succeeds; RabbitMQ down | Registration Request Outbox remains pending | M2 outbox and recovery tests reported | Confirm no false “published” flag on broker failure |
| Allocation Worker unavailable | Registration remains Pending, processes after recovery | M2 manual E2E reported | Verify inbox/retry behavior when inventory is missing |
| Allocation DB concurrency collision | Retry fresh decision, never overbook | Allocation processor source + tests reported | Review final exhaustion and surfaced errors |
| Allocation result received twice | Duplicate/decision conflict recognized | Reconciliation source + tests reported | Ensure ACK happens only after durable classification |
| Kafka ACK succeeds; SQL publication flag fails | Event may be sent twice with same MessageId | M3.3 explicit integration test reported | Keep this as regression invariant |
| Analytics SQL fails before transaction commit | Kafka offset not committed, retry on recovery | M3.4 consumer source + tests reported | Check replay after consumer restart |
| SQL commits; Kafka consumer offset commit fails | Redelivery; SQL ledger prevents double count | M3.4 code/design, tests reported | Keep this as explicit documented at-least-once window |
| Malformed/contradictory Kafka record | SQL quarantine; only then offset commit | Handler and quarantine source + tests reported | Verify bounded payload/reason fields |
| Replay interrupted after snapshot | Same snapshot/group resume, committed offsets authoritative | M3.5 source + six tests reported | Test manifest loss separately if A01 selected |
| Retention removes required offset | Replay must fail, not silently claim completion | M3.5 runner + retention integration case reported | Preserve this test in CI |

## 6. Reproducibility and configuration checklist

Read-only checks for the next stage:

- Existing `ConferenceProFlow.slnx` references only files actually in the project; no machine-specific external `Solution Items` paths.
- No nested accidental `docs/docs` folder; current Markdown links resolve.
- `.env` and User Secrets stay outside Git; `dotnet user-secrets` remains the local secret mechanism.
- `docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka config` validates interpolation without restarting any containers; beware: command output may include interpolated secrets, **do not paste its output publicly**.
- Each SQL migration targets only its owning service database, with `-- --replay` reserved for the guarded replay database.
- Current Analytics replay manifest and local Kafka data are **not deleted as part of audit or test cleanup**.
- Full Release build and solution tests run with Docker Desktop, but a green run is not a substitute for a fresh-clone walkthrough.

## 7. Recommended next steps

**M3.6B — Correctness & Resilience:** Walk the failure matrix against exact code branches and test names. Confirm or downgrade candidate defects; implement nothing until a reproducible gap is identified.

**M3.6C — Configuration/Security Hygiene:** Verify actual local/remote `.env` tracking, secrets exclusions, mode switches, SQL target guards and source-file paths. Keep public deployment out of scope.

**M3.6D — Local Reproducibility:** Perform a cold-start/fresh-clone walkthrough with documented commands and sample data; identify missing prerequisites without changing existing databases unnecessarily.

**M3.6E — Targeted Hardening:** Make only justified changes, each with a test that fails before the fix and passes after it. Candidates: manifest durability and poison-outbox escalation.

**M3.6F — Final Validation:** Solution Release build, all tests, `git diff --check`, review new documentation, GitHub Actions PR checks, final milestone close-out.

## 8. Reviewed sources (representative)

- [Registration API entry point](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Registration.Api/Program.cs)
- [Allocation Worker entry point](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Allocation.Worker/Program.cs)
- [Allocation processing](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Allocation.Worker/Processing/AllocationProcessingService.cs)
- [Registration reconciliation](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Registration.Api/Infrastructure/Inbox/AllocationResultReconciliationService.cs)
- [Analytics Worker entry point](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Analytics.Worker/Program.cs)
- [Kafka Analytics publisher](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Registration.Api/Infrastructure/Outbox/KafkaAnalyticsOutboxPublisherService.cs)
- [Kafka Analytics consumer](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Analytics.Worker/Messaging/Consumers/KafkaAnalyticsConsumerService.cs)
- [Bounded replay runner](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Analytics.Worker/Replay/KafkaAnalyticsReplayRunner.cs)
- [Replay snapshot](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Analytics.Worker/Replay/ReplaySnapshot.cs)
- [Docker Compose](https://github.com/AlanRacic/ConferenceProFlow/blob/master/infrastructure/compose.yaml)
- [GitHub Actions CI](https://github.com/AlanRacic/ConferenceProFlow/blob/master/.github/workflows/dotnet-ci.yml)
- [README](https://github.com/AlanRacic/ConferenceProFlow/blob/master/README.md), [Local development](https://github.com/AlanRacic/ConferenceProFlow/blob/master/docs/LOCAL_DEVELOPMENT.md) and [Replay design](https://github.com/AlanRacic/ConferenceProFlow/blob/master/docs/KAFKA_REPLAY_DESIGN.md)

---

**Audit outcome:** Baseline recorded. No code changes or database migrations are authorized by this document alone. The next step is risk-based verification (M3.6B), not a redesign.

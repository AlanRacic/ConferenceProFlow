# ConferenceProFlow — Reliable Kafka Publishing (M3.3)

**Scope:** Registration-side `RegistrationFinalizedV1` event contract, SQL Analytics Transactional Outbox, and background Kafka publisher.  
**Status:** Implemented and locally verified in M3.3; full solution regression/PR CI still needs to be confirmed for release.  
**Prerequisite:** M2.6 Registration ↔ Allocation workflow and M3.2 local Kafka infrastructure.  
**Not implemented in M3.3:** Analytics Worker, Kafka consumer, analytics SQL projections, application-level offset management, or replay processing.

## 1. Purpose and end-to-end flow

ConferenceProFlow publishes **committed Registration finalization events**, not provisional Allocation decisions. Existing RabbitMQ messaging still owns the operational registration/allocation process; Kafka is an additional, independent analytics event stream.

```text
Registration API --RegistrationRequestedV1--> RabbitMQ --> Allocation Worker
Registration API <--AllocationDecidedV1------ RabbitMQ <-- Allocation Worker
       |
       | final status changed (Pending -> Confirmed or Rejected)
       v
Registration SQL transaction (one EF Core SaveChangesAsync)
  |-- update Registrations
  |-- insert AllocationResultInboxMessages
  +-- insert AnalyticsOutboxMessages (RegistrationFinalizedV1)
       |
       v
KafkaAnalyticsOutboxPublisherService (optional hosted service)
       |
       | keyed by SessionId; payload and MessageId reused across retries
       v
Kafka topic: conferenceproflow.registration-finalized.v1
       |
       +-- delivery acknowledgement --> mark SQL PublishedAtUtc

Analytics Worker / projections: planned in M3.4, NOT part of M3.3.
```

There is **no SQL-to-Kafka distributed transaction**. An acknowledged record may be delivered again if saving `PublishedAtUtc` fails. The intended guarantee is at-least-once publication plus a stable logical event identity; a future consumer must deduplicate.

## 2. Versioned event contract

- .NET type: `RegistrationFinalizedV1`.
- Event type: `registration.finalized.v1`.
- Kafka topic: `conferenceproflow.registration-finalized.v1`.
- Kafka message key: `SessionId.ToString("D")` (canonical GUID string).
- Kafka value: originally persisted JSON payload, not re-serialized at send time.
- Kafka headers: `message-id` (GUID), `event-type` (`registration.finalized.v1`).
- Fields: `messageId`, `registrationId`, `sessionId`, `status` (`confirmed` or `rejected`), `occurredAtUtc` (UTC).
- Participant names and email addresses are intentionally excluded.

Example (fictional identifiers):

```json
{
  "messageId": "84f99e64-c60c-45aa-929b-1a267a33911f",
  "registrationId": "7be88de0-21e7-485b-9283-8eb2603b3112",
  "sessionId": "d7263cac-475f-40a3-b149-a8c4db117c69",
  "status": "confirmed",
  "occurredAtUtc": "2026-11-23T09:30:00+00:00"
}
```

One logical finalization event is created per Registration. The `AnalyticsOutboxMessages.RegistrationId` unique index reinforces this invariant. A duplicate RabbitMQ result / compatible repeated decision must not create a new Analytics Outbox event. The `MessageId` is generated **once** at finalization and must not change on a Kafka retry.

## 3. Registration database and publisher behavior

The `AddRegistrationAnalyticsOutbox` EF Core migration adds `dbo.AnalyticsOutboxMessages` to the **existing** `ConferenceProFlowRegistration` database. Do not recreate the migration if already applied. Do not manually add the table in SQL Server.

| Column | Meaning |
|---|---|
| `Id` | Durable `MessageId` (primary key) |
| `RegistrationId` | Unique registration business key and foreign key |
| `MessageType` | `registration.finalized.v1` |
| `Payload` | Original event JSON |
| `OccurredAtUtc` | Event timestamp |
| `PublishedAtUtc` | Set only after Kafka delivery acknowledgement |
| `PublishAttempts` | Number of completed success/failure attempt recordings |
| `LastError` | Bounded error description from the last failed attempt |
| `NextAttemptAtUtc` | Retry eligibility timestamp |
| `QuarantinedAtUtc` | Reserved for excluded/quarantined records; **automatic quarantine policy is not implemented** |

Publisher implementation: `Infrastructure/Outbox/KafkaAnalyticsOutboxPublisherService.cs`.

- Hosted background service, independently enabled from the two RabbitMQ services.
- Selects up to **20** eligible rows, ordered by occurrence time, approximately once every **1 second**.
- Uses `Confluent.Kafka` producer with `Acks=All`, `EnableIdempotence=true`, and `MessageTimeoutMs=15000`.
- Deserializes and validates the stored payload and event type/identity before attempting publication.
- Uses the original JSON and `MessageId` on every attempt.
- On delivery success, calls `MarkPublished()` and saves SQL state.
- On a failed delivery, records `LastError`, increments `PublishAttempts`, and schedules exponential backoff capped at **60 seconds**. The row stays pending.
- Publication-state SQL update failures are retried by a later poll; Kafka may already contain the record.
- Only a **single active publisher instance** is supported. Durable multi-instance leasing/claiming is not implemented.

Kafka `EnableIdempotence=true` helps with producer-side retry duplicates **within a producer session**; it does not make SQL Server + Kafka exactly-once.

## 4. Local prerequisites and configuration

From `C:\work\ConferenceProFlow` (repository root), start the Docker Compose profile and initialize topics:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d
```

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\infrastructure\init-kafka-topics.ps1
```

Inspect the trusted script before using an execution-policy override; respect organization policy. Alternatively run the script normally if permitted. See [Kafka Local Development](KAFKA_LOCAL_DEVELOPMENT.md).

In Visual Studio, right-click **ConferenceProFlow.Registration.Api → Manage User Secrets**. Preserve all existing SQL Server and RabbitMQ settings and add the following **sections inside the existing JSON root**:

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "RegistrationFinalizedTopic": "conferenceproflow.registration-finalized.v1"
  },
  "KafkaAnalyticsPublisher": {
    "Enabled": true
  }
}
```

The JSON above illustrates **only the Kafka sections**; do not replace the real `secrets.json` and remove existing connection strings. For a complete business demonstration, also enable the Registration API's `OutboxPublisher:Enabled` and `AllocationResultConsumer:Enabled`, and the Allocation Worker's `RabbitMqConsumer:Enabled`, `AllocationInboxProcessor:Enabled`, `AllocationOutboxPublisher:Enabled`. Maintain existing RabbitMQ credentials in User Secrets.

- On Windows-hosted Registration API use `localhost:9092`.
- For a client running within the same Compose network use `kafka:19092` (provided by the M3.2 listener configuration).
- When Kafka publication is not needed, set `KafkaAnalyticsPublisher:Enabled=false` or remove that flag. Registrations still finalize through RabbitMQ; analytics outbox events remain durable and pending.
- Do not commit `secrets.json`, `.env`, passwords, or actual credentials.

Launch the two .NET applications in **separate** Visual Studio terminals:

```powershell
dotnet run --project src/ConferenceProFlow.Registration.Api/ConferenceProFlow.Registration.Api.csproj --launch-profile https
```

```powershell
dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj --launch-profile ConferenceProFlow.Allocation.Worker
```

Registration API development HTTPS endpoint: `https://localhost:7217`.

## 5. Reproducible publishing demonstration

Use [`ALLOCATION_E2E.http`](ALLOCATION_E2E.http), [`ALLOCATION_E2E_SEED.sql`](ALLOCATION_E2E_SEED.sql), and [`ALLOCATION_E2E_DEMO.md`](ALLOCATION_E2E_DEMO.md). These files are from the established M2.6 test workflow.

1. Create a **fresh** conference and session using the HTTP requests. Note the returned SessionId.
2. In Visual Studio **SQL Server Object Explorer**, open an Allocation database query and run the **development-only** seed for that SessionId: `Capacity=2`, `ReservedCount=0`. Do not seed production databases.
3. Create three distinct participants and three registrations using the HTTP demo. Existing named HTTP response variables avoid manual GUID entry for most requests.
4. Repeat Registration GETs until processing settles: expected **2 Confirmed**, **1 Rejected**, **0 Pending** for the new session.
5. In **Registration SQL**, filter analytics outbox rows by the new SessionId; expect three distinct events, two `confirmed` and one `rejected`. Wait until the asynchronous Kafka publisher marks all three `PublishedAtUtc` values non-null.
6. Read the Kafka topic and find the corresponding three `messageId` values and SessionId key. The business topic **may already contain historical records, including the original M3.2 smoke event**; do not count those as new finalization events.

### 5.1. SQL Server Object Explorer verification (Registration database)

Run against `ConferenceProFlowRegistration`; replace only the SessionId placeholder:

```sql
DECLARE @SessionId uniqueidentifier = 'PASTE-NEW-SESSION-GUID-HERE';

SELECT
    Id AS MessageId,
    RegistrationId,
    MessageType,
    JSON_VALUE(Payload, '$.sessionId') AS SessionId,
    JSON_VALUE(Payload, '$.status') AS FinalStatus,
    OccurredAtUtc,
    PublishedAtUtc,
    PublishAttempts,
    LastError,
    NextAttemptAtUtc
FROM dbo.AnalyticsOutboxMessages
WHERE TRY_CONVERT(uniqueidentifier, JSON_VALUE(Payload, '$.sessionId')) = @SessionId
ORDER BY OccurredAtUtc, Id;
```

Expected: three rows for a successful capacity-2 demo; all marked published, with `MessageType=registration.finalized.v1`. `PublishAttempts` is commonly 1, but can be larger after recoverable failures.

Optional count check:

```sql
DECLARE @SessionId uniqueidentifier = 'PASTE-NEW-SESSION-GUID-HERE';

SELECT
    COUNT(*) AS Total,
    SUM(CASE WHEN JSON_VALUE(Payload, '$.status') = 'confirmed' THEN 1 ELSE 0 END) AS Confirmed,
    SUM(CASE WHEN JSON_VALUE(Payload, '$.status') = 'rejected' THEN 1 ELSE 0 END) AS Rejected,
    SUM(CASE WHEN PublishedAtUtc IS NOT NULL THEN 1 ELSE 0 END) AS Published
FROM dbo.AnalyticsOutboxMessages
WHERE TRY_CONVERT(uniqueidentifier, JSON_VALUE(Payload, '$.sessionId')) = @SessionId;
```

### 5.2. Read Kafka records (local development)

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic conferenceproflow.registration-finalized.v1 --from-beginning --timeout-ms 15000 --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true
```

This reads **all currently retained** records from all topic partitions, not just one session. Check the JSON `messageId`, `registrationId`, `sessionId`, `status`, and printed Kafka message key. Events sharing a SessionId normally go to the same partition under the configured key strategy; offsets are **partition-local**, not global. A timeout after reading the available records is not itself a failure.

## 6. Kafka outage and recovery demonstration

Keep SQL Server, RabbitMQ, Registration API and Allocation Worker running. Only stop Kafka:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka stop kafka
```

Submit another **new participant** and registration for a session whose inventory is already provisioned. If the session is full, the result should become `Rejected` with `Session capacity reached.` even while Kafka is unavailable.

Inspect the newest `dbo.AnalyticsOutboxMessages` record using the query above. While Kafka is offline:

- `PublishedAtUtc` remains `NULL`;
- `LastError`, `PublishAttempts`, and `NextAttemptAtUtc` may be populated after the publisher times out and records a failure;
- Registration business status remains finalized; do not manually resend the RabbitMQ decision.

Restart **only** Kafka:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d --no-deps kafka
```

After the broker is ready and the scheduled retry is eligible, the running publisher should send the **same durable event** and persist `PublishedAtUtc`. The `Id` and `Payload` should not change. The publisher itself does not need a restart for this scenario.

Do **not** use `docker compose down -v`, which would remove named data volumes. These checks demonstrate local outage recovery, not Kafka cluster high availability.

## 7. Automated tests

Docker Desktop is required for SQL Server and Kafka Testcontainers tests. The integration tests use **independent test containers and topics**; they do not need to reuse your local development database or existing business topic.

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release
```

```powershell
dotnet test tests/ConferenceProFlow.Registration.UnitTests/ConferenceProFlow.Registration.UnitTests.csproj --configuration Release --no-build --verbosity normal
```

```powershell
dotnet test tests/ConferenceProFlow.Registration.IntegrationTests/ConferenceProFlow.Registration.IntegrationTests.csproj --configuration Release --no-build --filter "FullyQualifiedName~AnalyticsOutboxReconciliationIntegrationTests|FullyQualifiedName~KafkaAnalyticsOutboxPublisherIntegrationTests" --verbosity normal
```

The **Kafka Publisher integration suite has four xUnit test cases**, consisting of two `[Theory]` cases (Confirmed and Rejected) and two `[Fact]` cases (unavailable broker/restart and Kafka ACK followed by injected SQL update failure). Important: a stale build can report only three cases; rebuild if test discovery is unexpectedly missing a case.

The fourth test uses an EF Core `SaveChangesInterceptor` **in the test host only** to reject the publication-status SQL update *after* Kafka acknowledges the message. It proves that the original Outbox event remains pending, can be published again, and keeps its original payload/MessageId. It does **not** prove exactly-once delivery.

Final pre-PR validation (run only after all M3.3F documentation changes are in place):

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal
```

The end-to-end cross-service demonstration above remains **manual**; the selected automated suites do not assert every deployment/runtime combination.

## 8. Troubleshooting

| Symptom | What to check |
|---|---|
| Kafka publisher does not start | User Secrets `KafkaAnalyticsPublisher:Enabled`, `Kafka:BootstrapServers`, `Kafka:RegistrationFinalizedTopic` and application startup logs |
| Registration finalizes but no analytics row exists | Confirm `AddRegistrationAnalyticsOutbox` migration and that the registration actually made a new `Pending` → final transition; inspect reconciliation errors |
| Analytics row is present but never published | Kafka broker health, topic initialization, connection listener, `NextAttemptAtUtc`, `LastError`, and service flag |
| Outbox `PublishAttempts` increases | Inspect bounded last error and Kafka availability; polling/retry are asynchronous |
| Duplicate records visible in Kafka | At-least-once publishing is expected if Kafka ACK succeeded but SQL update failed; compare stable `messageId` |
| Kafka topic contains old smoke JSON | Original M3.2 smoke record may be retained; future Analytics consumer must validate/quarantine or explicitly reset **development-only** topic with authorization |
| Kafka CLI consumer stops after a timeout | `--timeout-ms` ends an idle console consumer; check earlier output before treating it as failure |
| Test output shows only 3 instead of 4 cases | Check file `KafkaAnalyticsOutboxPublisherIntegrationTests.cs`, rebuild without `--no-build`, and inspect xUnit discovery |
| Script execution blocked in Windows | Review `init-kafka-topics.ps1` then use process-only `powershell.exe -NoProfile -ExecutionPolicy Bypass -File ...` if permitted |

## 9. Explicit limitations and next phase

- The Kafka publisher is **single-active-instance**: no database claim/lease mechanism for multi-replica coordination.
- Kafka locally uses one KRaft node, plaintext listeners, replication factor 1 and development-only storage. This is not an HA deployment.
- At-least-once publication allows duplicates. `EnableIdempotence` is not an end-to-end cross-system transaction.
- Permanent bad records are **not automatically quarantined** by this publisher; the `QuarantinedAtUtc` column exists, but a complete operational quarantine/replay workflow is not implemented.
- New finalized registrations produce new events; earlier finalized registrations are not backfilled by the schema migration.
- Analytics Worker, consumer group management, SQL projections, deduplication and application-level replay are **M3.4–M3.5**, not implemented here.
- Automatic SessionInventory provisioning, public authentication/authorization, production monitoring and distributed scaling remain outside this scope.

See [Kafka Analytics Design](KAFKA_ANALYTICS_DESIGN.md) for the full M3 architecture and [Kafka Local Development](KAFKA_LOCAL_DEVELOPMENT.md) for infrastructure-only checks.

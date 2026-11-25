# Kafka Analytics Outbox — Operational Visibility (M3.6E2)

## Scope

This change improves **diagnosis and escalation**, not message-delivery semantics. The Registration API continues to publish `RegistrationFinalizedV1` from `AnalyticsOutboxMessages` to Kafka with the existing at-least-once approach. A successful Kafka acknowledgement is still required before `PublishedAtUtc` is saved; a failed delivery remains durable and eligible for the existing retry/backoff cycle.

No EF Core migration, Kafka topic change, queue, API endpoint, paid service, or additional runtime dependency is required.

## New behavior

- The publisher classifies failures as `PayloadValidation`, `KafkaDelivery`, or `Unexpected` for diagnostics only.
- `PayloadValidation` means local contract / JSON / identity validation failed **before** calling Kafka. In practice it usually requires a correction or operator investigation.
- `KafkaDelivery` means the validated event reached the Kafka client but the client reported a Kafka exception. The root cause **can be temporary or persistent** (for example a broker outage or invalid broker configuration). Do not equate the category with a guarantee of transience.
- `Unexpected` covers other errors, for which no cause is assumed.
- `LastError` records the diagnostic category followed by the original exception type/message, with the existing 2,000-character persistence limit.
- Every failed attempt continues to produce a warning-level structured log with `MessageId`, category, attempt count and next retry timestamp. Logs do **not** include the event JSON payload.
- On reaching `KafkaAnalyticsPublisher:AlertAfterAttempts` (default **5**) and every 10 further attempts, the publisher emits an **error-level operational alert**. These are application logs, not an external pager or monitoring integration.
- The outbox record is **not** automatically quarantined, deleted, marked published or sent to a new queue because of these alerts. The existing retry logic remains active.

### Configuration

The default threshold needs no configuration. To override it in **Registration API User Secrets** (not Git):

```json
{
  "KafkaAnalyticsPublisher": {
    "Enabled": true,
    "AlertAfterAttempts": 5
  }
}
```

Merge these properties into the **existing** User Secrets JSON object; do not replace existing credentials/settings. Allowed threshold range: 1–10,000.

### SQL verification

Use Visual Studio **SQL Server Object Explorer** and select the **ConferenceProFlowRegistration** database. Run `docs/KAFKA_OUTBOX_OPERATIONAL_CHECKS.sql` (read-only):

1. Inspect overall publication-state distribution.
2. Identify still-pending rows with at least the configured number of failed attempts.
3. Inspect all pending / retry-scheduled rows, even those with no failed attempts.

`PublishAttempts` includes attempted sends and may include a successful publish attempt. Do not infer that a high historical counter means the row is currently stuck: always filter on `PublishedAtUtc IS NULL` and `QuarantinedAtUtc IS NULL`.

If **zero rows** appear in the attention query, no currently pending records meet that threshold. It does **not** prove that Kafka will always be available or that future events cannot become stuck.

### Operator workflow (local demonstration)

1. Inspect the API warning/error logs for structured identifiers and the failure category.
2. Find the corresponding row in `AnalyticsOutboxMessages` using `MessageId` and inspect `LastError`, `PublishAttempts` and `NextAttemptAtUtc`.
3. Verify Kafka availability, topic, broker configuration and payload contract as appropriate.
4. Fix the underlying cause and let the existing retry mechanism publish the **same original payload and MessageId**.
5. Verify `PublishedAtUtc` becomes non-null and `LastError` / `NextAttemptAtUtc` are cleared.

**Do not manually mark a record published, delete an event, rewrite its MessageId, or reset consumer offsets.** Those actions can cause lost data or misleading state. Keep manual remediation outside the scope of this portfolio milestone.

## Automated verification

The new `AnalyticsOutboxPublishAlertPolicyTests` cover: threshold bounds, escalation cadence, payload versus Kafka delivery diagnosis, text formatting, and durability of a repeatedly failed original event.

Run after Release Build:

```powershell
dotnet test tests/ConferenceProFlow.Registration.UnitTests/ConferenceProFlow.Registration.UnitTests.csproj --configuration Release --no-build --filter "FullyQualifiedName~AnalyticsOutboxPublishAlertPolicyTests" --verbosity normal
```

Then rerun the **four** existing Kafka publisher integration cases against Testcontainers (Kafka and SQL Server) to verify that normal delivery, broker failure/recovery, and the Kafka-ACK/SQL-failure scenario remain unchanged:

```powershell
dotnet test tests/ConferenceProFlow.Registration.IntegrationTests/ConferenceProFlow.Registration.IntegrationTests.csproj --configuration Release --no-build --filter "FullyQualifiedName~KafkaAnalyticsOutboxPublisherIntegrationTests" --verbosity normal
```

## Known limitations

- No external alert routing or dashboards; logs and read-only SQL remain the inspection tools.
- Error-category classification is intentionally conservative; not every Kafka exception is transient and not every unexpected error is permanent.
- Repeated validation failures remain pending and retry until the underlying issue is fixed. The error log raises visibility; it does not stop retry traffic.
- The publisher remains single-active-instance; multi-instance leasing is outside this milestone.
- `QuarantinedAtUtc` is an existing state field; this change **does not add an automatic producer-side quarantine transition**.

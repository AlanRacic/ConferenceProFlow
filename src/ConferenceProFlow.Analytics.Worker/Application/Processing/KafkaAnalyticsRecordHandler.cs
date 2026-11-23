using System.Text.Json;
using Confluent.Kafka;
using ConferenceProFlow.Analytics.Worker.Messaging.Contracts;

namespace ConferenceProFlow.Analytics.Worker.Application.Processing;

public enum KafkaAnalyticsRecordDisposition
{
    Applied,
    DuplicateMessage,
    DuplicateBusinessEvent,
    Quarantined
}

/// <summary>
/// Handles a SINGLE consumed Kafka record. Returning from HandleAsync means
/// SQL processing or quarantine has durably completed; only then may a
/// separate consumer caller commit the offset. SQL exceptions are propagated.
/// </summary>
public sealed class KafkaAnalyticsRecordHandler(
    AnalyticsEventProcessingService processor,
    AnalyticsQuarantineService quarantine)
{
    private const int MaxIncomingPayloadChars = 64 * 1024;

    public async Task<KafkaAnalyticsRecordDisposition> HandleAsync(
        ConsumeResult<string, string> record,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);

        if (!TryReadValidEvent(record, out var integrationEvent, out var error))
        {
            await quarantine.PersistAsync(record, error!, cancellationToken);
            return KafkaAnalyticsRecordDisposition.Quarantined;
        }

        // ProcessAsync owns the atomic SQL ledger/projection transaction.
        // Do NOT quarantine an SQL exception: let the consumer retry the record.
        var outcome = await processor.ProcessAsync(integrationEvent!, cancellationToken);
        switch (outcome.Outcome)
        {
            case AnalyticsProcessingOutcome.Applied:
                return KafkaAnalyticsRecordDisposition.Applied;

            case AnalyticsProcessingOutcome.DuplicateMessage:
                return KafkaAnalyticsRecordDisposition.DuplicateMessage;

            case AnalyticsProcessingOutcome.DuplicateBusinessEvent:
                return KafkaAnalyticsRecordDisposition.DuplicateBusinessEvent;

            case AnalyticsProcessingOutcome.Conflict:
                // The original business decision is untouched. A conflict is
                // durable only after this separate quarantine INSERT succeeds.
                await quarantine.PersistAsync(
                    record, "Conflicting finalization for a previously processed message or registration.",
                    cancellationToken);
                return KafkaAnalyticsRecordDisposition.Quarantined;

            default:
                throw new InvalidOperationException("Unknown Analytics processing outcome.");
        }
    }

    private static bool TryReadValidEvent(
        ConsumeResult<string, string> record,
        out AnalyticsRegistrationFinalizedV1? integrationEvent,
        out string? error)
    {
        integrationEvent = null;
        error = null;

        var payload = record.Message?.Value;
        if (string.IsNullOrWhiteSpace(payload))
        {
            error = "Kafka record has an empty or null JSON payload.";
            return false;
        }

        if (payload.Length > MaxIncomingPayloadChars)
        {
            error = "Kafka record exceeds the supported JSON payload size.";
            return false;
        }

        try
        {
            integrationEvent = JsonSerializer.Deserialize<AnalyticsRegistrationFinalizedV1>(payload);
            if (integrationEvent is null)
            {
                error = "Kafka record JSON deserialized to null.";
                return false;
            }

            integrationEvent.Validate();
        }
        catch (JsonException)
        {
            error = "Kafka record is not a valid registration.finalized.v1 JSON object.";
            return false;
        }
        catch (ArgumentException)
        {
            error = "Kafka record has invalid registration.finalized.v1 fields.";
            return false;
        }

        // The M3.3 publisher uses Guid.ToString("D") as the Kafka message key.
        // Compare canonical spellings, not merely Guid.Parse, so formatting
        // drift and a missing or wrong partition key are rejected.
        var canonicalKey = integrationEvent.SessionId.ToString("D");
        if (!string.Equals(record.Message?.Key, canonicalKey, StringComparison.Ordinal))
        {
            error = "Kafka message key does not match the canonical sessionId.";
            return false;
        }

        return true;
    }
}

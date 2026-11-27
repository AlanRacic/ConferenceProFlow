using System.Diagnostics.Metrics;
using ConferenceProFlow.Registration.Api.Domain.Enums;

namespace ConferenceProFlow.Registration.Api.Observability;

/// <summary>
/// Low-cardinality business metrics. These are best-effort telemetry signals,
/// not a durable business ledger. They must never determine a registration outcome.
/// </summary>
internal static class RegistrationMetrics
{
    internal const string MeterName = "ConferenceProFlow.Registration.Api.Metrics";

    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> FinalizedCounter = Meter.CreateCounter<long>(
        "conferenceproflow.registration.finalized",
        unit: "1",
        description: "Registrations transitioned from Pending to a final status and committed.");

    private static readonly Histogram<double> FinalizationDuration = Meter.CreateHistogram<double>(
        "conferenceproflow.registration.finalization.duration",
        unit: "s",
        description: "Elapsed seconds from persisted registration creation to final status.");

    /// <summary>
    /// Call ONLY after the transaction persisting Pending -> Confirmed/Rejected has committed.
    /// The metric can be lost if the process stops immediately after commit; the database
    /// remains the source of truth. No registration/message IDs are used as metric labels.
    /// </summary>
    internal static void RecordFinalized(
        RegistrationStatus status,
        DateTimeOffset createdAtUtc,
        DateTimeOffset finalizedAtUtc)
    {
        var outcome = status switch
        {
            RegistrationStatus.Confirmed => "confirmed",
            RegistrationStatus.Rejected => "rejected",
            _ => null
        };

        if (outcome is null)
            return;

        var durationSeconds = (finalizedAtUtc - createdAtUtc).TotalSeconds;
        if (!double.IsFinite(durationSeconds) || durationSeconds < 0)
            return;

        var outcomeTag = new KeyValuePair<string, object?>("outcome", outcome);
        FinalizedCounter.Add(1, outcomeTag);
        FinalizationDuration.Record(durationSeconds, outcomeTag);
    }
}

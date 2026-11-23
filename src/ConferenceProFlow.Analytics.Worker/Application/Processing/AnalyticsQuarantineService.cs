using Confluent.Kafka;
using ConferenceProFlow.Analytics.Worker.Domain.Entities;
using ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Analytics.Worker.Application.Processing;

/// <summary>
/// Durably records one problem record per Kafka log position. A successful
/// SaveChanges is required before the caller may commit its Kafka offset.
/// This service NEVER swallows an SQL failure.
/// </summary>
public sealed class AnalyticsQuarantineService(AnalyticsDbContext db)
{
    private const int MaxKeyChars = 256;
    private const int MaxPayloadChars = 4096;
    private const int MaxReasonChars = 500;

    public async Task PersistAsync(
        ConsumeResult<string, string> record,
        string reason,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);

        // Re-reading the same position after a failed Kafka offset commit is safe.
        var alreadySaved = await db.QuarantinedAnalyticsEvents.AsNoTracking()
            .AnyAsync(x => x.Topic == record.Topic &&
                           x.Partition == record.Partition.Value &&
                           x.Offset == record.Offset.Value, cancellationToken);
        if (alreadySaved)
            return;

        db.QuarantinedAnalyticsEvents.Add(new QuarantinedAnalyticsEvent
        {
            Topic = record.Topic,
            Partition = record.Partition.Value,
            Offset = record.Offset.Value,
            MessageKey = Bound(record.Message?.Key, MaxKeyChars),
            Payload = Bound(record.Message?.Value ?? "<null>", MaxPayloadChars)!,
            Reason = Bound(reason, MaxReasonChars)!,
            ReceivedAtUtc = DateTimeOffset.UtcNow
        });

        // One atomic insert with a DB primary key (topic, partition, offset).
        // SQL unavailability and other failures MUST propagate: no offset commit.
        await db.SaveChangesAsync(cancellationToken);
    }

    private static string? Bound(string? value, int maxCharacters) =>
        value is null || value.Length <= maxCharacters
            ? value
            : value[..maxCharacters];
}

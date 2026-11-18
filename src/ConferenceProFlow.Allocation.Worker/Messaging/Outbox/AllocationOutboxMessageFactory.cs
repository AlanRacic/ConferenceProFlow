using System.Text.Json;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.Worker.Messaging.Outbox;

/// <summary>
/// Creates a single immutable payload for durable storage; retries will publish this stored payload and MessageId rather than building a new event.
/// </summary>
public static class AllocationOutboxMessageFactory
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static AllocationOutboxMessage Create(AllocationEntity allocation)
    {
        ArgumentNullException.ThrowIfNull(allocation);

        var messageId = Guid.NewGuid();
        var occurredAtUtc = DateTimeOffset.UtcNow;

        var integrationEvent = AllocationDecisionEventFactory.Create(
            allocation,
            messageId,
            occurredAtUtc);

        var payload = JsonSerializer.Serialize(integrationEvent, JsonOptions);

        return AllocationOutboxMessage.Create(
            messageId,
            allocation.Id,
            AllocationDecidedV1.MessageType,
            payload,
            occurredAtUtc);
    }
}
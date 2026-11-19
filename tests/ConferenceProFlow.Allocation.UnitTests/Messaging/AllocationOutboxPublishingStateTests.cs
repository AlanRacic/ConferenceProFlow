using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;
using ConferenceProFlow.Allocation.Worker.Messaging.Outbox;

namespace ConferenceProFlow.Allocation.UnitTests.Messaging;

public sealed class AllocationOutboxPublishingStateTests
{
    private static AllocationOutboxMessage CreateValid()
    {
        var now = DateTimeOffset.UtcNow;
        var messageId = Guid.NewGuid();
        var allocationId = Guid.NewGuid();
        var payload = System.Text.Json.JsonSerializer.Serialize(
            new AllocationDecidedV1(
                messageId,
                allocationId,
                Guid.NewGuid(),
                Guid.NewGuid(),
                AllocationDecidedV1.ConfirmedStatus,
                null,
                now),
            System.Text.Json.JsonSerializerOptions.Web);

        return AllocationOutboxMessage.Create(
            messageId, allocationId, AllocationDecidedV1.MessageType, payload, now);
    }

    [Fact]
    public void MarkPublished_AfterConfirmation_ClearsRetryState()
    {
        var message = CreateValid();
        message.RecordFailure("Temporary broker failure", DateTimeOffset.UtcNow);
        message.MarkPublished(DateTimeOffset.UtcNow);

        Assert.NotNull(message.PublishedAtUtc);
        Assert.Equal(2, message.PublishAttempts);
        Assert.Null(message.NextAttemptAtUtc);
        Assert.Null(message.LastError);
    }

    [Fact]
    public void RecordFailure_UsesBoundedExponentialBackoff()
    {
        var message = CreateValid();
        var before = DateTimeOffset.UtcNow;
        message.RecordFailure("Unroutable", before);
        Assert.Equal(1, message.PublishAttempts);
        Assert.Equal(before.AddSeconds(10), message.NextAttemptAtUtc);
        Assert.Null(message.PublishedAtUtc);

        Assert.Equal(TimeSpan.FromSeconds(20), AllocationOutboxRetryPolicy.GetDelay(2));
        Assert.Equal(TimeSpan.FromSeconds(300), AllocationOutboxRetryPolicy.GetDelay(10));
    }

    [Fact]
    public void Quarantine_PermanentInvalidPayload_DoesNotScheduleRetry()
    {
        var message = CreateValid();
        message.Quarantine("Invalid payload", DateTimeOffset.UtcNow);

        Assert.Equal(1, message.PublishAttempts);
        Assert.NotNull(message.QuarantinedAtUtc);
        Assert.Null(message.NextAttemptAtUtc);
        Assert.Null(message.PublishedAtUtc);
    }

    [Fact]
    public void ValidConfirmedPayload_HasNoPermanentError()
    {
        Assert.Null(AllocationOutboxPayloadValidator.GetPermanentError(CreateValid()));
    }

    [Fact]
    public void MismatchedMessageId_IsRejected()
    {
        var valid = CreateValid();
        var mismatch = AllocationOutboxMessage.Create(
            Guid.NewGuid(), valid.AllocationId, valid.MessageType, valid.Payload, valid.OccurredAtUtc);

        Assert.NotNull(AllocationOutboxPayloadValidator.GetPermanentError(mismatch));
    }

    [Fact]
    public void InvalidJson_IsRejected()
    {
        var valid = CreateValid();
        var malformed = AllocationOutboxMessage.Create(
            valid.Id, valid.AllocationId, valid.MessageType, "{ invalid json", valid.OccurredAtUtc);

        Assert.NotNull(AllocationOutboxPayloadValidator.GetPermanentError(malformed));
    }
}
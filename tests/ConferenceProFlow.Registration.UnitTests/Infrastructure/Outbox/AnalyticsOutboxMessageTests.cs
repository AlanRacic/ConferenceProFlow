using System.Text.Json;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Infrastructure.Outbox;

public sealed class AnalyticsOutboxMessageTests
{
    [Theory]
    [InlineData(RegistrationFinalizedV1.ConfirmedStatus)]
    [InlineData(RegistrationFinalizedV1.RejectedStatus)]
    public void FromEvent_PreservesStableIdentityAndPayload(string status)
    {
        var integrationEvent = RegistrationFinalizedV1.Create(
            Guid.NewGuid(), Guid.NewGuid(), status,
            new DateTimeOffset(2026, 11, 20, 12, 0, 0, TimeSpan.Zero));

        var outbox = AnalyticsOutboxMessage.FromEvent(integrationEvent);
        var parsed = JsonSerializer.Deserialize<RegistrationFinalizedV1>(outbox.Payload);

        Assert.NotNull(parsed);
        Assert.Equal(integrationEvent, parsed);
        Assert.Equal(integrationEvent.MessageId, outbox.Id);
        Assert.Equal(integrationEvent.RegistrationId, outbox.RegistrationId);
        Assert.Equal(RegistrationFinalizedV1.EventType, outbox.MessageType);
        Assert.Equal(integrationEvent.OccurredAtUtc, outbox.OccurredAtUtc);
        Assert.Equal(0, outbox.PublishAttempts);
        Assert.Null(outbox.PublishedAtUtc);
        Assert.Null(outbox.LastError);
        Assert.Null(outbox.NextAttemptAtUtc);
        Assert.Null(outbox.QuarantinedAtUtc);
    }

    [Fact]
    public void FromEvent_RejectsInvalidEvent()
    {
        var invalidEvent = new RegistrationFinalizedV1(
            Guid.Empty, Guid.NewGuid(), Guid.NewGuid(),
            RegistrationFinalizedV1.ConfirmedStatus, DateTimeOffset.UtcNow);

        Assert.Throws<ArgumentException>(() => AnalyticsOutboxMessage.FromEvent(invalidEvent));
    }

    [Fact]
    public void FromEvent_RejectsNull()
    {
        Assert.Throws<ArgumentNullException>(() => AnalyticsOutboxMessage.FromEvent(null!));
    }
}

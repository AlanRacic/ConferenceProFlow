using Xunit;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;

namespace ConferenceProFlow.Registration.UnitTests.Infrastructure.Outbox;

public sealed class AnalyticsOutboxPublishStateTests
{
    private static AnalyticsOutboxMessage NewOutbox() => AnalyticsOutboxMessage.FromEvent(
        RegistrationFinalizedV1.Create(Guid.NewGuid(), Guid.NewGuid(),
            RegistrationFinalizedV1.ConfirmedStatus));

    [Fact]
    public void MarkPublished_RequiresUtcAndClearsFailureState()
    {
        var outbox = NewOutbox();
        var now = DateTimeOffset.UtcNow;
        outbox.RecordPublishFailure(now, "temporary failure", TimeSpan.FromSeconds(2));

        outbox.MarkPublished(now.AddSeconds(5));

        Assert.Equal(now.AddSeconds(5), outbox.PublishedAtUtc);
        Assert.Equal(2, outbox.PublishAttempts);
        Assert.Null(outbox.LastError);
        Assert.Null(outbox.NextAttemptAtUtc);
    }

    [Fact]
    public void RecordPublishFailure_KeepsEventPendingAndSchedulesRetry()
    {
        var outbox = NewOutbox();
        var now = DateTimeOffset.UtcNow;
        outbox.RecordPublishFailure(now, "kafka unavailable", TimeSpan.FromSeconds(4));

        Assert.Null(outbox.PublishedAtUtc);
        Assert.Equal(1, outbox.PublishAttempts);
        Assert.Equal(now.AddSeconds(4), outbox.NextAttemptAtUtc);
        Assert.Equal("kafka unavailable", outbox.LastError);
    }

    [Fact]
    public void MarkPublished_DoesNotDoubleCountSuccessfulDelivery()
    {
        var outbox = NewOutbox();
        var now = DateTimeOffset.UtcNow;
        outbox.MarkPublished(now);
        outbox.MarkPublished(now.AddMinutes(1));

        Assert.Equal(1, outbox.PublishAttempts);
        Assert.Equal(now, outbox.PublishedAtUtc);
    }

    [Fact]
    public void RecordPublishFailure_RejectsAlreadyPublishedEvent()
    {
        var outbox = NewOutbox();
        outbox.MarkPublished(DateTimeOffset.UtcNow);

        Assert.Throws<InvalidOperationException>(() =>
            outbox.RecordPublishFailure(DateTimeOffset.UtcNow, "failure", TimeSpan.FromSeconds(1)));
    }
}

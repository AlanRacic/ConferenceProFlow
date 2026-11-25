using Confluent.Kafka;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Infrastructure.Outbox;

public sealed class AnalyticsOutboxPublishAlertPolicyTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(10001)]
    public void InvalidThreshold_IsRejected(int threshold)
    {
        Assert.False(AnalyticsOutboxPublishAlertPolicy.IsValidThreshold(threshold));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            AnalyticsOutboxPublishAlertPolicy.ShouldEscalate(10, threshold));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10000)]
    public void ValidThreshold_IsAccepted(int threshold)
    {
        Assert.True(AnalyticsOutboxPublishAlertPolicy.IsValidThreshold(threshold));
    }

    [Theory]
    [InlineData(0, false)]
    [InlineData(4, false)]
    [InlineData(5, true)]
    [InlineData(6, false)]
    [InlineData(14, false)]
    [InlineData(15, true)]
    [InlineData(24, false)]
    [InlineData(25, true)]
    public void Escalation_IsRateLimitedByAttemptCount(int attempts, bool expected)
    {
        Assert.Equal(expected, AnalyticsOutboxPublishAlertPolicy.ShouldEscalate(attempts, 5));
    }

    [Fact]
    public void LocalPayloadFailure_IsDistinguishedFromKafkaDeliveryFailure()
    {
        Assert.Equal(
            AnalyticsOutboxPublishFailureCategory.PayloadValidation,
            AnalyticsOutboxPublishAlertPolicy.Classify(new InvalidOperationException("invalid payload"), false));

        var brokerError = new KafkaException(new Error(ErrorCode.Local_AllBrokersDown, "broker unavailable"));
        Assert.Equal(
            AnalyticsOutboxPublishFailureCategory.KafkaDelivery,
            AnalyticsOutboxPublishAlertPolicy.Classify(brokerError, true));

        Assert.Equal(
            AnalyticsOutboxPublishFailureCategory.Unexpected,
            AnalyticsOutboxPublishAlertPolicy.Classify(new TimeoutException("unexpected timeout"), true));
    }

    [Fact]
    public void FailureDescription_ContainsCategoryAndExceptionType()
    {
        var description = AnalyticsOutboxPublishAlertPolicy.Describe(
            AnalyticsOutboxPublishFailureCategory.PayloadValidation,
            new InvalidOperationException("Unexpected analytics event type."));
        Assert.StartsWith("[PayloadValidation] InvalidOperationException:", description);
    }

    [Fact]
    public void RepeatedFailures_KeepOriginalEventIdentityAndPendingState()
    {
        var finalization = RegistrationFinalizedV1.Create(
            Guid.NewGuid(), Guid.NewGuid(), RegistrationFinalizedV1.ConfirmedStatus);
        var outbox = AnalyticsOutboxMessage.FromEvent(finalization);
        var originalPayload = outbox.Payload;
        var now = DateTimeOffset.UtcNow;

        for (var i = 0; i < 5; i++)
            outbox.RecordPublishFailure(
                now.AddMinutes(i),
                "[PayloadValidation] InvalidOperationException: invalid event type",
                TimeSpan.FromSeconds(60));

        Assert.Equal(5, outbox.PublishAttempts);
        Assert.Equal(finalization.MessageId, outbox.Id);
        Assert.Equal(finalization.RegistrationId, outbox.RegistrationId);
        Assert.Equal(originalPayload, outbox.Payload);
        Assert.Null(outbox.PublishedAtUtc);
        Assert.Null(outbox.QuarantinedAtUtc);
        Assert.NotNull(outbox.NextAttemptAtUtc);
        Assert.True(AnalyticsOutboxPublishAlertPolicy.ShouldEscalate(outbox.PublishAttempts, 5));
    }
}

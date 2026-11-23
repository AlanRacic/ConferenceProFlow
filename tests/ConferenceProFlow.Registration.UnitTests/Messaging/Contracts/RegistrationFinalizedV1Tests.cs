using System.Text.Json;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Messaging.Contracts;

public sealed class RegistrationFinalizedV1Tests
{
    [Theory]
    [InlineData(RegistrationFinalizedV1.ConfirmedStatus)]
    [InlineData(RegistrationFinalizedV1.RejectedStatus)]
    public void Create_ValidFinalStatus_CreatesValidEvent(string status)
    {
        var registrationId = Guid.NewGuid();
        var sessionId = Guid.NewGuid();
        var occurredAtUtc = new DateTimeOffset(2026, 11, 20, 10, 0, 0, TimeSpan.Zero);

        var integrationEvent = RegistrationFinalizedV1.Create(
            registrationId, sessionId, status, occurredAtUtc);

        Assert.NotEqual(Guid.Empty, integrationEvent.MessageId);
        Assert.Equal(registrationId, integrationEvent.RegistrationId);
        Assert.Equal(sessionId, integrationEvent.SessionId);
        Assert.Equal(status, integrationEvent.Status);
        Assert.Equal(occurredAtUtc, integrationEvent.OccurredAtUtc);
        integrationEvent.Validate();
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("Confirmed")]
    [InlineData("")]
    [InlineData("cancelled")]
    public void Create_UnsupportedStatus_Throws(string status)
    {
        Assert.Throws<ArgumentException>(() =>
            RegistrationFinalizedV1.Create(Guid.NewGuid(), Guid.NewGuid(), status));
    }

    [Fact]
    public void Create_EmptyRegistrationId_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            RegistrationFinalizedV1.Create(Guid.Empty, Guid.NewGuid(), RegistrationFinalizedV1.ConfirmedStatus));
    }

    [Fact]
    public void Create_EmptySessionId_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            RegistrationFinalizedV1.Create(Guid.NewGuid(), Guid.Empty, RegistrationFinalizedV1.ConfirmedStatus));
    }

    [Fact]
    public void Create_NonUtcTimestamp_Throws()
    {
        var nonUtc = new DateTimeOffset(2026, 11, 20, 12, 0, 0, TimeSpan.FromHours(1));

        Assert.Throws<ArgumentException>(() =>
            RegistrationFinalizedV1.Create(Guid.NewGuid(), Guid.NewGuid(),
                RegistrationFinalizedV1.RejectedStatus, nonUtc));
    }

    [Fact]
    public void Validate_EmptyMessageId_Throws()
    {
        var integrationEvent = new RegistrationFinalizedV1(
            Guid.Empty, Guid.NewGuid(), Guid.NewGuid(),
            RegistrationFinalizedV1.ConfirmedStatus, DateTimeOffset.UtcNow);

        Assert.Throws<ArgumentException>(() => integrationEvent.Validate());
    }

    [Fact]
    public void Serialize_UsesContractFieldNames_AndRoundTripsWithoutChangingIdentity()
    {
        var integrationEvent = RegistrationFinalizedV1.Create(
            Guid.NewGuid(), Guid.NewGuid(), RegistrationFinalizedV1.ConfirmedStatus,
            new DateTimeOffset(2026, 11, 20, 10, 0, 0, TimeSpan.Zero));

        var json = JsonSerializer.Serialize(integrationEvent);
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        Assert.Equal(integrationEvent.MessageId, root.GetProperty("messageId").GetGuid());
        Assert.Equal(integrationEvent.RegistrationId, root.GetProperty("registrationId").GetGuid());
        Assert.Equal(integrationEvent.SessionId, root.GetProperty("sessionId").GetGuid());
        Assert.Equal("confirmed", root.GetProperty("status").GetString());
        Assert.True(root.TryGetProperty("occurredAtUtc", out _));
        Assert.Equal(5, root.EnumerateObject().Count());

        var restored = JsonSerializer.Deserialize<RegistrationFinalizedV1>(json);
        Assert.NotNull(restored);
        restored.Validate();
        Assert.Equal(integrationEvent, restored);
    }

    [Fact]
    public void EventKey_UsesCanonicalSessionGuid()
    {
        var integrationEvent = RegistrationFinalizedV1.Create(
            Guid.NewGuid(), Guid.Parse("d7263cac-475f-40a3-b149-a8c4db117c69"),
            RegistrationFinalizedV1.RejectedStatus);

        var kafkaKey = integrationEvent.SessionId.ToString("D");

        Assert.Equal("d7263cac-475f-40a3-b149-a8c4db117c69", kafkaKey);
    }
}

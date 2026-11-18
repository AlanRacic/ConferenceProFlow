using System.Text.Json;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.UnitTests.Messaging;

public sealed class AllocationDecisionEventFactoryTests
{
    [Fact]
    public void Create_ConfirmedAllocation_MapsIdsAndHasNoRejectionReason()
    {
        var createdAtUtc = DateTimeOffset.UtcNow;
        var allocation = AllocationEntity.CreateConfirmed(
            Guid.NewGuid(), Guid.NewGuid(), createdAtUtc);
        var messageId = Guid.NewGuid();
        var occurredAtUtc = createdAtUtc.AddSeconds(1);

        var result = AllocationDecisionEventFactory.Create(
            allocation, messageId, occurredAtUtc);

        Assert.Equal(messageId, result.MessageId);
        Assert.Equal(allocation.Id, result.AllocationId);
        Assert.Equal(allocation.RegistrationId, result.RegistrationId);
        Assert.Equal(allocation.SessionId, result.SessionId);
        Assert.Equal(AllocationDecidedV1.ConfirmedStatus, result.Status);
        Assert.Null(result.RejectionReason);
        Assert.Equal(occurredAtUtc, result.OccurredAtUtc);
    }

    [Fact]
    public void Create_RejectedAllocation_PreservesRejectionReason()
    {
        var allocation = AllocationEntity.CreateRejected(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "Session capacity reached.",
            DateTimeOffset.UtcNow);

        var result = AllocationDecisionEventFactory.Create(
            allocation, Guid.NewGuid(), DateTimeOffset.UtcNow);

        Assert.Equal(AllocationDecidedV1.RejectedStatus, result.Status);
        Assert.Equal("Session capacity reached.", result.RejectionReason);
        Assert.Equal(allocation.Id, result.AllocationId);
    }

    [Fact]
    public void Create_NullAllocation_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            AllocationDecisionEventFactory.Create(
                null!, Guid.NewGuid(), DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Create_EmptyMessageId_ThrowsArgumentException()
    {
        var allocation = CreateConfirmedAllocation();

        Assert.Throws<ArgumentException>(() =>
            AllocationDecisionEventFactory.Create(
                allocation, Guid.Empty, DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Create_NonUtcTimestamp_ThrowsArgumentException()
    {
        var allocation = CreateConfirmedAllocation();
        var nonUtc = new DateTimeOffset(
            2027, 6, 1, 10, 0, 0, TimeSpan.FromHours(2));

        Assert.Throws<ArgumentException>(() =>
            AllocationDecisionEventFactory.Create(
                allocation, Guid.NewGuid(), nonUtc));
    }

    [Fact]
    public void Create_DefaultTimestamp_ThrowsArgumentException()
    {
        var allocation = CreateConfirmedAllocation();

        Assert.Throws<ArgumentException>(() =>
            AllocationDecisionEventFactory.Create(
                allocation, Guid.NewGuid(), default));
    }

    [Fact]
    public void Serialize_WithWebDefaults_UsesVersionedJsonShape()
    {
        var allocation = AllocationEntity.CreateRejected(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "Session capacity reached.",
            DateTimeOffset.UtcNow);
        var message = AllocationDecisionEventFactory.Create(
            allocation, Guid.NewGuid(), DateTimeOffset.UtcNow);

        var json = JsonSerializer.Serialize(
            message,
            new JsonSerializerOptions(JsonSerializerDefaults.Web));

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        Assert.Equal(message.MessageId.ToString("D"),
            root.GetProperty("messageId").GetString());
        Assert.Equal(message.AllocationId.ToString("D"),
            root.GetProperty("allocationId").GetString());
        Assert.Equal(message.RegistrationId.ToString("D"),
            root.GetProperty("registrationId").GetString());
        Assert.Equal(message.SessionId.ToString("D"),
            root.GetProperty("sessionId").GetString());
        Assert.Equal("rejected", root.GetProperty("status").GetString());
        Assert.Equal("Session capacity reached.",
            root.GetProperty("rejectionReason").GetString());
        Assert.True(DateTimeOffset.TryParse(
            root.GetProperty("occurredAtUtc").GetString(),
            out var occurredAtUtc));
        Assert.Equal(TimeSpan.Zero, occurredAtUtc.Offset);
    }

    private static AllocationEntity CreateConfirmedAllocation()
    {
        return AllocationEntity.CreateConfirmed(
            Guid.NewGuid(), Guid.NewGuid(), DateTimeOffset.UtcNow);
    }
}

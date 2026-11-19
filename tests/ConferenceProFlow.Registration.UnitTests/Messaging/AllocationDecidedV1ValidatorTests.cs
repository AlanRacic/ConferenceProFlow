using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Messaging;

public sealed class AllocationDecidedV1ValidatorTests
{
    private static AllocationDecidedV1 Valid(string status = AllocationDecidedV1.ConfirmedStatus) =>
        new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(),
            status,
            status == AllocationDecidedV1.RejectedStatus ? "Session capacity reached." : null,
            DateTimeOffset.UtcNow);

    [Fact]
    public void ConfirmedEvent_IsValid() => Assert.Null(AllocationDecidedV1Validator.GetError(Valid()));

    [Fact]
    public void RejectedEvent_IsValid() => Assert.Null(AllocationDecidedV1Validator.GetError(
            Valid(AllocationDecidedV1.RejectedStatus)));

    [Fact]
    public void EmptyMessageId_IsInvalid() => Assert.NotNull(AllocationDecidedV1Validator.GetError(
            Valid() with { MessageId = Guid.Empty }));

    [Fact]
    public void UnknownStatus_IsInvalid() => Assert.NotNull(AllocationDecidedV1Validator.GetError(
            Valid() with { Status = "cancelled" }));

    [Fact]
    public void ConfirmedWithRejectionReason_IsInvalid() => Assert.NotNull(AllocationDecidedV1Validator.GetError(
            Valid() with { RejectionReason = "Unexpected reason" }));

    [Fact]
    public void RejectedWithoutReason_IsInvalid() => Assert.NotNull(AllocationDecidedV1Validator.GetError(
            Valid(AllocationDecidedV1.RejectedStatus) with { RejectionReason = null }));

    [Fact]
    public void NonUtcTimestamp_IsInvalid() => Assert.NotNull(AllocationDecidedV1Validator.GetError(
            Valid() with { OccurredAtUtc = DateTimeOffset.Now.ToOffset(TimeSpan.FromHours(2)) }));
}
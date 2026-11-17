using ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;
using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using ConferenceProFlow.Allocation.Worker.Domain.Enums;
using ConferenceProFlow.Allocation.Worker.Processing;
using Microsoft.EntityFrameworkCore;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.IntegrationTests.Processing;

[Collection("AllocationSqlServer")]
public sealed class AllocationProcessingServiceIntegrationTests
{
    private readonly AllocationSqlServerFixture _fixture;

    public AllocationProcessingServiceIntegrationTests(AllocationSqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task AvailableSeat_CreatesConfirmedAllocationAndCompletesInbox()
    {
        var request = await SeedPendingRequestAsync(capacity: 2);

        var result = await ProcessAsync(request.MessageId);

        Assert.Equal(AllocationProcessingResult.Confirmed, result);

        await using var db = _fixture.CreateDbContext();
        var token = TestContext.Current.CancellationToken;

        var inventory = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == request.SessionId, token);
        var allocation = await db.Allocations.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == request.RegistrationId, token);
        var inbox = await db.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == request.MessageId, token);

        Assert.Equal(1, inventory.ReservedCount);
        Assert.Equal(AllocationStatus.Confirmed, allocation.Status);
        Assert.Null(allocation.RejectionReason);
        Assert.NotNull(inbox.ProcessedAtUtc);
    }

    [Fact]
    public async Task FullSession_CreatesRejectedAllocationWithoutChangingCapacity()
    {
        var request = await SeedPendingRequestAsync(capacity: 1);
        var token = TestContext.Current.CancellationToken;

        await using (var setupDb = _fixture.CreateDbContext())
        {
            var inventory = await setupDb.SessionInventories
                .SingleAsync(x => x.SessionId == request.SessionId, token);

            Assert.True(inventory.TryReserveSeat());
            setupDb.Allocations.Add(AllocationEntity.CreateConfirmed(
                Guid.NewGuid(), request.SessionId, DateTimeOffset.UtcNow));
            await setupDb.SaveChangesAsync(token);
        }

        var result = await ProcessAsync(request.MessageId);

        Assert.Equal(AllocationProcessingResult.Rejected, result);

        await using var db = _fixture.CreateDbContext();
        var savedInventory = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == request.SessionId, token);
        var allocation = await db.Allocations.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == request.RegistrationId, token);
        var inbox = await db.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == request.MessageId, token);

        Assert.Equal(1, savedInventory.ReservedCount);
        Assert.Equal(AllocationStatus.Rejected, allocation.Status);
        Assert.Equal("Session capacity reached.", allocation.RejectionReason);
        Assert.NotNull(inbox.ProcessedAtUtc);
    }

    [Fact]
    public async Task MissingInventory_RemainsPendingUntilInventoryIsCreated()
    {
        var request = await SeedPendingRequestAsync(capacity: null);
        var token = TestContext.Current.CancellationToken;

        var firstResult = await ProcessAsync(request.MessageId);
        Assert.Equal(AllocationProcessingResult.WaitingForInventory, firstResult);

        await using (var checkDb = _fixture.CreateDbContext())
        {
            var inbox = await checkDb.InboxMessages.AsNoTracking()
                .SingleAsync(x => x.Id == request.MessageId, token);

            Assert.Null(inbox.ProcessedAtUtc);
            Assert.False(await checkDb.Allocations.AnyAsync(
                x => x.RegistrationId == request.RegistrationId, token));
        }

        await using (var setupDb = _fixture.CreateDbContext())
        {
            setupDb.SessionInventories.Add(SessionInventory.Create(
                request.SessionId, 2, DateTimeOffset.UtcNow));
            await setupDb.SaveChangesAsync(token);
        }

        var secondResult = await ProcessAsync(request.MessageId);
        Assert.Equal(AllocationProcessingResult.Confirmed, secondResult);
    }

    [Fact]
    public async Task ProcessingTwice_DoesNotReserveSecondSeat()
    {
        var request = await SeedPendingRequestAsync(capacity: 2);
        var token = TestContext.Current.CancellationToken;

        Assert.Equal(AllocationProcessingResult.Confirmed,
            await ProcessAsync(request.MessageId));
        Assert.Equal(AllocationProcessingResult.AlreadyProcessed,
            await ProcessAsync(request.MessageId));

        await using var db = _fixture.CreateDbContext();
        var inventory = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == request.SessionId, token);
        var allocationCount = await db.Allocations.CountAsync(
            x => x.RegistrationId == request.RegistrationId, token);

        Assert.Equal(1, inventory.ReservedCount);
        Assert.Equal(1, allocationCount);
    }

    [Fact]
    public async Task ExistingAllocation_CompletesInboxWithoutReservingAgain()
    {
        var request = await SeedPendingRequestAsync(capacity: 2);
        var token = TestContext.Current.CancellationToken;

        await using (var setupDb = _fixture.CreateDbContext())
        {
            var inventory = await setupDb.SessionInventories
                .SingleAsync(x => x.SessionId == request.SessionId, token);
            Assert.True(inventory.TryReserveSeat());
            setupDb.Allocations.Add(AllocationEntity.CreateConfirmed(
                request.RegistrationId, request.SessionId, DateTimeOffset.UtcNow));
            await setupDb.SaveChangesAsync(token);
        }

        var result = await ProcessAsync(request.MessageId);
        Assert.Equal(AllocationProcessingResult.ExistingAllocation, result);

        await using var db = _fixture.CreateDbContext();
        var savedInventory = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == request.SessionId, token);
        var inbox = await db.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == request.MessageId, token);
        var allocationCount = await db.Allocations.CountAsync(
            x => x.RegistrationId == request.RegistrationId, token);

        Assert.Equal(1, savedInventory.ReservedCount);
        Assert.Equal(1, allocationCount);
        Assert.NotNull(inbox.ProcessedAtUtc);
    }

    [Fact]
    public async Task UnknownMessage_ReturnsNotFound()
    {
        var result = await ProcessAsync(Guid.NewGuid());

        Assert.Equal(AllocationProcessingResult.NotFound, result);
    }

    [Fact]
    public async Task ExistingAllocationForDifferentSession_DoesNotCompleteInbox()
    {
        var request = await SeedPendingRequestAsync(capacity: 2);
        var token = TestContext.Current.CancellationToken;
        var otherSessionId = Guid.NewGuid();

        await using (var setupDb = _fixture.CreateDbContext())
        {
            setupDb.SessionInventories.Add(SessionInventory.Create(otherSessionId, 2, DateTimeOffset.UtcNow));
            await setupDb.SaveChangesAsync(token);

            setupDb.Allocations.Add(AllocationEntity.CreateConfirmed(request.RegistrationId, otherSessionId, DateTimeOffset.UtcNow));
            await setupDb.SaveChangesAsync(token);
        }

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await ProcessAsync(request.MessageId);
        });

        await using var db = _fixture.CreateDbContext();
        var inbox = await db.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == request.MessageId, token);

        Assert.Null(inbox.ProcessedAtUtc);
    }

    private async Task<(Guid MessageId, Guid RegistrationId, Guid SessionId)>
        SeedPendingRequestAsync(int? capacity)
    {
        var messageId = Guid.NewGuid();
        var registrationId = Guid.NewGuid();
        var sessionId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;

        await using var db = _fixture.CreateDbContext();

        if (capacity is not null)
        {
            db.SessionInventories.Add(SessionInventory.Create(sessionId, capacity.Value, now));
        }

        db.InboxMessages.Add(InboxMessage.Create(messageId, registrationId, sessionId, now, now));

        await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        return (messageId, registrationId, sessionId);
    }

    private async Task<AllocationProcessingResult> ProcessAsync(Guid messageId)
    {
        await using var db = _fixture.CreateDbContext();
        var service = new AllocationProcessingService(db);

        return await service.ProcessAsync(messageId, TestContext.Current.CancellationToken);
    }
}
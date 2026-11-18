using System.Text.Json;
using ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;
using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using ConferenceProFlow.Allocation.Worker.Domain.Enums;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;
using ConferenceProFlow.Allocation.Worker.Messaging.Outbox;
using ConferenceProFlow.Allocation.Worker.Processing;
using Microsoft.EntityFrameworkCore;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.IntegrationTests.Messaging;

[Collection("AllocationSqlServer")]
public sealed class AllocationOutboxIntegrationTests
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly AllocationSqlServerFixture _fixture;

    public AllocationOutboxIntegrationTests(AllocationSqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ConfirmedAllocation_PersistsInboxInventoryAndResultEvent()
    {
        var request = await SeedPendingRequestAsync(capacity: 2);
        var result = await ProcessAsync(request.MessageId);

        Assert.Equal(AllocationProcessingResult.Confirmed, result);

        await using var db = _fixture.CreateDbContext();
        var token = TestContext.Current.CancellationToken;
        var inventory = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == request.SessionId, token);
        var inbox = await db.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == request.MessageId, token);
        var allocation = await db.Allocations.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == request.RegistrationId, token);
        var outbox = await db.AllocationOutboxMessages.AsNoTracking()
            .SingleAsync(x => x.AllocationId == allocation.Id, token);

        Assert.Equal(1, inventory.ReservedCount);
        Assert.NotNull(inbox.ProcessedAtUtc);
        Assert.Equal(AllocationStatus.Confirmed, allocation.Status);
        Assert.Equal(AllocationDecidedV1.MessageType, outbox.MessageType);
        Assert.Null(outbox.PublishedAtUtc);
        Assert.Equal(0, outbox.PublishAttempts);
        Assert.Null(outbox.NextAttemptAtUtc);
        Assert.Null(outbox.QuarantinedAtUtc);

        var integrationEvent = Deserialize(outbox.Payload);
        Assert.Equal(outbox.Id, integrationEvent.MessageId);
        Assert.Equal(allocation.Id, integrationEvent.AllocationId);
        Assert.Equal(request.RegistrationId, integrationEvent.RegistrationId);
        Assert.Equal(request.SessionId, integrationEvent.SessionId);
        Assert.Equal(AllocationDecidedV1.ConfirmedStatus, integrationEvent.Status);
        Assert.Null(integrationEvent.RejectionReason);
        Assert.Equal(outbox.OccurredAtUtc, integrationEvent.OccurredAtUtc);
    }

    [Fact]
    public async Task FullSession_PersistsRejectedDecisionAndOutboxWithoutReservingSeat()
    {
        var request = await SeedPendingRequestAsync(capacity: 1);
        var token = TestContext.Current.CancellationToken;

        // Reserve the only seat for a different registration.
        await using (var setupDb = _fixture.CreateDbContext())
        {
            var inventory = await setupDb.SessionInventories
                .SingleAsync(x => x.SessionId == request.SessionId, token);
            Assert.True(inventory.TryReserveSeat());
            setupDb.Allocations.Add(AllocationEntity.CreateConfirmed(
                Guid.NewGuid(), request.SessionId, DateTimeOffset.UtcNow));
            await setupDb.SaveChangesAsync(token);
        }

        Assert.Equal(AllocationProcessingResult.Rejected,
            await ProcessAsync(request.MessageId));

        await using var db = _fixture.CreateDbContext();
        var inventoryAfter = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == request.SessionId, token);
        var allocation = await db.Allocations.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == request.RegistrationId, token);
        var outbox = await db.AllocationOutboxMessages.AsNoTracking()
            .SingleAsync(x => x.AllocationId == allocation.Id, token);
        var inbox = await db.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == request.MessageId, token);

        Assert.Equal(1, inventoryAfter.ReservedCount);
        Assert.Equal(AllocationStatus.Rejected, allocation.Status);
        Assert.Equal("Session capacity reached.", allocation.RejectionReason);
        Assert.NotNull(inbox.ProcessedAtUtc);

        var integrationEvent = Deserialize(outbox.Payload);
        Assert.Equal(AllocationDecidedV1.RejectedStatus, integrationEvent.Status);
        Assert.Equal("Session capacity reached.", integrationEvent.RejectionReason);
        Assert.Equal(allocation.Id, integrationEvent.AllocationId);
        Assert.Null(outbox.PublishedAtUtc);
    }

    [Fact]
    public async Task RepeatedProcessing_DoesNotCreateAnotherResultEvent()
    {
        var request = await SeedPendingRequestAsync(capacity: 2);

        Assert.Equal(AllocationProcessingResult.Confirmed,
            await ProcessAsync(request.MessageId));
        Assert.Equal(AllocationProcessingResult.AlreadyProcessed,
            await ProcessAsync(request.MessageId));

        await using var db = _fixture.CreateDbContext();
        var token = TestContext.Current.CancellationToken;
        var allocation = await db.Allocations.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == request.RegistrationId, token);
        var inventory = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == request.SessionId, token);

        Assert.Equal(1, inventory.ReservedCount);
        Assert.Equal(1, await db.AllocationOutboxMessages.CountAsync(
            x => x.AllocationId == allocation.Id, token));
    }

    [Fact]
    public async Task ExistingAllocation_CompletesPendingInboxAndAddsMissingOutbox()
    {
        var request = await SeedPendingRequestAsync(capacity: 2);
        var token = TestContext.Current.CancellationToken;
        Guid allocationId;

        await using (var setupDb = _fixture.CreateDbContext())
        {
            var inventory = await setupDb.SessionInventories
                .SingleAsync(x => x.SessionId == request.SessionId, token);
            Assert.True(inventory.TryReserveSeat());
            var allocation = AllocationEntity.CreateConfirmed(
                request.RegistrationId, request.SessionId, DateTimeOffset.UtcNow);
            allocationId = allocation.Id;
            setupDb.Allocations.Add(allocation);
            await setupDb.SaveChangesAsync(token);
        }

        Assert.Equal(AllocationProcessingResult.ExistingAllocation,
            await ProcessAsync(request.MessageId));

        await using var db = _fixture.CreateDbContext();
        var inventoryAfter = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == request.SessionId, token);
        var inbox = await db.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == request.MessageId, token);
        var outbox = await db.AllocationOutboxMessages.AsNoTracking()
            .SingleAsync(x => x.AllocationId == allocationId, token);

        Assert.Equal(1, inventoryAfter.ReservedCount);
        Assert.NotNull(inbox.ProcessedAtUtc);
        Assert.Equal(AllocationDecidedV1.ConfirmedStatus, Deserialize(outbox.Payload).Status);
    }

    [Fact]
    public async Task UniqueAllocationId_PreventsTwoLogicalOutboxEvents()
    {
        var request = await SeedPendingRequestAsync(capacity: 2);
        Assert.Equal(AllocationProcessingResult.Confirmed,
            await ProcessAsync(request.MessageId));

        var token = TestContext.Current.CancellationToken;
        await using var duplicateDb = _fixture.CreateDbContext();
        var allocation = await duplicateDb.Allocations.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == request.RegistrationId, token);

        duplicateDb.AllocationOutboxMessages.Add(
            AllocationOutboxMessageFactory.Create(allocation));

        await Assert.ThrowsAsync<DbUpdateException>(async () =>
            await duplicateDb.SaveChangesAsync(token));

        await using var verificationDb = _fixture.CreateDbContext();
        Assert.Equal(1, await verificationDb.AllocationOutboxMessages.CountAsync(
            x => x.AllocationId == allocation.Id, token));
    }

    [Fact]
    public async Task MissingInventory_LeavesInboxPendingWithoutCreatingOutbox()
    {
        var request = await SeedPendingRequestAsync(capacity: null);
        Assert.Equal(AllocationProcessingResult.WaitingForInventory,
            await ProcessAsync(request.MessageId));

        await using var db = _fixture.CreateDbContext();
        var token = TestContext.Current.CancellationToken;
        var inbox = await db.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == request.MessageId, token);
        Assert.Null(inbox.ProcessedAtUtc);
        Assert.False(await db.Allocations.AnyAsync(
            x => x.RegistrationId == request.RegistrationId, token));
        // The shared fixture can contain Outbox records from other tests.
        // Verify only the Outbox events associated with this registration.
        var relatedOutboxExists = await (
            from outbox in db.AllocationOutboxMessages
            join allocation in db.Allocations
                on outbox.AllocationId equals allocation.Id
            where allocation.RegistrationId == request.RegistrationId
            select outbox.Id
        ).AnyAsync(token);

        Assert.False(relatedOutboxExists);
    }

    [Fact]
    public async Task StaleInventoryVersion_RollsBackSecondAllocationAndOutbox()
    {
        var token = TestContext.Current.CancellationToken;
        var sessionId = Guid.NewGuid();
        var firstMessageId = Guid.NewGuid();
        var secondMessageId = Guid.NewGuid();
        var firstRegistrationId = Guid.NewGuid();
        var secondRegistrationId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;

        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.SessionInventories.Add(SessionInventory.Create(sessionId, 1, now));
            seedDb.InboxMessages.Add(InboxMessage.Create(
                firstMessageId, firstRegistrationId, sessionId, now, now));
            seedDb.InboxMessages.Add(InboxMessage.Create(
                secondMessageId, secondRegistrationId, sessionId, now, now));
            await seedDb.SaveChangesAsync(token);
        }

        await using var firstDb = _fixture.CreateDbContext();
        await using var secondDb = _fixture.CreateDbContext();

        var firstInventory = await firstDb.SessionInventories
            .SingleAsync(x => x.SessionId == sessionId, token);
        var secondInventory = await secondDb.SessionInventories
            .SingleAsync(x => x.SessionId == sessionId, token);
        var firstInbox = await firstDb.InboxMessages
            .SingleAsync(x => x.Id == firstMessageId, token);
        var secondInbox = await secondDb.InboxMessages
            .SingleAsync(x => x.Id == secondMessageId, token);

        Assert.True(firstInventory.TryReserveSeat());
        Assert.True(secondInventory.TryReserveSeat());

        var firstAllocation = AllocationEntity.CreateConfirmed(firstRegistrationId, sessionId, now);
        var secondAllocation = AllocationEntity.CreateConfirmed(secondRegistrationId, sessionId, now);
        firstDb.Allocations.Add(firstAllocation);
        secondDb.Allocations.Add(secondAllocation);
        firstDb.AllocationOutboxMessages.Add(AllocationOutboxMessageFactory.Create(firstAllocation));
        secondDb.AllocationOutboxMessages.Add(AllocationOutboxMessageFactory.Create(secondAllocation));
        firstInbox.MarkProcessed(now);
        secondInbox.MarkProcessed(now);

        await firstDb.SaveChangesAsync(token);
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(async () => await secondDb.SaveChangesAsync(token));

        await using (var verifyDb = _fixture.CreateDbContext())
        {
            var inventory = await verifyDb.SessionInventories.AsNoTracking()
                .SingleAsync(x => x.SessionId == sessionId, token);
            var unprocessedInbox = await verifyDb.InboxMessages.AsNoTracking()
                .SingleAsync(x => x.Id == secondMessageId, token);

            Assert.Equal(1, inventory.ReservedCount);
            Assert.Null(unprocessedInbox.ProcessedAtUtc);
            Assert.Equal(1, await verifyDb.Allocations.CountAsync(
                x => x.SessionId == sessionId, token));
            Assert.Equal(1, await verifyDb.AllocationOutboxMessages.CountAsync(
                x => x.AllocationId == firstAllocation.Id, token));
            Assert.False(await verifyDb.AllocationOutboxMessages.AnyAsync(
                x => x.AllocationId == secondAllocation.Id, token));
        }

        Assert.Equal(AllocationProcessingResult.Rejected, await ProcessAsync(secondMessageId));

        await using var finalDb = _fixture.CreateDbContext();
        var finalAllocations = await finalDb.Allocations.AsNoTracking()
            .Where(x => x.SessionId == sessionId)
            .ToListAsync(token);

        Assert.Single(finalAllocations, x => x.Status == AllocationStatus.Confirmed);
        Assert.Single(finalAllocations, x => x.Status == AllocationStatus.Rejected);
        var finalAllocationIds = finalAllocations.Select(x => x.Id).ToArray();
        Assert.Equal(2, await finalDb.AllocationOutboxMessages.CountAsync(
            x => finalAllocationIds.Contains(x.AllocationId), token));
    }

    private async Task<(Guid MessageId, Guid RegistrationId, Guid SessionId)>
        SeedPendingRequestAsync(int? capacity)
    {
        var messageId = Guid.NewGuid();
        var registrationId = Guid.NewGuid();
        var sessionId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;
        var token = TestContext.Current.CancellationToken;

        await using var db = _fixture.CreateDbContext();

        if (capacity is not null)
        {
            db.SessionInventories.Add(SessionInventory.Create(
                sessionId, capacity.Value, now));
        }

        db.InboxMessages.Add(InboxMessage.Create(
            messageId, registrationId, sessionId, now, now));

        await db.SaveChangesAsync(token);
        return (messageId, registrationId, sessionId);
    }

    private async Task<AllocationProcessingResult> ProcessAsync(Guid messageId)
    {
        await using var db = _fixture.CreateDbContext();
        var service = new AllocationProcessingService(db);
        return await service.ProcessAsync(
            messageId,
            TestContext.Current.CancellationToken);
    }

    private static AllocationDecidedV1 Deserialize(string payload)
    {
        return JsonSerializer.Deserialize<AllocationDecidedV1>(payload, JsonOptions)
            ?? throw new InvalidOperationException("Could not deserialize outbox event.");
    }
}
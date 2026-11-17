using ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;
using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using ConferenceProFlow.Allocation.Worker.Domain.Enums;
using ConferenceProFlow.Allocation.Worker.Processing;
using Microsoft.EntityFrameworkCore;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.IntegrationTests.Processing;

[Collection("AllocationSqlServer")]
public sealed class AllocationConcurrencyIntegrationTests
{
    private readonly AllocationSqlServerFixture _fixture;

    public AllocationConcurrencyIntegrationTests(AllocationSqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task StaleInventoryVersion_RollsBackAllocationAndInboxUpdate()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
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
            await seedDb.SaveChangesAsync(cancellationToken);
        }

        // Both independent DbContexts read the same original RowVersion.
        await using var firstDb = _fixture.CreateDbContext();
        await using var secondDb = _fixture.CreateDbContext();

        var firstInventory = await firstDb.SessionInventories
            .SingleAsync(x => x.SessionId == sessionId, cancellationToken);
        var secondInventory = await secondDb.SessionInventories
            .SingleAsync(x => x.SessionId == sessionId, cancellationToken);
        var firstInbox = await firstDb.InboxMessages
            .SingleAsync(x => x.Id == firstMessageId, cancellationToken);
        var secondInbox = await secondDb.InboxMessages
            .SingleAsync(x => x.Id == secondMessageId, cancellationToken);

        Assert.True(firstInventory.TryReserveSeat());
        Assert.True(secondInventory.TryReserveSeat());

        firstDb.Allocations.Add(AllocationEntity.CreateConfirmed(
            firstRegistrationId, sessionId, now));
        firstInbox.MarkProcessed(now);
        secondDb.Allocations.Add(AllocationEntity.CreateConfirmed(
            secondRegistrationId, sessionId, now));
        secondInbox.MarkProcessed(now);

        // One transaction commits; the other must detect a stale RowVersion.
        await firstDb.SaveChangesAsync(cancellationToken);

        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(async () =>
        {
            await secondDb.SaveChangesAsync(cancellationToken);
        });

        await using (var verifyDb = _fixture.CreateDbContext())
        {
            var inventory = await verifyDb.SessionInventories.AsNoTracking()
                .SingleAsync(x => x.SessionId == sessionId, cancellationToken);
            var allocations = await verifyDb.Allocations.AsNoTracking()
                .Where(x => x.SessionId == sessionId)
                .ToListAsync(cancellationToken);
            var pendingInbox = await verifyDb.InboxMessages.AsNoTracking()
                .SingleAsync(x => x.Id == secondMessageId, cancellationToken);

            Assert.Equal(1, inventory.ReservedCount);
            Assert.Single(allocations);
            Assert.Equal(firstRegistrationId, allocations[0].RegistrationId);
            Assert.Null(pendingInbox.ProcessedAtUtc);
        }

        // A fresh business decision sees that capacity is now exhausted.
        var result = await ProcessAsync(secondMessageId, cancellationToken);
        Assert.Equal(AllocationProcessingResult.Rejected, result);

        await using var finalDb = _fixture.CreateDbContext();
        var finalInventory = await finalDb.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == sessionId, cancellationToken);
        var finalAllocations = await finalDb.Allocations.AsNoTracking()
            .Where(x => x.SessionId == sessionId)
            .ToListAsync(cancellationToken);
        var finalInbox = await finalDb.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == secondMessageId, cancellationToken);

        Assert.Equal(1, finalInventory.ReservedCount);
        Assert.Equal(2, finalAllocations.Count);
        Assert.Single(finalAllocations, x => x.Status == AllocationStatus.Confirmed);
        Assert.Single(finalAllocations, x => x.Status == AllocationStatus.Rejected);
        Assert.NotNull(finalInbox.ProcessedAtUtc);
    }

    [Fact]
    public async Task ConcurrentProcessingOfSameMessage_CreatesOnlyOneAllocation()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var sessionId = Guid.NewGuid();
        var messageId = Guid.NewGuid();
        var registrationId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;

        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.SessionInventories.Add(
                SessionInventory.Create(sessionId, 2, now));
            seedDb.InboxMessages.Add(InboxMessage.Create(
                messageId, registrationId, sessionId, now, now));
            await seedDb.SaveChangesAsync(cancellationToken);
        }

        // Every parallel operation gets a separate DbContext.
        var tasks = Enumerable.Range(0, 8)
            .Select(_ => ProcessAsync(messageId, cancellationToken));
        var results = await Task.WhenAll(tasks);

        Assert.Single(results, x => x == AllocationProcessingResult.Confirmed);
        Assert.All(results, result => Assert.Contains(result, new[]
        {
            AllocationProcessingResult.Confirmed,
            AllocationProcessingResult.AlreadyProcessed,
            AllocationProcessingResult.ExistingAllocation
        }));

        await using var db = _fixture.CreateDbContext();
        var inventory = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == sessionId, cancellationToken);
        var allocationCount = await db.Allocations.CountAsync(
            x => x.RegistrationId == registrationId, cancellationToken);
        var inbox = await db.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == messageId, cancellationToken);

        Assert.Equal(1, inventory.ReservedCount);
        Assert.Equal(1, allocationCount);
        Assert.NotNull(inbox.ProcessedAtUtc);
    }

    [Fact]
    public async Task HundredConcurrentRequests_ConfirmOnlyTenAvailableSeats()
    {
        const int capacity = 10;
        const int requestCount = 100;
        const int maximumParallelProcessors = 8;

        var cancellationToken = TestContext.Current.CancellationToken;
        var sessionId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;
        var requests = Enumerable.Range(0, requestCount)
            .Select(_ => (
                MessageId: Guid.NewGuid(),
                RegistrationId: Guid.NewGuid()))
            .ToArray();

        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.SessionInventories.Add(SessionInventory.Create(sessionId, capacity, now));

            foreach (var request in requests)
            {
                seedDb.InboxMessages.Add(InboxMessage.Create(
                    request.MessageId,
                    request.RegistrationId,
                    sessionId,
                    now,
                    now));
            }

            await seedDb.SaveChangesAsync(cancellationToken);
        }

        // Stress the SQL row using many concurrent processing operations.
        // Bound active connections so the test is reproducible on laptops.
        using var gate = new SemaphoreSlim(maximumParallelProcessors);

        var tasks = requests.Select(async request =>
        {
            await gate.WaitAsync(cancellationToken);
            try
            {
                return await ProcessAsync(request.MessageId, cancellationToken);
            }
            finally
            {
                gate.Release();
            }
        });

        var results = await Task.WhenAll(tasks);

        Assert.Equal(capacity, results.Count(
            x => x == AllocationProcessingResult.Confirmed));
        Assert.Equal(requestCount - capacity, results.Count(
            x => x == AllocationProcessingResult.Rejected));

        await using var db = _fixture.CreateDbContext();
        var inventory = await db.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == sessionId, cancellationToken);
        var allocations = await db.Allocations.AsNoTracking()
            .Where(x => x.SessionId == sessionId)
            .ToListAsync(cancellationToken);
        var inboxMessages = await db.InboxMessages.AsNoTracking()
            .Where(x => x.SessionId == sessionId)
            .ToListAsync(cancellationToken);

        Assert.Equal(capacity, inventory.ReservedCount);
        Assert.Equal(requestCount, allocations.Count);
        Assert.Equal(capacity, allocations.Count(
            x => x.Status == AllocationStatus.Confirmed));
        Assert.Equal(requestCount - capacity, allocations.Count(
            x => x.Status == AllocationStatus.Rejected));
        Assert.Equal(requestCount, allocations.Select(x => x.RegistrationId)
            .Distinct().Count());
        Assert.Equal(requestCount, inboxMessages.Count);
        Assert.All(inboxMessages, message => Assert.NotNull(message.ProcessedAtUtc));
    }

    private async Task<AllocationProcessingResult> ProcessAsync(
        Guid messageId,
        CancellationToken cancellationToken)
    {
        await using var db = _fixture.CreateDbContext();
        var processor = new AllocationProcessingService(db);
        return await processor.ProcessAsync(messageId, cancellationToken);
    }
}
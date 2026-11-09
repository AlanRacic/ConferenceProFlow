using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;
using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using ConferenceProFlow.Allocation.Worker.Domain.Enums;
using ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Allocation.IntegrationTests.Persistence;

[Collection("AllocationSqlServer")]
public sealed class AllocationPersistenceTests
{
    private readonly AllocationSqlServerFixture _fixture;

    public AllocationPersistenceTests(AllocationSqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task SessionInventory_CanBePersistedAndRetrieved()
    {
        var sessionId = Guid.NewGuid();

        var inventory = SessionInventory.Create(
            sessionId,
            100,
            DateTimeOffset.UtcNow);

        await using (var db = _fixture.CreateDbContext())
        {
            db.SessionInventories.Add(inventory);

            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var verificationDb = _fixture.CreateDbContext();

        var saved = await verificationDb.SessionInventories
            .AsNoTracking()
            .SingleAsync(x => x.SessionId == sessionId, TestContext.Current.CancellationToken);

        Assert.Equal(100, saved.Capacity);
        Assert.Equal(0, saved.ReservedCount);
        Assert.NotEmpty(saved.RowVersion);
    }

    [Fact]
    public async Task ConfirmedAllocation_CanBePersisted()
    {
        var sessionId = await CreateInventoryAsync();

        var registrationId = Guid.NewGuid();

        var allocation = AllocationEntity.CreateConfirmed(
            registrationId,
            sessionId,
            DateTimeOffset.UtcNow);

        await using (var db = _fixture.CreateDbContext())
        {
            db.Allocations.Add(allocation);

            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var verificationDb = _fixture.CreateDbContext();

        var saved = await verificationDb.Allocations
            .AsNoTracking()
            .SingleAsync(
                x => x.RegistrationId == registrationId,
                TestContext.Current.CancellationToken);

        Assert.Equal(AllocationStatus.Confirmed, saved.Status);
        Assert.Null(saved.RejectionReason);
    }

    [Fact]
    public async Task RejectedAllocation_CanBePersisted()
    {
        var sessionId = await CreateInventoryAsync();

        var registrationId = Guid.NewGuid();

        var allocation = AllocationEntity.CreateRejected(
            registrationId,
            sessionId,
            "Session capacity reached.",
            DateTimeOffset.UtcNow);

        await using (var db = _fixture.CreateDbContext())
        {
            db.Allocations.Add(allocation);

            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var verificationDb = _fixture.CreateDbContext();

        var saved = await verificationDb.Allocations
            .AsNoTracking()
            .SingleAsync(
                x => x.RegistrationId == registrationId,
                TestContext.Current.CancellationToken);

        Assert.Equal(AllocationStatus.Rejected, saved.Status);
        Assert.Equal("Session capacity reached.", saved.RejectionReason);
    }

    [Fact]
    public async Task DuplicateRegistrationId_IsRejectedByDatabase()
    {
        var sessionId = await CreateInventoryAsync();

        var registrationId = Guid.NewGuid();

        await using (var db = _fixture.CreateDbContext())
        {
            db.Allocations.Add(AllocationEntity.CreateConfirmed(
                    registrationId,
                    sessionId,
                    DateTimeOffset.UtcNow));

            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var duplicateDb = _fixture.CreateDbContext();

        duplicateDb.Allocations.Add(AllocationEntity.CreateConfirmed(
                registrationId,
                sessionId,
                DateTimeOffset.UtcNow));

        await Assert.ThrowsAsync<DbUpdateException>(async () =>
        {
            await duplicateDb.SaveChangesAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task Allocation_WithUnknownSession_IsRejectedByForeignKey()
    {
        var allocation = AllocationEntity.CreateConfirmed(
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        await using var db = _fixture.CreateDbContext();

        db.Allocations.Add(allocation);

        await Assert.ThrowsAsync<DbUpdateException>(async () =>
        {
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task ReservedCount_AboveCapacity_IsRejectedByCheckConstraint()
    {
        var sessionId = await CreateInventoryAsync(capacity: 2);

        await using var db = _fixture.CreateDbContext();

        var invalidReservedCount = 3;

        await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                UPDATE [SessionInventories]
                SET [ReservedCount] = {invalidReservedCount}
                WHERE [SessionId] = {sessionId}
                """,
                TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task ReservedCount_NegativeValue_IsRejectedByCheckConstraint()
    {
        var sessionId = await CreateInventoryAsync(capacity: 2);

        await using var db = _fixture.CreateDbContext();

        var invalidReservedCount = -1;

        await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                UPDATE [SessionInventories]
                SET [ReservedCount] = {invalidReservedCount}
                WHERE [SessionId] = {sessionId}
                """,
                TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task Allocation_InvalidStatus_IsRejectedByCheckConstraint()
    {
        var sessionId = await CreateInventoryAsync();

        var allocation = AllocationEntity.CreateConfirmed(
            Guid.NewGuid(),
            sessionId,
            DateTimeOffset.UtcNow);

        await using var db = _fixture.CreateDbContext();

        db.Allocations.Add(allocation);

        await db.SaveChangesAsync(TestContext.Current.CancellationToken);

        var invalidStatus = 3;

        await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                UPDATE [Allocations]
                SET [Status] = {invalidStatus}
                WHERE [Id] = {allocation.Id}
                """,
                TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task ConfirmedAllocation_WithRejectionReason_IsRejectedByDatabase()
    {
        var sessionId = await CreateInventoryAsync();

        var allocation = AllocationEntity.CreateConfirmed(
            Guid.NewGuid(),
            sessionId,
            DateTimeOffset.UtcNow);

        await using var db = _fixture.CreateDbContext();

        db.Allocations.Add(allocation);

        await db.SaveChangesAsync(TestContext.Current.CancellationToken);

        var invalidReason = "Unexpected rejection reason";

        await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                UPDATE [Allocations]
                SET [RejectionReason] = {invalidReason}
                WHERE [Id] = {allocation.Id}
                """,
                TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task SessionInventory_StaleRowVersion_ThrowsConcurrencyException()
    {
        var sessionId = await CreateInventoryAsync(capacity: 2);

        await using var firstDb = _fixture.CreateDbContext();
        await using var secondDb = _fixture.CreateDbContext();

        var firstInventory = await firstDb.SessionInventories
            .SingleAsync(
                x => x.SessionId == sessionId,
                TestContext.Current.CancellationToken);

        var secondInventory = await secondDb.SessionInventories
            .SingleAsync(
                x => x.SessionId == sessionId,
                TestContext.Current.CancellationToken);

        var originalRowVersion = firstInventory.RowVersion.ToArray();

        firstDb.Entry(firstInventory)
            .Property(x => x.ReservedCount)
            .CurrentValue = 1;

        await firstDb.SaveChangesAsync(TestContext.Current.CancellationToken);

        Assert.False(originalRowVersion.SequenceEqual(firstInventory.RowVersion));

        secondDb.Entry(secondInventory)
            .Property(x => x.ReservedCount)
            .CurrentValue = 1;

        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(
            async () =>
            {
                await secondDb.SaveChangesAsync(TestContext.Current.CancellationToken);
            });
    }

    private async Task<Guid> CreateInventoryAsync(int capacity = 10)
    {
        var sessionId = Guid.NewGuid();

        var inventory = SessionInventory.Create(
            sessionId,
            capacity,
            DateTimeOffset.UtcNow);

        await using var db = _fixture.CreateDbContext();

        db.SessionInventories.Add(inventory);

        await db.SaveChangesAsync(TestContext.Current.CancellationToken);

        return sessionId;
    }
}
using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Domain.Enums;
using ConferenceProFlow.Registration.Api.Infrastructure.Inbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;
using Xunit;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.IntegrationTests.Messaging;

/// <summary>One SQL Server Testcontainer shared across this test class.</summary>
public sealed class ResultReconciliationSqlFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _sql = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04").Build();

    public RegistrationDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<RegistrationDbContext>()
            .UseSqlServer(_sql.GetConnectionString())
            .Options;
        return new RegistrationDbContext(options);
    }

    public async ValueTask InitializeAsync()
    {
        await _sql.StartAsync();
        await using var db = CreateDbContext();
        await db.Database.MigrateAsync();
    }

    public async ValueTask DisposeAsync() => await _sql.DisposeAsync();
}

/// <summary>
/// Real SQL Server tests for transactional result reconciliation.
/// Test methods use unique RegistrationIds and share an isolated test database.
/// </summary>
public sealed class AllocationResultReconciliationIntegrationTests : IClassFixture<ResultReconciliationSqlFixture>
{
    private readonly ResultReconciliationSqlFixture _fixture;

    public AllocationResultReconciliationIntegrationTests(ResultReconciliationSqlFixture fixture)
    {
        _fixture = fixture;
    }

    private RegistrationDbContext CreateDbContext() => _fixture.CreateDbContext();

    private async Task<(Guid RegistrationId, Guid SessionId)> SeedRegistrationAsync()
    {
        var start = new DateTimeOffset(2027, 6, 15, 9, 0, 0, TimeSpan.Zero);
        var conference = Conference.Create("ConferenceProFlow Test Conference", start, start.AddDays(2));
        var session = ConferenceSession.Create(conference.Id, "Distributed systems", start.AddHours(1), start.AddHours(2));
        var participant = Participant.Create("Alex Morgan", $"alex.morgan.{Guid.NewGuid():N}@example.com");
        var registration = RegistrationEntity.Create(participant.Id, session.Id, DateTimeOffset.UtcNow);

        await using var db = CreateDbContext();
        db.Conferences.Add(conference);
        db.ConferenceSessions.Add(session);
        db.Participants.Add(participant);
        db.Registrations.Add(registration);
        await db.SaveChangesAsync();

        return (registration.Id, session.Id);
    }

    private static AllocationDecidedV1 ResultFor(Guid registrationId, Guid sessionId,
        string status = AllocationDecidedV1.ConfirmedStatus) =>
        new(Guid.NewGuid(), Guid.NewGuid(), registrationId, sessionId, status,
            status == AllocationDecidedV1.RejectedStatus ? "Session capacity reached." : null,
            DateTimeOffset.UtcNow);

    [Fact]
    public async Task ConfirmedResult_UpdatesRegistrationAndPersistsReceipt()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = ResultFor(registrationId, sessionId);
        await using (var db = CreateDbContext())
        {
            var outcome = await new AllocationResultReconciliationService(db).ApplyAsync(decision, TestContext.Current.CancellationToken);
            Assert.Equal(AllocationResultReconciliationResult.Applied, outcome);
        }

        await using var verify = CreateDbContext();
        var reg = await verify.Registrations.AsNoTracking().SingleAsync(x => x.Id == registrationId, TestContext.Current.CancellationToken);
        Assert.Equal(RegistrationStatus.Confirmed, reg.Status);
        Assert.Null(reg.RejectionReason);
        var receipt = await verify.AllocationResultInboxMessages.SingleAsync(x => x.Id == decision.MessageId, TestContext.Current.CancellationToken);
        Assert.Equal(decision.AllocationId, receipt.AllocationId);
    }

    [Fact]
    public async Task RejectedResult_PreservesReason()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = ResultFor(registrationId, sessionId, AllocationDecidedV1.RejectedStatus);
        await using (var db = CreateDbContext())
            Assert.Equal(AllocationResultReconciliationResult.Applied,
                await new AllocationResultReconciliationService(db).ApplyAsync(decision, TestContext.Current.CancellationToken));

        await using var verify = CreateDbContext();
        var reg = await verify.Registrations.AsNoTracking().SingleAsync(x => x.Id == registrationId, TestContext.Current.CancellationToken);
        Assert.Equal(RegistrationStatus.Rejected, reg.Status);
        Assert.Equal("Session capacity reached.", reg.RejectionReason);
        Assert.Equal(1, await verify.AllocationResultInboxMessages.CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SameMessageRedelivered_IsIdempotent()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = ResultFor(registrationId, sessionId);
        await using (var db = CreateDbContext())
            Assert.Equal(AllocationResultReconciliationResult.Applied,
                await new AllocationResultReconciliationService(db).ApplyAsync(decision, TestContext.Current.CancellationToken));
        await using (var db = CreateDbContext())
            Assert.Equal(AllocationResultReconciliationResult.DuplicateMessage,
                await new AllocationResultReconciliationService(db).ApplyAsync(decision, TestContext.Current.CancellationToken));

        await using var verify = CreateDbContext();
        Assert.Equal(1, await verify.AllocationResultInboxMessages.CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SameDecisionWithNewMessageId_IsBusinessDuplicate()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var original = ResultFor(registrationId, sessionId);
        await using (var db = CreateDbContext())
            Assert.Equal(AllocationResultReconciliationResult.Applied,
                await new AllocationResultReconciliationService(db).ApplyAsync(original, TestContext.Current.CancellationToken));
        await using (var db = CreateDbContext())
            Assert.Equal(AllocationResultReconciliationResult.DuplicateDecision,
                await new AllocationResultReconciliationService(db).ApplyAsync(
                    original with { MessageId = Guid.NewGuid() }, TestContext.Current.CancellationToken));

        await using var verify = CreateDbContext();
        Assert.Equal(1, await verify.AllocationResultInboxMessages.CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ConflictingDecision_DoesNotOverwriteFinalStatus()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var confirmed = ResultFor(registrationId, sessionId);
        await using (var db = CreateDbContext())
            Assert.Equal(AllocationResultReconciliationResult.Applied,
                await new AllocationResultReconciliationService(db).ApplyAsync(confirmed, TestContext.Current.CancellationToken));

        var conflicting = ResultFor(registrationId, sessionId, AllocationDecidedV1.RejectedStatus);
        await using (var db = CreateDbContext())
            Assert.Equal(AllocationResultReconciliationResult.Conflict,
                await new AllocationResultReconciliationService(db).ApplyAsync(conflicting, TestContext.Current.CancellationToken));

        await using var verify = CreateDbContext();
        var reg = await verify.Registrations.AsNoTracking().SingleAsync(x => x.Id == registrationId, TestContext.Current.CancellationToken);
        Assert.Equal(RegistrationStatus.Confirmed, reg.Status);
        Assert.Equal(1, await verify.AllocationResultInboxMessages.CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task WrongSession_ReturnsConflictWithoutReceipt()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        await using (var db = CreateDbContext())
            Assert.Equal(AllocationResultReconciliationResult.Conflict,
                await new AllocationResultReconciliationService(db).ApplyAsync(
                    ResultFor(registrationId, Guid.NewGuid()), TestContext.Current.CancellationToken));

        await using var verify = CreateDbContext();
        Assert.Equal(0, await verify.AllocationResultInboxMessages.CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
        Assert.Equal(RegistrationStatus.Pending,
            (await verify.Registrations.SingleAsync(x => x.Id == registrationId, TestContext.Current.CancellationToken)).Status);
    }

    [Fact]
    public async Task UnknownRegistration_ReturnsExplicitOutcome()
    {
        await using var db = CreateDbContext();
        var outcome = await new AllocationResultReconciliationService(db)
            .ApplyAsync(ResultFor(Guid.NewGuid(), Guid.NewGuid()), TestContext.Current.CancellationToken);
        Assert.Equal(AllocationResultReconciliationResult.UnknownRegistration, outcome);
    }
}
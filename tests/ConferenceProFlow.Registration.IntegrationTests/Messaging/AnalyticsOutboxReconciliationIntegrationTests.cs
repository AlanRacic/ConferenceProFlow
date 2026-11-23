using System.Text.Json;
using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Domain.Enums;
using ConferenceProFlow.Registration.Api.Infrastructure.Inbox;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.IntegrationTests.Messaging;

/// <summary>
/// Verifies Registration status, result Inbox and Analytics Outbox using the project's existing SQL Server Testcontainers fixture (not EF InMemory).
/// </summary>
public sealed class AnalyticsOutboxReconciliationIntegrationTests :
    IClassFixture<ResultReconciliationSqlFixture>
{
    private readonly ResultReconciliationSqlFixture _fixture;

    public AnalyticsOutboxReconciliationIntegrationTests(ResultReconciliationSqlFixture fixture) => _fixture = fixture;

    private async Task<(Guid RegistrationId, Guid SessionId)> SeedRegistrationAsync()
    {
        var start = new DateTimeOffset(2027, 6, 15, 9, 0, 0, TimeSpan.Zero);
        var conference = Conference.Create("Kafka Analytics Integration Conference", start, start.AddDays(2));
        var session = ConferenceSession.Create(
            conference.Id, "Reliable Kafka finalization", start.AddHours(1), start.AddHours(2));
        var participant = Participant.Create(
            "Alex Morgan", $"alex.morgan.{Guid.NewGuid():N}@example.com");
        var registration = RegistrationEntity.Create(
            participant.Id, session.Id, DateTimeOffset.UtcNow);

        await using var db = _fixture.CreateDbContext();
        db.Conferences.Add(conference);
        db.ConferenceSessions.Add(session);
        db.Participants.Add(participant);
        db.Registrations.Add(registration);
        await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        return (registration.Id, session.Id);
    }

    private static AllocationDecidedV1 DecisionFor(
        Guid registrationId, Guid sessionId, string status = AllocationDecidedV1.ConfirmedStatus,
        Guid? allocationId = null, Guid? messageId = null) => new(
            messageId ?? Guid.NewGuid(),
            allocationId ?? Guid.NewGuid(),
            registrationId,
            sessionId,
            status,
            status == AllocationDecidedV1.RejectedStatus ? "Session capacity reached." : null,
            DateTimeOffset.UtcNow);

    [Theory]
    [InlineData(AllocationDecidedV1.ConfirmedStatus, RegistrationFinalizedV1.ConfirmedStatus)]
    [InlineData(AllocationDecidedV1.RejectedStatus, RegistrationFinalizedV1.RejectedStatus)]
    public async Task Finalization_PersistsStatusInboxAndOneAnalyticsEvent(
        string allocationStatus, string expectedFinalStatus)
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = DecisionFor(registrationId, sessionId, allocationStatus);

        await using (var db = _fixture.CreateDbContext())
        {
            var result = await new AllocationResultReconciliationService(db)
                .ApplyAsync(decision, TestContext.Current.CancellationToken);
            Assert.Equal(AllocationResultReconciliationResult.Applied, result);
        }

        await using var verify = _fixture.CreateDbContext();
        var registration = await verify.Registrations.AsNoTracking()
            .SingleAsync(x => x.Id == registrationId, TestContext.Current.CancellationToken);
        var inbox = await verify.AllocationResultInboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == decision.MessageId, TestContext.Current.CancellationToken);
        var outbox = await verify.AnalyticsOutboxMessages.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken);
        var payload = JsonSerializer.Deserialize<RegistrationFinalizedV1>(outbox.Payload);

        Assert.NotNull(payload);
        Assert.Equal(registrationId, payload.RegistrationId);
        Assert.Equal(sessionId, payload.SessionId);
        Assert.Equal(expectedFinalStatus, payload.Status);
        Assert.Equal(outbox.Id, payload.MessageId);
        Assert.Equal(inbox.RegistrationId, outbox.RegistrationId);
        Assert.Equal(RegistrationFinalizedV1.EventType, outbox.MessageType);
        Assert.Null(outbox.PublishedAtUtc);
        Assert.Equal(0, outbox.PublishAttempts);
        Assert.Equal(
            allocationStatus == AllocationDecidedV1.ConfirmedStatus
                ? RegistrationStatus.Confirmed : RegistrationStatus.Rejected,
            registration.Status);
    }

    [Fact]
    public async Task DuplicateMessageAndDecision_DoNotDuplicateAnalyticsOutbox()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = DecisionFor(registrationId, sessionId);

        await using (var db = _fixture.CreateDbContext())
        {
            var result = await new AllocationResultReconciliationService(db)
                .ApplyAsync(decision, TestContext.Current.CancellationToken);
            Assert.Equal(AllocationResultReconciliationResult.Applied, result);
        }
        await using (var db = _fixture.CreateDbContext())
        {
            var result = await new AllocationResultReconciliationService(db)
                .ApplyAsync(decision, TestContext.Current.CancellationToken);
            Assert.Equal(AllocationResultReconciliationResult.DuplicateMessage, result);
        }
        // Keep the same business decision, including OccurredAtUtc.
        // Only the transport MessageId changes to simulate republishing the same decision.
        var sameDecisionNewTransportMessage = decision with { MessageId = Guid.NewGuid() };
        await using (var db = _fixture.CreateDbContext())
        {
            var result = await new AllocationResultReconciliationService(db)
                .ApplyAsync(sameDecisionNewTransportMessage, TestContext.Current.CancellationToken);
            Assert.Equal(AllocationResultReconciliationResult.DuplicateDecision, result);
        }

        await using var verify = _fixture.CreateDbContext();
        Assert.Equal(1, await verify.AnalyticsOutboxMessages
            .CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
        Assert.Equal(1, await verify.AllocationResultInboxMessages
            .CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Conflict_DoesNotChangeStatusOrCreateAnalyticsEvent()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var wrongSessionDecision = DecisionFor(registrationId, Guid.NewGuid());

        await using (var db = _fixture.CreateDbContext())
        {
            var result = await new AllocationResultReconciliationService(db)
                .ApplyAsync(wrongSessionDecision, TestContext.Current.CancellationToken);
            Assert.Equal(AllocationResultReconciliationResult.Conflict, result);
        }

        await using var verify = _fixture.CreateDbContext();
        Assert.Equal(RegistrationStatus.Pending,
            (await verify.Registrations.SingleAsync(
                x => x.Id == registrationId, TestContext.Current.CancellationToken)).Status);
        Assert.Equal(0, await verify.AllocationResultInboxMessages
            .CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
        Assert.Equal(0, await verify.AnalyticsOutboxMessages
            .CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
    }
}
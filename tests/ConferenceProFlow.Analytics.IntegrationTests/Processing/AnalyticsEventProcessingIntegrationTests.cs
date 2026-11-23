using System.Text.Json;
using ConferenceProFlow.Analytics.IntegrationTests.Persistence;
using ConferenceProFlow.Analytics.Worker.Application.Processing;
using ConferenceProFlow.Analytics.Worker.Domain.Entities;
using ConferenceProFlow.Analytics.Worker.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Analytics.IntegrationTests.Processing;

/// <summary>
/// M3.4D SQL integration tests. Each test uses a real migrated SQL Server
/// Testcontainer; parallel operations create independent DbContexts.
/// Kafka offset handling and invalid-record quarantine are M3.4E tests.
/// </summary>
public sealed class AnalyticsEventProcessingIntegrationTests(AnalyticsSqlFixture fixture)
    : IClassFixture<AnalyticsSqlFixture>
{
    private static AnalyticsRegistrationFinalizedV1 NewEvent(Guid sessionId, string status =
        AnalyticsRegistrationFinalizedV1.ConfirmedStatus) => new(
            Guid.NewGuid(), Guid.NewGuid(), sessionId, status, DateTimeOffset.UtcNow);

    private async Task<AnalyticsProcessingResult> ProcessAsync(
        AnalyticsRegistrationFinalizedV1 evt, CancellationToken ct)
    {
        await using var db = fixture.CreateDbContext();
        var processor = new AnalyticsEventProcessingService(db);
        return await processor.ProcessAsync(evt, ct);
    }

    [Fact]
    public async Task ThreeFinalizations_ProduceTwoConfirmedAndOneRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        var sessionId = Guid.NewGuid();
        var events = new[]
        {
            NewEvent(sessionId),
            NewEvent(sessionId),
            NewEvent(sessionId, AnalyticsRegistrationFinalizedV1.RejectedStatus)
        };
        foreach (var evt in events)
            Assert.Equal(AnalyticsProcessingOutcome.Applied, (await ProcessAsync(evt, ct)).Outcome);

        await using var db = fixture.CreateDbContext();
        var stats = await db.SessionRegistrationStats.SingleAsync(x => x.SessionId == sessionId, ct);
        Assert.Equal(2, stats.ConfirmedCount);
        Assert.Equal(1, stats.RejectedCount);
        Assert.Equal(3, await db.ProcessedAnalyticsEvents.CountAsync(x => x.SessionId == sessionId, ct));
    }

    [Fact]
    public async Task ExactDuplicateMessage_DoesNotIncreaseCounter()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent(Guid.NewGuid());
        Assert.Equal(AnalyticsProcessingOutcome.Applied, (await ProcessAsync(evt, ct)).Outcome);
        Assert.Equal(AnalyticsProcessingOutcome.DuplicateMessage, (await ProcessAsync(evt, ct)).Outcome);

        await using var db = fixture.CreateDbContext();
        var stats = await db.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct);
        Assert.Equal(1, stats.ConfirmedCount);
        Assert.Equal(0, stats.RejectedCount);
        Assert.Equal(1, await db.ProcessedAnalyticsEvents.CountAsync(x => x.RegistrationId == evt.RegistrationId, ct));
    }

    [Fact]
    public async Task EquivalentFinalizationWithDifferentMessageId_IsBusinessDuplicate()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent(Guid.NewGuid());
        Assert.Equal(AnalyticsProcessingOutcome.Applied, (await ProcessAsync(evt, ct)).Outcome);
        var replay = evt with { MessageId = Guid.NewGuid() };
        Assert.Equal(AnalyticsProcessingOutcome.DuplicateBusinessEvent,
            (await ProcessAsync(replay, ct)).Outcome);

        await using var db = fixture.CreateDbContext();
        Assert.Equal(1, await db.ProcessedAnalyticsEvents.CountAsync(x => x.RegistrationId == evt.RegistrationId, ct));
        Assert.Equal(1, (await db.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct)).ConfirmedCount);
    }

    [Fact]
    public async Task ChangedFinalStatusForRegistration_IsConflictAndOriginalRemains()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent(Guid.NewGuid());
        Assert.Equal(AnalyticsProcessingOutcome.Applied, (await ProcessAsync(evt, ct)).Outcome);
        var conflicting = evt with
        {
            MessageId = Guid.NewGuid(),
            Status = AnalyticsRegistrationFinalizedV1.RejectedStatus
        };
        Assert.Equal(AnalyticsProcessingOutcome.Conflict, (await ProcessAsync(conflicting, ct)).Outcome);

        await using var db = fixture.CreateDbContext();
        Assert.Equal(1, (await db.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct)).ConfirmedCount);
        Assert.Equal(0, (await db.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct)).RejectedCount);
        Assert.Equal(evt.MessageId,
            (await db.ProcessedAnalyticsEvents.SingleAsync(x => x.RegistrationId == evt.RegistrationId, ct)).MessageId);
    }

    [Fact]
    public async Task ReusedMessageIdWithDifferentRegistration_IsConflict()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent(Guid.NewGuid());
        Assert.Equal(AnalyticsProcessingOutcome.Applied, (await ProcessAsync(evt, ct)).Outcome);
        var conflicting = evt with { RegistrationId = Guid.NewGuid() };
        Assert.Equal(AnalyticsProcessingOutcome.Conflict, (await ProcessAsync(conflicting, ct)).Outcome);
        await using var db = fixture.CreateDbContext();
        Assert.Equal(1, await db.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == evt.MessageId, ct));
    }

    [Fact]
    public async Task ParallelDistinctRegistrations_SameNewSession_HaveNoLostUpdates()
    {
        var ct = TestContext.Current.CancellationToken;
        var sessionId = Guid.NewGuid();
        var events = Enumerable.Range(0, 12).Select(_ => NewEvent(sessionId)).ToArray();
        var outcomes = await Task.WhenAll(events.Select(evt => ProcessAsync(evt, ct)));
        Assert.All(outcomes, outcome => Assert.Equal(AnalyticsProcessingOutcome.Applied, outcome.Outcome));

        await using var db = fixture.CreateDbContext();
        Assert.Equal(12, (await db.SessionRegistrationStats.SingleAsync(x => x.SessionId == sessionId, ct)).ConfirmedCount);
        Assert.Equal(12, await db.ProcessedAnalyticsEvents.CountAsync(x => x.SessionId == sessionId, ct));
    }

    [Fact]
    public async Task ParallelExactDuplicates_OnlyOneApplies()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent(Guid.NewGuid());
        var outcomes = await Task.WhenAll(Enumerable.Range(0, 6).Select(_ => ProcessAsync(evt, ct)));
        Assert.Equal(1, outcomes.Count(x => x.Outcome == AnalyticsProcessingOutcome.Applied));
        Assert.Equal(5, outcomes.Count(x => x.Outcome == AnalyticsProcessingOutcome.DuplicateMessage));

        await using var db = fixture.CreateDbContext();
        Assert.Equal(1, (await db.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct)).ConfirmedCount);
        Assert.Equal(1, await db.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == evt.MessageId, ct));
    }

    [Fact]
    public async Task ProjectionFailure_RollsBackAlreadyInsertedProcessingRecord()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent(Guid.NewGuid());
        await using (var seed = fixture.CreateDbContext())
        {
            seed.SessionRegistrationStats.Add(new SessionRegistrationStat
            {
                SessionId = evt.SessionId,
                ConfirmedCount = long.MaxValue,
                RejectedCount = 0,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            });
            await seed.SaveChangesAsync(ct);
        }

        // bigint overflow must fail the atomic UPDATE after the ledger INSERT.
        await Assert.ThrowsAnyAsync<Exception>(() => ProcessAsync(evt, ct));

        await using var verify = fixture.CreateDbContext();
        Assert.Equal(0, await verify.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == evt.MessageId, ct));
        Assert.Equal(long.MaxValue,
            (await verify.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct)).ConfirmedCount);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("CONFIRMED")]
    public async Task InvalidStatus_IsRejectedBeforeSqlWrite(string status)
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent(Guid.NewGuid(), status);
        await Assert.ThrowsAsync<ArgumentException>(() => ProcessAsync(evt, ct));

        await using var verify = fixture.CreateDbContext();
        Assert.Equal(0, await verify.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == evt.MessageId, ct));
    }

    [Fact]
    public void EventContract_UsesSameCamelCaseJsonFieldsAsRegistrationPublisher()
    {
        var evt = NewEvent(Guid.NewGuid());
        var json = JsonSerializer.Serialize(evt);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("messageId", out _));
        Assert.True(doc.RootElement.TryGetProperty("registrationId", out _));
        Assert.True(doc.RootElement.TryGetProperty("sessionId", out _));
        Assert.True(doc.RootElement.TryGetProperty("status", out _));
        Assert.True(doc.RootElement.TryGetProperty("occurredAtUtc", out _));

        var restored = JsonSerializer.Deserialize<AnalyticsRegistrationFinalizedV1>(json);
        Assert.NotNull(restored);
        restored.Validate();
        Assert.Equal(evt, restored);
    }
}

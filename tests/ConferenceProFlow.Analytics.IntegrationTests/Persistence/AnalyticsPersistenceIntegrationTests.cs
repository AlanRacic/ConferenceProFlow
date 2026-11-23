using ConferenceProFlow.Analytics.Worker.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Analytics.IntegrationTests.Persistence;

/// <summary>
/// M3.4B persistence-only checks against real SQL Server. These do NOT claim
/// consumer idempotency or concurrency-safe projections (M3.4C-D).
/// </summary>
public sealed class AnalyticsPersistenceIntegrationTests(AnalyticsSqlFixture fixture)
    : IClassFixture<AnalyticsSqlFixture>
{
    private static ProcessedAnalyticsEvent EventFor(Guid sessionId, Guid? registrationId = null) => new()
    {
        MessageId = Guid.NewGuid(),
        RegistrationId = registrationId ?? Guid.NewGuid(),
        SessionId = sessionId,
        Status = "confirmed",
        OccurredAtUtc = DateTimeOffset.UtcNow,
        ProcessedAtUtc = DateTimeOffset.UtcNow
    };

    [Fact]
    public async Task InitialMigration_CreatesAllThreeAnalyticsTables()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = fixture.CreateDbContext();

        var applied = (await db.Database.GetAppliedMigrationsAsync(ct)).ToArray();
        Assert.Contains(applied, name => name.EndsWith("_InitialAnalyticsPersistence", StringComparison.Ordinal));
        // These queries must succeed against the migrated SQL schema.
        // Do not assume test order or that previous cases left tables empty.
        _ = await db.ProcessedAnalyticsEvents.CountAsync(ct);
        _ = await db.SessionRegistrationStats.CountAsync(ct);
        _ = await db.QuarantinedAnalyticsEvents.CountAsync(ct);
    }

    [Fact]
    public async Task ProcessedEvent_EnforcesUniqueRegistrationId()
    {
        var ct = TestContext.Current.CancellationToken;
        var sessionId = Guid.NewGuid();
        var registrationId = Guid.NewGuid();
        await using (var db = fixture.CreateDbContext())
        {
            db.ProcessedAnalyticsEvents.Add(EventFor(sessionId, registrationId));
            await db.SaveChangesAsync(ct);
        }

        await using (var db = fixture.CreateDbContext())
        {
            db.ProcessedAnalyticsEvents.Add(EventFor(sessionId, registrationId));
            await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync(ct));
        }

        await using var verify = fixture.CreateDbContext();
        Assert.Equal(1, await verify.ProcessedAnalyticsEvents
            .CountAsync(x => x.RegistrationId == registrationId, ct));
    }

    [Fact]
    public async Task ProcessedEvent_RejectsUnsupportedStatus()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = fixture.CreateDbContext();
        var row = EventFor(Guid.NewGuid());
        row.Status = "pending";
        db.ProcessedAnalyticsEvents.Add(row);
        await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync(ct));
    }

    [Fact]
    public async Task SessionStats_EnforcesNonnegativeCounters()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = fixture.CreateDbContext();
        db.SessionRegistrationStats.Add(new SessionRegistrationStat
        {
            SessionId = Guid.NewGuid(),
            ConfirmedCount = -1,
            RejectedCount = 0,
            UpdatedAtUtc = DateTimeOffset.UtcNow
        });
        await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync(ct));
    }

    [Fact]
    public async Task Quarantine_LocationIsUnique()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = $"conferenceproflow.m34b.{Guid.NewGuid():N}";
        var original = new QuarantinedAnalyticsEvent
        {
            Topic = topic,
            Partition = 0,
            Offset = 4,
            MessageKey = "invalid-key",
            Payload = "{bad-json}",
            Reason = "Malformed JSON",
            ReceivedAtUtc = DateTimeOffset.UtcNow
        };
        await using (var db = fixture.CreateDbContext())
        {
            db.QuarantinedAnalyticsEvents.Add(original);
            await db.SaveChangesAsync(ct);
        }

        await using (var db = fixture.CreateDbContext())
        {
            db.QuarantinedAnalyticsEvents.Add(new QuarantinedAnalyticsEvent
            {
                Topic = topic,
                Partition = 0,
                Offset = 4,
                Payload = "different payload",
                Reason = "Conflicting retry",
                ReceivedAtUtc = DateTimeOffset.UtcNow
            });
            await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync(ct));
        }
    }

    [Fact]
    public async Task OneSaveChanges_DoesNotLeaveStatWhenProcessedEventInsertFails()
    {
        var ct = TestContext.Current.CancellationToken;
        var sessionId = Guid.NewGuid();
        var registrationId = Guid.NewGuid();
        await using (var db = fixture.CreateDbContext())
        {
            db.ProcessedAnalyticsEvents.Add(EventFor(sessionId, registrationId));
            await db.SaveChangesAsync(ct);
        }

        await using (var db = fixture.CreateDbContext())
        {
            db.ProcessedAnalyticsEvents.Add(EventFor(sessionId, registrationId));
            db.SessionRegistrationStats.Add(new SessionRegistrationStat
            {
                SessionId = sessionId,
                ConfirmedCount = 1,
                RejectedCount = 0,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            });
            // EF Core groups both writes in a transaction for this SaveChanges.
            await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync(ct));
        }

        await using var verify = fixture.CreateDbContext();
        Assert.Null(await verify.SessionRegistrationStats
            .SingleOrDefaultAsync(x => x.SessionId == sessionId, ct));
    }
}

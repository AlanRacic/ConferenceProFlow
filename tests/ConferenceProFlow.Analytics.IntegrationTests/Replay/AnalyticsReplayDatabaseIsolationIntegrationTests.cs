using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Domain.Entities;
using ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;

namespace ConferenceProFlow.Analytics.IntegrationTests.Replay;

/// <summary>
/// Uses one disposable SQL Server container with TWO independently migrated
/// databases. Does not access or modify the developer's local SQL databases.
/// </summary>
public sealed class AnalyticsReplayDatabaseIsolationIntegrationTests
{
    [Fact]
    public async Task ExistingEfMigration_CreatesIndependentReplayDatabase_WithoutChangingLiveData()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var sql = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
        await sql.StartAsync(ct);

        var liveConnection = new SqlConnectionStringBuilder(sql.GetConnectionString())
        {
            InitialCatalog = AnalyticsReplayOptions.LiveDatabaseName
        }.ConnectionString;

        var replayConnection = new SqlConnectionStringBuilder(sql.GetConnectionString())
        {
            InitialCatalog = AnalyticsReplayOptions.DatabaseName
        }.ConnectionString;

        AnalyticsReplaySafety.ValidateOrThrow(new AnalyticsReplayOptions
        {
            Enabled = true,
            GroupId = "conferenceproflow.analytics.replay.isolationtest"
        }, liveConnection, replayConnection, liveConsumerEnabled: false);

        await using (var live = CreateContext(liveConnection))
        {
            await live.Database.MigrateAsync(ct);
            live.SessionRegistrationStats.Add(new SessionRegistrationStat
            {
                SessionId = Guid.NewGuid(),
                ConfirmedCount = 4,
                RejectedCount = 1,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            });
            await live.SaveChangesAsync(ct);
        }

        await using (var replay = CreateContext(replayConnection))
        {
            // Reuse the SAME InitialAnalyticsPersistence migration, not a fork.
            await replay.Database.MigrateAsync(ct);
            var applied = (await replay.Database.GetAppliedMigrationsAsync(ct)).ToArray();
            Assert.Contains(applied, name =>
                name.EndsWith("_InitialAnalyticsPersistence", StringComparison.Ordinal));
            Assert.Equal(0, await replay.SessionRegistrationStats.CountAsync(ct));

            replay.SessionRegistrationStats.Add(new SessionRegistrationStat
            {
                SessionId = Guid.NewGuid(),
                ConfirmedCount = 2,
                RejectedCount = 1,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            });
            await replay.SaveChangesAsync(ct);
        }

        await using (var liveCheck = CreateContext(liveConnection))
        {
            Assert.Equal(1, await liveCheck.SessionRegistrationStats.CountAsync(ct));
            var liveStat = await liveCheck.SessionRegistrationStats.SingleAsync(ct);
            Assert.Equal(4, liveStat.ConfirmedCount);
            Assert.Equal(1, liveStat.RejectedCount);
        }

        await using (var replayCheck = CreateContext(replayConnection))
        {
            Assert.Equal(1, await replayCheck.SessionRegistrationStats.CountAsync(ct));
            var replayStat = await replayCheck.SessionRegistrationStats.SingleAsync(ct);
            Assert.Equal(2, replayStat.ConfirmedCount);
            Assert.Equal(1, replayStat.RejectedCount);
        }
    }

    private static AnalyticsDbContext CreateContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<AnalyticsDbContext>()
            .UseSqlServer(connectionString)
            .Options;
        return new AnalyticsDbContext(options);
    }
}

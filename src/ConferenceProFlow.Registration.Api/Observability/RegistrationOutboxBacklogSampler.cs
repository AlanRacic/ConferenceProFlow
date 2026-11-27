using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Registration.Api.Observability;

/// <summary>Samples two durable SQL Outboxes every 30 s. No SQL work runs inside an ObservableGauge callback.</summary>
internal sealed class RegistrationOutboxBacklogSampler(
    IServiceScopeFactory scopeFactory,
    ILogger<RegistrationOutboxBacklogSampler> logger) : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval);
        try
        {
            do
            {
                await SampleSafelyAsync(stoppingToken);
            }
            while (await timer.WaitForNextTickAsync(stoppingToken));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
    }

    private async Task SampleSafelyAsync(CancellationToken token)
    {
        try
        {
            using var scope = scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<RegistrationDbContext>();

            // Pending includes retries scheduled for the future; quarantined rows are excluded.
            var rabbitPendingQuery = db.OutboxMessages.AsNoTracking()
                .Where(x => x.PublishedAtUtc == null && x.QuarantinedAtUtc == null);
            var rabbitCount = await rabbitPendingQuery.CountAsync(token);
            var rabbitOldest = await rabbitPendingQuery
                .Select(x => (DateTimeOffset?)x.OccurredAtUtc).MinAsync(token);

            var analyticsPendingQuery = db.AnalyticsOutboxMessages.AsNoTracking()
                .Where(x => x.PublishedAtUtc == null && x.QuarantinedAtUtc == null);
            var analyticsCount = await analyticsPendingQuery.CountAsync(token);
            var analyticsOldest = await analyticsPendingQuery
                .Select(x => (DateTimeOffset?)x.OccurredAtUtc).MinAsync(token);

            var now = DateTimeOffset.UtcNow;
            // Atomic combined update: do not publish partial snapshots if any query fails.
            RegistrationOutboxBacklogMetrics.RecordSnapshot(
                rabbitCount, RegistrationOutboxBacklogMetrics.AgeSeconds(rabbitOldest, now),
                analyticsCount, RegistrationOutboxBacklogMetrics.AgeSeconds(analyticsOldest, now));
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            // Observability must not affect business processing or stop the host.
            // Cached data automatically expires after 2 minutes without a successful poll.
            logger.LogWarning(ex, "Registration Outbox backlog snapshot failed; retrying on next interval.");
        }
    }
}

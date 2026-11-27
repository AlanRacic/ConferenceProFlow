using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Allocation.Worker.Observability;

/// <summary>Samples durable Allocation Outbox state every 30 s, independently of publishing.</summary>
internal sealed class AllocationOutboxBacklogSampler(
    IServiceScopeFactory scopeFactory,
    ILogger<AllocationOutboxBacklogSampler> logger) : BackgroundService
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
            // Normal shutdown.
        }
    }

    private async Task SampleSafelyAsync(CancellationToken token)
    {
        try
        {
            using var scope = scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AllocationDbContext>();
            var pendingQuery = db.AllocationOutboxMessages.AsNoTracking()
                .Where(x => x.PublishedAtUtc == null && x.QuarantinedAtUtc == null);
            var count = await pendingQuery.CountAsync(token);
            var oldest = await pendingQuery
                .Select(x => (DateTimeOffset?)x.OccurredAtUtc).MinAsync(token);

            AllocationOutboxBacklogMetrics.RecordSnapshot(
                count,
                AllocationOutboxBacklogMetrics.AgeSeconds(oldest, DateTimeOffset.UtcNow));
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Allocation Outbox backlog snapshot failed; retrying on next interval.");
        }
    }
}

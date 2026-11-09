namespace ConferenceProFlow.Allocation.Worker;

public sealed class AllocationBackgroundService(ILogger<AllocationBackgroundService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Allocation Worker started. Message processing is not configured yet.");

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Expected during graceful shutdown.
        }

        logger.LogInformation("Allocation Worker stopped.");
    }
}
namespace ConferenceProFlow.Analytics.Worker;

/// <summary>
/// M3.4A foundation: starts and stops cleanly, but intentionally does not
/// consume Kafka events or write to any database. Replaced/extended in M3.4C.
/// </summary>
public sealed class AnalyticsWorkerBackgroundService(
    ILogger<AnalyticsWorkerBackgroundService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "Analytics Worker foundation started (M3.4A). Kafka consumption is not implemented yet.");

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal, graceful shutdown via Ctrl+C or host cancellation.
        }

        logger.LogInformation("Analytics Worker foundation stopped.");
    }
}

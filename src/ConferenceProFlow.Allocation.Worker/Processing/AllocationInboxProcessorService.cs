using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Observability;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace ConferenceProFlow.Allocation.Worker.Processing;

public sealed class AllocationInboxProcessorService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<AllocationInboxProcessorService> _logger;
    private readonly AllocationInboxProcessorOptions _options;

    // Transient processing failures are deferred in memory. SQL Inbox remains the durable source of truth; failed messages are not acknowledged/deleted.
    private readonly Dictionary<Guid, DateTimeOffset> _retryAfterUtc = new();

    public AllocationInboxProcessorService(
        IServiceScopeFactory scopeFactory,
        IOptions<AllocationInboxProcessorOptions> options,
        ILogger<AllocationInboxProcessorService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _options = options.Value;

        if (_options.BatchSize is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "BatchSize must be between 1 and 100.");
        }

        if (_options.PollIntervalSeconds is < 1 or > 3600)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "PollIntervalSeconds must be between 1 and 3600.");
        }

        if (_options.FailureRetrySeconds is < 1 or > 3600)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "FailureRetrySeconds must be between 1 and 3600.");
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Allocation Inbox processor started (batch size {BatchSize}, poll interval {PollIntervalSeconds}s).",
            _options.BatchSize,
            _options.PollIntervalSeconds);

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_options.PollIntervalSeconds));
        try
        {
            do
            {
                try
                {
                    await ProcessBatchAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    // A temporary database outage should not terminate the host.
                    _logger.LogError(
                        exception,
                        "Allocation Inbox polling failed; will retry on the next poll.");
                }
            }
            while (await timer.WaitForNextTickAsync(stoppingToken));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }

        _logger.LogInformation("Allocation Inbox processor stopped.");
    }

    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;

        // Remove expired per-message cooldowns before building the SQL query.
        foreach (var messageId in _retryAfterUtc
            .Where(entry => entry.Value <= now)
            .Select(entry => entry.Key)
            .ToArray())
        {
            _retryAfterUtc.Remove(messageId);
        }

        var deferredIds = _retryAfterUtc.Keys.ToArray();

        await using var queryScope = _scopeFactory.CreateAsyncScope();
        var db = queryScope.ServiceProvider.GetRequiredService<AllocationDbContext>();

        // Do not allow an older message without a SessionInventory to block ready requests. It becomes eligible automatically after provisioning.
        IQueryable<InboxMessage> query = db.InboxMessages
            .AsNoTracking()
            .Where(message =>
                message.ProcessedAtUtc == null &&
                db.SessionInventories.Any(inventory =>
                    inventory.SessionId == message.SessionId));

        if (deferredIds.Length > 0)
        {
            query = query.Where(message => !deferredIds.Contains(message.Id));
        }

        // Fetch the persisted tracing context with the same existing SQL query;
        // do not introduce an additional database round-trip for each message.
        var pendingItems = await query
            .OrderBy(message => message.ReceivedAtUtc)
            .ThenBy(message => message.Id)
            .Select(message => new
            {
                message.Id,
                message.TraceParent,
                message.TraceState
            })
            .Take(_options.BatchSize)
            .ToListAsync(cancellationToken);

        foreach (var pendingItem in pendingItems)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var messageId = pendingItem.Id;

            // SQL Inbox is a durable asynchronous boundary: create a NEW processing
            // span linked to the previously persisted consumer context, rather than
            // treating the finished consumer span as a live synchronous parent.
            using var activity = AllocationInboxProcessingTelemetry.StartActivity(
                pendingItem.TraceParent,
                pendingItem.TraceState);

            // A new scope/DbContext per message keeps EF tracking isolated, including after concurrency retries or SQL failures.
            await using var processingScope = _scopeFactory.CreateAsyncScope();
            var processor = processingScope.ServiceProvider.GetRequiredService<AllocationProcessingService>();

            // Measure the actual ProcessAsync attempt, not the queue polling interval.
            // Stopwatch uses a monotonic clock and does not change processing behavior.
            var processingStart = Stopwatch.GetTimestamp();
            try
            {
                var result = await processor.ProcessAsync(messageId, cancellationToken);
                AllocationInboxMetrics.RecordResult(result, Stopwatch.GetElapsedTime(processingStart));
                activity?.SetTag("conferenceproflow.allocation.processing.result", result.ToString());

                if (result is AllocationProcessingResult.WaitingForInventory
                    or AllocationProcessingResult.NotFound)
                {
                    Defer(messageId);
                }
                else
                {
                    _retryAfterUtc.Remove(messageId);
                }

                _logger.LogInformation(
                    "Allocation Inbox message {MessageId} processing result: {Result}.",
                    messageId, result);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Host shutdown is not a business-processing error.
                activity?.SetTag("conferenceproflow.allocation.processing.result", "cancelled");
                throw;
            }
            catch (Exception exception)
            {
                // Keep the existing retry behavior; observability only records the failure.
                activity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
                activity?.SetTag("conferenceproflow.allocation.processing.result", "error");
                AllocationInboxMetrics.RecordFailure(Stopwatch.GetElapsedTime(processingStart));
                Defer(messageId);
                _logger.LogError(
                    exception,
                    "Allocation Inbox message {MessageId} failed; it remains pending.",
                    messageId);
            }
        }
    }

    private void Defer(Guid messageId)
    {
        _retryAfterUtc[messageId] = DateTimeOffset.UtcNow.AddSeconds(
            _options.FailureRetrySeconds);
    }
}

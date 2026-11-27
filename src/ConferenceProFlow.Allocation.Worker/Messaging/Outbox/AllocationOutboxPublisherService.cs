using System.Diagnostics;
using System.Text;
using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Observability;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;

namespace ConferenceProFlow.Allocation.Worker.Messaging.Outbox;

/// <summary>
/// Publishes durable allocation decisions using at-least-once delivery.
/// A broker confirmation is required before recording PublishedAtUtc.
/// This service does not provision the Registration-owned result queue.
/// </summary>
public sealed class AllocationOutboxPublisherService : BackgroundService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(5);
    private const int BatchSize = 20;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<AllocationOutboxPublisherService> _logger;
    private readonly ConnectionFactory _connectionFactory;
    private readonly TimeSpan _publishTimeout;

    private IConnection? _connection;
    private IChannel? _channel;

    public AllocationOutboxPublisherService(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<AllocationOutboxPublisherService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        var timeoutSeconds = configuration.GetValue<int?>("AllocationOutboxPublisher:PublishTimeoutSeconds") ?? 15;
        if (timeoutSeconds is < 1 or > 120)
            throw new InvalidOperationException("AllocationOutboxPublisher:PublishTimeoutSeconds must be between 1 and 120.");
        _publishTimeout = TimeSpan.FromSeconds(timeoutSeconds);
        _connectionFactory = new ConnectionFactory
        {
            HostName = Required(configuration, "RabbitMq:HostName"),
            Port = configuration.GetValue<int?>("RabbitMq:Port") ?? 5672,
            VirtualHost = Required(configuration, "RabbitMq:VirtualHost"),
            UserName = Required(configuration, "RabbitMq:UserName"),
            Password = Required(configuration, "RabbitMq:Password"),
            ClientProvidedName = "ConferenceProFlow.Allocation.ResultPublisher",
            AutomaticRecoveryEnabled = false,
            RequestedConnectionTimeout = TimeSpan.FromSeconds(10)
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Allocation Outbox Publisher started.");
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await PublishPendingAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Allocation Outbox publishing cycle failed.");
                    await ResetConnectionAsync();
                }
                try
                {
                    await Task.Delay(PollInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
        finally
        {
            await ResetConnectionAsync();
            _logger.LogInformation("Allocation Outbox Publisher stopped.");
        }
    }

    private async Task PublishPendingAsync(CancellationToken cancellationToken)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AllocationDbContext>();
        var now = DateTimeOffset.UtcNow;
        var messages = await db.AllocationOutboxMessages
            .Where(x => x.PublishedAtUtc == null &&
                        x.QuarantinedAtUtc == null &&
                        (x.NextAttemptAtUtc == null || x.NextAttemptAtUtc <= now))
            .OrderBy(x => x.OccurredAtUtc)
            .ThenBy(x => x.Id)
            .Take(BatchSize)
            .ToListAsync(cancellationToken);

        foreach (var message in messages)
        {
            var permanentError = AllocationOutboxPayloadValidator.GetPermanentError(message);
            if (permanentError is not null)
            {
                message.Quarantine(permanentError, DateTimeOffset.UtcNow);
                await db.SaveChangesAsync(cancellationToken);
                _logger.LogError(
                    "Quarantined allocation result {MessageId}: {Reason}",
                    message.Id, permanentError);
                continue;
            }

            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            attemptCts.CancelAfter(_publishTimeout);

            // The Allocation decision was committed before this background publication.
            // Its persisted W3C metadata is a LINK, not a synchronous parent.
            using var publishActivity = AllocationResultPublishTelemetry.StartActivity(
                message.TraceParent,
                message.TraceState);

            publishActivity?.SetTag("messaging.system", "rabbitmq");
            publishActivity?.SetTag("messaging.operation.name", "send");
            publishActivity?.SetTag("messaging.destination.name", MessagingTopology.ExchangeName);

            try
            {
                await EnsureConnectionAsync(attemptCts.Token);
                var channel = _channel ?? throw new InvalidOperationException("RabbitMQ channel unavailable.");
                var properties = new BasicProperties
                {
                    MessageId = message.Id.ToString("D"),
                    Type = message.MessageType,
                    ContentType = "application/json",
                    Persistent = true,
                    // Metadata only. Never modify the durable business JSON or MessageId.
                    Headers = AllocationResultPublishTelemetry.CreateHeaders(
                        publishActivity,
                        message.TraceParent,
                        message.TraceState)
                };

                // Await broker confirmation. With mandatory=true and confirmation tracking,
                // an unroutable message causes BasicPublishAsync to fail.
                await channel.BasicPublishAsync(
                    exchange: MessagingTopology.ExchangeName,
                    routingKey: MessagingTopology.AllocationDecidedRoutingKey,
                    mandatory: true,
                    basicProperties: properties,
                    body: Encoding.UTF8.GetBytes(message.Payload),
                    cancellationToken: attemptCts.Token);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Host shutdown is not a failed publication attempt.
                throw;
            }
            catch (Exception ex)
            {
                publishActivity?.SetStatus(ActivityStatusCode.Error, ex.GetType().Name);

                var error = ex is OperationCanceledException && attemptCts.IsCancellationRequested
                    ? $"RabbitMQ publishing timed out after {_publishTimeout.TotalSeconds:0} seconds; outcome uncertain."
                    : string.IsNullOrWhiteSpace(ex.Message) ? ex.GetType().Name : ex.Message;
                message.RecordFailure(error, DateTimeOffset.UtcNow);
                await db.SaveChangesAsync(cancellationToken);
                _logger.LogWarning(ex,
                    "Allocation result {MessageId} failed to publish. Attempt {Attempts}, next retry {RetryAt}.",
                    message.Id, message.PublishAttempts, message.NextAttemptAtUtc);
                await ResetConnectionAsync();
                // Avoid a burst of broker connection failures across the batch.
                break;
            }

            // If SQL persistence fails here, the broker may already have the message.
            // At-least-once redelivery is intentional; M2.6D deduplicates.
            message.MarkPublished(DateTimeOffset.UtcNow);
            await db.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Published allocation result {MessageId}.", message.Id);
        }
    }

    private async Task EnsureConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection?.IsOpen == true && _channel?.IsOpen == true)
            return;

        await ResetConnectionAsync();
        _connection = await _connectionFactory.CreateConnectionAsync(cancellationToken);
        _channel = await _connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true),
            cancellationToken);
        // The Registration result consumer owns the queue and binding (M2.6D).
        // Do NOT mark an unroutable event as published.
        await _channel.ExchangeDeclareAsync(
            exchange: MessagingTopology.ExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);
    }

    private async Task ResetConnectionAsync()
    {
        if (_channel is not null)
        {
            try { await _channel.DisposeAsync(); }
            catch (Exception ex) { _logger.LogWarning(ex, "Could not dispose publisher channel."); }
            finally { _channel = null; }
        }
        if (_connection is not null)
        {
            try { await _connection.DisposeAsync(); }
            catch (Exception ex) { _logger.LogWarning(ex, "Could not dispose publisher connection."); }
            finally { _connection = null; }
        }
    }

    private static string Required(IConfiguration configuration, string key)
    {
        var value = configuration[key];
        return !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"Missing configuration '{key}'.");
    }
}

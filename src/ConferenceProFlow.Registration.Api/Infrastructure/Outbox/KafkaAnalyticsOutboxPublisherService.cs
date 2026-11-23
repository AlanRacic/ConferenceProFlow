using System.Text.Json;
using Confluent.Kafka;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

/// <summary>
/// Single-active-instance development publisher for RegistrationFinalizedV1 events.
/// Kafka may receive the same logical event twice if SQL publication-state persistence
/// fails after a successful Kafka acknowledgement. M3.4 consumer must be idempotent.
/// Multi-instance coordination / leasing is NOT implemented.
/// </summary>
public sealed class KafkaAnalyticsOutboxPublisherService : BackgroundService
{
    private const int BatchSize = 20;
    private const int PollIntervalMilliseconds = 1000;
    private const string DefaultTopic = "conferenceproflow.registration-finalized.v1";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<KafkaAnalyticsOutboxPublisherService> _logger;
    private readonly string _topic;
    private readonly IProducer<string, string> _producer;

    public KafkaAnalyticsOutboxPublisherService(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<KafkaAnalyticsOutboxPublisherService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;

        var bootstrapServers = configuration["Kafka:BootstrapServers"];
        if (string.IsNullOrWhiteSpace(bootstrapServers))
            throw new InvalidOperationException(
                "Kafka:BootstrapServers must be configured when KafkaAnalyticsPublisher:Enabled is true.");

        _topic = configuration["Kafka:RegistrationFinalizedTopic"] ?? DefaultTopic;
        if (string.IsNullOrWhiteSpace(_topic))
            throw new InvalidOperationException("Kafka topic must not be empty.");

        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = "conferenceproflow-registration-analytics",
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 15000
        }).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka Analytics Outbox publisher enabled; topic {Topic}", _topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessBatchAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // A temporary SQL outage must not terminate the Registration API.
                _logger.LogError(ex, "Kafka Analytics Outbox polling failed; will retry");
            }

            try
            {
                await Task.Delay(PollIntervalMilliseconds, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        List<Guid> messageIds;
        await using (var scope = _scopeFactory.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RegistrationDbContext>();
            var now = DateTimeOffset.UtcNow;
            messageIds = await db.AnalyticsOutboxMessages
                .AsNoTracking()
                .Where(x => x.PublishedAtUtc == null &&
                            x.QuarantinedAtUtc == null &&
                            (x.NextAttemptAtUtc == null || x.NextAttemptAtUtc <= now))
                .OrderBy(x => x.OccurredAtUtc)
                .ThenBy(x => x.Id)
                .Select(x => x.Id)
                .Take(BatchSize)
                .ToListAsync(cancellationToken);
        }

        foreach (var messageId in messageIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await PublishOneAsync(messageId, cancellationToken);
        }
    }

    private async Task PublishOneAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<RegistrationDbContext>();

        var outbox = await db.AnalyticsOutboxMessages
            .SingleOrDefaultAsync(x => x.Id == messageId, cancellationToken);

        if (outbox is null || outbox.PublishedAtUtc is not null || outbox.QuarantinedAtUtc is not null)
            return;
        if (outbox.NextAttemptAtUtc is { } retryAt && retryAt > DateTimeOffset.UtcNow)
            return;

        // All retries must use the *original* durable payload and MessageId.
        // There must not be a fresh RegistrationFinalizedV1.Create() call here.
        try
        {
            if (outbox.MessageType != RegistrationFinalizedV1.EventType)
                throw new InvalidOperationException("Unexpected analytics event type.");

            var integrationEvent = JsonSerializer.Deserialize<RegistrationFinalizedV1>(outbox.Payload)
                ?? throw new JsonException("Analytics outbox payload is empty.");
            integrationEvent.Validate();
            if (integrationEvent.MessageId != outbox.Id ||
                integrationEvent.RegistrationId != outbox.RegistrationId)
                throw new InvalidOperationException("Analytics outbox identity does not match its payload.");

            await _producer.ProduceAsync(_topic, new Message<string, string>
            {
                Key = integrationEvent.SessionId.ToString("D"),
                Value = outbox.Payload,
                Headers = new Headers
                {
                    { "message-id", System.Text.Encoding.UTF8.GetBytes(outbox.Id.ToString("D")) },
                    { "event-type", System.Text.Encoding.UTF8.GetBytes(outbox.MessageType) }
                }
            }, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Keep this row pending. SQL failure here is retried by the outer loop.
            // Failure details are intentionally bounded to fit the SQL column.
            var retrySeconds = Math.Min(60, Math.Pow(2, Math.Min(outbox.PublishAttempts, 6)));
            outbox.RecordPublishFailure(DateTimeOffset.UtcNow,
                ex.GetType().Name + ": " + ex.Message,
                TimeSpan.FromSeconds(retrySeconds));
            await db.SaveChangesAsync(cancellationToken);
            _logger.LogWarning("Kafka Analytics publish failed for {MessageId}; attempt {Attempt}",
                outbox.Id, outbox.PublishAttempts);
            return;
        }

        // Kafka acknowledged the event. If this DB save fails, the event remains
        // pending and may be published again; at-least-once semantics are explicit.
        outbox.MarkPublished(DateTimeOffset.UtcNow);
        await db.SaveChangesAsync(cancellationToken);
        _logger.LogInformation("Kafka Analytics event {MessageId} published to {Topic}", outbox.Id, _topic);
    }

    public override void Dispose()
    {
        _producer.Dispose();
        base.Dispose();
    }
}

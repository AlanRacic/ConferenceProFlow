using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using ConferenceProFlow.Registration.Api.Observability;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

/// <summary>
/// Single-active-instance development publisher for RegistrationFinalizedV1 events.
/// Kafka may receive the same logical event twice if SQL publication-state persistence
/// fails after a successful Kafka acknowledgement. Analytics consumers must be idempotent.
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
    private readonly int _alertAfterAttempts;
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
        _alertAfterAttempts = configuration.GetValue<int?>("KafkaAnalyticsPublisher:AlertAfterAttempts")
            ?? AnalyticsOutboxPublishAlertPolicy.DefaultAlertAfterAttempts;
        if (!AnalyticsOutboxPublishAlertPolicy.IsValidThreshold(_alertAfterAttempts))
            throw new InvalidOperationException(
                "KafkaAnalyticsPublisher:AlertAfterAttempts must be between 1 and 10000.");
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

        // The original reconciliation span may have finished long before this
        // polling iteration. The new producer span LINKs to persisted context.
        using var publishActivity = KafkaAnalyticsPublishTelemetry.StartActivity(
            outbox.TraceParent, outbox.TraceState);
        publishActivity?.SetTag("messaging.destination.name", _topic);

        // All retries must use the *original* durable payload and MessageId.
        // There must not be a fresh RegistrationFinalizedV1.Create() call here.
        // Failure category is for observability ONLY: no automatic discard or quarantine.
        var payloadValidated = false;
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
            payloadValidated = true;

            var headers = KafkaAnalyticsPublishTelemetry.CreateHeaders(
                outbox.Id,
                outbox.MessageType,
                publishActivity,
                outbox.TraceParent,
                outbox.TraceState);

            await _producer.ProduceAsync(_topic, new Message<string, string>
            {
                Key = integrationEvent.SessionId.ToString("D"),
                Value = outbox.Payload,
                Headers = headers
            }, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Host shutdown is not a failed publication attempt.
            throw;
        }
        catch (Exception ex)
        {
            publishActivity?.SetStatus(ActivityStatusCode.Error, ex.GetType().Name);

            // Keep this row pending. Do NOT mark it published or silently discard it.
            // Retain the existing exponential backoff (capped at 60 seconds).
            // If this SQL write fails, the outer polling loop will retry.
            var kind = AnalyticsOutboxPublishAlertPolicy.Classify(ex, payloadValidated);
            var retrySeconds = Math.Min(60, Math.Pow(2, Math.Min(outbox.PublishAttempts, 6)));
            outbox.RecordPublishFailure(
                DateTimeOffset.UtcNow,
                AnalyticsOutboxPublishAlertPolicy.Describe(kind, ex),
                TimeSpan.FromSeconds(retrySeconds));
            await db.SaveChangesAsync(cancellationToken);
            if (AnalyticsOutboxPublishAlertPolicy.ShouldEscalate(
                    outbox.PublishAttempts, _alertAfterAttempts))
            {
                // Escalate periodically (not every retry) so persistent failures are visible.
                // Operators must inspect the row; KafkaDelivery is not necessarily transient.
                _logger.LogError(
                    "Kafka Analytics Outbox requires attention: MessageId {MessageId}, RegistrationId {RegistrationId}, " +
                    "Category {FailureCategory}, Attempts {Attempts}, NextAttemptAtUtc {NextAttemptAtUtc}. " +
                    "Event remains pending; inspect AnalyticsOutboxMessages.",
                    outbox.Id, outbox.RegistrationId, kind, outbox.PublishAttempts, outbox.NextAttemptAtUtc);
            }
            else
            {
                _logger.LogWarning(
                    "Kafka Analytics publish failed: MessageId {MessageId}, Category {FailureCategory}, " +
                    "Attempts {Attempts}, NextAttemptAtUtc {NextAttemptAtUtc}",
                    outbox.Id, kind, outbox.PublishAttempts, outbox.NextAttemptAtUtc);
            }
            return;
        }

        // Kafka acknowledged the event. If this DB save fails, the event remains
        // pending and may be published again; at-least-once semantics are explicit.
        try
        {
            outbox.MarkPublished(DateTimeOffset.UtcNow);
            await db.SaveChangesAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            // Preserve the existing retry-on-next-poll semantics when SQL fails.
            publishActivity?.SetStatus(ActivityStatusCode.Error, ex.GetType().Name);
            throw;
        }

        _logger.LogInformation("Kafka Analytics event {MessageId} published to {Topic}", outbox.Id, _topic);
    }

    public override void Dispose()
    {
        _producer.Dispose();
        base.Dispose();
    }
}

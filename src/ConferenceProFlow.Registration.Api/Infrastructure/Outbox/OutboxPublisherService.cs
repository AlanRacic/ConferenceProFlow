using System.Text;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

public sealed class OutboxPublisherService : BackgroundService
{
    private static readonly TimeSpan PollingInterval = TimeSpan.FromSeconds(5);

    private readonly TimeSpan _publishTimeout;

    private const int BatchSize = 20;

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OutboxPublisherService> _logger;
    private readonly ConnectionFactory _connectionFactory;

    private IConnection? _connection;
    private IChannel? _channel;

    public OutboxPublisherService(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<OutboxPublisherService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;

        var publishTimeoutSeconds = configuration.GetValue<int?>("OutboxPublisher:PublishTimeoutSeconds") ?? 15;

        if (publishTimeoutSeconds < 1 || publishTimeoutSeconds > 120)
        {
            throw new InvalidOperationException("OutboxPublisher:PublishTimeoutSeconds must be between 1 and 120.");
        }

        _publishTimeout = TimeSpan.FromSeconds(publishTimeoutSeconds);

        _connectionFactory = new ConnectionFactory
        {
            HostName = Required(configuration, "RabbitMq:HostName"),
            Port = configuration.GetValue<int?>("RabbitMq:Port") ?? 5672,
            VirtualHost = Required(configuration, "RabbitMq:VirtualHost"),
            UserName = Required(configuration, "RabbitMq:UserName"),
            Password = Required(configuration, "RabbitMq:Password"),

            ClientProvidedName = "ConferenceProFlow.Registration.OutboxPublisher",

            AutomaticRecoveryEnabled = false
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Publisher started.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await PublishPendingMessagesAsync(stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Outbox publishing cycle failed.");

                    await ResetRabbitMqAsync();
                }

                try
                {
                    await Task.Delay(PollingInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
        finally
        {
            await ResetRabbitMqAsync();

            _logger.LogInformation("Outbox Publisher stopped.");
        }
    }

    private async Task PublishPendingMessagesAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();

        var db = scope.ServiceProvider.GetRequiredService<RegistrationDbContext>();

        var utcNow = DateTimeOffset.UtcNow;

        var messages = await db.OutboxMessages
            .Where(x =>
                x.PublishedAtUtc == null &&
                x.QuarantinedAtUtc == null &&
                (x.NextAttemptAtUtc == null ||
                 x.NextAttemptAtUtc <= utcNow))
            .OrderBy(x => x.OccurredAtUtc)
            .ThenBy(x => x.Id)
            .Take(BatchSize)
            .ToListAsync(cancellationToken);

        if (messages.Count == 0)
        {
            return;
        }

        foreach (var message in messages)
        {
            // Validate known message contracts before contacting RabbitMQ.
            var permanentError = OutboxPayloadValidator.GetPermanentError(message);

            if (permanentError is not null)
            {
                message.Quarantine(permanentError, DateTimeOffset.UtcNow);

                await db.SaveChangesAsync(cancellationToken);

                _logger.LogError(
                    "Outbox message {MessageId} was quarantined. Reason: {Reason}",
                    message.Id,
                    permanentError);

                continue;
            }

            // Limit the RabbitMQ attempt while respecting application shutdown.
            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            attemptCts.CancelAfter(_publishTimeout);

            try
            {
                await EnsureRabbitMqConnectionAsync(attemptCts.Token);

                var channel = _channel
                    ?? throw new InvalidOperationException("RabbitMQ channel is not initialized.");

                var properties = new BasicProperties
                {
                    MessageId = message.Id.ToString("D"),
                    ContentType = "application/json",
                    Type = message.MessageType,
                    Persistent = true
                };

                var body = Encoding.UTF8.GetBytes(message.Payload);

                // Publisher confirmation is awaited with a bounded token.
                await channel.BasicPublishAsync(
                    exchange: MessagingTopology.ExchangeName,
                    routingKey: message.MessageType,
                    mandatory: true,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: attemptCts.Token);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                // Application shutdown is not a publishing failure.
                throw;
            }
            catch (Exception ex)
            {
                var error =
                    ex is OperationCanceledException &&
                    attemptCts.IsCancellationRequested
                        ? $"RabbitMQ publishing attempt exceeded " +
                          $"{_publishTimeout.TotalSeconds:0} seconds; " +
                          "publication outcome is uncertain."
                        : string.IsNullOrWhiteSpace(ex.Message)
                            ? ex.GetType().Name
                            : ex.Message;

                message.RecordFailure(error, DateTimeOffset.UtcNow);

                await db.SaveChangesAsync(cancellationToken);

                _logger.LogWarning(
                    ex,
                    "Publishing failed for Outbox message {MessageId}. " +
                    "Attempt {PublishAttempts}. Next retry at {NextAttemptAtUtc}.",
                    message.Id,
                    message.PublishAttempts,
                    message.NextAttemptAtUtc);

                await ResetRabbitMqAsync();

                continue;
            }

            // Only record success after the broker confirms publication.
            message.MarkPublished(DateTimeOffset.UtcNow);

            await db.SaveChangesAsync(cancellationToken);

            _logger.LogInformation("Published Outbox message {MessageId}.", message.Id);
        }
    }

    private async Task EnsureRabbitMqConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection?.IsOpen == true && _channel?.IsOpen == true)
        {
            return;
        }

        await ResetRabbitMqAsync();

        _connection = await _connectionFactory.CreateConnectionAsync(cancellationToken);

        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);

        _channel = await _connection.CreateChannelAsync(
            channelOptions,
            cancellationToken);

        await _channel.ExchangeDeclareAsync(
            exchange: MessagingTopology.ExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        await _channel.QueueDeclareAsync(
            queue: MessagingTopology.RegistrationRequestsQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);

        await _channel.QueueBindAsync(
            queue: MessagingTopology.RegistrationRequestsQueue,
            exchange: MessagingTopology.ExchangeName,
            routingKey: MessagingTopology.RegistrationRequestedRoutingKey,
            cancellationToken: cancellationToken);

        _logger.LogInformation("RabbitMQ connection and messaging topology initialized.");
    }

    private async Task ResetRabbitMqAsync()
    {
        if (_channel is not null)
        {
            try
            {
                await _channel.DisposeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing RabbitMQ channel.");
            }
            finally
            {
                _channel = null;
            }
        }

        if (_connection is not null)
        {
            try
            {
                await _connection.DisposeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing RabbitMQ connection.");
            }
            finally
            {
                _connection = null;
            }
        }
    }

    private static string Required(IConfiguration configuration, string key)
    {
        var value = configuration[key];

        return !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"Required configuration '{key}' is missing.");
    }
}
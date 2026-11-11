using System.Text;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

public sealed class OutboxPublisherService : BackgroundService
{
    private static readonly TimeSpan PollingInterval = TimeSpan.FromSeconds(5);

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

        var messages = await db.OutboxMessages
            .Where(x => x.PublishedAtUtc == null)
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
            try
            {
                await EnsureRabbitMqConnectionAsync(cancellationToken);

                var channel = _channel ?? throw new InvalidOperationException("RabbitMQ channel is not initialized.");

                var properties = new BasicProperties
                {
                    MessageId = message.Id.ToString("D"),
                    ContentType = "application/json",
                    Type = message.MessageType,
                    Persistent = true
                };

                var body = Encoding.UTF8.GetBytes(message.Payload);

                // Await the broker's publisher confirmation.
                await channel.BasicPublishAsync(
                    exchange: MessagingTopology.ExchangeName,
                    routingKey: message.MessageType,
                    mandatory: true,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: cancellationToken);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                var error = string.IsNullOrWhiteSpace(ex.Message)
                    ? ex.GetType().Name
                    : ex.Message;

                message.RecordFailure(error);

                await db.SaveChangesAsync(cancellationToken);

                _logger.LogWarning(ex, "Publishing failed for Outbox message {MessageId}.", message.Id);

                await ResetRabbitMqAsync();

                // Retry during a subsequent polling cycle.
                return;
            }

            // This runs only after successful broker confirmation.
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
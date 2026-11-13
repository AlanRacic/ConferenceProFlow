using ConferenceProFlow.Allocation.Worker.Messaging.Inbox;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ConferenceProFlow.Allocation.Worker.Messaging.Consumers;

public sealed class RegistrationRequestConsumerService : BackgroundService
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

    private readonly IServiceScopeFactory _scopeFactory;

    private readonly ILogger<RegistrationRequestConsumerService> _logger;

    private readonly ConnectionFactory _connectionFactory;

    public RegistrationRequestConsumerService(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<RegistrationRequestConsumerService> logger)
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

            ClientProvidedName = "ConferenceProFlow.Allocation.RegistrationConsumer",

            // We manage reconnection in the BackgroundService.
            AutomaticRecoveryEnabled = false,

            ConsumerDispatchConcurrency = 1,

            RequestedHeartbeat = TimeSpan.FromSeconds(15),

            RequestedConnectionTimeout = TimeSpan.FromSeconds(10)
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Registration Request Consumer started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunConsumerSessionAsync(stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RabbitMQ consumer session failed.");
            }

            try
            {
                await Task.Delay(ReconnectDelay, stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger.LogInformation("Registration Request Consumer stopped.");
    }

    private async Task RunConsumerSessionAsync(CancellationToken stoppingToken)
    {
        await using var connection =
            await _connectionFactory.CreateConnectionAsync(stoppingToken);

        await using var consumerChannel =
            await connection.CreateChannelAsync(
                cancellationToken: stoppingToken);

        // A separate publisher-confirm channel is used for moving invalid or conflicting messages to the parking queue.
        var confirmOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);

        await using var parkingChannel =
            await connection.CreateChannelAsync(
                confirmOptions,
                stoppingToken);

        // Ensure the shared event exchange exists.
        await consumerChannel.ExchangeDeclareAsync(
            exchange: MessagingTopology.ExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // Ensure the Allocation-owned queue exists.
        await consumerChannel.QueueDeclareAsync(
            queue: MessagingTopology.RegistrationRequestsQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // Ensure registration events are routed to this queue.
        await consumerChannel.QueueBindAsync(
            queue: MessagingTopology.RegistrationRequestsQueue,
            exchange: MessagingTopology.ExchangeName,
            routingKey: MessagingTopology.RegistrationRequestedRoutingKey,
            cancellationToken: stoppingToken);

        // A durable parking queue for messages requiring investigation.
        await parkingChannel.QueueDeclareAsync(
            queue: MessagingTopology.FailedRegistrationRequestsQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // Process one unacknowledged delivery at a time.
        await consumerChannel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken);

        var restartSignal = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        connection.ConnectionShutdownAsync += (_, _) =>
        {
            restartSignal.TrySetResult();
            return Task.CompletedTask;
        };

        consumerChannel.ChannelShutdownAsync += (_, _) =>
        {
            restartSignal.TrySetResult();
            return Task.CompletedTask;
        };

        parkingChannel.ChannelShutdownAsync += (_, _) =>
        {
            restartSignal.TrySetResult();
            return Task.CompletedTask;
        };

        var consumer = new AsyncEventingBasicConsumer(consumerChannel);

        consumer.ReceivedAsync += async (_, delivery) =>
        {
            try
            {
                await HandleDeliveryAsync(
                    delivery,
                    consumerChannel,
                    parkingChannel,
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                // Do not ACK an incomplete delivery.
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Message processing failed. " +
                    "The consumer session will be restarted.");

                // The unacknowledged delivery will be eligible for redelivery after the channel closes.
                restartSignal.TrySetResult();
            }
        };

        var consumerTag = await consumerChannel.BasicConsumeAsync(
            queue: MessagingTopology.RegistrationRequestsQueue,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation(
            "RabbitMQ consumer registered. ConsumerTag: {ConsumerTag}",
            consumerTag);

        // Stay connected until the broker/channel fails, a processing failure requests reconnection, or shutdown begins.
        await restartSignal.Task.WaitAsync(stoppingToken);

        _logger.LogWarning("RabbitMQ consumer session ended. Reconnecting.");
    }

    private async Task HandleDeliveryAsync(
        BasicDeliverEventArgs delivery,
        IChannel consumerChannel,
        IChannel parkingChannel,
        CancellationToken cancellationToken)
    {
        if (delivery.RoutingKey !=
                MessagingTopology.RegistrationRequestedRoutingKey ||
            delivery.BasicProperties.Type !=
                RegistrationRequestedV1.MessageType ||
            delivery.BasicProperties.ContentType != "application/json")
        {
            await ParkAndAcknowledgeAsync(
                delivery,
                consumerChannel,
                parkingChannel,
                "Unexpected routing key, message type, or content type.",
                cancellationToken);

            return;
        }

        var isValid = RegistrationRequestedV1Parser.TryParse(
            delivery.Body.Span,
            out var message,
            out var error);

        if (!isValid || message is null)
        {
            await ParkAndAcknowledgeAsync(
                delivery,
                consumerChannel,
                parkingChannel,
                error ?? "Invalid message payload.",
                cancellationToken);

            return;
        }

        if (!Guid.TryParse(
                delivery.BasicProperties.MessageId,
                out var headerMessageId) ||
            headerMessageId != message.MessageId)
        {
            await ParkAndAcknowledgeAsync(
                delivery,
                consumerChannel,
                parkingChannel,
                "AMQP MessageId does not match the JSON payload.",
                cancellationToken);

            return;
        }

        // Each delivery gets its own DI scope and DbContext.
        using var scope = _scopeFactory.CreateScope();

        var inboxService = scope.ServiceProvider
            .GetRequiredService<InboxReceiveService>();

        var result = await inboxService.ReceiveAsync(
            message,
            DateTimeOffset.UtcNow,
            cancellationToken);

        switch (result)
        {
            case InboxReceiveResult.Stored:
            case InboxReceiveResult.DuplicateMessage:
            case InboxReceiveResult.DuplicateRegistration:

                await consumerChannel.BasicAckAsync(
                    delivery.DeliveryTag,
                    multiple: false,
                    cancellationToken: cancellationToken);

                _logger.LogInformation(
                    "Registration request received. " +
                    "MessageId: {MessageId}, " +
                    "RegistrationId: {RegistrationId}, " +
                    "Result: {Result}",
                    message.MessageId,
                    message.RegistrationId,
                    result);

                break;

            case InboxReceiveResult.Conflict:

                await ParkAndAcknowledgeAsync(
                    delivery,
                    consumerChannel,
                    parkingChannel,
                    "Conflicting registration or message identity.",
                    cancellationToken);

                break;

            default:
                throw new InvalidOperationException($"Unexpected Inbox receive result: {result}");
        }
    }

    private async Task ParkAndAcknowledgeAsync(
        BasicDeliverEventArgs delivery,
        IChannel consumerChannel,
        IChannel parkingChannel,
        string reason,
        CancellationToken cancellationToken)
    {
        var properties = new BasicProperties
        {
            Persistent = true,

            MessageId = delivery.BasicProperties.MessageId,

            Type = delivery.BasicProperties.Type,

            ContentType = delivery.BasicProperties.ContentType,

            Headers = new Dictionary<string, object?>
            {
                ["x-parking-reason"] = reason,
                ["x-original-routing-key"] = delivery.RoutingKey
            }
        };

        // The default exchange routes directly to the queue.
        // Confirmations are enabled on parkingChannel.
        await parkingChannel.BasicPublishAsync(
            exchange: "",
            routingKey: MessagingTopology.FailedRegistrationRequestsQueue,
            mandatory: true,
            basicProperties: properties,
            body: delivery.Body,
            cancellationToken: cancellationToken);

        // ACK the original only after the parking publish succeeds.
        await consumerChannel.BasicAckAsync(
            delivery.DeliveryTag,
            multiple: false,
            cancellationToken: cancellationToken);

        _logger.LogWarning(
            "Message moved to the failed-registration queue. " +
            "MessageId: {MessageId}, Reason: {Reason}",
            delivery.BasicProperties.MessageId,
            reason);
    }

    private static string Required(IConfiguration configuration, string key)
    {
        var value = configuration[key];

        return !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"Required configuration '{key}' is missing.");
    }
}
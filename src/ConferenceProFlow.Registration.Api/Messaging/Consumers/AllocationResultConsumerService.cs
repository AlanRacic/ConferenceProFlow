using System.Text.Json;
using ConferenceProFlow.Registration.Api.Infrastructure.Inbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ConferenceProFlow.Registration.Api.Messaging.Consumers;

/// <summary>
/// Consumes AllocationDecidedV1 events and reconciles the Registration status.
/// A delivery is acknowledged only after durable reconciliation or a confirmed publication to the Registration-owned parking queue.
/// </summary>
public sealed class AllocationResultConsumerService : BackgroundService
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan ParkingPublishTimeout = TimeSpan.FromSeconds(15);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<AllocationResultConsumerService> _logger;
    private readonly ConnectionFactory _connectionFactory;

    public AllocationResultConsumerService(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<AllocationResultConsumerService> logger)
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
            ClientProvidedName = "ConferenceProFlow.Registration.AllocationResultConsumer",
            AutomaticRecoveryEnabled = false,
            ConsumerDispatchConcurrency = 1,
            RequestedHeartbeat = TimeSpan.FromSeconds(15),
            RequestedConnectionTimeout = TimeSpan.FromSeconds(10)
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Allocation Result Consumer started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunConsumerSessionAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Allocation Result Consumer session failed. Reconnecting.");
            }

            try
            {
                await Task.Delay(ReconnectDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger.LogInformation("Allocation Result Consumer stopped.");
    }

    private async Task RunConsumerSessionAsync(CancellationToken stoppingToken)
    {
        await using var connection = await _connectionFactory.CreateConnectionAsync(stoppingToken);
        await using var consumerChannel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        // Publishing to the parking queue requires broker confirmations.
        var confirmOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);

        await using var parkingChannel = await connection.CreateChannelAsync(confirmOptions, stoppingToken);

        await consumerChannel.ExchangeDeclareAsync(
            exchange: MessagingTopology.ExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // The Registration service owns this result queue and its binding.
        await consumerChannel.QueueDeclareAsync(
            queue: MessagingTopology.RegistrationAllocationResultsQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        await consumerChannel.QueueBindAsync(
            queue: MessagingTopology.RegistrationAllocationResultsQueue,
            exchange: MessagingTopology.ExchangeName,
            routingKey: MessagingTopology.AllocationDecidedRoutingKey,
            cancellationToken: stoppingToken);

        await parkingChannel.QueueDeclareAsync(
            queue: MessagingTopology.FailedAllocationResultsQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // Serial delivery per consumer; SQL constraints still enforce cross-instance safety.
        await consumerChannel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken);

        var restartSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

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
                await HandleDeliveryAsync(delivery, consumerChannel, parkingChannel, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // No ACK: closing the channel will make this delivery eligible for redelivery.
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Allocation result processing failed. Restarting the session without ACK.");
                restartSignal.TrySetResult();
            }
        };

        var consumerTag = await consumerChannel.BasicConsumeAsync(
            queue: MessagingTopology.RegistrationAllocationResultsQueue,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation("Allocation Result Consumer registered: {ConsumerTag}", consumerTag);
        await restartSignal.Task.WaitAsync(stoppingToken);
        _logger.LogWarning("Allocation Result Consumer session ended; reconnecting.");
    }

    private async Task HandleDeliveryAsync(
        BasicDeliverEventArgs delivery,
        IChannel consumerChannel,
        IChannel parkingChannel,
        CancellationToken cancellationToken)
    {
        if (delivery.RoutingKey != MessagingTopology.AllocationDecidedRoutingKey ||
            delivery.BasicProperties.Type != AllocationDecidedV1.MessageType ||
            delivery.BasicProperties.ContentType != "application/json")
        {
            await ParkAndAcknowledgeAsync(
                delivery, consumerChannel, parkingChannel,
                "Unexpected routing key, message type, or content type.", cancellationToken);
            return;
        }

        AllocationDecidedV1? result;
        try
        {
            result = JsonSerializer.Deserialize<AllocationDecidedV1>(delivery.Body.Span, JsonOptions);
        }
        catch (JsonException ex)
        {
            await ParkAndAcknowledgeAsync(
                delivery, consumerChannel, parkingChannel,
                $"Invalid JSON payload: {ex.GetType().Name}", cancellationToken);
            return;
        }

        if (result is null)
        {
            await ParkAndAcknowledgeAsync(
                delivery, consumerChannel, parkingChannel,
                "JSON payload is null.", cancellationToken);
            return;
        }

        var validationError = AllocationDecidedV1Validator.GetError(result);
        if (validationError is not null)
        {
            await ParkAndAcknowledgeAsync(
                delivery, consumerChannel, parkingChannel, validationError, cancellationToken);
            return;
        }

        if (!Guid.TryParse(delivery.BasicProperties.MessageId, out var headerMessageId) ||
            headerMessageId != result.MessageId)
        {
            await ParkAndAcknowledgeAsync(
                delivery, consumerChannel, parkingChannel,
                "AMQP MessageId does not match the JSON payload.", cancellationToken);
            return;
        }

        // Each delivery has a fresh scoped DbContext.
        using var scope = _scopeFactory.CreateScope();
        var reconciliation = scope.ServiceProvider.GetRequiredService<AllocationResultReconciliationService>();
        var outcome = await reconciliation.ApplyAsync(result, cancellationToken);

        switch (outcome)
        {
            case AllocationResultReconciliationResult.Applied:
            case AllocationResultReconciliationResult.DuplicateMessage:
            case AllocationResultReconciliationResult.DuplicateDecision:
                await consumerChannel.BasicAckAsync(
                    delivery.DeliveryTag, multiple: false, cancellationToken: cancellationToken);
                _logger.LogInformation(
                    "Allocation result handled. MessageId: {MessageId}, RegistrationId: {RegistrationId}, Outcome: {Outcome}",
                    result.MessageId, result.RegistrationId, outcome);
                break;

            case AllocationResultReconciliationResult.Conflict:
                await ParkAndAcknowledgeAsync(
                    delivery, consumerChannel, parkingChannel,
                    "Conflicting allocation decision for the message, allocation, session, or registration.",
                    cancellationToken);
                break;

            case AllocationResultReconciliationResult.UnknownRegistration:
                // With the current transactional Registration Outbox, a result for an unknown Registration should not be a normal ordering race. Park for investigation.
                await ParkAndAcknowledgeAsync(
                    delivery, consumerChannel, parkingChannel,
                    "Unknown registration. Investigate before any manual replay.",
                    cancellationToken);
                break;

            default:
                throw new InvalidOperationException($"Unexpected reconciliation outcome: {outcome}");
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

        using var publishCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        publishCts.CancelAfter(ParkingPublishTimeout);

        // The default exchange routes directly to the declared parking queue.
        // Publisher confirms + mandatory prevent acknowledging a message not safely parked.
        await parkingChannel.BasicPublishAsync(
            exchange: "",
            routingKey: MessagingTopology.FailedAllocationResultsQueue,
            mandatory: true,
            basicProperties: properties,
            body: delivery.Body,
            cancellationToken: publishCts.Token);

        await consumerChannel.BasicAckAsync(
            delivery.DeliveryTag, multiple: false, cancellationToken: cancellationToken);

        _logger.LogWarning(
            "Allocation result parked. MessageId: {MessageId}, Reason: {Reason}",
            delivery.BasicProperties.MessageId, reason);
    }

    private static string Required(IConfiguration configuration, string key)
    {
        var value = configuration[key];
        return !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"Required configuration '{key}' is missing.");
    }
}
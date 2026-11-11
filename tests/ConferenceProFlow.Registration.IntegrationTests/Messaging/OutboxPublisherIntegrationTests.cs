using System.Globalization;
using System.Text.Json;
using System.Text;
using ConferenceProFlow.Registration.Api.Features.Registrations.Messages;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.IntegrationTests.Infrastructure;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace ConferenceProFlow.Registration.IntegrationTests.Messaging;

[Collection("SqlServer")]
public sealed class OutboxPublisherIntegrationTests : IAsyncLifetime
{
    private readonly SqlServerFixture _sqlFixture;
    private readonly RabbitMqFixture _rabbitMqFixture = new();

    private string? _connectionString;

    public OutboxPublisherIntegrationTests(SqlServerFixture sqlFixture)
    {
        _sqlFixture = sqlFixture;
    }

    public async ValueTask InitializeAsync()
    {
        // Start an isolated RabbitMQ container for this test.
        await _rabbitMqFixture.InitializeAsync();

        // Create an isolated SQL Server database for this test.
        var connectionBuilder = new SqlConnectionStringBuilder(_sqlFixture.ConnectionString)
        {
            InitialCatalog = $"ConferenceProFlowMessagingTests_{Guid.NewGuid():N}"
        };

        _connectionString = connectionBuilder.ConnectionString;

        await using var db = CreateDbContext();

        await db.Database.MigrateAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_connectionString is not null)
            {
                await using var db = CreateDbContext();

                await db.Database.EnsureDeletedAsync();
            }
        }
        finally
        {
            // Always dispose the isolated RabbitMQ container.
            await _rabbitMqFixture.DisposeAsync();
        }
    }

    private RegistrationDbContext CreateDbContext()
    {
        if (_connectionString is null)
        {
            throw new InvalidOperationException("Messaging test database is not initialized.");
        }

        var options = new DbContextOptionsBuilder<RegistrationDbContext>()
            .UseSqlServer(_connectionString)
            .Options;

        return new RegistrationDbContext(options);
    }

    private async Task<RegistrationRequestedV1> CreateOutboxMessageAsync(string messageType = RegistrationRequestedV1.MessageType)
    {
        var integrationEvent = new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        var payload = JsonSerializer.Serialize(
            integrationEvent,
            JsonSerializerOptions.Web);

        var outboxMessage = OutboxMessage.Create(
            integrationEvent.MessageId,
            messageType,
            payload,
            integrationEvent.OccurredAtUtc);

        await using var db = CreateDbContext();

        db.OutboxMessages.Add(outboxMessage);

        await db.SaveChangesAsync(TestContext.Current.CancellationToken);

        return integrationEvent;
    }

    private ServiceProvider CreatePublisherProvider(int rabbitMqPort)
    {
        var rabbitMqUri = _rabbitMqFixture.ConnectionUri;

        var settings = new Dictionary<string, string?>
        {
            ["RabbitMq:HostName"] = rabbitMqUri.Host,
            ["RabbitMq:Port"] =
                rabbitMqPort.ToString(CultureInfo.InvariantCulture),
            ["RabbitMq:VirtualHost"] = "/",
            ["RabbitMq:UserName"] = "rabbitmq",
            ["RabbitMq:Password"] = "rabbitmq"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(settings)
            .Build();

        var services = new ServiceCollection();

        services.AddSingleton<IConfiguration>(configuration);

        services.AddLogging();

        services.AddDbContext<RegistrationDbContext>(options =>
            options.UseSqlServer(_connectionString
                ?? throw new InvalidOperationException("Messaging test database is not initialized.")));

        services.AddHostedService<OutboxPublisherService>();

        return services.BuildServiceProvider();
    }

    private async Task<OutboxMessage> WaitForOutboxStateAsync(
        Guid messageId,
        Func<OutboxMessage, bool> expectedState)
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var deadline = DateTimeOffset.UtcNow.AddSeconds(45);

        while (DateTimeOffset.UtcNow < deadline)
        {
            await using var db = CreateDbContext();

            var message = await db.OutboxMessages
                .AsNoTracking()
                .SingleAsync(
                    x => x.Id == messageId,
                    cancellationToken);

            if (expectedState(message))
            {
                return message;
            }

            await Task.Delay(
                TimeSpan.FromMilliseconds(250),
                cancellationToken);
        }

        throw new TimeoutException($"Outbox message {messageId} did not reach the expected state.");
    }

    [Fact]
    public async Task PublishPendingMessage_PublishesToRabbitMqAndUpdatesOutbox()
    {
        // Arrange
        var cancellationToken = TestContext.Current.CancellationToken;

        var integrationEvent = await CreateOutboxMessageAsync();

        var rabbitMqUri = _rabbitMqFixture.ConnectionUri;

        await using var provider = CreatePublisherProvider(rabbitMqUri.Port);

        var publisher = provider.GetRequiredService<IHostedService>();

        // Act
        await publisher.StartAsync(cancellationToken);

        try
        {
            var savedMessage = await WaitForOutboxStateAsync(
                integrationEvent.MessageId,
                message => message.PublishedAtUtc is not null);

            // Assert SQL Server publishing state.
            Assert.NotNull(savedMessage.PublishedAtUtc);
            Assert.Equal(1, savedMessage.PublishAttempts);
            Assert.Null(savedMessage.LastError);

            // Connect directly to the test RabbitMQ container.
            var connectionFactory = new ConnectionFactory
            {
                Uri = rabbitMqUri
            };

            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);

            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            // Read the published message from the test queue.
            var delivery = await channel.BasicGetAsync(
                MessagingTopology.RegistrationRequestsQueue,
                autoAck: false,
                cancellationToken: cancellationToken);

            Assert.NotNull(delivery);

            Assert.Equal(
                MessagingTopology.ExchangeName,
                delivery.Exchange);

            Assert.Equal(
                RegistrationRequestedV1.MessageType,
                delivery.RoutingKey);

            Assert.Equal(
                integrationEvent.MessageId.ToString("D"),
                delivery.BasicProperties.MessageId);

            Assert.Equal(
                "application/json",
                delivery.BasicProperties.ContentType);

            var receivedEvent = JsonSerializer.Deserialize<RegistrationRequestedV1>(
                    delivery.Body.Span,
                    JsonSerializerOptions.Web);

            Assert.NotNull(receivedEvent);
            Assert.Equal(integrationEvent, receivedEvent);

            // Acknowledge the test message after verification.
            await channel.BasicAckAsync(
                delivery.DeliveryTag,
                multiple: false,
                cancellationToken: cancellationToken);
        }
        finally
        {
            await publisher.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task PublishPendingMessage_AfterBrokerFailure_CanRecover()
    {
        // Arrange
        var cancellationToken = TestContext.Current.CancellationToken;

        var integrationEvent = await CreateOutboxMessageAsync();

        // Phase 1: Configure an unavailable RabbitMQ endpoint.
        await using (var unavailableProvider = CreatePublisherProvider(rabbitMqPort: 1))
        {
            var unavailablePublisher = unavailableProvider.GetRequiredService<IHostedService>();

            await unavailablePublisher.StartAsync(cancellationToken);

            try
            {
                // Wait until publishing failure is persisted.
                var failedMessage = await WaitForOutboxStateAsync(
                    integrationEvent.MessageId,
                    message =>
                        message.PublishAttempts > 0 &&
                        !string.IsNullOrWhiteSpace(message.LastError));

                Assert.Null(failedMessage.PublishedAtUtc);

                Assert.True(failedMessage.PublishAttempts >= 1);

                Assert.False(string.IsNullOrWhiteSpace(failedMessage.LastError));
            }
            finally
            {
                await unavailablePublisher.StopAsync(CancellationToken.None);
            }
        }

        // Phase 2: Restart publishing with a working RabbitMQ endpoint.
        var rabbitMqUri = _rabbitMqFixture.ConnectionUri;

        await using var recoveredProvider = CreatePublisherProvider(rabbitMqUri.Port);

        var recoveredPublisher = recoveredProvider.GetRequiredService<IHostedService>();

        await recoveredPublisher.StartAsync(cancellationToken);

        try
        {
            var publishedMessage = await WaitForOutboxStateAsync(
                integrationEvent.MessageId,
                message => message.PublishedAtUtc is not null);

            Assert.NotNull(publishedMessage.PublishedAtUtc);

            Assert.True(publishedMessage.PublishAttempts >= 2);

            Assert.Null(publishedMessage.LastError);

            // Verify that the message reached RabbitMQ.
            var connectionFactory = new ConnectionFactory
            {
                Uri = rabbitMqUri
            };

            await using var connection =
                await connectionFactory.CreateConnectionAsync(cancellationToken);

            await using var channel =
                await connection.CreateChannelAsync(
                    cancellationToken: cancellationToken);

            var delivery = await channel.BasicGetAsync(
                MessagingTopology.RegistrationRequestsQueue,
                autoAck: false,
                cancellationToken: cancellationToken);

            Assert.NotNull(delivery);

            Assert.Equal(
                integrationEvent.MessageId.ToString("D"),
                delivery.BasicProperties.MessageId);

            await channel.BasicAckAsync(
                delivery.DeliveryTag,
                multiple: false,
                cancellationToken: cancellationToken);
        }
        finally
        {
            await recoveredPublisher.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task UnroutableMessage_RemainsUnpublishedAndRecordsFailure()
    {
        // Arrange
        var cancellationToken = TestContext.Current.CancellationToken;

        var integrationEvent = await CreateOutboxMessageAsync("registration.unroutable.v1");

        var rabbitMqUri = _rabbitMqFixture.ConnectionUri;

        await using var provider = CreatePublisherProvider(rabbitMqUri.Port);

        var publisher = provider.GetRequiredService<IHostedService>();

        // Act
        await publisher.StartAsync(cancellationToken);

        try
        {
            var failedMessage = await WaitForOutboxStateAsync(
                integrationEvent.MessageId,
                message =>
                    message.PublishAttempts > 0 &&
                    !string.IsNullOrWhiteSpace(message.LastError));

            // Assert
            Assert.Null(failedMessage.PublishedAtUtc);

            Assert.True(failedMessage.PublishAttempts >= 1);

            Assert.False(string.IsNullOrWhiteSpace(failedMessage.LastError));
        }
        finally
        {
            await publisher.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task UnroutableMessage_DoesNotBlockFollowingValidMessage()
    {
        // Arrange
        var cancellationToken = TestContext.Current.CancellationToken;

        // Create the failing message first.
        var unroutableEvent = await CreateOutboxMessageAsync("registration.unroutable.v1");

        // Create a valid message afterward.
        var validEvent = await CreateOutboxMessageAsync();

        var rabbitMqUri = _rabbitMqFixture.ConnectionUri;

        await using var provider = CreatePublisherProvider(rabbitMqUri.Port);

        var publisher = provider.GetRequiredService<IHostedService>();

        // Act
        await publisher.StartAsync(cancellationToken);

        try
        {
            // The valid message must eventually be published.
            var publishedMessage = await WaitForOutboxStateAsync(
                validEvent.MessageId,
                message => message.PublishedAtUtc is not null);

            // Verify that the failing message remains unpublished.
            await using var db = CreateDbContext();

            var failedMessage = await db.OutboxMessages
                .AsNoTracking()
                .SingleAsync(
                    x => x.Id == unroutableEvent.MessageId,
                    cancellationToken);

            // Assert
            Assert.Null(failedMessage.PublishedAtUtc);

            Assert.True(failedMessage.PublishAttempts >= 1);

            Assert.NotNull(publishedMessage.PublishedAtUtc);

            Assert.Equal(
                RegistrationRequestedV1.MessageType,
                publishedMessage.MessageType);
        }
        finally
        {
            await publisher.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task PreviouslyDeliveredMessage_CanBePublishedAgain()
    {
        // Arrange
        var cancellationToken = TestContext.Current.CancellationToken;

        var integrationEvent = await CreateOutboxMessageAsync();

        var rabbitMqUri = _rabbitMqFixture.ConnectionUri;

        var connectionFactory = new ConnectionFactory
        {
            Uri = rabbitMqUri
        };

        // Simulate a message already accepted by RabbitMQ.
        await using (var connection =
            await connectionFactory.CreateConnectionAsync(
                cancellationToken))
        {
            var channelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true);

            await using var channel =
                await connection.CreateChannelAsync(
                    channelOptions,
                    cancellationToken);

            await channel.ExchangeDeclareAsync(
                exchange: MessagingTopology.ExchangeName,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            await channel.QueueDeclareAsync(
                queue: MessagingTopology.RegistrationRequestsQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: MessagingTopology.RegistrationRequestsQueue,
                exchange: MessagingTopology.ExchangeName,
                routingKey: RegistrationRequestedV1.MessageType,
                cancellationToken: cancellationToken);

            var properties = new BasicProperties
            {
                MessageId = integrationEvent.MessageId.ToString("D"),
                ContentType = "application/json",
                Type = RegistrationRequestedV1.MessageType,
                Persistent = true
            };

            var payload = JsonSerializer.Serialize(
                integrationEvent,
                JsonSerializerOptions.Web);

            await channel.BasicPublishAsync(
                exchange: MessagingTopology.ExchangeName,
                routingKey: RegistrationRequestedV1.MessageType,
                mandatory: true,
                basicProperties: properties,
                body: Encoding.UTF8.GetBytes(payload),
                cancellationToken: cancellationToken);
        }

        // The message was delivered, but SQL was not updated.
        await using (var db = CreateDbContext())
        {
            var pendingMessage = await db.OutboxMessages
                .AsNoTracking()
                .SingleAsync(
                    x => x.Id == integrationEvent.MessageId,
                    cancellationToken);

            Assert.Null(pendingMessage.PublishedAtUtc);
        }

        // Start the real publisher.
        await using var provider = CreatePublisherProvider(rabbitMqUri.Port);

        var publisher = provider.GetRequiredService<IHostedService>();

        await publisher.StartAsync(cancellationToken);

        try
        {
            var publishedMessage = await WaitForOutboxStateAsync(
                integrationEvent.MessageId,
                message => message.PublishedAtUtc is not null);

            Assert.NotNull(publishedMessage.PublishedAtUtc);

            // Read both deliveries from RabbitMQ.
            await using var connection =
                await connectionFactory.CreateConnectionAsync(
                    cancellationToken);

            await using var channel =
                await connection.CreateChannelAsync(
                    cancellationToken: cancellationToken);

            var firstDelivery = await channel.BasicGetAsync(
                MessagingTopology.RegistrationRequestsQueue,
                autoAck: false,
                cancellationToken: cancellationToken);

            var secondDelivery = await channel.BasicGetAsync(
                MessagingTopology.RegistrationRequestsQueue,
                autoAck: false,
                cancellationToken: cancellationToken);

            Assert.NotNull(firstDelivery);
            Assert.NotNull(secondDelivery);

            var expectedMessageId =
                integrationEvent.MessageId.ToString("D");

            Assert.Equal(
                expectedMessageId,
                firstDelivery.BasicProperties.MessageId);

            Assert.Equal(
                expectedMessageId,
                secondDelivery.BasicProperties.MessageId);

            await channel.BasicAckAsync(
                firstDelivery.DeliveryTag,
                multiple: false,
                cancellationToken: cancellationToken);

            await channel.BasicAckAsync(
                secondDelivery.DeliveryTag,
                multiple: false,
                cancellationToken: cancellationToken);
        }
        finally
        {
            await publisher.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task DeferredMessage_DoesNotBlockReadyMessage()
    {
        // Arrange
        var cancellationToken = TestContext.Current.CancellationToken;

        var deferredEvent = await CreateOutboxMessageAsync();

        // Simulate an Outbox message with repeated failed publishing attempts.
        await using (var db = CreateDbContext())
        {
            var deferredMessage = await db.OutboxMessages.SingleAsync(
                    x => x.Id == deferredEvent.MessageId,
                    cancellationToken);

            var failedAtUtc = DateTimeOffset.UtcNow;

            for (var attempt = 0; attempt < 7; attempt++)
            {
                deferredMessage.RecordFailure("Simulated temporary broker failure", failedAtUtc);
            }

            await db.SaveChangesAsync(cancellationToken);
        }

        // Create a new message that is immediately eligible.
        var readyEvent = await CreateOutboxMessageAsync();

        var rabbitMqUri = _rabbitMqFixture.ConnectionUri;

        await using var provider = CreatePublisherProvider(rabbitMqUri.Port);

        var publisher = provider.GetRequiredService<IHostedService>();

        // Act
        await publisher.StartAsync(cancellationToken);

        try
        {
            var publishedMessage = await WaitForOutboxStateAsync(
                readyEvent.MessageId,
                message => message.PublishedAtUtc is not null);

            // Assert: The ready message was published.
            Assert.NotNull(publishedMessage.PublishedAtUtc);

            Assert.Equal(1, publishedMessage.PublishAttempts);

            // Assert: The deferred message was not retried.
            await using var db = CreateDbContext();

            var deferredMessage = await db.OutboxMessages
                .AsNoTracking()
                .SingleAsync(
                    x => x.Id == deferredEvent.MessageId,
                    cancellationToken);

            Assert.Null(deferredMessage.PublishedAtUtc);

            Assert.Equal(7, deferredMessage.PublishAttempts);

            Assert.NotNull(deferredMessage.NextAttemptAtUtc);

            Assert.True(
                deferredMessage.NextAttemptAtUtc >
                DateTimeOffset.UtcNow);
        }
        finally
        {
            await publisher.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task InvalidPayload_IsQuarantinedWithoutBlockingValidMessage()
    {
        // Arrange
        var cancellationToken = TestContext.Current.CancellationToken;

        var invalidMessageId = Guid.NewGuid();

        var invalidMessage = OutboxMessage.Create(
            invalidMessageId,
            RegistrationRequestedV1.MessageType,
            "{ invalid json",
            DateTimeOffset.UtcNow);

        await using (var db = CreateDbContext())
        {
            db.OutboxMessages.Add(invalidMessage);

            await db.SaveChangesAsync(cancellationToken);
        }

        // Create a valid message after the invalid message.
        var validEvent = await CreateOutboxMessageAsync();

        var rabbitMqUri = _rabbitMqFixture.ConnectionUri;

        await using var provider = CreatePublisherProvider(rabbitMqUri.Port);

        var publisher = provider.GetRequiredService<IHostedService>();

        // Act
        await publisher.StartAsync(cancellationToken);

        try
        {
            var quarantinedMessage = await WaitForOutboxStateAsync(
                invalidMessageId,
                message => message.QuarantinedAtUtc is not null);

            var publishedMessage = await WaitForOutboxStateAsync(
                validEvent.MessageId,
                message => message.PublishedAtUtc is not null);

            // Assert: Invalid message was quarantined.
            Assert.NotNull(quarantinedMessage.QuarantinedAtUtc);

            Assert.Null(quarantinedMessage.PublishedAtUtc);

            Assert.Null(quarantinedMessage.NextAttemptAtUtc);

            Assert.NotNull(quarantinedMessage.LastError);

            // Assert: Valid message was published.
            Assert.NotNull(publishedMessage.PublishedAtUtc);

            Assert.Null(publishedMessage.QuarantinedAtUtc);

            // Verify that RabbitMQ contains the valid message.
            var connectionFactory = new ConnectionFactory
            {
                Uri = rabbitMqUri
            };

            await using var connection =
                await connectionFactory.CreateConnectionAsync(cancellationToken);

            await using var channel =
                await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            var delivery = await channel.BasicGetAsync(
                MessagingTopology.RegistrationRequestsQueue,
                autoAck: false,
                cancellationToken: cancellationToken);

            Assert.NotNull(delivery);

            Assert.Equal(
                validEvent.MessageId.ToString("D"),
                delivery.BasicProperties.MessageId);

            await channel.BasicAckAsync(
                delivery.DeliveryTag,
                multiple: false,
                cancellationToken: cancellationToken);
        }
        finally
        {
            await publisher.StopAsync(CancellationToken.None);
        }
    }
}
using System.Globalization;
using System.Text;
using System.Text.Json;

using ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;
using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Messaging;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;
using ConferenceProFlow.Allocation.Worker.Messaging.Consumers;
using ConferenceProFlow.Allocation.Worker.Messaging.Inbox;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RabbitMQ.Client;

namespace ConferenceProFlow.Allocation.IntegrationTests.Messaging;

[Collection("AllocationSqlServer")]
public sealed class RegistrationRequestConsumerIntegrationTests : IAsyncLifetime
{
    private readonly AllocationSqlServerFixture _sqlFixture;

    private readonly AllocationRabbitMqFixture _rabbitMqFixture = new();

    private IHost? _consumerHost;

    public RegistrationRequestConsumerIntegrationTests(AllocationSqlServerFixture sqlFixture)
    {
        _sqlFixture = sqlFixture;
    }

    public async ValueTask InitializeAsync()
    {
        await _rabbitMqFixture.InitializeAsync();

        var cancellationToken = TestContext.Current.CancellationToken;

        var factory = new ConnectionFactory
        {
            Uri = _rabbitMqFixture.ConnectionUri
        };

        await using var connection = await factory.CreateConnectionAsync(cancellationToken);

        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

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
            routingKey: MessagingTopology.RegistrationRequestedRoutingKey,
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: MessagingTopology.FailedRegistrationRequestsQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await StopConsumerAsync();
        }
        finally
        {
            await _rabbitMqFixture.DisposeAsync();
        }
    }

    private async Task StartConsumerAsync()
    {
        if (_consumerHost is not null)
        {
            throw new InvalidOperationException("Consumer host is already running.");
        }

        await using var db = _sqlFixture.CreateDbContext();

        var connectionString = db.Database.GetConnectionString()
            ?? throw new InvalidOperationException("SQL Server test connection string is missing.");

        var rabbitMqFactory = new ConnectionFactory
        {
            Uri = _rabbitMqFixture.ConnectionUri
        };

        var settings = new Dictionary<string, string?>
        {
            ["RabbitMq:HostName"] = rabbitMqFactory.HostName,

            ["RabbitMq:Port"] = rabbitMqFactory.Port.ToString(CultureInfo.InvariantCulture),

            ["RabbitMq:VirtualHost"] = rabbitMqFactory.VirtualHost,

            ["RabbitMq:UserName"] = rabbitMqFactory.UserName,

            ["RabbitMq:Password"] = rabbitMqFactory.Password
        };

        var builder = Host.CreateApplicationBuilder();

        builder.Configuration.AddInMemoryCollection(settings);

        builder.Services.AddDbContext<AllocationDbContext>(
            options => options.UseSqlServer(connectionString));

        builder.Services.AddScoped<InboxReceiveService>();

        builder.Services.AddHostedService<RegistrationRequestConsumerService>();

        _consumerHost = builder.Build();

        await _consumerHost.StartAsync(TestContext.Current.CancellationToken);
    }

    private async Task StopConsumerAsync()
    {
        if (_consumerHost is null)
        {
            return;
        }

        var host = _consumerHost;

        _consumerHost = null;

        try
        {
            await host.StopAsync(CancellationToken.None);
        }
        finally
        {
            host.Dispose();
        }
    }

    private async Task PublishMessageAsync(RegistrationRequestedV1 message)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(
            message,
            JsonSerializerOptions.Web);

        await PublishRawMessageAsync(
            payload,
            message.MessageId);
    }

    private async Task PublishRawMessageAsync(byte[] payload, Guid messageId)
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var factory = new ConnectionFactory
        {
            Uri = _rabbitMqFixture.ConnectionUri
        };

        await using var connection =
            await factory.CreateConnectionAsync(cancellationToken);

        var confirmOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);

        await using var channel =
            await connection.CreateChannelAsync(
                confirmOptions,
                cancellationToken);

        var properties = new BasicProperties
        {
            MessageId = messageId.ToString("D"),
            Type = RegistrationRequestedV1.MessageType,
            ContentType = "application/json",
            Persistent = true
        };

        await channel.BasicPublishAsync(
            exchange: MessagingTopology.ExchangeName,
            routingKey: MessagingTopology.RegistrationRequestedRoutingKey,
            mandatory: true,
            basicProperties: properties,
            body: payload,
            cancellationToken: cancellationToken);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string failureMessage)
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);

        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }

        throw new TimeoutException(failureMessage);
    }

    private async Task WaitForInboxMessageAsync(Guid messageId)
    {
        await WaitUntilAsync(async () =>
            {
                await using var db = _sqlFixture.CreateDbContext();

                return await db.InboxMessages
                    .AsNoTracking()
                    .AnyAsync(x => x.Id == messageId, TestContext.Current.CancellationToken);
            }, $"Inbox message {messageId} was not persisted.");
    }

    private async Task<(string? MessageId, string Payload, bool HasReason)>WaitForParkedMessageAsync()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var factory = new ConnectionFactory
        {
            Uri = _rabbitMqFixture.ConnectionUri
        };

        await using var connection = await factory.CreateConnectionAsync(cancellationToken);

        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);

        while (DateTimeOffset.UtcNow < deadline)
        {
            var delivery = await channel.BasicGetAsync(
                MessagingTopology.FailedRegistrationRequestsQueue,
                autoAck: true,
                cancellationToken: cancellationToken);

            if (delivery is not null)
            {
                return (delivery.BasicProperties.MessageId,
                    Encoding.UTF8.GetString(delivery.Body.Span),
                    delivery.BasicProperties.Headers?.ContainsKey("x-parking-reason") == true
                );
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }

        throw new TimeoutException("No message appeared in the parking queue.");
    }

    private static RegistrationRequestedV1 CreateMessage()
    {
        return new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);
    }

    [Fact]
    public async Task ValidMessage_IsPersistedInInbox()
    {
        // Arrange
        var message = CreateMessage();

        await StartConsumerAsync();

        // Act
        await PublishMessageAsync(message);

        await WaitForInboxMessageAsync(message.MessageId);

        // Assert
        await using var db = _sqlFixture.CreateDbContext();

        var savedMessage = await db.InboxMessages
            .AsNoTracking()
            .SingleAsync(
                x => x.Id == message.MessageId,
                TestContext.Current.CancellationToken);

        Assert.Equal(
            message.RegistrationId,
            savedMessage.RegistrationId);

        Assert.Equal(
            message.SessionId,
            savedMessage.SessionId);

        Assert.Equal(
            message.OccurredAtUtc,
            savedMessage.OccurredAtUtc);

        Assert.Null(savedMessage.ProcessedAtUtc);
    }

    [Fact]
    public async Task DuplicateMessageId_CreatesOnlyOneInboxRecord()
    {
        // Arrange
        var message = CreateMessage();

        var followingMessage = CreateMessage();

        // Publish before starting the consumer.
        await PublishMessageAsync(message);

        await PublishMessageAsync(message);

        await PublishMessageAsync(followingMessage);

        // Act
        await StartConsumerAsync();

        await WaitForInboxMessageAsync(followingMessage.MessageId);

        // Assert
        await using var db = _sqlFixture.CreateDbContext();

        var count = await db.InboxMessages
            .CountAsync(x => x.RegistrationId == message.RegistrationId,
                TestContext.Current.CancellationToken);

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task DuplicateRegistrationId_CreatesOnlyOneInboxRecord()
    {
        // Arrange
        var original = CreateMessage();

        var repeated = original with
        {
            MessageId = Guid.NewGuid()
        };

        var followingMessage = CreateMessage();

        await PublishMessageAsync(original);

        await PublishMessageAsync(repeated);

        await PublishMessageAsync(followingMessage);

        // Act
        await StartConsumerAsync();

        await WaitForInboxMessageAsync(followingMessage.MessageId);

        // Assert
        await using var db = _sqlFixture.CreateDbContext();

        var messages = await db.InboxMessages
            .AsNoTracking()
            .Where(x =>
                x.RegistrationId == original.RegistrationId)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(messages);

        Assert.Equal(original.MessageId, messages[0].Id);
    }

    [Fact]
    public async Task InvalidJson_IsMovedToParkingQueue()
    {
        // Arrange
        var messageId = Guid.NewGuid();

        var invalidPayload = Encoding.UTF8.GetBytes("{ invalid json");

        await StartConsumerAsync();

        // Act
        await PublishRawMessageAsync(invalidPayload, messageId);

        var parkedMessage = await WaitForParkedMessageAsync();

        // Assert
        Assert.Equal(messageId.ToString("D"), parkedMessage.MessageId);

        Assert.Equal("{ invalid json", parkedMessage.Payload);

        Assert.True(parkedMessage.HasReason);

        await using var db = _sqlFixture.CreateDbContext();

        var exists = await db.InboxMessages
            .AnyAsync(x => x.Id == messageId,
                TestContext.Current.CancellationToken);

        Assert.False(exists);
    }

    [Fact]
    public async Task ConflictingRegistration_IsMovedToParkingQueue()
    {
        // Arrange
        var original = CreateMessage();

        var conflicting = original with
        {
            MessageId = Guid.NewGuid(),
            SessionId = Guid.NewGuid()
        };

        await StartConsumerAsync();

        await PublishMessageAsync(original);

        await WaitForInboxMessageAsync(original.MessageId);

        // Act
        await PublishMessageAsync(conflicting);

        var parkedMessage = await WaitForParkedMessageAsync();

        // Assert
        Assert.Equal(
            conflicting.MessageId.ToString("D"),
            parkedMessage.MessageId);

        Assert.True(parkedMessage.HasReason);

        await using var db = _sqlFixture.CreateDbContext();

        var savedMessages = await db.InboxMessages
            .AsNoTracking()
            .Where(x =>
                x.RegistrationId == original.RegistrationId)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(savedMessages);

        Assert.Equal(original.SessionId, savedMessages[0].SessionId);
    }
}
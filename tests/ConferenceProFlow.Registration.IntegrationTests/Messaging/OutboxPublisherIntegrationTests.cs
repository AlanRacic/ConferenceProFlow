using System.Globalization;
using System.Text.Json;
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
public sealed class OutboxPublisherIntegrationTests : IClassFixture<RabbitMqFixture>, IAsyncLifetime
{
    private readonly SqlServerFixture _sqlFixture;
    private readonly RabbitMqFixture _rabbitMqFixture;

    private string? _connectionString;

    public OutboxPublisherIntegrationTests(
        SqlServerFixture sqlFixture,
        RabbitMqFixture rabbitMqFixture)
    {
        _sqlFixture = sqlFixture;
        _rabbitMqFixture = rabbitMqFixture;
    }

    public async ValueTask InitializeAsync()
    {
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
        if (_connectionString is null)
        {
            return;
        }

        await using var db = CreateDbContext();

        await db.Database.EnsureDeletedAsync();
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

    private async Task<RegistrationRequestedV1> CreateOutboxMessageAsync()
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
            RegistrationRequestedV1.MessageType,
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
}
using System.Globalization;
using System.Text;
using ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;
using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Messaging;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;
using ConferenceProFlow.Allocation.Worker.Messaging.Outbox;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.IntegrationTests.Messaging;

/// <summary>
/// Each test gets its own SQL database AND RabbitMQ broker. This is essential: older Allocation integration tests can leave unpublished Outbox records in their shared database, which must not leak into the publisher test batch.
/// </summary>
[Collection("AllocationSqlServer")]
public sealed class AllocationOutboxPublisherIntegrationTests : IAsyncLifetime
{
    private readonly AllocationSqlServerFixture _sqlFixture;
    private readonly AllocationRabbitMqFixture _rabbitFixture = new();
    private string? _connectionString;
    private IHost? _publisherHost;

    public AllocationOutboxPublisherIntegrationTests(AllocationSqlServerFixture sqlFixture)
    {
        _sqlFixture = sqlFixture;
    }

    public async ValueTask InitializeAsync()
    {
        var token = TestContext.Current.CancellationToken;
        await using var sharedDb = _sqlFixture.CreateDbContext();
        var sharedConnection = sharedDb.Database.GetConnectionString()
            ?? throw new InvalidOperationException("Missing SQL fixture connection string.");

        // Use a separate database within the already running SQL Testcontainer.
        var connectionBuilder = new SqlConnectionStringBuilder(sharedConnection)
        {
            InitialCatalog = $"AllocationPublisher_{Guid.NewGuid():N}"
        };
        _connectionString = connectionBuilder.ConnectionString;

        await using (var db = CreateDbContext())
        {
            await db.Database.MigrateAsync(token);
        }
        await _rabbitFixture.InitializeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await StopPublisherAsync();
            await _rabbitFixture.DisposeAsync();
        }
        finally
        {
            if (_connectionString is not null)
            {
                await using var db = CreateDbContext();
                await db.Database.EnsureDeletedAsync();
            }
        }
    }

    private AllocationDbContext CreateDbContext()
    {
        var connectionString = _connectionString
            ?? throw new InvalidOperationException("Publisher test database not initialized.");
        var options = new DbContextOptionsBuilder<AllocationDbContext>()
            .UseSqlServer(connectionString)
            .Options;
        return new AllocationDbContext(options);
    }

    private async Task<Guid> SeedResultAsync(bool malformed = false)
    {
        await using var db = CreateDbContext();
        var now = DateTimeOffset.UtcNow;
        var sessionId = Guid.NewGuid();
        var inventory = SessionInventory.Create(sessionId, 2, now);
        Assert.True(inventory.TryReserveSeat());
        var allocation = AllocationEntity.CreateConfirmed(Guid.NewGuid(), sessionId, now);
        var valid = AllocationOutboxMessageFactory.Create(allocation);
        var outbox = malformed
            ? AllocationOutboxMessage.Create(
                valid.Id, valid.AllocationId, valid.MessageType, "{ invalid json", valid.OccurredAtUtc)
            : valid;

        db.SessionInventories.Add(inventory);
        db.Allocations.Add(allocation);
        db.AllocationOutboxMessages.Add(outbox);
        await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        return outbox.Id;
    }

    private async Task<AllocationOutboxMessage> ReadOutboxAsync(Guid messageId)
    {
        await using var db = CreateDbContext();
        return await db.AllocationOutboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == messageId, TestContext.Current.CancellationToken);
    }

    private async Task DeclareResultQueueAsync()
    {
        var token = TestContext.Current.CancellationToken;
        var factory = new ConnectionFactory { Uri = _rabbitFixture.ConnectionUri };
        await using var connection = await factory.CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);

        await channel.ExchangeDeclareAsync(
            exchange: MessagingTopology.ExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: token);
        await channel.QueueDeclareAsync(
            queue: MessagingTopology.RegistrationAllocationResultsQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: token);
        await channel.QueueBindAsync(
            queue: MessagingTopology.RegistrationAllocationResultsQueue,
            exchange: MessagingTopology.ExchangeName,
            routingKey: MessagingTopology.AllocationDecidedRoutingKey,
            cancellationToken: token);
    }

    private async Task StartPublisherAsync()
    {
        if (_publisherHost is not null)
            throw new InvalidOperationException("Publisher already started.");

        var rabbit = new ConnectionFactory { Uri = _rabbitFixture.ConnectionUri };
        var settings = new Dictionary<string, string?>
        {
            ["RabbitMq:HostName"] = rabbit.HostName,
            ["RabbitMq:Port"] = rabbit.Port.ToString(CultureInfo.InvariantCulture),
            ["RabbitMq:VirtualHost"] = rabbit.VirtualHost,
            ["RabbitMq:UserName"] = rabbit.UserName,
            ["RabbitMq:Password"] = rabbit.Password,
            ["AllocationOutboxPublisher:PublishTimeoutSeconds"] = "3"
        };

        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddDbContext<AllocationDbContext>(
            options => options.UseSqlServer(_connectionString));
        builder.Services.AddHostedService<AllocationOutboxPublisherService>();
        _publisherHost = builder.Build();
        await _publisherHost.StartAsync(TestContext.Current.CancellationToken);
    }

    private async Task StopPublisherAsync()
    {
        if (_publisherHost is null) return;
        var host = _publisherHost;
        _publisherHost = null;
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await host.StopAsync(timeout.Token);
        }
        finally
        {
            host.Dispose();
        }
    }

    private static async Task WaitUntilAsync(
        Func<Task<bool>> condition, string failure, int maxSeconds = 50)
    {
        var token = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(maxSeconds);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(250, token);
        }
        throw new TimeoutException(failure);
    }

    private async Task<(string? MessageId, string? Type, string? ContentType, bool Persistent, string Body)>
        GetPublishedDeliveryAsync()
    {
        var token = TestContext.Current.CancellationToken;
        var factory = new ConnectionFactory { Uri = _rabbitFixture.ConnectionUri };
        await using var connection = await factory.CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(15);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var delivery = await channel.BasicGetAsync(
                MessagingTopology.RegistrationAllocationResultsQueue,
                autoAck: true,
                cancellationToken: token);
            if (delivery is not null)
            {
                return (
                    delivery.BasicProperties.MessageId,
                    delivery.BasicProperties.Type,
                    delivery.BasicProperties.ContentType,
                    delivery.BasicProperties.DeliveryMode == DeliveryModes.Persistent,
                    Encoding.UTF8.GetString(delivery.Body.Span));
            }
            await Task.Delay(250, token);
        }
        throw new TimeoutException("Allocation result did not arrive in the result queue.");
    }

    [Fact]
    public async Task ConfirmedResult_UsesStoredPayloadAndOriginalMessageId()
    {
        await DeclareResultQueueAsync();
        var messageId = await SeedResultAsync();
        var original = await ReadOutboxAsync(messageId);

        await StartPublisherAsync();
        await WaitUntilAsync(async () =>
            (await ReadOutboxAsync(messageId)).PublishedAtUtc is not null,
            "The Outbox record was not marked published.");

        var delivered = await GetPublishedDeliveryAsync();
        var saved = await ReadOutboxAsync(messageId);
        Assert.Equal(messageId.ToString("D"), delivered.MessageId);
        Assert.Equal(AllocationDecidedV1.MessageType, delivered.Type);
        Assert.Equal("application/json", delivered.ContentType);
        Assert.True(delivered.Persistent);
        Assert.Equal(original.Payload, delivered.Body);
        Assert.Equal(1, saved.PublishAttempts);
        Assert.Null(saved.LastError);
        Assert.Null(saved.NextAttemptAtUtc);
    }

    [Fact]
    public async Task UnroutableResult_IsRetriedRatherThanMarkedPublished()
    {
        // No Registration-side result queue or binding exists in this broker.
        // Mandatory publish + tracked confirms must reject the unroutable event.
        var messageId = await SeedResultAsync();
        await StartPublisherAsync();
        await WaitUntilAsync(async () =>
            (await ReadOutboxAsync(messageId)).PublishAttempts >= 1,
            "The unroutable publish attempt was not recorded.");

        var saved = await ReadOutboxAsync(messageId);
        Assert.Null(saved.PublishedAtUtc);
        Assert.Null(saved.QuarantinedAtUtc);
        Assert.NotNull(saved.LastError);
        Assert.NotNull(saved.NextAttemptAtUtc);
        Assert.Equal(1, saved.PublishAttempts);
    }

    [Fact]
    public async Task InvalidStoredJson_IsQuarantinedBeforePublication()
    {
        await DeclareResultQueueAsync();
        var messageId = await SeedResultAsync(malformed: true);
        await StartPublisherAsync();
        await WaitUntilAsync(async () =>
            (await ReadOutboxAsync(messageId)).QuarantinedAtUtc is not null,
            "Malformed result was not quarantined.");

        var saved = await ReadOutboxAsync(messageId);
        Assert.Null(saved.PublishedAtUtc);
        Assert.Null(saved.NextAttemptAtUtc);
        Assert.Equal(1, saved.PublishAttempts);
        Assert.NotNull(saved.LastError);

        var factory = new ConnectionFactory { Uri = _rabbitFixture.ConnectionUri };
        var token = TestContext.Current.CancellationToken;
        await using var connection = await factory.CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        var unexpected = await channel.BasicGetAsync(
            MessagingTopology.RegistrationAllocationResultsQueue,
            autoAck: true,
            cancellationToken: token);
        Assert.Null(unexpected);
    }

    [Fact]
    public async Task BrokerOutage_RetriesStoredEventAfterBrokerRecovery()
    {
        var messageId = await SeedResultAsync();
        await _rabbitFixture.StopBrokerAsync(TestContext.Current.CancellationToken);
        try
        {
            await StartPublisherAsync();
            await WaitUntilAsync(async () =>
                (await ReadOutboxAsync(messageId)).PublishAttempts >= 1,
                "Broker outage was not recorded as a failed attempt.",
                maxSeconds: 40);

            var failed = await ReadOutboxAsync(messageId);
            Assert.Null(failed.PublishedAtUtc);
            Assert.NotNull(failed.NextAttemptAtUtc);
        }
        finally
        {
            // Ensure the broker is running for the test's clean-up path.
            await _rabbitFixture.StartBrokerAsync(TestContext.Current.CancellationToken);
        }

        await DeclareResultQueueAsync();
        await WaitUntilAsync(async () =>
            (await ReadOutboxAsync(messageId)).PublishedAtUtc is not null,
            "Publisher did not recover after RabbitMQ restarted.",
            maxSeconds: 75);

        var delivered = await GetPublishedDeliveryAsync();
        var saved = await ReadOutboxAsync(messageId);
        Assert.Equal(messageId.ToString("D"), delivered.MessageId);
        Assert.True(saved.PublishAttempts >= 2);
        Assert.Null(saved.LastError);
    }
}
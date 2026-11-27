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
/// End-to-end publisher verification with an isolated SQL database and RabbitMQ broker.
/// No telemetry listener is registered: this tests the durable trace-context fallback.
/// </summary>
[Collection("AllocationSqlServer")]
public sealed class AllocationResultTraceHeadersIntegrationTests : IAsyncLifetime
{
    private const string TraceParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private const string TraceState = "vendor=value";

    private readonly AllocationSqlServerFixture _sqlFixture;
    private readonly AllocationRabbitMqFixture _rabbitFixture = new();
    private string? _connectionString;
    private IHost? _host;

    public AllocationResultTraceHeadersIntegrationTests(AllocationSqlServerFixture sqlFixture)
    {
        _sqlFixture = sqlFixture;
    }

    public async ValueTask InitializeAsync()
    {
        var token = TestContext.Current.CancellationToken;
        await using var shared = _sqlFixture.CreateDbContext();
        var sharedConnection = shared.Database.GetConnectionString()
            ?? throw new InvalidOperationException("Missing test SQL connection string.");
        _connectionString = new SqlConnectionStringBuilder(sharedConnection)
        {
            InitialCatalog = $"AllocationTracePublisher_{Guid.NewGuid():N}"
        }.ConnectionString;

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
            if (_host is not null)
            {
                var host = _host;
                _host = null;
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

    [Fact]
    public async Task AllocationResult_PublishesStoredW3CHeadersWithoutChangingIdentityOrPayload()
    {
        var token = TestContext.Current.CancellationToken;
        await DeclareResultQueueAsync(token);

        Guid messageId;
        string originalPayload;
        await using (var db = CreateDbContext())
        {
            var now = DateTimeOffset.UtcNow;
            var sessionId = Guid.NewGuid();
            var inventory = SessionInventory.Create(sessionId, 2, now);
            Assert.True(inventory.TryReserveSeat());
            var allocation = AllocationEntity.CreateConfirmed(Guid.NewGuid(), sessionId, now);
            var outbox = AllocationOutboxMessageFactory.Create(allocation, TraceParent, TraceState);
            messageId = outbox.Id;
            originalPayload = outbox.Payload;
            db.SessionInventories.Add(inventory);
            db.Allocations.Add(allocation);
            db.AllocationOutboxMessages.Add(outbox);
            await db.SaveChangesAsync(token);
        }

        await StartPublisherAsync(token);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(50);
        while (DateTimeOffset.UtcNow < deadline)
        {
            await using var db = CreateDbContext();
            var saved = await db.AllocationOutboxMessages.AsNoTracking()
                .SingleAsync(x => x.Id == messageId, token);
            if (saved.PublishedAtUtc is not null)
                break;
            await Task.Delay(250, token);
        }
        await using (var db = CreateDbContext())
        {
            var saved = await db.AllocationOutboxMessages.AsNoTracking()
                .SingleAsync(x => x.Id == messageId, token);
            Assert.NotNull(saved.PublishedAtUtc);
            Assert.Equal(TraceParent, saved.TraceParent);
            Assert.Equal(TraceState, saved.TraceState);
        }

        var factory = new ConnectionFactory { Uri = _rabbitFixture.ConnectionUri };
        await using var connection = await factory.CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        BasicGetResult? delivery = null;
        var receiveDeadline = DateTimeOffset.UtcNow.AddSeconds(15);
        while (DateTimeOffset.UtcNow < receiveDeadline)
        {
            delivery = await channel.BasicGetAsync(
                MessagingTopology.RegistrationAllocationResultsQueue,
                autoAck: true,
                cancellationToken: token);
            if (delivery is not null) break;
            await Task.Delay(250, token);
        }

        Assert.NotNull(delivery);
        Assert.Equal(messageId.ToString("D"), delivery.BasicProperties.MessageId);
        Assert.Equal(AllocationDecidedV1.MessageType, delivery.BasicProperties.Type);
        Assert.Equal("application/json", delivery.BasicProperties.ContentType);
        Assert.Equal(DeliveryModes.Persistent, delivery.BasicProperties.DeliveryMode);
        Assert.Equal(originalPayload, Encoding.UTF8.GetString(delivery.Body.Span));
        var headers = delivery.BasicProperties.Headers;
        Assert.NotNull(headers);
        Assert.True(headers.TryGetValue("traceparent", out var sentParent));
        Assert.True(headers.TryGetValue("tracestate", out var sentState));
        Assert.Equal(TraceParent, ReadHeader(sentParent));
        Assert.Equal(TraceState, ReadHeader(sentState));
    }

    private static string ReadHeader(object? value) => value switch
    {
        byte[] bytes => Encoding.ASCII.GetString(bytes),
        ReadOnlyMemory<byte> bytes => Encoding.ASCII.GetString(bytes.Span),
        string text => text,
        _ => throw new InvalidOperationException("Unsupported RabbitMQ trace-header representation.")
    };

    private AllocationDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<AllocationDbContext>()
            .UseSqlServer(_connectionString ?? throw new InvalidOperationException("Test database missing."))
            .Options;
        return new AllocationDbContext(options);
    }

    private async Task DeclareResultQueueAsync(CancellationToken token)
    {
        var factory = new ConnectionFactory { Uri = _rabbitFixture.ConnectionUri };
        await using var connection = await factory.CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await channel.ExchangeDeclareAsync(
            MessagingTopology.ExchangeName, ExchangeType.Topic, durable: true,
            autoDelete: false, cancellationToken: token);
        await channel.QueueDeclareAsync(
            MessagingTopology.RegistrationAllocationResultsQueue,
            durable: true, exclusive: false, autoDelete: false, cancellationToken: token);
        await channel.QueueBindAsync(
            MessagingTopology.RegistrationAllocationResultsQueue,
            MessagingTopology.ExchangeName,
            MessagingTopology.AllocationDecidedRoutingKey,
            cancellationToken: token);
    }

    private async Task StartPublisherAsync(CancellationToken token)
    {
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
        _host = builder.Build();
        await _host.StartAsync(token);
    }
}

using System.Globalization;
using System.Text;
using System.Text.Json;
using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Domain.Enums;
using ConferenceProFlow.Registration.Api.Infrastructure.Inbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Messaging.Consumers;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;
using Xunit;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.IntegrationTests.Messaging;

/// <summary>
/// End-to-end tests of the Registration-owned RabbitMQ result consumer against real RabbitMQ and SQL Server (the existing ResultReconciliationSqlFixture).
/// A fresh RabbitMQ container and test host are created for each test.
/// </summary>
public sealed class AllocationResultConsumerIntegrationTests :
    IClassFixture<ResultReconciliationSqlFixture>, IAsyncLifetime
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(40);

    private readonly ResultReconciliationSqlFixture _sqlFixture;
    private readonly RabbitMqContainer _rabbitMq = new RabbitMqBuilder("rabbitmq:4.2.9-management").Build();

    private IHost? _consumerHost;

    public AllocationResultConsumerIntegrationTests(ResultReconciliationSqlFixture sqlFixture)
    {
        _sqlFixture = sqlFixture;
    }

    public async ValueTask InitializeAsync()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await _rabbitMq.StartAsync(cancellationToken);

        // Declare the queue before publishing, so a test never depends on the timing of the consumer's own topology initialization.
        var factory = CreateRabbitFactory();
        await using var connection = await factory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await channel.ExchangeDeclareAsync(
            exchange: MessagingTopology.ExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: MessagingTopology.RegistrationAllocationResultsQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);

        await channel.QueueBindAsync(
            queue: MessagingTopology.RegistrationAllocationResultsQueue,
            exchange: MessagingTopology.ExchangeName,
            routingKey: MessagingTopology.AllocationDecidedRoutingKey,
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: MessagingTopology.FailedAllocationResultsQueue,
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
            await _rabbitMq.DisposeAsync();
        }
    }

    private ConnectionFactory CreateRabbitFactory() => new()
    {
        Uri = new Uri(_rabbitMq.GetConnectionString())
    };

    private RegistrationDbContext CreateDbContext() => _sqlFixture.CreateDbContext();

    private async Task StartConsumerAsync(string? connectionStringOverride = null)
    {
        if (_consumerHost is not null)
            throw new InvalidOperationException("Consumer host is already running.");

        await using var db = CreateDbContext();
        var connectionString = connectionStringOverride
            ?? db.Database.GetConnectionString()
            ?? throw new InvalidOperationException("Test SQL connection string is unavailable.");

        var rabbitFactory = CreateRabbitFactory();
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["RabbitMq:HostName"] = rabbitFactory.HostName,
            ["RabbitMq:Port"] = rabbitFactory.Port.ToString(CultureInfo.InvariantCulture),
            ["RabbitMq:VirtualHost"] = rabbitFactory.VirtualHost,
            ["RabbitMq:UserName"] = rabbitFactory.UserName,
            ["RabbitMq:Password"] = rabbitFactory.Password
        });

        builder.Services.AddDbContext<RegistrationDbContext>(
            options => options.UseSqlServer(connectionString));
        builder.Services.AddScoped<AllocationResultReconciliationService>();
        builder.Services.AddHostedService<AllocationResultConsumerService>();

        _consumerHost = builder.Build();
        await _consumerHost.StartAsync(TestContext.Current.CancellationToken);

        await WaitUntilAsync(async () =>
        {
            var (_, consumers) = await MainQueueStateAsync();
            return consumers == 1;
        }, "Registration result consumer did not register with RabbitMQ.");
    }

    private async Task StopConsumerAsync()
    {
        if (_consumerHost is null)
            return;

        var host = _consumerHost;
        _consumerHost = null;
        try
        {
            // Teardown must remain possible even if xUnit canceled a test.
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await host.StopAsync(stopCts.Token);
        }
        finally
        {
            host.Dispose();
        }
    }

    private async Task<(Guid RegistrationId, Guid SessionId)> SeedRegistrationAsync()
    {
        var start = new DateTimeOffset(2027, 6, 15, 9, 0, 0, TimeSpan.Zero);
        var conference = Conference.Create("Result Consumer Integration Conference", start, start.AddDays(2));
        var session = ConferenceSession.Create(conference.Id, "Reliable event processing", start.AddHours(1), start.AddHours(2));
        var participant = Participant.Create("Alex Morgan", $"alex.morgan.{Guid.NewGuid():N}@example.com");
        var registration = RegistrationEntity.Create(participant.Id, session.Id, DateTimeOffset.UtcNow);

        await using var db = CreateDbContext();
        db.Conferences.Add(conference);
        db.ConferenceSessions.Add(session);
        db.Participants.Add(participant);
        db.Registrations.Add(registration);
        await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        return (registration.Id, session.Id);
    }

    private static AllocationDecidedV1 DecisionFor(
        Guid registrationId, Guid sessionId, string status = AllocationDecidedV1.ConfirmedStatus) =>
        new(
            Guid.NewGuid(),
            Guid.NewGuid(),
            registrationId,
            sessionId,
            status,
            status == AllocationDecidedV1.RejectedStatus ? "Session capacity reached." : null,
            DateTimeOffset.UtcNow);

    private async Task PublishAsync(AllocationDecidedV1 decision)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(decision, JsonSerializerOptions.Web);
        await PublishRawAsync(payload, decision.MessageId);
    }

    private async Task PublishRawAsync(byte[] payload, Guid messageId)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var factory = CreateRabbitFactory();
        await using var connection = await factory.CreateConnectionAsync(cancellationToken);
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);
        await using var channel = await connection.CreateChannelAsync(options, cancellationToken);

        await channel.BasicPublishAsync(
            exchange: MessagingTopology.ExchangeName,
            routingKey: MessagingTopology.AllocationDecidedRoutingKey,
            mandatory: true,
            basicProperties: new BasicProperties
            {
                Persistent = true,
                MessageId = messageId.ToString("D"),
                Type = AllocationDecidedV1.MessageType,
                ContentType = "application/json"
            },
            body: payload,
            cancellationToken: cancellationToken);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string failureMessage)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.Add(WaitTimeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await condition())
                return;
            await Task.Delay(PollInterval, cancellationToken);
        }
        throw new TimeoutException(failureMessage);
    }

    private async Task WaitForReceiptAsync(Guid messageId)
    {
        await WaitUntilAsync(async () =>
        {
            await using var db = CreateDbContext();
            return await db.AllocationResultInboxMessages.AsNoTracking()
                .AnyAsync(x => x.Id == messageId, TestContext.Current.CancellationToken);
        }, $"Allocation decision {messageId} was not persisted in the Registration Inbox.");
    }

    private async Task<(uint Ready, uint Consumers)> MainQueueStateAsync()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var factory = CreateRabbitFactory();
        await using var connection = await factory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        var state = await channel.QueueDeclarePassiveAsync(
            MessagingTopology.RegistrationAllocationResultsQueue, cancellationToken);
        return (state.MessageCount, state.ConsumerCount);
    }

    private async Task AssertQueueDrainedAfterStopAsync()
    {
        // With prefetch=1, persisting a later fence message proves that all preceding deliveries were ACKed (otherwise the consumer could not receive the fence). The short delay lets the fence ACK complete.
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var fence = DecisionFor(registrationId, sessionId);
        await PublishAsync(fence);
        await WaitForReceiptAsync(fence.MessageId);
        await Task.Delay(TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken);

        await StopConsumerAsync();
        await WaitUntilAsync(async () =>
        {
            var (ready, consumers) = await MainQueueStateAsync();
            return ready == 0 && consumers == 0;
        }, "A delivery was requeued after consumer shutdown instead of being ACKed.");
    }

    private async Task<(string? MessageId, string Payload, bool HasReason)> WaitForParkedMessageAsync()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var factory = CreateRabbitFactory();
        var deadline = DateTimeOffset.UtcNow.Add(WaitTimeout);
        await using var connection = await factory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var received = await channel.BasicGetAsync(
                MessagingTopology.FailedAllocationResultsQueue,
                autoAck: true,
                cancellationToken: cancellationToken);
            if (received is not null)
            {
                return (
                    received.BasicProperties.MessageId,
                    Encoding.UTF8.GetString(received.Body.Span),
                    received.BasicProperties.Headers?.ContainsKey("x-parking-reason") == true);
            }
            await Task.Delay(PollInterval, cancellationToken);
        }
        throw new TimeoutException("No allocation result appeared in the parking queue.");
    }

    [Fact]
    public async Task ConfirmedResult_UpdatesRegistration_PersistsInbox_AndAcknowledges()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = DecisionFor(registrationId, sessionId);
        await StartConsumerAsync();
        await PublishAsync(decision);
        await WaitForReceiptAsync(decision.MessageId);
        await AssertQueueDrainedAfterStopAsync();

        await using var db = CreateDbContext();
        var registration = await db.Registrations.AsNoTracking()
            .SingleAsync(x => x.Id == registrationId, TestContext.Current.CancellationToken);
        Assert.Equal(RegistrationStatus.Confirmed, registration.Status);
        Assert.Null(registration.RejectionReason);
        var receipt = await db.AllocationResultInboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == decision.MessageId, TestContext.Current.CancellationToken);
        Assert.Equal(decision.AllocationId, receipt.AllocationId);
    }

    [Fact]
    public async Task RejectedResult_PreservesReason_AndAcknowledges()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = DecisionFor(registrationId, sessionId, AllocationDecidedV1.RejectedStatus);
        await StartConsumerAsync();
        await PublishAsync(decision);
        await WaitForReceiptAsync(decision.MessageId);
        await AssertQueueDrainedAfterStopAsync();

        await using var db = CreateDbContext();
        var registration = await db.Registrations.AsNoTracking()
            .SingleAsync(x => x.Id == registrationId, TestContext.Current.CancellationToken);
        Assert.Equal(RegistrationStatus.Rejected, registration.Status);
        Assert.Equal("Session capacity reached.", registration.RejectionReason);
    }

    [Fact]
    public async Task DuplicateDeliveryAndBusinessDuplicate_DoNotCreateAnotherReceipt()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var original = DecisionFor(registrationId, sessionId);
        var repeatedDecision = original with { MessageId = Guid.NewGuid() };

        await StartConsumerAsync();
        await PublishAsync(original);
        await WaitForReceiptAsync(original.MessageId);
        await PublishAsync(original); // Same transport message.
        await PublishAsync(repeatedDecision); // Different MessageId, same business decision.
        await AssertQueueDrainedAfterStopAsync();

        await using var db = CreateDbContext();
        Assert.Equal(1, await db.AllocationResultInboxMessages
            .CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
        Assert.Equal(RegistrationStatus.Confirmed,
            (await db.Registrations.SingleAsync(x => x.Id == registrationId,
                TestContext.Current.CancellationToken)).Status);
    }

    [Fact]
    public async Task ConflictingDecision_IsParked_AndDoesNotOverwriteFinalStatus()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var original = DecisionFor(registrationId, sessionId);
        var conflict = DecisionFor(registrationId, sessionId, AllocationDecidedV1.RejectedStatus);

        await StartConsumerAsync();
        await PublishAsync(original);
        await WaitForReceiptAsync(original.MessageId);
        await PublishAsync(conflict);
        var parked = await WaitForParkedMessageAsync();
        await AssertQueueDrainedAfterStopAsync();

        Assert.Equal(conflict.MessageId.ToString("D"), parked.MessageId);
        Assert.True(parked.HasReason);
        await using var db = CreateDbContext();
        Assert.Equal(RegistrationStatus.Confirmed,
            (await db.Registrations.SingleAsync(x => x.Id == registrationId,
                TestContext.Current.CancellationToken)).Status);
        Assert.Equal(1, await db.AllocationResultInboxMessages
            .CountAsync(x => x.RegistrationId == registrationId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task InvalidJson_IsParked_WithoutInboxReceipt()
    {
        var messageId = Guid.NewGuid();
        const string rawJson = "{ invalid json";
        await StartConsumerAsync();
        await PublishRawAsync(Encoding.UTF8.GetBytes(rawJson), messageId);
        var parked = await WaitForParkedMessageAsync();
        await AssertQueueDrainedAfterStopAsync();

        Assert.Equal(messageId.ToString("D"), parked.MessageId);
        Assert.Equal(rawJson, parked.Payload);
        Assert.True(parked.HasReason);
        await using var db = CreateDbContext();
        Assert.False(await db.AllocationResultInboxMessages
            .AnyAsync(x => x.Id == messageId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task MismatchedAmqpMessageId_IsParked_WithoutUpdatingRegistration()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = DecisionFor(registrationId, sessionId);
        await StartConsumerAsync();
        await PublishRawAsync(
            JsonSerializer.SerializeToUtf8Bytes(decision, JsonSerializerOptions.Web),
            Guid.NewGuid()); // AMQP header does not match JSON MessageId.
        var parked = await WaitForParkedMessageAsync();
        await AssertQueueDrainedAfterStopAsync();

        Assert.True(parked.HasReason);
        await using var db = CreateDbContext();
        Assert.Equal(RegistrationStatus.Pending,
            (await db.Registrations.SingleAsync(x => x.Id == registrationId,
                TestContext.Current.CancellationToken)).Status);
        Assert.False(await db.AllocationResultInboxMessages
            .AnyAsync(x => x.Id == decision.MessageId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UnknownRegistration_IsParked_WithoutCreatingInboxReceipt()
    {
        var decision = DecisionFor(Guid.NewGuid(), Guid.NewGuid());
        await StartConsumerAsync();
        await PublishAsync(decision);
        var parked = await WaitForParkedMessageAsync();
        await AssertQueueDrainedAfterStopAsync();

        Assert.Equal(decision.MessageId.ToString("D"), parked.MessageId);
        Assert.True(parked.HasReason);
        await using var db = CreateDbContext();
        Assert.False(await db.AllocationResultInboxMessages
            .AnyAsync(x => x.Id == decision.MessageId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ConsumerRestart_ProcessesResultPublishedWhileStopped()
    {
        var (firstRegistrationId, firstSessionId) = await SeedRegistrationAsync();
        var first = DecisionFor(firstRegistrationId, firstSessionId);
        await StartConsumerAsync();
        await PublishAsync(first);
        await WaitForReceiptAsync(first.MessageId);
        await StopConsumerAsync();

        var (pendingRegistrationId, pendingSessionId) = await SeedRegistrationAsync();
        var pending = DecisionFor(pendingRegistrationId, pendingSessionId);
        await PublishAsync(pending);
        var (readyBeforeRestart, consumersBeforeRestart) = await MainQueueStateAsync();
        Assert.Equal(0u, consumersBeforeRestart);
        Assert.True(readyBeforeRestart >= 1);

        await StartConsumerAsync();
        await WaitForReceiptAsync(pending.MessageId);
        await AssertQueueDrainedAfterStopAsync();

        await using var db = CreateDbContext();
        Assert.Equal(RegistrationStatus.Confirmed,
            (await db.Registrations.AsNoTracking().SingleAsync(
                x => x.Id == pendingRegistrationId,
                TestContext.Current.CancellationToken)).Status);
        Assert.Equal(1, await db.AllocationResultInboxMessages
            .CountAsync(x => x.Id == pending.MessageId,
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task RabbitMqBrokerRestart_ConsumerReconnectsAndProcessesResult()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await StartConsumerAsync();

        // Restart the RabbitMQ application inside the same Testcontainer, keeping the durable queues and their bindings.
        var stop = await _rabbitMq.ExecAsync(new[] { "rabbitmqctl", "stop_app" }, cancellationToken);
        Assert.Equal(0, stop.ExitCode);
        var start = await _rabbitMq.ExecAsync(new[] { "rabbitmqctl", "start_app" }, cancellationToken);
        Assert.Equal(0, start.ExitCode);

        await WaitUntilAsync(async () =>
        {
            try
            {
                var (_, consumers) = await MainQueueStateAsync();
                return consumers == 1;
            }
            catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException)
            {
                // The broker may still be starting up.
                return false;
            }
        }, "Registration result consumer did not reconnect after RabbitMQ restarted.");

        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = DecisionFor(registrationId, sessionId);
        await PublishAsync(decision);
        await WaitForReceiptAsync(decision.MessageId);
        await AssertQueueDrainedAfterStopAsync();
    }

    [Fact]
    public async Task SqlUnavailable_DeliveryIsRedeliveredAfterConsumerRestart()
    {
        var (registrationId, sessionId) = await SeedRegistrationAsync();
        var decision = DecisionFor(registrationId, sessionId);
        await using var db = CreateDbContext();
        var validConnectionString = db.Database.GetConnectionString()
            ?? throw new InvalidOperationException("Test SQL connection string is unavailable.");
        var broken = new SqlConnectionStringBuilder(validConnectionString)
        {
            InitialCatalog = $"MissingDatabase_{Guid.NewGuid():N}",
            ConnectTimeout = 2
        }.ConnectionString;

        await StartConsumerAsync(broken);
        await PublishAsync(decision);
        // Ready=0 and consumer=1 means the broker has delivered the message, but no durable processing can succeed against the nonexistent DB.
        await WaitUntilAsync(async () =>
        {
            var (ready, consumers) = await MainQueueStateAsync();
            return ready == 0 && consumers == 1;
        }, "Broken-SQL consumer did not receive the message.");
        await StopConsumerAsync();

        await WaitUntilAsync(async () =>
        {
            var (ready, consumers) = await MainQueueStateAsync();
            return ready == 1 && consumers == 0;
        }, "Unacknowledged delivery was not returned to the queue on shutdown.");
        Assert.False(await db.AllocationResultInboxMessages
            .AnyAsync(x => x.Id == decision.MessageId, TestContext.Current.CancellationToken));

        await StartConsumerAsync();
        await WaitForReceiptAsync(decision.MessageId);
        await AssertQueueDrainedAfterStopAsync();
        Assert.Equal(RegistrationStatus.Confirmed,
            (await db.Registrations.AsNoTracking().SingleAsync(
                x => x.Id == registrationId, TestContext.Current.CancellationToken)).Status);
    }
}
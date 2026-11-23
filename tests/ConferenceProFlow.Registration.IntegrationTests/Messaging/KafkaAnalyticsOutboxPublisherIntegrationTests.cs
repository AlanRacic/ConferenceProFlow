using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Infrastructure.Inbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.Kafka;
using Xunit;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.IntegrationTests.Messaging;

/// <summary>
/// Exercises the REAL hosted Kafka publisher against Kafka and SQL Server Testcontainers. Each test uses a separate topic and registration.
/// The local developer Kafka broker and database are not used.
/// </summary>
public sealed class KafkaAnalyticsOutboxPublisherIntegrationTests :
    IClassFixture<ResultReconciliationSqlFixture>, IAsyncLifetime
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(65);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);

    private readonly ResultReconciliationSqlFixture _sql;
    private readonly KafkaContainer _kafka = new KafkaBuilder("apache/kafka:4.4.0").Build();
    private readonly string _topic = $"conferenceproflow.m33e.{Guid.NewGuid():N}";

    private IHost? _publisherHost;

    public KafkaAnalyticsOutboxPublisherIntegrationTests(ResultReconciliationSqlFixture sql)
        => _sql = sql;

    public async ValueTask InitializeAsync()
    {
        await _kafka.StartAsync(TestContext.Current.CancellationToken);

        // Explicit creation. The test does not rely on Kafka auto-topic creation.
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress()
        }).Build();

        await admin.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = _topic,
                NumPartitions = 3,
                ReplicationFactor = 1
            }
        });
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await StopPublisherAsync();
        }
        finally
        {
            await _kafka.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(AllocationDecidedV1.ConfirmedStatus, RegistrationFinalizedV1.ConfirmedStatus)]
    [InlineData(AllocationDecidedV1.RejectedStatus, RegistrationFinalizedV1.RejectedStatus)]
    public async Task FinalizedRegistration_IsPublishedWithOriginalPayloadAndKey(
        string decisionStatus, string expectedFinalStatus)
    {
        var (registrationId, sessionId, messageId, originalPayload) =
            await SeedFinalizedRegistrationAsync(decisionStatus);

        await StartPublisherAsync(_kafka.GetBootstrapAddress());
        var published = await WaitForPublicationAsync(messageId);
        var delivered = ConsumeOne();

        Assert.Equal(sessionId.ToString("D"), delivered.Message.Key);
        Assert.Equal(originalPayload, delivered.Message.Value);
        Assert.True(delivered.Message.Headers.TryGetLastBytes("message-id", out var idHeader));
        Assert.Equal(messageId.ToString("D"), Encoding.UTF8.GetString(idHeader));
        Assert.True(delivered.Message.Headers.TryGetLastBytes("event-type", out var typeHeader));
        Assert.Equal(RegistrationFinalizedV1.EventType, Encoding.UTF8.GetString(typeHeader));

        var parsed = JsonSerializer.Deserialize<RegistrationFinalizedV1>(delivered.Message.Value);
        Assert.NotNull(parsed);
        Assert.Equal(messageId, parsed.MessageId);
        Assert.Equal(registrationId, parsed.RegistrationId);
        Assert.Equal(sessionId, parsed.SessionId);
        Assert.Equal(expectedFinalStatus, parsed.Status);

        Assert.NotNull(published.PublishedAtUtc);
        Assert.Equal(1, published.PublishAttempts);
        Assert.Null(published.LastError);
        Assert.Null(published.NextAttemptAtUtc);
    }

    [Fact]
    public async Task KafkaUnavailable_PersistsFailure_ThenPublisherRestartRecoversSameEvent()
    {
        var (_, sessionId, messageId, originalPayload) =
            await SeedFinalizedRegistrationAsync(AllocationDecidedV1.ConfirmedStatus);

        // Port 1 is deliberately unreachable; the live Kafka test container remains available for the subsequent recovery phase.
        await StartPublisherAsync("127.0.0.1:1");

        await WaitUntilAsync(async () =>
        {
            var row = await ReadOutboxAsync(messageId);
            return row.PublishAttempts >= 1 && row.PublishedAtUtc is null &&
                   row.LastError is not null && row.NextAttemptAtUtc is not null;
        }, "Kafka delivery failure was not durably recorded in Analytics Outbox.");

        var failed = await ReadOutboxAsync(messageId);
        Assert.Null(failed.PublishedAtUtc);
        Assert.NotNull(failed.LastError);
        Assert.NotNull(failed.NextAttemptAtUtc);

        // Restart the hosted service with a reachable broker. Do not recreate the Outbox row: its original MessageId and JSON payload must be reused.
        await StopPublisherAsync();
        await StartPublisherAsync(_kafka.GetBootstrapAddress());

        var published = await WaitForPublicationAsync(messageId);
        var delivered = ConsumeOne();

        Assert.Equal(sessionId.ToString("D"), delivered.Message.Key);
        Assert.Equal(originalPayload, delivered.Message.Value);
        var payload = JsonSerializer.Deserialize<RegistrationFinalizedV1>(delivered.Message.Value);
        Assert.NotNull(payload);
        Assert.Equal(messageId, payload.MessageId);

        Assert.NotNull(published.PublishedAtUtc);
        Assert.True(published.PublishAttempts >= 2);
        Assert.Null(published.LastError);
        Assert.Null(published.NextAttemptAtUtc);
    }

    /// <summary>
    /// Deterministic SQL/Kafka dual-write gap: Kafka ACK succeeds but the subsequent EF Core publication-state SaveChanges is rejected.
    /// After restarting a publisher without fault injection, the SAME durable event is published again. At-least-once behavior is explicit.
    /// </summary>
    [Fact]
    public async Task KafkaAcknowledged_ButSqlPublicationUpdateFails_RetryUsesSameMessageId()
    {
        var (_, sessionId, messageId, originalPayload) =
            await SeedFinalizedRegistrationAsync(AllocationDecidedV1.ConfirmedStatus);

        var sqlFault = new RejectPublicationStateUpdateInterceptor();
        await StartPublisherAsync(_kafka.GetBootstrapAddress(), sqlFault);

        // This interceptor only fires after ProduceAsync has completed with a Kafka delivery acknowledgement and MarkPublished has been called.
        await WaitUntilAsync(
            () => Task.FromResult(sqlFault.RejectedUpdates > 0),
            "Kafka ACK followed by simulated SQL publication-state failure was not observed.");

        // Stop the faulty instance before starting a clean one. The event must still be pending in SQL because the status update never committed.
        await StopPublisherAsync();
        var pending = await ReadOutboxAsync(messageId);
        Assert.Null(pending.PublishedAtUtc);
        Assert.Equal(originalPayload, pending.Payload);
        Assert.Equal(0, pending.PublishAttempts);
        Assert.True(sqlFault.RejectedUpdates >= 1);

        // Kafka has received the record even though the SQL row is still pending.
        var firstDelivery = ConsumeOne();
        Assert.Equal(sessionId.ToString("D"), firstDelivery.Message.Key);
        Assert.Equal(originalPayload, firstDelivery.Message.Value);

        // Restarting with normal SQL persistence re-sends the SAME outbox row.
        await StartPublisherAsync(_kafka.GetBootstrapAddress());
        var published = await WaitForPublicationAsync(messageId);
        Assert.NotNull(published.PublishedAtUtc);
        Assert.Equal(originalPayload, published.Payload);
        Assert.Equal(messageId, published.Id);

        // The topic has at least two transport deliveries, both with identical logical event identity. A future Analytics consumer must deduplicate.
        var deliveries = ConsumeMany(2);
        foreach (var delivery in deliveries)
        {
            Assert.Equal(sessionId.ToString("D"), delivery.Message.Key);
            Assert.Equal(originalPayload, delivery.Message.Value);
            var evt = JsonSerializer.Deserialize<RegistrationFinalizedV1>(delivery.Message.Value);
            Assert.NotNull(evt);
            Assert.Equal(messageId, evt.MessageId);
        }
        Assert.Equal(deliveries[0].Partition, deliveries[1].Partition);
        Assert.NotEqual(deliveries[0].Offset, deliveries[1].Offset);
    }

    private async Task<(Guid RegistrationId, Guid SessionId, Guid MessageId, string Payload)>
        SeedFinalizedRegistrationAsync(string decisionStatus)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var start = new DateTimeOffset(2027, 6, 15, 9, 0, 0, TimeSpan.Zero);
        var conference = Conference.Create("M3.3E Kafka Integration Conference", start, start.AddDays(2));
        var session = ConferenceSession.Create(conference.Id,
            "Kafka publisher persistence", start.AddHours(1), start.AddHours(2));
        var participant = Participant.Create("Morgan Ellis",
            $"morgan.ellis.{Guid.NewGuid():N}@example.com");
        var registration = RegistrationEntity.Create(participant.Id, session.Id, DateTimeOffset.UtcNow);

        await using (var db = _sql.CreateDbContext())
        {
            db.Conferences.Add(conference);
            db.ConferenceSessions.Add(session);
            db.Participants.Add(participant);
            db.Registrations.Add(registration);
            await db.SaveChangesAsync(cancellationToken);
        }

        var decision = new AllocationDecidedV1(
            Guid.NewGuid(), Guid.NewGuid(), registration.Id, session.Id,
            decisionStatus,
            decisionStatus == AllocationDecidedV1.RejectedStatus ? "Session capacity reached." : null,
            DateTimeOffset.UtcNow);

        await using (var db = _sql.CreateDbContext())
        {
            var result = await new AllocationResultReconciliationService(db)
                .ApplyAsync(decision, cancellationToken);
            Assert.Equal(AllocationResultReconciliationResult.Applied, result);
        }

        await using var verify = _sql.CreateDbContext();
        var outbox = await verify.AnalyticsOutboxMessages.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == registration.Id, cancellationToken);
        Assert.Null(outbox.PublishedAtUtc);
        Assert.Equal(0, outbox.PublishAttempts);
        return (registration.Id, session.Id, outbox.Id, outbox.Payload);
    }

    private async Task StartPublisherAsync(
        string bootstrapServers, SaveChangesInterceptor? publicationSaveInterceptor = null)
    {
        if (_publisherHost is not null)
            throw new InvalidOperationException("A publisher host is already running.");

        await using var db = _sql.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()
            ?? throw new InvalidOperationException("Missing Testcontainers SQL connection string.");

        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Kafka:BootstrapServers"] = bootstrapServers,
            ["Kafka:RegistrationFinalizedTopic"] = _topic
        });
        builder.Services.AddDbContext<RegistrationDbContext>(options =>
        {
            options.UseSqlServer(connectionString);
            if (publicationSaveInterceptor is not null)
                options.AddInterceptors(publicationSaveInterceptor);
        });
        builder.Services.AddHostedService<KafkaAnalyticsOutboxPublisherService>();

        _publisherHost = builder.Build();
        await _publisherHost.StartAsync(TestContext.Current.CancellationToken);
    }

    private async Task StopPublisherAsync()
    {
        if (_publisherHost is null)
            return;

        var host = _publisherHost;
        _publisherHost = null;
        try
        {
            // Do not reuse a canceled xUnit token for test cleanup.
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(25));
            await host.StopAsync(timeout.Token);
        }
        finally
        {
            host.Dispose();
        }
    }

    private async Task<AnalyticsOutboxMessage> ReadOutboxAsync(Guid messageId)
    {
        await using var db = _sql.CreateDbContext();
        return await db.AnalyticsOutboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == messageId, TestContext.Current.CancellationToken);
    }

    private async Task<AnalyticsOutboxMessage> WaitForPublicationAsync(Guid messageId)
    {
        await WaitUntilAsync(async () =>
        {
            var row = await ReadOutboxAsync(messageId);
            return row.PublishedAtUtc is not null;
        }, $"Analytics Outbox {messageId} was not marked published after Kafka delivery.");
        return await ReadOutboxAsync(messageId);
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

    private List<ConsumeResult<string, string>> ConsumeMany(int minimumCount)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"conferenceproflow-m33e-replay-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();
        consumer.Subscribe(_topic);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(35));

        var deliveries = new List<ConsumeResult<string, string>>();
        try
        {
            while (deliveries.Count < minimumCount)
                deliveries.Add(consumer.Consume(timeout.Token));
        }
        catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Expected {minimumCount} Kafka deliveries in {_topic}, but received {deliveries.Count}.");
        }
        finally
        {
            consumer.Close();
        }

        return deliveries;
    }

    private sealed class RejectPublicationStateUpdateInterceptor : SaveChangesInterceptor
    {
        private int _rejectedUpdates;

        public int RejectedUpdates => Volatile.Read(ref _rejectedUpdates);

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            if (eventData.Context is RegistrationDbContext db &&
                db.ChangeTracker.Entries<AnalyticsOutboxMessage>().Any(entry =>
                    entry.State == EntityState.Modified &&
                    entry.Entity.PublishedAtUtc is not null))
            {
                Interlocked.Increment(ref _rejectedUpdates);
                throw new InvalidOperationException(
                    "Injected SQL persistence failure after Kafka delivery acknowledgement.");
            }

            return new ValueTask<InterceptionResult<int>>(result);
        }
    }

    private ConsumeResult<string, string> ConsumeOne()
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"conferenceproflow-m33e-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();
        consumer.Subscribe(_topic);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(35));
        try
        {
            return consumer.Consume(timeout.Token);
        }
        catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Kafka did not deliver a record from {_topic} within 35 seconds.");
        }
        finally
        {
            consumer.Close();
        }
    }
}

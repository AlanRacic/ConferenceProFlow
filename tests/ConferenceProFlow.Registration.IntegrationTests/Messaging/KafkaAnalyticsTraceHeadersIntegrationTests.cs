using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.Kafka;
using Xunit;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.IntegrationTests.Messaging;

/// <summary>
/// End-to-end Kafka header check against SQL Server + Kafka Testcontainers.
/// Does not depend on a local OTLP collector or an ActivityListener.
/// </summary>
public sealed class KafkaAnalyticsTraceHeadersIntegrationTests :
    IClassFixture<ResultReconciliationSqlFixture>, IAsyncLifetime
{
    private const string OriginalTraceParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private const string OriginalTraceState = "vendor=value";

    private readonly ResultReconciliationSqlFixture _sql;
    private readonly KafkaContainer _kafka = new KafkaBuilder("apache/kafka:4.4.0").Build();
    private readonly string _topic = $"conferenceproflow.m42-trace-{Guid.NewGuid():N}";
    private IHost? _publisherHost;

    public KafkaAnalyticsTraceHeadersIntegrationTests(ResultReconciliationSqlFixture sql)
        => _sql = sql;

    public async ValueTask InitializeAsync()
    {
        await _kafka.StartAsync(TestContext.Current.CancellationToken);
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress()
        }).Build();
        await admin.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = _topic,
                NumPartitions = 1,
                ReplicationFactor = 1
            }
        });
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_publisherHost is not null)
            {
                using var stopTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(25));
                try
                {
                    await _publisherHost.StopAsync(stopTimeout.Token);
                }
                finally
                {
                    _publisherHost.Dispose();
                    _publisherHost = null;
                }
            }
        }
        finally
        {
            await _kafka.DisposeAsync();
        }
    }

    [Fact]
    public async Task PersistedOutboxTraceContext_ReachesKafkaHeadersWithOriginalEventIdentity()
    {
        var ct = TestContext.Current.CancellationToken;
        var start = new DateTimeOffset(2027, 6, 15, 9, 0, 0, TimeSpan.Zero);
        var conference = Conference.Create("Kafka trace verification", start, start.AddDays(2));
        var session = ConferenceSession.Create(
            conference.Id, "Kafka tracing", start.AddHours(1), start.AddHours(2));
        var participant = Participant.Create(
            "Morgan Ellis", $"morgan.ellis.{Guid.NewGuid():N}@example.com");
        var registration = RegistrationEntity.Create(
            participant.Id, session.Id, DateTimeOffset.UtcNow);

        var finalized = RegistrationFinalizedV1.Create(
            registration.Id, session.Id, RegistrationFinalizedV1.ConfirmedStatus);
        var outbox = AnalyticsOutboxMessage.FromEvent(
            finalized, OriginalTraceParent, OriginalTraceState);
        var originalPayload = outbox.Payload;

        await using (var db = _sql.CreateDbContext())
        {
            db.Conferences.Add(conference);
            db.ConferenceSessions.Add(session);
            db.Participants.Add(participant);
            db.Registrations.Add(registration);
            db.AnalyticsOutboxMessages.Add(outbox);
            await db.SaveChangesAsync(ct);
        }

        await StartPublisherAsync();
        await WaitUntilPublishedAsync(outbox.Id, ct);
        var delivered = ConsumeOne(ct);

        Assert.Equal(session.Id.ToString("D"), delivered.Message.Key);
        Assert.Equal(originalPayload, delivered.Message.Value);
        Assert.Equal(outbox.Id.ToString("D"), Read(delivered.Message.Headers, "message-id"));
        Assert.Equal(RegistrationFinalizedV1.EventType, Read(delivered.Message.Headers, "event-type"));
        Assert.Equal(OriginalTraceParent, Read(delivered.Message.Headers, "traceparent"));
        Assert.Equal(OriginalTraceState, Read(delivered.Message.Headers, "tracestate"));

        await using var verify = _sql.CreateDbContext();
        var published = await verify.AnalyticsOutboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == outbox.Id, ct);
        Assert.NotNull(published.PublishedAtUtc);
        Assert.Equal(1, published.PublishAttempts);
        Assert.Equal(originalPayload, published.Payload);
        Assert.Equal(OriginalTraceParent, published.TraceParent);
        Assert.Equal(OriginalTraceState, published.TraceState);
    }

    private async Task StartPublisherAsync()
    {
        await using var db = _sql.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()
            ?? throw new InvalidOperationException("Testcontainers SQL connection string is missing.");
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Kafka:BootstrapServers"] = _kafka.GetBootstrapAddress(),
            ["Kafka:RegistrationFinalizedTopic"] = _topic
        });
        builder.Services.AddDbContext<RegistrationDbContext>(options =>
            options.UseSqlServer(connectionString));
        builder.Services.AddHostedService<KafkaAnalyticsOutboxPublisherService>();
        _publisherHost = builder.Build();
        await _publisherHost.StartAsync(TestContext.Current.CancellationToken);
    }

    private async Task WaitUntilPublishedAsync(Guid id, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(65);
        while (DateTimeOffset.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            await using var db = _sql.CreateDbContext();
            var published = await db.AnalyticsOutboxMessages.AsNoTracking()
                .AnyAsync(x => x.Id == id && x.PublishedAtUtc != null, ct);
            if (published)
                return;
            await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
        }
        throw new TimeoutException("Kafka publication was not acknowledged in time.");
    }

    private ConsumeResult<string, string> ConsumeOne(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"conferenceproflow-m42-trace-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();
        consumer.Subscribe(_topic);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(35));
        try
        {
            return consumer.Consume(timeout.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException("Kafka trace test did not receive a record within 35 seconds.");
        }
        finally
        {
            consumer.Close();
        }
    }

    private static string Read(Headers headers, string name)
    {
        Assert.NotNull(headers);
        Assert.True(headers.TryGetLastBytes(name, out var raw));
        return Encoding.UTF8.GetString(raw);
    }
}

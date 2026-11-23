using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ConferenceProFlow.Analytics.IntegrationTests.Persistence;
using ConferenceProFlow.Analytics.Worker.Application.Processing;
using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Analytics.Worker.Messaging.Consumers;
using ConferenceProFlow.Analytics.Worker.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;

namespace ConferenceProFlow.Analytics.IntegrationTests.Messaging;

/// <summary>
/// Actual Kafka + actual SQL Server via Testcontainers. Verifies end-to-end
/// durable SQL effects and consumer-group offsets, not just helper methods.
/// </summary>
[Collection("AnalyticsM34ESerial")]
public sealed class KafkaAnalyticsDurableConsumerIntegrationTests(AnalyticsSqlFixture sql)
    : IClassFixture<AnalyticsSqlFixture>
{
    [Fact]
    public async Task ValidDuplicateAndMalformedRecords_UpdateSqlExactlyOnce_ThenCommitOffsets()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var kafka = new KafkaBuilder("apache/kafka:4.4.0").Build();
        await kafka.StartAsync(ct);

        var bootstrap = kafka.GetBootstrapAddress();
        var topic = $"conferenceproflow.m34e.{Guid.NewGuid():N}";
        var group = $"conferenceproflow.m34e.group.{Guid.NewGuid():N}";
        await CreateTopicAsync(bootstrap, topic);

        var sessionId = Guid.NewGuid();
        var first = NewEvent(sessionId, AnalyticsRegistrationFinalizedV1.ConfirmedStatus);
        var second = NewEvent(sessionId, AnalyticsRegistrationFinalizedV1.ConfirmedStatus);
        var third = NewEvent(sessionId, AnalyticsRegistrationFinalizedV1.RejectedStatus);

        await ProduceAsync(bootstrap, topic, first.SessionId.ToString("D"), JsonSerializer.Serialize(first), ct);
        await ProduceAsync(bootstrap, topic, second.SessionId.ToString("D"), JsonSerializer.Serialize(second), ct);
        await ProduceAsync(bootstrap, topic, first.SessionId.ToString("D"), JsonSerializer.Serialize(first), ct); // replay
        await ProduceAsync(bootstrap, topic, "test-malformed", "{broken-json", ct);
        await ProduceAsync(bootstrap, topic, third.SessionId.ToString("D"), JsonSerializer.Serialize(third), ct);

        using var host = BuildWorkerHost(bootstrap, topic, group);
        await host.StartAsync(ct);
        try
        {
            await WaitUntilAsync(async () =>
            {
                await using var verify = sql.CreateDbContext();
                var stats = await verify.SessionRegistrationStats.SingleOrDefaultAsync(x => x.SessionId == sessionId, ct);
                var ledgerCount = await verify.ProcessedAnalyticsEvents.CountAsync(x => x.SessionId == sessionId, ct);
                var quarantineCount = await verify.QuarantinedAnalyticsEvents.CountAsync(x => x.Topic == topic, ct);
                return stats is { ConfirmedCount: 2, RejectedCount: 1 } &&
                       ledgerCount == 3 && quarantineCount == 1 &&
                       GetCommittedOffset(bootstrap, group, topic) == 5;
            }, TimeSpan.FromSeconds(100), ct);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        await using var db = sql.CreateDbContext();
        Assert.Equal(3, await db.ProcessedAnalyticsEvents.CountAsync(x => x.SessionId == sessionId, ct));
        Assert.Equal(1, await db.QuarantinedAnalyticsEvents.CountAsync(x => x.Topic == topic, ct));
        Assert.Equal(5, GetCommittedOffset(bootstrap, group, topic));
    }

    [Fact]
    public async Task SqlFailure_PreventsOffsetCommit_UntilDatabaseRecovers()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var kafka = new KafkaBuilder("apache/kafka:4.4.0").Build();
        await kafka.StartAsync(ct);

        var bootstrap = kafka.GetBootstrapAddress();
        var topic = $"conferenceproflow.m34e.{Guid.NewGuid():N}";
        var group = $"conferenceproflow.m34e.group.{Guid.NewGuid():N}";
        await CreateTopicAsync(bootstrap, topic);

        var evt = NewEvent(Guid.NewGuid(), AnalyticsRegistrationFinalizedV1.ConfirmedStatus);
        await using (var seed = sql.CreateDbContext())
        {
            seed.SessionRegistrationStats.Add(new ConferenceProFlow.Analytics.Worker.Domain.Entities.SessionRegistrationStat
            {
                SessionId = evt.SessionId,
                ConfirmedCount = long.MaxValue,
                RejectedCount = 0,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            });
            await seed.SaveChangesAsync(ct);
        }
        await ProduceAsync(bootstrap, topic, evt.SessionId.ToString("D"), JsonSerializer.Serialize(evt), ct);

        var failureLogger = new KafkaConsumerFailureLoggerProvider();
        using var host = BuildWorkerHost(bootstrap, topic, group, failureLogger);
        await host.StartAsync(ct);
        try
        {
            // Await a logged consumer failure so the no-commit assertion is
            // meaningful (rather than passing before the first poll).
            await failureLogger.FirstFailure.Task.WaitAsync(TimeSpan.FromSeconds(90), ct);

            await using (var verify = sql.CreateDbContext())
            {
                Assert.Equal(0, await verify.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == evt.MessageId, ct));
                Assert.Equal(0, await verify.QuarantinedAnalyticsEvents.CountAsync(x => x.Topic == topic, ct));
            }
            Assert.True(GetCommittedOffset(bootstrap, group, topic) < 1);

            // Repair the simulated projection failure while the worker remains
            // active. Its restart loop must replay and successfully commit.
            await using (var repair = sql.CreateDbContext())
            {
                await repair.SessionRegistrationStats.Where(x => x.SessionId == evt.SessionId)
                    .ExecuteUpdateAsync(setters => setters.SetProperty(x => x.ConfirmedCount, 0L), ct);
            }

            await WaitUntilAsync(async () =>
            {
                await using var verify = sql.CreateDbContext();
                var ledgerCount = await verify.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == evt.MessageId, ct);
                var stats = await verify.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct);
                return ledgerCount == 1 && stats.ConfirmedCount == 1 &&
                       GetCommittedOffset(bootstrap, group, topic) == 1;
            }, TimeSpan.FromSeconds(100), ct);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    private IHost BuildWorkerHost(
        string bootstrap,
        string topic,
        string group,
        ILoggerProvider? extraLogger = null)
    {
        using var context = sql.CreateDbContext();
        var connectionString = context.Database.GetConnectionString()
            ?? throw new InvalidOperationException("SQL fixture connection string is missing.");

        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        if (extraLogger is not null)
            builder.Logging.AddProvider(extraLogger);

        builder.Services.AddDbContext<AnalyticsDbContext>(options =>
            options.UseSqlServer(connectionString));
        builder.Services.AddScoped<AnalyticsEventProcessingService>();
        builder.Services.AddScoped<AnalyticsQuarantineService>();
        builder.Services.AddScoped<KafkaAnalyticsRecordHandler>();
        builder.Services.AddSingleton<IOptions<AnalyticsKafkaConsumerOptions>>(Options.Create(
            new AnalyticsKafkaConsumerOptions
            {
                Enabled = true,
                ObserveOnly = false,
                BootstrapServers = bootstrap,
                Topic = topic,
                GroupId = group
            }));
        builder.Services.AddHostedService<KafkaAnalyticsConsumerService>();
        return builder.Build();
    }

    private static AnalyticsRegistrationFinalizedV1 NewEvent(Guid session, string status) =>
        new(Guid.NewGuid(), Guid.NewGuid(), session, status, DateTimeOffset.UtcNow);

    private static async Task CreateTopicAsync(string bootstrap, string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrap
        }).Build();
        await admin.CreateTopicsAsync(new[]
        {
            new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }
        });
    }

    private static async Task ProduceAsync(
        string bootstrap, string topic, string key, string value, CancellationToken cancellationToken)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.All
        }).Build();
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = key,
            Value = value
        }, cancellationToken);
    }

    private static long GetCommittedOffset(string bootstrap, string group, string topic)
    {
        using var checker = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = group,
            EnableAutoCommit = false
        }).Build();
        var committed = checker.Committed(
            new[] { new TopicPartition(topic, new Partition(0)) },
            TimeSpan.FromSeconds(3));
        return committed.Single().Offset.Value;
    }

    private static async Task WaitUntilAsync(
        Func<Task<bool>> predicate, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < timeout)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await predicate()) return;
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        }
        throw new TimeoutException("Kafka/SQL Analytics integration did not reach the expected state.");
    }

    private sealed class KafkaConsumerFailureLoggerProvider : ILoggerProvider
    {
        public TaskCompletionSource<bool> FirstFailure { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ILogger CreateLogger(string categoryName) => new FailureLogger(this, categoryName);
        public void Dispose() { }

        private sealed class FailureLogger(
            KafkaConsumerFailureLoggerProvider owner, string categoryName) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Error;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
                Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (logLevel >= LogLevel.Error &&
                    categoryName.Contains(nameof(KafkaAnalyticsConsumerService), StringComparison.Ordinal))
                {
                    owner.FirstFailure.TrySetResult(true);
                }
            }
        }
    }
}

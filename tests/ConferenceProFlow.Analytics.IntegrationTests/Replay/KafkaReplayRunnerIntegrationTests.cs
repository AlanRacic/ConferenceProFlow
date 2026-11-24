using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ConferenceProFlow.Analytics.Worker.Application.Processing;
using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Domain.Entities;
using ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Analytics.Worker.Messaging.Contracts;
using ConferenceProFlow.Analytics.Worker.Replay;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.Kafka;
using Testcontainers.MsSql;

namespace ConferenceProFlow.Analytics.IntegrationTests.Replay;

/// <summary>
/// M3.5E: exercises the real bounded Replay Runner against disposable Kafka
/// and SQL Server containers, including offset commits and real EF migrations.
/// This suite NEVER touches the developer's local Kafka or SQL databases.
/// Class methods run serially because they reset their own disposable replay DB.
/// </summary>
[Collection("AnalyticsM34ESerial")]
public sealed class KafkaReplayRunnerIntegrationTests(ReplayRunnerInfrastructureFixture fixture)
    : IClassFixture<ReplayRunnerInfrastructureFixture>
{
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task EmptyKafkaTopic_CompletesAllSnapshotPartitions_WithoutSqlWrites(int partitionCount)
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.ResetReplayDatabaseAsync(ct);
        var topic = NewTopic();
        var group = NewGroup();
        try
        {
            await CreateTopicAsync(topic, partitionCount);
            await RunReplayAsync(topic, group, resume: false, ct: ct);

            var snapshot = await LoadManifestAsync(group, ct);
            Assert.True(snapshot.Completed);
            Assert.True(snapshot.AllReached);
            Assert.Equal(partitionCount, snapshot.Partitions.Count);
            Assert.All(snapshot.Partitions, p =>
            {
                Assert.Equal(0, p.Low);
                Assert.Equal(0, p.HighExclusive);
                Assert.Equal(0, p.NextOffset);
            });
            Assert.Equal(0, snapshot.Applied);
            Assert.Equal(0, snapshot.Quarantined);

            await using var db = fixture.CreateReplayDb();
            Assert.Equal(0, await db.ProcessedAnalyticsEvents.CountAsync(ct));
            Assert.Equal(0, await db.SessionRegistrationStats.CountAsync(ct));
            Assert.Equal(0, await db.QuarantinedAnalyticsEvents.CountAsync(ct));
        }
        finally
        {
            DeleteTestManifest(group);
        }
    }

    [Fact]
    public async Task ThreePartitions_DuplicatesQuarantineAndLateRecord_RespectCapturedHighWatermarksAndLiveIsolation()
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.ResetReplayDatabaseAsync(ct);
        var topic = NewTopic();
        var group = NewGroup();
        var liveGroup = $"conferenceproflow.analytics.v1.test.{Guid.NewGuid():N}";
        var sessionId = Guid.NewGuid();
        var a = NewEvent(sessionId, AnalyticsRegistrationFinalizedV1.ConfirmedStatus);
        var b = NewEvent(sessionId, AnalyticsRegistrationFinalizedV1.ConfirmedStatus);
        var c = NewEvent(sessionId, AnalyticsRegistrationFinalizedV1.RejectedStatus);
        var late = NewEvent(sessionId, AnalyticsRegistrationFinalizedV1.ConfirmedStatus);
        try
        {
            await CreateTopicAsync(topic, 3);
            await using (var live = fixture.CreateLiveDb())
            {
                live.SessionRegistrationStats.Add(new SessionRegistrationStat
                {
                    SessionId = sessionId,
                    ConfirmedCount = 77,
                    RejectedCount = 12,
                    UpdatedAtUtc = DateTimeOffset.UtcNow
                });
                await live.SaveChangesAsync(ct);
            }

            // Explicit partitions make all three recorded bounds deterministic.
            await ProduceAsync(topic, 0, a.SessionId.ToString("D"), JsonSerializer.Serialize(a), ct);
            await ProduceAsync(topic, 0, a.SessionId.ToString("D"), JsonSerializer.Serialize(a), ct);
            await ProduceAsync(topic, 1, b.SessionId.ToString("D"), JsonSerializer.Serialize(b), ct);
            await ProduceAsync(topic, 1, "invalid", "{broken-json", ct);
            await ProduceAsync(topic, 2, c.SessionId.ToString("D"), JsonSerializer.Serialize(c), ct);

            // Establish a DIFFERENT group's committed offset. Replay may not move it.
            CommitGroupOffset(liveGroup, topic, partition: 0, nextOffset: 1);
            Assert.Equal(1, ReadGroupOffset(liveGroup, topic, partition: 0));

            // Synchronous hook executes AFTER the runner captures + saves its
            // snapshot but BEFORE it assigns and processes any records.
            // This is stronger than racing an asynchronous producer against replay.
            using var lateProducer = NewProducer();
            var injected = new SnapshotCaptureHookLogger(() =>
                lateProducer.ProduceAsync(
                    new TopicPartition(topic, new Partition(1)),
                    new Message<string, string>
                    {
                        Key = late.SessionId.ToString("D"),
                        Value = JsonSerializer.Serialize(late)
                    }).GetAwaiter().GetResult());

            await RunReplayAsync(topic, group, resume: false, ct: ct, logger: injected);
            Assert.True(injected.Fired);

            var snapshot = await LoadManifestAsync(group, ct);
            Assert.True(snapshot.Completed);
            Assert.True(snapshot.AllReached);
            Assert.Equal(3, snapshot.Applied);
            Assert.Equal(1, snapshot.DuplicateMessages);
            Assert.Equal(0, snapshot.DuplicateBusinessEvents);
            Assert.Equal(1, snapshot.Quarantined);
            Assert.Equal(2, snapshot.Partitions.Single(x => x.Partition == 0).HighExclusive);
            Assert.Equal(2, snapshot.Partitions.Single(x => x.Partition == 1).HighExclusive);
            Assert.Equal(1, snapshot.Partitions.Single(x => x.Partition == 2).HighExclusive);

            // The late record exists in Kafka at offset 2, but MUST NOT be in SQL.
            Assert.Equal(2, ReadGroupOffset(group, topic, partition: 0));
            Assert.Equal(2, ReadGroupOffset(group, topic, partition: 1));
            Assert.Equal(1, ReadGroupOffset(group, topic, partition: 2));
            Assert.Equal(1, ReadGroupOffset(liveGroup, topic, partition: 0));

            await using (var replay = fixture.CreateReplayDb())
            {
                var stats = await replay.SessionRegistrationStats.SingleAsync(x => x.SessionId == sessionId, ct);
                Assert.Equal(2, stats.ConfirmedCount);
                Assert.Equal(1, stats.RejectedCount);
                Assert.Equal(3, await replay.ProcessedAnalyticsEvents.CountAsync(x => x.SessionId == sessionId, ct));
                Assert.False(await replay.ProcessedAnalyticsEvents.AnyAsync(x => x.MessageId == late.MessageId, ct));
                Assert.Equal(1, await replay.QuarantinedAnalyticsEvents.CountAsync(x => x.Topic == topic, ct));
            }
            await using (var live = fixture.CreateLiveDb())
            {
                var stats = await live.SessionRegistrationStats.SingleAsync(x => x.SessionId == sessionId, ct);
                Assert.Equal(77, stats.ConfirmedCount);
                Assert.Equal(12, stats.RejectedCount);
                Assert.Equal(0, await live.ProcessedAnalyticsEvents.CountAsync(x => x.SessionId == sessionId, ct));
            }
        }
        finally
        {
            DeleteTestManifest(group);
        }
    }

    [Fact]
    public async Task SqlFailure_PreventsCommit_ExplicitResumeKeepsOriginalSnapshotAndDoesNotCountTwice()
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.ResetReplayDatabaseAsync(ct);
        var topic = NewTopic();
        var group = NewGroup();
        var successful = NewEvent(Guid.NewGuid(), AnalyticsRegistrationFinalizedV1.ConfirmedStatus);
        var blocked = NewEvent(Guid.NewGuid(), AnalyticsRegistrationFinalizedV1.ConfirmedStatus);
        try
        {
            await CreateTopicAsync(topic, 1);

            await ProduceAsync(topic, 0, successful.SessionId.ToString("D"), JsonSerializer.Serialize(successful), ct);
            await ProduceAsync(topic, 0, blocked.SessionId.ToString("D"), JsonSerializer.Serialize(blocked), ct);

            // Keep the replay DB empty for the runner's mandatory preflight.
            // Fail the ledger SaveChanges of ONLY the second event. The first
            // event is durably processed/committed; the second remains pending.
            // Unlike a logger hook, this fault occurs during actual processing,
            // after the runner has persisted its replay snapshot manifest.
            var sqlFault = new FailOnceOnEventSaveInterceptor(blocked.MessageId);
            var injectedFailure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                RunReplayAsync(topic, group, resume: false, ct: ct, faultInterceptor: sqlFault));
            Assert.Contains("Injected test-only SQL persistence failure", injectedFailure.Message);
            Assert.True(sqlFault.Triggered, "The targeted SQL persistence fault was not reached.");
            Assert.True(File.Exists(KafkaAnalyticsReplayRunner.GetManifestPath(group)),
                "Replay snapshot must exist after an event-processing SQL failure.");
            var interrupted = await LoadManifestAsync(group, ct);
            Assert.False(interrupted.Completed);
            Assert.Equal(1, interrupted.Partitions.Single().NextOffset);
            Assert.Equal(1, ReadGroupOffset(group, topic, partition: 0));

            await using (var replay = fixture.CreateReplayDb())
            {
                Assert.True(await replay.ProcessedAnalyticsEvents.AnyAsync(x => x.MessageId == successful.MessageId, ct));
                Assert.False(await replay.ProcessedAnalyticsEvents.AnyAsync(x => x.MessageId == blocked.MessageId, ct));
                Assert.Equal(1, (await replay.SessionRegistrationStats.SingleAsync(x => x.SessionId == successful.SessionId, ct)).ConfirmedCount);
                // The blocked event never committed either its ledger row or a statistics row.
                Assert.False(await replay.SessionRegistrationStats.AnyAsync(
                    x => x.SessionId == blocked.SessionId, ct));
            }

            // The fault injector is NOT registered for this separate Resume call.
            // The original snapshot and the committed first offset are reused.
            await RunReplayAsync(topic, group, resume: true, ct: ct);

            var completed = await LoadManifestAsync(group, ct);
            Assert.True(completed.Completed);
            Assert.True(completed.AllReached);
            Assert.Equal(2, ReadGroupOffset(group, topic, partition: 0));
            await using (var replay = fixture.CreateReplayDb())
            {
                Assert.Equal(1, (await replay.SessionRegistrationStats.SingleAsync(x => x.SessionId == successful.SessionId, ct)).ConfirmedCount);
                Assert.Equal(1, (await replay.SessionRegistrationStats.SingleAsync(x => x.SessionId == blocked.SessionId, ct)).ConfirmedCount);
                Assert.Equal(1, await replay.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == successful.MessageId, ct));
                Assert.Equal(1, await replay.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == blocked.MessageId, ct));
            }
        }
        finally
        {
            DeleteTestManifest(group);
        }
    }

    [Fact]
    public async Task CompletedReplay_RefusesResume_AndFreshRunCannotReuseCommittedGroupEvenWithoutManifest()
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.ResetReplayDatabaseAsync(ct);
        var topic = NewTopic();
        var group = NewGroup();
        var evt = NewEvent(Guid.NewGuid(), AnalyticsRegistrationFinalizedV1.RejectedStatus);
        try
        {
            await CreateTopicAsync(topic, 1);
            await ProduceAsync(topic, 0, evt.SessionId.ToString("D"), JsonSerializer.Serialize(evt), ct);
            await RunReplayAsync(topic, group, resume: false, ct: ct);
            var complete = await LoadManifestAsync(group, ct);
            Assert.True(complete.Completed);

            var resumed = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                RunReplayAsync(topic, group, resume: true, ct: ct));
            Assert.True(resumed.Message.Contains("already completed", StringComparison.OrdinalIgnoreCase));
            Assert.Equal(1, ReadGroupOffset(group, topic, partition: 0));

            // Simulate loss of the local manifest on another machine. Never
            // silently treat a previously committed group as a fresh run.
            DeleteTestManifest(group);
            await fixture.ResetReplayDatabaseAsync(ct); // test container ONLY
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                RunReplayAsync(topic, group, resume: false, ct: ct));
            Assert.True(refused.Message.Contains("already has committed Kafka offsets", StringComparison.OrdinalIgnoreCase));
            Assert.Equal(1, ReadGroupOffset(group, topic, partition: 0));

            await using var replay = fixture.CreateReplayDb();
            Assert.Equal(0, await replay.ProcessedAnalyticsEvents.CountAsync(ct));
        }
        finally
        {
            DeleteTestManifest(group);
        }
    }

    [Fact]
    public async Task MissingRetainedRecord_ResumeRefusesToDeclareCompleteOrSkipTheGap()
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.ResetReplayDatabaseAsync(ct);
        var topic = NewTopic();
        var group = NewGroup();
        var broken = NewEvent(Guid.NewGuid(), AnalyticsRegistrationFinalizedV1.ConfirmedStatus);
        var later = NewEvent(Guid.NewGuid(), AnalyticsRegistrationFinalizedV1.RejectedStatus);
        try
        {
            await CreateTopicAsync(topic, 1);
            await ProduceAsync(topic, 0, broken.SessionId.ToString("D"), JsonSerializer.Serialize(broken), ct);
            await ProduceAsync(topic, 0, later.SessionId.ToString("D"), JsonSerializer.Serialize(later), ct);
            // A test-only EF persistence fault interrupts processing at offset 0,
            // AFTER snapshot capture but BEFORE any SQL event is committed.
            // This creates an incomplete snapshot to test Kafka retention loss.
            var sqlFault = new FailOnceOnEventSaveInterceptor(broken.MessageId);
            var injectedFailure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                RunReplayAsync(topic, group, resume: false, ct: ct, faultInterceptor: sqlFault));
            Assert.Contains("Injected test-only SQL persistence failure", injectedFailure.Message);
            Assert.True(sqlFault.Triggered, "The targeted SQL persistence fault was not reached.");
            Assert.True(File.Exists(KafkaAnalyticsReplayRunner.GetManifestPath(group)),
                "Replay snapshot must exist after an event-processing SQL failure.");
            Assert.Equal(Offset.Unset.Value, ReadGroupOffset(group, topic, partition: 0));
            Assert.False((await LoadManifestAsync(group, ct)).Completed);

            // DeleteRecords is used ONLY on a unique topic in the disposable
            // Kafka Testcontainer. It simulates a retention-advanced log start.
            using (var admin = NewAdmin())
            {
                await admin.DeleteRecordsAsync(
                    new[] { new TopicPartitionOffset(topic, new Partition(0), new Offset(1)) },
                    new DeleteRecordsOptions { OperationTimeout = TimeSpan.FromSeconds(20) });
            }
            await WaitForLowWatermarkAsync(topic, expectedLow: 1, ct);

            // The fault injector no longer participates in Resume. Nevertheless,
            // Kafka has deleted an offset required by the original snapshot.
            // A correct runner MUST reject the incomplete history.
            var failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                RunReplayAsync(topic, group, resume: true, ct: ct));
            Assert.True(failure.Message.Contains("retention advanced", StringComparison.OrdinalIgnoreCase));
            Assert.False((await LoadManifestAsync(group, ct)).Completed);
            Assert.Equal(Offset.Unset.Value, ReadGroupOffset(group, topic, partition: 0));

            await using var verify = fixture.CreateReplayDb();
            Assert.Equal(0, await verify.ProcessedAnalyticsEvents.CountAsync(ct));
            Assert.Equal(0, await verify.QuarantinedAnalyticsEvents.CountAsync(ct));
        }
        finally
        {
            DeleteTestManifest(group);
        }
    }

    private static AnalyticsRegistrationFinalizedV1 NewEvent(Guid sessionId, string status) =>
        new(Guid.NewGuid(), Guid.NewGuid(), sessionId, status, DateTimeOffset.UtcNow);

    private static string NewTopic() => $"conferenceproflow.m35e.{Guid.NewGuid():N}";
    private static string NewGroup() => $"conferenceproflow.analytics.replay.test.{Guid.NewGuid():N}";

    private async Task CreateTopicAsync(string topic, int partitions)
    {
        using var admin = NewAdmin();
        await admin.CreateTopicsAsync(new[]
        {
            new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }
        });
    }

    private IAdminClient NewAdmin() => new AdminClientBuilder(new AdminClientConfig
    {
        BootstrapServers = fixture.BootstrapServers
    }).Build();

    private IProducer<string, string> NewProducer() =>
        new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = fixture.BootstrapServers,
            Acks = Acks.All
        }).Build();

    private async Task ProduceAsync(string topic, int partition, string key, string value, CancellationToken ct)
    {
        using var producer = NewProducer();
        await producer.ProduceAsync(new TopicPartition(topic, new Partition(partition)),
            new Message<string, string> { Key = key, Value = value }, ct);
    }

    private long ReadGroupOffset(string group, string topic, int partition)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = fixture.BootstrapServers,
            GroupId = group,
            EnableAutoCommit = false
        }).Build();
        return consumer.Committed(new[] { new TopicPartition(topic, new Partition(partition)) },
            TimeSpan.FromSeconds(10)).Single().Offset.Value;
    }

    private void CommitGroupOffset(string group, string topic, int partition, long nextOffset)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = fixture.BootstrapServers,
            GroupId = group,
            EnableAutoCommit = false
        }).Build();
        consumer.Assign(new TopicPartitionOffset(topic, new Partition(partition), new Offset(0)));
        consumer.Commit(new[] { new TopicPartitionOffset(topic, new Partition(partition), new Offset(nextOffset)) });
    }

    private async Task WaitForLowWatermarkAsync(string topic, long expectedLow, CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = fixture.BootstrapServers,
            GroupId = NewGroup(),
            EnableAutoCommit = false
        }).Build();
        var end = DateTimeOffset.UtcNow.AddSeconds(30);
        var tp = new TopicPartition(topic, new Partition(0));
        while (DateTimeOffset.UtcNow < end)
        {
            ct.ThrowIfCancellationRequested();
            if (consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5)).Low.Value >= expectedLow)
                return;
            await Task.Delay(250, ct);
        }
        throw new TimeoutException("Kafka Testcontainer did not advance its low watermark after DeleteRecords.");
    }

    private async Task RunReplayAsync(string topic, string group, bool resume, CancellationToken ct,
        ILogger<KafkaAnalyticsReplayRunner>? logger = null,
        FailOnceOnEventSaveInterceptor? faultInterceptor = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AnalyticsDbContext>(options =>
        {
            options.UseSqlServer(fixture.ReplayConnectionString);
            if (faultInterceptor is not null)
                options.AddInterceptors(faultInterceptor);
        });
        services.AddScoped<AnalyticsEventProcessingService>();
        services.AddScoped<AnalyticsQuarantineService>();
        services.AddScoped<KafkaAnalyticsRecordHandler>();
        using var provider = services.BuildServiceProvider(validateScopes: true);
        var options = new AnalyticsReplayOptions
        {
            Enabled = true,
            BootstrapServers = fixture.BootstrapServers,
            Topic = topic,
            GroupId = group,
            Resume = resume,
            MaxIdleSeconds = 60
        };
        var runner = new KafkaAnalyticsReplayRunner(options,
            provider.GetRequiredService<IServiceScopeFactory>(),
            logger ?? NullLogger<KafkaAnalyticsReplayRunner>.Instance);
        await runner.RunAsync(ct);
    }

    private static Task<ReplaySnapshot> LoadManifestAsync(string group, CancellationToken ct) =>
        ReplaySnapshotFile.LoadAsync(KafkaAnalyticsReplayRunner.GetManifestPath(group), ct);

    private static void DeleteTestManifest(string group)
    {
        // Only randomly named manifests created by this disposable test suite.
        var path = KafkaAnalyticsReplayRunner.GetManifestPath(group);
        if (File.Exists(path)) File.Delete(path);
        if (File.Exists(path + ".lock")) File.Delete(path + ".lock");
    }

    /// <summary>
    /// M3.5E test-only fault injection. A fresh replay database remains empty
    /// during preflight. Only a selected event's EF ledger SaveChanges is
    /// interrupted; all other event processing uses real SQL Server.
    /// Each test run gets a distinct interceptor, and Resume omits it.
    /// </summary>
    private sealed class FailOnceOnEventSaveInterceptor(Guid targetMessageId) : SaveChangesInterceptor
    {
        private int _triggered;
        public bool Triggered => Volatile.Read(ref _triggered) == 1;

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            if (eventData.Context is AnalyticsDbContext db &&
                db.ChangeTracker.Entries<ProcessedAnalyticsEvent>()
                    .Any(entry => entry.State == EntityState.Added &&
                                  entry.Entity.MessageId == targetMessageId) &&
                Interlocked.CompareExchange(ref _triggered, 1, 0) == 0)
            {
                throw new InvalidOperationException(
                    "Injected test-only SQL persistence failure for MessageId " + targetMessageId);
            }
            return base.SavingChangesAsync(eventData, result, cancellationToken);
        }
    }

    private sealed class SnapshotCaptureHookLogger(Action onCaptured)
        : ILogger<KafkaAnalyticsReplayRunner>
    {
        private int _fired;
        public bool Fired => Volatile.Read(ref _fired) == 1;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (formatter(state, exception).Contains("Captured new Kafka replay snapshot:",
                    StringComparison.Ordinal) && Interlocked.CompareExchange(ref _fired, 1, 0) == 0)
                onCaptured();
        }
    }
}

/// <summary>
/// One Kafka and one SQL Server Testcontainer for the whole test class. The
/// replay database is explicitly reset INSIDE the throwaway SQL container per
/// test; the live test database is never reset by replay and is read-only to it.
/// </summary>
public sealed class ReplayRunnerInfrastructureFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _sql = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
    private readonly KafkaContainer _kafka = new KafkaBuilder("apache/kafka:4.4.0").Build();

    public string BootstrapServers => _kafka.GetBootstrapAddress();
    public string ReplayConnectionString { get; private set; } = string.Empty;
    private string LiveConnectionString { get; set; } = string.Empty;

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _sql.StartAsync(ct);
        await _kafka.StartAsync(ct);
        var baseConnection = new SqlConnectionStringBuilder(_sql.GetConnectionString());
        ReplayConnectionString = new SqlConnectionStringBuilder(baseConnection.ConnectionString)
        {
            InitialCatalog = AnalyticsReplayOptions.DatabaseName
        }.ConnectionString;
        LiveConnectionString = new SqlConnectionStringBuilder(baseConnection.ConnectionString)
        {
            InitialCatalog = AnalyticsReplayOptions.LiveDatabaseName
        }.ConnectionString;
        await using (var live = CreateLiveDb())
            await live.Database.MigrateAsync(ct);
        await ResetReplayDatabaseAsync(ct);
    }

    public AnalyticsDbContext CreateLiveDb() => CreateContext(LiveConnectionString);
    public AnalyticsDbContext CreateReplayDb() => CreateContext(ReplayConnectionString);

    public async Task ResetReplayDatabaseAsync(CancellationToken ct)
    {
        // This is an entirely disposable Testcontainer DB; NEVER run against
        // user connection strings or existing local databases.
        await using var db = CreateReplayDb();
        await db.Database.EnsureDeletedAsync(ct);
        await db.Database.MigrateAsync(ct);
    }

    private static AnalyticsDbContext CreateContext(string connection) =>
        new(new DbContextOptionsBuilder<AnalyticsDbContext>().UseSqlServer(connection).Options);

    public async ValueTask DisposeAsync()
    {
        await _kafka.DisposeAsync();
        await _sql.DisposeAsync();
    }
}

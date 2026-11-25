using System.Security.Cryptography;
using System.Text;
using Confluent.Kafka;
using ConferenceProFlow.Analytics.Worker.Application.Processing;
using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ConferenceProFlow.Analytics.Worker.Replay;

/// <summary>
/// M3.5C: a one-shot Kafka snapshot reader. Explicit partition assignment is
/// intentional: the replay group is used exclusively for offset commits; this
/// local tool does NOT participate in group rebalancing or parallel replay.
///
/// SQL-first processing/quarantine is reused unchanged from M3.4E. The run
/// exits successfully ONLY when each partition's captured high watermark is
/// reached. Failed SQL work or a failed Kafka commit aborts the whole run.
/// </summary>
public sealed class KafkaAnalyticsReplayRunner(
    AnalyticsReplayOptions settings,
    IServiceScopeFactory scopeFactory,
    ILogger<KafkaAnalyticsReplayRunner> logger)
{
    private static readonly TimeSpan KafkaTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan PollTimeout = TimeSpan.FromMilliseconds(500);

    public static ConsumerConfig CreateConsumerConfig(AnalyticsReplayOptions replay)
    {
        ArgumentNullException.ThrowIfNull(replay);
        if (!replay.Enabled || string.IsNullOrWhiteSpace(replay.BootstrapServers) ||
            string.IsNullOrWhiteSpace(replay.GroupId) || string.IsNullOrWhiteSpace(replay.Topic))
            throw new ArgumentException("Replay requires explicit Kafka settings.", nameof(replay));

        return new ConsumerConfig
        {
            BootstrapServers = replay.BootstrapServers,
            GroupId = replay.GroupId,
            AllowAutoCreateTopics = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            EnablePartitionEof = true
        };
    }

    /// <summary>
    /// Resolves a stable snapshot manifest location. By default, use the user's
    /// persistent local application-data directory, not the OS temp directory.
    /// An explicit override must be an absolute path (outside the repository).
    /// Do not silently fall back to temp: a lost manifest prevents safe Resume.
    /// </summary>
    public static string GetManifestPath(string groupId, string? snapshotDirectory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        string directory;
        if (string.IsNullOrWhiteSpace(snapshotDirectory))
        {
            var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            if (string.IsNullOrWhiteSpace(localAppData))
                throw new InvalidOperationException(
                    "Local application-data directory is unavailable. Set AnalyticsReplay:SnapshotDirectory to an absolute path.");
            directory = Path.Combine(localAppData, "ConferenceProFlow", "ReplaySnapshots");
        }
        else
        {
            if (!Path.IsPathFullyQualified(snapshotDirectory))
                throw new InvalidOperationException(
                    "AnalyticsReplay:SnapshotDirectory must be an absolute directory path.");
            directory = snapshotDirectory;
        }
        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(groupId));
        var stableId = Convert.ToHexString(digest)[..20];
        return Path.Combine(Path.GetFullPath(directory), stableId + ".json");
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        if (settings.MaxIdleSeconds is < 5 or > 600)
            throw new InvalidOperationException("AnalyticsReplay:MaxIdleSeconds must be between 5 and 600.");

        // Fail early before touching Kafka, including schema and fresh-target
        // checks. Program.cs already validates DB names and the group prefix.
        await CheckReplayDatabaseAsync(requireEmpty: !settings.Resume, cancellationToken);

        var config = CreateConsumerConfig(settings);
        var manifestPath = GetManifestPath(settings.GroupId, settings.SnapshotDirectory);
        Directory.CreateDirectory(Path.GetDirectoryName(manifestPath)!);
        // A local, exclusive per-group lock prevents two simultaneous replay
        // processes from using the same group and manifest on this machine.
        using var runLock = new FileStream(manifestPath + ".lock", FileMode.OpenOrCreate,
            FileAccess.ReadWrite, FileShare.None);
        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        ReplaySnapshot snapshot;
        if (settings.Resume)
        {
            if (!File.Exists(manifestPath))
                throw new InvalidOperationException("Resume requested, but the replay snapshot manifest does not exist.");
            snapshot = await ReplaySnapshotFile.LoadAsync(manifestPath, cancellationToken);
            ValidateResumeManifest(snapshot);
            logger.LogWarning("Resuming INCOMPLETE replay from its original snapshot: {Path}", manifestPath);
        }
        else
        {
            if (File.Exists(manifestPath))
                throw new InvalidOperationException(
                    "This replay group already has a local snapshot manifest. " +
                    "Choose a NEW GroupId for a fresh run, or set Resume=true for an interrupted run.");
            using var admin = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = settings.BootstrapServers
            }).Build();
            snapshot = CaptureSnapshot(admin, consumer, settings);
            // A fresh group must not already contain committed offsets, even if
            // its local manifest was deleted or this is another computer.
            var prior = GetCommittedOffsets(consumer, snapshot.Partitions);
            if (prior.Values.Any(offset => offset >= 0))
                throw new InvalidOperationException(
                    "Replay GroupId already has committed Kafka offsets. Use a new group for a fresh replay.");
            await ReplaySnapshotFile.SaveAsync(manifestPath, snapshot, cancellationToken);
            logger.LogInformation("Captured new Kafka replay snapshot: {Path}", manifestPath);
        }

        // Committed offsets are the truth after interruption. This deliberately
        // does not trust optimistic NextOffset values from a previous manifest
        // write; the SQL idempotency ledger covers Kafka commit ambiguity.
        var committed = GetCommittedOffsets(consumer, snapshot.Partitions);
        foreach (var window in snapshot.Partitions)
        {
            var next = committed[window.Partition] >= 0 ? committed[window.Partition] : window.Low;
            if (next < window.Low || next > window.HighExclusive)
                throw new InvalidOperationException(
                    "Replay group committed offset is outside its original snapshot; refusing to continue.");
            window.NextOffset = next;
            var tp = new TopicPartition(settings.Topic, new Partition(window.Partition));
            var current = consumer.QueryWatermarkOffsets(tp, KafkaTimeout);
            // A later low watermark implies Kafka retention has removed a
            // record required by the unfinished replay. Do not fake success.
            if (window.NextOffset < window.HighExclusive && current.Low.Value > window.NextOffset)
                throw new InvalidOperationException(
                    "Kafka retention advanced past an unprocessed snapshot offset; replay is incomplete.");
            if (current.High.Value < window.HighExclusive)
                throw new InvalidOperationException(
                    "Kafka high watermark is now below the captured snapshot boundary.");

            logger.LogInformation(
                "Replay partition={Partition} earliest={Low} targetExclusive={High} startingAt={Next}",
                window.Partition, window.Low, window.HighExclusive, window.NextOffset);
        }

        await ReplaySnapshotFile.SaveAsync(manifestPath, snapshot, cancellationToken);
        if (!snapshot.AllReached)
        {
            var assignments = snapshot.Partitions.Where(p => !p.IsComplete)
                .Select(p => new TopicPartitionOffset(
                    settings.Topic, new Partition(p.Partition), new Offset(p.NextOffset)))
                .ToList();
            consumer.Assign(assignments);
            await ProcessSnapshotAsync(consumer, snapshot, manifestPath, cancellationToken);
        }

        if (!snapshot.AllReached)
            throw new InvalidOperationException("Replay did not reach all captured partition boundaries.");

        snapshot.Completed = true;
        await ReplaySnapshotFile.SaveAsync(manifestPath, snapshot, cancellationToken);
        logger.LogInformation(
            "REPLAY COMPLETED: group={Group}, partitions={Partitions}, applied={Applied}, " +
            "duplicateMessages={DuplicateMessages}, duplicateBusinessEvents={DuplicateBusiness}, " +
            "quarantined={Quarantined}; manifest={Manifest}",
            settings.GroupId, snapshot.Partitions.Count, snapshot.Applied,
            snapshot.DuplicateMessages, snapshot.DuplicateBusinessEvents,
            snapshot.Quarantined, manifestPath);
        foreach (var p in snapshot.Partitions)
            logger.LogInformation("Completed partition {Partition}: [{Low},{High}), committedNext={Next}",
                p.Partition, p.Low, p.HighExclusive, p.NextOffset);
    }

    private async Task ProcessSnapshotAsync(
        IConsumer<string, string> consumer, ReplaySnapshot snapshot,
        string manifestPath, CancellationToken cancellationToken)
    {
        var windows = snapshot.Partitions.ToDictionary(p => p.Partition);
        var lastProgress = DateTimeOffset.UtcNow;
        var maxIdle = TimeSpan.FromSeconds(settings.MaxIdleSeconds);
        while (!snapshot.AllReached)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var record = consumer.Consume(PollTimeout);
            if (record is null || record.IsPartitionEOF || record.Message is null)
            {
                if (DateTimeOffset.UtcNow - lastProgress > maxIdle)
                    throw new TimeoutException(
                        "Replay stalled before reaching all captured high watermarks. " +
                        "It has NOT completed; use Resume=true after diagnosing Kafka/SQL availability.");
                continue;
            }
            if (!string.Equals(record.Topic, settings.Topic, StringComparison.Ordinal) ||
                !windows.TryGetValue(record.Partition.Value, out var partition))
                throw new InvalidOperationException("Consumed record from an unexpected replay topic/partition.");

            // A paused completed partition can have an already-buffered
            // post-snapshot record. Ignore it; NEVER process or commit it.
            if (partition.IsComplete)
            {
                consumer.Pause(new[] { record.TopicPartition });
                continue;
            }
            // Never consume, persist or COMMIT post-snapshot records.
            partition.ValidateIncoming(record.Offset.Value);

            KafkaAnalyticsRecordDisposition disposition;
            await using (var scope = scopeFactory.CreateAsyncScope())
            {
                var handler = scope.ServiceProvider.GetRequiredService<KafkaAnalyticsRecordHandler>();
                disposition = await handler.HandleAsync(record, cancellationToken);
            }

            // This call acknowledges next-offset for THIS replay group only.
            // Any failure aborts the run; no next record is processed.
            consumer.Commit(record);
            partition.MarkCommitted(record.Offset.Value);
            switch (disposition)
            {
                case KafkaAnalyticsRecordDisposition.Applied:
                    snapshot.Applied++;
                    break;
                case KafkaAnalyticsRecordDisposition.DuplicateMessage:
                    snapshot.DuplicateMessages++;
                    break;
                case KafkaAnalyticsRecordDisposition.DuplicateBusinessEvent:
                    snapshot.DuplicateBusinessEvents++;
                    break;
                case KafkaAnalyticsRecordDisposition.Quarantined:
                    snapshot.Quarantined++;
                    break;
                default:
                    throw new InvalidOperationException("Unhandled Analytics replay disposition.");
            }
            // Only commit-aware progress is persisted in this manifest. After a
            // crash the Kafka group remains authoritative for resumption.
            await ReplaySnapshotFile.SaveAsync(manifestPath, snapshot, cancellationToken);
            lastProgress = DateTimeOffset.UtcNow;
            if (partition.IsComplete)
                consumer.Pause(new[] { record.TopicPartition });
        }
    }

    private async Task CheckReplayDatabaseAsync(
        bool requireEmpty, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AnalyticsDbContext>();
        if (!await db.Database.CanConnectAsync(cancellationToken))
            throw new InvalidOperationException("Isolated replay SQL database is unavailable.");
        var pendingMigrations = await db.Database.GetPendingMigrationsAsync(cancellationToken);
        if (pendingMigrations.Any())
            throw new InvalidOperationException(
                "Apply the existing EF Core migrations to AnalyticsReplayDb before running replay.");
        if (!requireEmpty)
            return;

        if (await db.ProcessedAnalyticsEvents.AnyAsync(cancellationToken) ||
            await db.SessionRegistrationStats.AnyAsync(cancellationToken) ||
            await db.QuarantinedAnalyticsEvents.AnyAsync(cancellationToken))
            throw new InvalidOperationException(
                "A fresh replay requires an EMPTY isolated replay database. " +
                "No records were changed. Never clear the live Analytics database.");
    }

    private ReplaySnapshot CaptureSnapshot(
        IAdminClient admin, IConsumer<string, string> consumer, AnalyticsReplayOptions replay)
    {
        var metadata = admin.GetMetadata(replay.Topic, KafkaTimeout);
        var topic = metadata.Topics.SingleOrDefault(x => x.Topic == replay.Topic);
        if (topic is null || topic.Error.IsError || topic.Partitions.Count == 0 ||
            topic.Partitions.Any(x => x.Error.IsError))
            throw new InvalidOperationException("Kafka replay topic metadata is unavailable or invalid.");

        var windows = new List<ReplayPartitionWindow>();
        foreach (var partition in topic.Partitions.OrderBy(x => x.PartitionId))
        {
            var tp = new TopicPartition(replay.Topic, new Partition(partition.PartitionId));
            var bounds = consumer.QueryWatermarkOffsets(tp, KafkaTimeout);
            windows.Add(new ReplayPartitionWindow(
                partition.PartitionId, bounds.Low.Value, bounds.High.Value));
        }
        var snapshot = new ReplaySnapshot
        {
            BootstrapServers = replay.BootstrapServers,
            Topic = replay.Topic,
            GroupId = replay.GroupId,
            Database = AnalyticsReplayOptions.DatabaseName,
            CapturedAtUtc = DateTimeOffset.UtcNow,
            Partitions = windows
        };
        snapshot.Validate();
        return snapshot;
    }

    private Dictionary<int, long> GetCommittedOffsets(
        IConsumer<string, string> consumer, IReadOnlyCollection<ReplayPartitionWindow> windows)
    {
        var partitions = windows.Select(p =>
            new TopicPartition(settings.Topic, new Partition(p.Partition))).ToList();
        var committed = consumer.Committed(partitions, KafkaTimeout);
        if (committed.Count != windows.Count)
            throw new InvalidOperationException("Kafka returned incomplete committed-offset metadata.");
        return committed.ToDictionary(x => x.Partition.Value, x => x.Offset.Value);
    }

    private void ValidateResumeManifest(ReplaySnapshot snapshot)
    {
        if (snapshot.Completed)
            throw new InvalidOperationException("This replay already completed; choose a new group for a new run.");
        if (!string.Equals(snapshot.BootstrapServers, settings.BootstrapServers, StringComparison.Ordinal) ||
            !string.Equals(snapshot.Topic, settings.Topic, StringComparison.Ordinal) ||
            !string.Equals(snapshot.GroupId, settings.GroupId, StringComparison.Ordinal) ||
            !string.Equals(snapshot.Database, AnalyticsReplayOptions.DatabaseName, StringComparison.Ordinal))
            throw new InvalidOperationException(
                "Resume configuration differs from the original replay snapshot. No offsets were changed.");
        snapshot.Validate();
    }
}

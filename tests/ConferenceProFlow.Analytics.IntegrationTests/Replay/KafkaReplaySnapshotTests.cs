using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Replay;

namespace ConferenceProFlow.Analytics.IntegrationTests.Replay;

public sealed class KafkaReplaySnapshotTests
{
    [Fact]
    public void EmptyPartition_IsAlreadyComplete()
    {
        var window = new ReplayPartitionWindow(partition: 2, low: 7, highExclusive: 7);
        Assert.True(window.IsComplete);
        Assert.Equal(7, window.NextOffset);
    }

    [Fact]
    public void LastCapturedRecord_ReachesExclusiveHighWatermark()
    {
        var window = new ReplayPartitionWindow(partition: 0, low: 12, highExclusive: 14);
        Assert.False(window.IsComplete);
        window.MarkCommitted(12);
        Assert.False(window.IsComplete);
        window.MarkCommitted(13);
        Assert.True(window.IsComplete);
        Assert.Equal(14, window.NextOffset);
    }

    [Theory]
    [InlineData(10)]
    [InlineData(12)]
    [InlineData(14)]
    public void GapOrOutOfRangeOffset_IsRejected(int incomingOffset)
    {
        var window = new ReplayPartitionWindow(partition: 0, low: 11, highExclusive: 14);
        Assert.Throws<InvalidOperationException>(() => window.MarkCommitted(incomingOffset));
        Assert.Equal(11, window.NextOffset);
    }

    [Fact]
    public void InvalidWatermarkRange_IsRejected()
    {
        Assert.Throws<InvalidOperationException>(() =>
            new ReplayPartitionWindow(partition: 0, low: 10, highExclusive: 9));
    }

    [Fact]
    public void Snapshot_CannotClaimCompletionBeforeEveryPartitionFinishes()
    {
        var snapshot = NewSnapshot();
        snapshot.Completed = true;
        Assert.Throws<InvalidOperationException>(snapshot.Validate);
    }

    [Fact]
    public async Task ManifestRoundTrip_PreservesOriginalBoundaries()
    {
        var path = Path.Combine(Path.GetTempPath(), "ConferenceProFlowReplayTest-" +
            Guid.NewGuid().ToString("N") + ".json");
        try
        {
            var source = NewSnapshot();
            await ReplaySnapshotFile.SaveAsync(path, source, TestContext.Current.CancellationToken);
            var restored = await ReplaySnapshotFile.LoadAsync(path, TestContext.Current.CancellationToken);
            Assert.Equal(source.Topic, restored.Topic);
            Assert.Equal(source.GroupId, restored.GroupId);
            Assert.Equal(2, restored.Partitions.Count);
            Assert.Equal(5, restored.Partitions[0].HighExclusive);
            Assert.Equal(0, restored.Partitions[1].HighExclusive);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void ReplayConsumer_DisablesAutomaticOffsetCommits()
    {
        var config = KafkaAnalyticsReplayRunner.CreateConsumerConfig(new AnalyticsReplayOptions
        {
            Enabled = true,
            BootstrapServers = "127.0.0.1:9092",
            Topic = AnalyticsReplayOptions.FinalizedTopic,
            GroupId = AnalyticsReplayOptions.ReplayGroupPrefix + "unit-test"
        });
        Assert.False(config.EnableAutoCommit ?? true);
        Assert.False(config.EnableAutoOffsetStore ?? true);
        Assert.True(config.EnablePartitionEof ?? false);
    }

    [Fact]
    public void ManifestPath_DefaultsToPersistentLocalApplicationData()
    {
        var group = AnalyticsReplayOptions.ReplayGroupPrefix + Guid.NewGuid().ToString("N");
        var local = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        Assert.False(string.IsNullOrWhiteSpace(local));
        var directory = Path.Combine(local, "ConferenceProFlow", "ReplaySnapshots");
        var path = KafkaAnalyticsReplayRunner.GetManifestPath(group);
        Assert.Equal(path, KafkaAnalyticsReplayRunner.GetManifestPath(group));
        Assert.Equal(directory, Path.GetDirectoryName(path));
        Assert.EndsWith(".json", path, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ManifestPath_CustomAbsoluteDirectory_IsUsedWithoutCreatingFiles()
    {
        var directory = Path.Combine(Path.GetTempPath(),
            "ConferenceProFlow-M36E1-" + Guid.NewGuid().ToString("N"));
        var group = AnalyticsReplayOptions.ReplayGroupPrefix + Guid.NewGuid().ToString("N");
        var path = KafkaAnalyticsReplayRunner.GetManifestPath(group, directory);
        Assert.Equal(directory, Path.GetDirectoryName(path));
        Assert.False(Directory.Exists(directory));
    }

    [Theory]
    [InlineData("relative-snapshots")]
    [InlineData("./relative-snapshots")]
    public void ManifestPath_RejectsRelativeOverride(string directory)
    {
        var group = AnalyticsReplayOptions.ReplayGroupPrefix + "unit-test";
        var error = Assert.Throws<InvalidOperationException>(() =>
            KafkaAnalyticsReplayRunner.GetManifestPath(group, directory));
        Assert.True(error.Message.Contains("absolute", StringComparison.OrdinalIgnoreCase));
    }

    private static ReplaySnapshot NewSnapshot() => new()
    {
        BootstrapServers = "127.0.0.1:9092",
        Topic = AnalyticsReplayOptions.FinalizedTopic,
        GroupId = AnalyticsReplayOptions.ReplayGroupPrefix + "unit-test",
        Database = AnalyticsReplayOptions.DatabaseName,
        CapturedAtUtc = DateTimeOffset.UtcNow,
        Partitions =
        [
            new ReplayPartitionWindow(0, 2, 5),
            new ReplayPartitionWindow(1, 0, 0)
        ]
    };
}

using System.Text.Json;

namespace ConferenceProFlow.Analytics.Worker.Replay;

/// <summary>
/// Local-only, durable description of the original Kafka snapshot. The Kafka
/// group's committed offsets, not NextOffset in this file, are authoritative
/// after an interrupted run. This is not a distributed coordination service.
/// </summary>
public sealed class ReplaySnapshot
{
    public int Version { get; set; } = 1;
    public string BootstrapServers { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public string Database { get; set; } = string.Empty;
    public DateTimeOffset CapturedAtUtc { get; set; }
    public bool Completed { get; set; }
    public long Applied { get; set; }
    public long DuplicateMessages { get; set; }
    public long DuplicateBusinessEvents { get; set; }
    public long Quarantined { get; set; }
    public List<ReplayPartitionWindow> Partitions { get; set; } = [];

    public bool AllReached => Partitions.Count > 0 && Partitions.All(x => x.IsComplete);

    public void Validate()
    {
        if (Version != 1 || string.IsNullOrWhiteSpace(Topic) ||
            string.IsNullOrWhiteSpace(GroupId) || string.IsNullOrWhiteSpace(Database) ||
            string.IsNullOrWhiteSpace(BootstrapServers) ||
            Partitions.Count == 0 || Partitions.Select(x => x.Partition).Distinct().Count() != Partitions.Count)
        {
            throw new InvalidOperationException("Replay snapshot manifest has invalid metadata.");
        }

        foreach (var partition in Partitions)
            partition.Validate();
        if (Completed && !AllReached)
            throw new InvalidOperationException("Replay manifest claims completion with unfinished partitions.");
    }
}

/// <summary>
/// A captured [Low, HighExclusive) interval in one topic partition.
/// Consumers must not count or commit any record outside this interval.
/// </summary>
public sealed class ReplayPartitionWindow
{
    public int Partition { get; set; }
    public long Low { get; set; }
    public long HighExclusive { get; set; }
    public long NextOffset { get; set; }
    public bool IsComplete => NextOffset == HighExclusive;

    public ReplayPartitionWindow() { } // System.Text.Json

    public ReplayPartitionWindow(int partition, long low, long highExclusive)
    {
        Partition = partition;
        Low = low;
        HighExclusive = highExclusive;
        NextOffset = low;
        Validate();
    }

    public void Validate()
    {
        if (Partition < 0 || Low < 0 || HighExclusive < Low ||
            NextOffset < Low || NextOffset > HighExclusive)
            throw new InvalidOperationException("Replay partition window has invalid offset boundaries.");
    }

    public void ValidateIncoming(long offset)
    {
        Validate();
        if (IsComplete || offset >= HighExclusive)
            throw new InvalidOperationException("Kafka record is outside the captured replay snapshot.");
        if (offset != NextOffset)
            throw new InvalidOperationException(
                "Kafka offset gap/out-of-order record detected. Replay cannot claim completion.");
    }

    public void MarkCommitted(long recordOffset)
    {
        ValidateIncoming(recordOffset);
        NextOffset = checked(recordOffset + 1);
    }
}

public static class ReplaySnapshotFile
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public static async Task<ReplaySnapshot> LoadAsync(string path, CancellationToken cancellationToken)
    {
        var json = await File.ReadAllTextAsync(path, cancellationToken);
        ReplaySnapshot snapshot;
        try
        {
            snapshot = JsonSerializer.Deserialize<ReplaySnapshot>(json, SerializerOptions)
                ?? throw new InvalidOperationException("Replay snapshot manifest is empty.");
        }
        catch (JsonException)
        {
            throw new InvalidOperationException("Replay snapshot manifest is malformed.");
        }
        snapshot.Validate();
        return snapshot;
    }

    public static async Task SaveAsync(
        string path, ReplaySnapshot snapshot, CancellationToken cancellationToken)
    {
        snapshot.Validate();
        var directory = Path.GetDirectoryName(path)
            ?? throw new InvalidOperationException("Replay snapshot path has no directory.");
        Directory.CreateDirectory(directory);
        // Same-volume atomic replace. Interrupted writes cannot produce a
        // half-written target file (the temporary file may remain on disk).
        var temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            await File.WriteAllTextAsync(temporary,
                JsonSerializer.Serialize(snapshot, SerializerOptions), cancellationToken);
            File.Move(temporary, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temporary))
                File.Delete(temporary);
        }
    }
}

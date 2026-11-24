using Microsoft.Data.SqlClient;

namespace ConferenceProFlow.Analytics.Worker.Configuration;

/// <summary>
/// Fail-closed preflight for an isolated, local replay. Invoked by both the
/// runtime entry point and EF Core design-time factory. Never logs credentials.
/// This is configuration protection, not a substitute for SQL permissions.
/// </summary>
public static class AnalyticsReplaySafety
{
    public static void ValidateOrThrow(
        AnalyticsReplayOptions replay,
        string? liveConnectionString,
        string? replayConnectionString,
        bool liveConsumerEnabled)
    {
        ArgumentNullException.ThrowIfNull(replay);

        if (!replay.Enabled)
            throw new InvalidOperationException("Replay must be explicitly enabled (AnalyticsReplay:Enabled=true).");

        if (liveConsumerEnabled)
            throw new InvalidOperationException(
                "Replay configuration cannot be used while AnalyticsKafkaConsumer:Enabled=true. " +
                "Disable the ordinary consumer for this worker process first.");

        if (string.IsNullOrWhiteSpace(replay.BootstrapServers))
            throw new InvalidOperationException("AnalyticsReplay:BootstrapServers is required.");

        if (!string.Equals(replay.Topic, AnalyticsReplayOptions.FinalizedTopic, StringComparison.Ordinal))
            throw new InvalidOperationException("Replay must use the supported registration-finalized topic.");

        if (string.IsNullOrWhiteSpace(replay.GroupId) ||
            !replay.GroupId.StartsWith(AnalyticsReplayOptions.ReplayGroupPrefix, StringComparison.Ordinal) ||
            !IsValidGroupSuffix(replay.GroupId[AnalyticsReplayOptions.ReplayGroupPrefix.Length..]))
        {
            throw new InvalidOperationException(
                "AnalyticsReplay:GroupId must use a distinct 'conferenceproflow.analytics.replay.<run-id>' group.");
        }

        var live = ParseConnection(liveConnectionString, "ConnectionStrings:AnalyticsDb");
        var target = ParseConnection(replayConnectionString, "ConnectionStrings:AnalyticsReplayDb");

        if (!string.Equals(live.InitialCatalog, AnalyticsReplayOptions.LiveDatabaseName, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                "ConnectionStrings:AnalyticsDb must identify the expected live Analytics database before replay setup.");

        if (!string.Equals(target.InitialCatalog, AnalyticsReplayOptions.DatabaseName, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                "Replay SQL target must be the separate ConferenceProFlowAnalyticsReplay database.");

        if (!string.IsNullOrWhiteSpace(target.AttachDBFilename))
            throw new InvalidOperationException("Replay connections using AttachDbFilename are not supported.");

        // M3.5 is a local demonstration on the SAME existing SQL Server.
        // Require identical server configuration rather than guessing whether
        // different aliases (e.g., localhost vs 127.0.0.1) refer to one server.
        if (!string.Equals(live.DataSource, target.DataSource, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                "Replay and live database connections must specify the same SQL Server Data Source.");
    }

    private static bool IsValidGroupSuffix(string suffix)
    {
        if (suffix.Length is < 1 or > 80 || !char.IsAsciiLetterOrDigit(suffix[0]))
            return false;

        return suffix.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.');
    }

    private static SqlConnectionStringBuilder ParseConnection(string? connection, string settingName)
    {
        if (string.IsNullOrWhiteSpace(connection))
            throw new InvalidOperationException($"{settingName} must be configured in User Secrets.");

        try
        {
            var builder = new SqlConnectionStringBuilder(connection);
            if (string.IsNullOrWhiteSpace(builder.DataSource) ||
                string.IsNullOrWhiteSpace(builder.InitialCatalog))
            {
                throw new InvalidOperationException($"{settingName} must specify Server and Database.");
            }

            return builder;
        }
        catch (ArgumentException)
        {
            // Intentionally do not include the connection string or exception
            // details, since either can contain local credentials.
            throw new InvalidOperationException($"{settingName} is not a valid SQL Server connection string.");
        }
    }
}

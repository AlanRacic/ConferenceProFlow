using ConferenceProFlow.Analytics.Worker.Configuration;

namespace ConferenceProFlow.Analytics.IntegrationTests.Replay;

public sealed class AnalyticsReplaySafetyTests
{
    // Fake credentials in isolated configuration-only unit-style tests.
    private const string LiveSql =
        "Server=127.0.0.1,14333;Database=ConferenceProFlowAnalytics;User Id=sa;" +
        "Password=Example-Not-A-Real-Password;Encrypt=True;TrustServerCertificate=True;";

    private const string ReplaySql =
        "Server=127.0.0.1,14333;Database=ConferenceProFlowAnalyticsReplay;User Id=sa;" +
        "Password=Example-Not-A-Real-Password;Encrypt=True;TrustServerCertificate=True;";

    private static AnalyticsReplayOptions ValidOptions() => new()
    {
        Enabled = true,
        BootstrapServers = "127.0.0.1:9092",
        Topic = AnalyticsReplayOptions.FinalizedTopic,
        GroupId = "conferenceproflow.analytics.replay.demo01"
    };

    [Fact]
    public void SeparateReplayDatabaseAndGroup_PassPreflight()
    {
        AnalyticsReplaySafety.ValidateOrThrow(ValidOptions(), LiveSql, ReplaySql, liveConsumerEnabled: false);
    }

    [Fact]
    public void ReplayMustBeExplicitlyEnabled()
    {
        var options = ValidOptions();
        options.Enabled = false;
        Assert.Throws<InvalidOperationException>(() =>
            AnalyticsReplaySafety.ValidateOrThrow(options, LiveSql, ReplaySql, false));
    }

    [Theory]
    [InlineData("conferenceproflow.analytics.v1")]
    [InlineData("conferenceproflow.analytics.preview.v1")]
    [InlineData("conferenceproflow.analytics.replay.")]
    [InlineData("conferenceproflow.analytics.replay. bad")]
    [InlineData("")]
    public void LivePreviewOrInvalidGroup_IsRejected(string groupId)
    {
        var options = ValidOptions();
        options.GroupId = groupId;
        Assert.Throws<InvalidOperationException>(() =>
            AnalyticsReplaySafety.ValidateOrThrow(options, LiveSql, ReplaySql, false));
    }

    [Fact]
    public void UnexpectedTopic_IsRejected()
    {
        var options = ValidOptions();
        options.Topic = "conferenceproflow.kafka-smoke.v1";
        Assert.Throws<InvalidOperationException>(() =>
            AnalyticsReplaySafety.ValidateOrThrow(options, LiveSql, ReplaySql, false));
    }

    [Fact]
    public void LiveDatabaseAsReplayTarget_IsRejected()
    {
        Assert.Throws<InvalidOperationException>(() =>
            AnalyticsReplaySafety.ValidateOrThrow(ValidOptions(), LiveSql, LiveSql, false));
    }

    [Fact]
    public void WrongReplayDatabaseName_IsRejected()
    {
        var wrong = ReplaySql.Replace(
            "ConferenceProFlowAnalyticsReplay", "ConferenceProFlowAnalyticsWrong", StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(() =>
            AnalyticsReplaySafety.ValidateOrThrow(ValidOptions(), LiveSql, wrong, false));
    }

    [Fact]
    public void DifferentSqlServer_IsRejected()
    {
        var wrong = ReplaySql.Replace("127.0.0.1,14333", "127.0.0.1,15000", StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(() =>
            AnalyticsReplaySafety.ValidateOrThrow(ValidOptions(), LiveSql, wrong, false));
    }

    [Fact]
    public void OrdinaryKafkaConsumerMustBeDisabledDuringReplaySetup()
    {
        Assert.Throws<InvalidOperationException>(() =>
            AnalyticsReplaySafety.ValidateOrThrow(ValidOptions(), LiveSql, ReplaySql, true));
    }

    [Fact]
    public void MissingReplayConnection_IsRejectedWithoutCredentialsInError()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            AnalyticsReplaySafety.ValidateOrThrow(ValidOptions(), LiveSql, null, false));
        Assert.DoesNotContain("Example-Not-A-Real-Password", exception.Message);
    }

    [Fact]
    public void MalformedReplayConnection_IsRejectedWithoutEchoingInput()
    {
        const string invalid = "not-a-valid-key=value;Password=Example-Not-A-Real-Password";
        var exception = Assert.Throws<InvalidOperationException>(() =>
            AnalyticsReplaySafety.ValidateOrThrow(ValidOptions(), LiveSql, invalid, false));
        Assert.DoesNotContain("Example-Not-A-Real-Password", exception.Message);
    }
}

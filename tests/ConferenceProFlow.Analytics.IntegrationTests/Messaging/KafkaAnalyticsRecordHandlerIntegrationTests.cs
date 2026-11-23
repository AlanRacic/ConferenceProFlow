using System.Text.Json;
using Confluent.Kafka;
using ConferenceProFlow.Analytics.IntegrationTests.Persistence;
using ConferenceProFlow.Analytics.Worker.Application.Processing;
using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Domain.Entities;
using ConferenceProFlow.Analytics.Worker.Messaging.Consumers;
using ConferenceProFlow.Analytics.Worker.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Analytics.IntegrationTests.Messaging;

[CollectionDefinition("AnalyticsM34ESerial", DisableParallelization = true)]
public sealed class AnalyticsM34ESerialCollection { }

/// <summary>
/// Real SQL Server validation of record classification and durable quarantine.
/// No Kafka broker is required for these tests; transport is tested separately.
/// </summary>
[Collection("AnalyticsM34ESerial")]
public sealed class KafkaAnalyticsRecordHandlerIntegrationTests(AnalyticsSqlFixture fixture)
    : IClassFixture<AnalyticsSqlFixture>
{
    private static readonly string TestTopic = "conferenceproflow.registration-finalized.v1";

    private static AnalyticsRegistrationFinalizedV1 NewEvent(Guid? sessionId = null) =>
        new(Guid.NewGuid(), Guid.NewGuid(), sessionId ?? Guid.NewGuid(),
            AnalyticsRegistrationFinalizedV1.ConfirmedStatus, DateTimeOffset.UtcNow);

    private static ConsumeResult<string, string> Record(
        string? payload, string? key, long offset = 0) => new()
    {
        Topic = TestTopic,
        Partition = new Partition(0),
        Offset = new Offset(offset),
        Message = new Message<string, string> { Key = key!, Value = payload! }
    };

    private async Task<KafkaAnalyticsRecordDisposition> HandleAsync(
        ConsumeResult<string, string> record, CancellationToken ct)
    {
        await using var db = fixture.CreateDbContext();
        var handler = new KafkaAnalyticsRecordHandler(
            new AnalyticsEventProcessingService(db), new AnalyticsQuarantineService(db));
        return await handler.HandleAsync(record, ct);
    }

    [Fact]
    public void DurableConfig_RequiresDistinctGroupAndDisablesAutoCommit()
    {
        var options = new AnalyticsKafkaConsumerOptions
        {
            Enabled = true,
            ObserveOnly = false,
            GroupId = "conferenceproflow.analytics.v1"
        };
        Assert.True(options.IsValidForDurable());

        var config = KafkaAnalyticsConsumerService.CreateConsumerConfig(options);
        Assert.False(config.EnableAutoCommit ?? true);
        Assert.False(config.EnableAutoOffsetStore ?? true);
        Assert.False(config.AllowAutoCreateTopics ?? true);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);

        options.GroupId = AnalyticsKafkaConsumerOptions.PreviewGroupId;
        Assert.False(options.IsValidForDurable());
        options.GroupId = "conferenceproflow.analytics.v1";
        options.ObserveOnly = true;
        Assert.False(options.IsValidForDurable());
    }

    [Fact]
    public async Task ValidEventAndExactReplay_IncrementCounterOnce()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent();
        var record = Record(JsonSerializer.Serialize(evt), evt.SessionId.ToString("D"));

        Assert.Equal(KafkaAnalyticsRecordDisposition.Applied, await HandleAsync(record, ct));
        Assert.Equal(KafkaAnalyticsRecordDisposition.DuplicateMessage, await HandleAsync(record, ct));

        await using var verify = fixture.CreateDbContext();
        Assert.Equal(1, await verify.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == evt.MessageId, ct));
        Assert.Equal(1, (await verify.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct)).ConfirmedCount);
        Assert.Equal(0, await verify.QuarantinedAnalyticsEvents.CountAsync(x => x.Offset == 0 && x.Topic == TestTopic, ct));
    }

    [Theory]
    [InlineData("{broken-json")]
    [InlineData("null")]
    [InlineData("{\"status\":\"confirmed\"}")]
    public async Task MalformedOrIncompleteJson_IsQuarantinedOnlyOnceAtKafkaPosition(string payload)
    {
        var ct = TestContext.Current.CancellationToken;
        var offset = Random.Shared.NextInt64(10_000, 1_000_000_000);
        var record = Record(payload, "irrelevant-key", offset);

        Assert.Equal(KafkaAnalyticsRecordDisposition.Quarantined, await HandleAsync(record, ct));
        Assert.Equal(KafkaAnalyticsRecordDisposition.Quarantined, await HandleAsync(record, ct));

        await using var verify = fixture.CreateDbContext();
        Assert.Equal(1, await verify.QuarantinedAnalyticsEvents.CountAsync(
            x => x.Topic == TestTopic && x.Partition == 0 && x.Offset == offset, ct));
    }

    [Fact]
    public async Task WrongKafkaKey_IsQuarantinedWithoutBusinessWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent();
        var offset = Random.Shared.NextInt64(10_000, 1_000_000_000);
        var record = Record(JsonSerializer.Serialize(evt), Guid.NewGuid().ToString("D"), offset);

        Assert.Equal(KafkaAnalyticsRecordDisposition.Quarantined, await HandleAsync(record, ct));

        await using var verify = fixture.CreateDbContext();
        Assert.Equal(0, await verify.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == evt.MessageId, ct));
        Assert.Null(await verify.SessionRegistrationStats.SingleOrDefaultAsync(x => x.SessionId == evt.SessionId, ct));
        var row = await verify.QuarantinedAnalyticsEvents.SingleAsync(x => x.Offset == offset && x.Topic == TestTopic, ct);
        Assert.True(row.Reason.Contains("key", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task ConflictingFinalization_IsQuarantinedButOriginalProjectionIsUntouched()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent();
        var conflicting = evt with
        {
            MessageId = Guid.NewGuid(),
            Status = AnalyticsRegistrationFinalizedV1.RejectedStatus
        };
        var offset = Random.Shared.NextInt64(10_000, 1_000_000_000);

        Assert.Equal(KafkaAnalyticsRecordDisposition.Applied,
            await HandleAsync(Record(JsonSerializer.Serialize(evt), evt.SessionId.ToString("D"), offset + 1), ct));
        Assert.Equal(KafkaAnalyticsRecordDisposition.Quarantined,
            await HandleAsync(Record(JsonSerializer.Serialize(conflicting), evt.SessionId.ToString("D"), offset), ct));

        await using var verify = fixture.CreateDbContext();
        var stats = await verify.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct);
        Assert.Equal(1, stats.ConfirmedCount);
        Assert.Equal(0, stats.RejectedCount);
        Assert.Equal(1, await verify.ProcessedAnalyticsEvents.CountAsync(x => x.RegistrationId == evt.RegistrationId, ct));
        Assert.Equal(1, await verify.QuarantinedAnalyticsEvents.CountAsync(x => x.Offset == offset && x.Topic == TestTopic, ct));
    }

    [Fact]
    public async Task SqlProjectionFailure_IsRetriableAndIsNotMisclassifiedAsQuarantine()
    {
        var ct = TestContext.Current.CancellationToken;
        var evt = NewEvent();
        var offset = Random.Shared.NextInt64(10_000, 1_000_000_000);
        await using (var seed = fixture.CreateDbContext())
        {
            seed.SessionRegistrationStats.Add(new SessionRegistrationStat
            {
                SessionId = evt.SessionId,
                ConfirmedCount = long.MaxValue,
                RejectedCount = 0,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            });
            await seed.SaveChangesAsync(ct);
        }

        await Assert.ThrowsAnyAsync<Exception>(() => HandleAsync(
            Record(JsonSerializer.Serialize(evt), evt.SessionId.ToString("D"), offset), ct));

        await using var verify = fixture.CreateDbContext();
        Assert.Equal(0, await verify.ProcessedAnalyticsEvents.CountAsync(x => x.MessageId == evt.MessageId, ct));
        Assert.Equal(0, await verify.QuarantinedAnalyticsEvents.CountAsync(x => x.Offset == offset && x.Topic == TestTopic, ct));
        Assert.Equal(long.MaxValue,
            (await verify.SessionRegistrationStats.SingleAsync(x => x.SessionId == evt.SessionId, ct)).ConfirmedCount);
    }

    [Fact]
    public async Task InvalidJsonLargePayload_IsSafelyBoundedInQuarantine()
    {
        var ct = TestContext.Current.CancellationToken;
        var offset = Random.Shared.NextInt64(10_000, 1_000_000_000);
        var record = Record(new string('x', 80_000), new string('k', 400), offset);

        Assert.Equal(KafkaAnalyticsRecordDisposition.Quarantined, await HandleAsync(record, ct));

        await using var verify = fixture.CreateDbContext();
        var quarantined = await verify.QuarantinedAnalyticsEvents.SingleAsync(
            x => x.Topic == TestTopic && x.Offset == offset, ct);
        Assert.Equal(4096, quarantined.Payload.Length);
        Assert.Equal(256, quarantined.MessageKey!.Length);
        Assert.True(quarantined.Reason.Length <= 500);
    }
}

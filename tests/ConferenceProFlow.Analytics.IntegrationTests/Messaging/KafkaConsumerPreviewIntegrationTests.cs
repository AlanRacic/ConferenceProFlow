using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Messaging.Consumers;
using Testcontainers.Kafka;

namespace ConferenceProFlow.Analytics.IntegrationTests.Messaging;

/// <summary>
/// M3.4C transport checks; no SQL writes or Kafka offset commits.
/// This deliberately does not claim event processing is implemented.
/// </summary>
public sealed class KafkaConsumerPreviewIntegrationTests
{
    [Fact]
    public void PreviewConfiguration_DisablesAutomaticOffsetCommitAndStorage()
    {
        var options = new AnalyticsKafkaConsumerOptions { Enabled = true };

        var config = KafkaAnalyticsConsumerPreviewService.CreateConsumerConfig(options);

        Assert.False(config.EnableAutoCommit ?? true);
        Assert.False(config.EnableAutoOffsetStore ?? true);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.False(config.AllowAutoCreateTopics ?? true);
        Assert.False(new AnalyticsKafkaConsumerOptions
        {
            Enabled = true,
            ObserveOnly = false
        }.IsValid());
    }

    [Fact]
    public async Task WithoutOffsetCommit_SecondConsumerCanReadTheSameRecord()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var kafka = new KafkaBuilder("apache/kafka:4.4.0").Build();
        await kafka.StartAsync(cancellationToken);

        var bootstrap = kafka.GetBootstrapAddress();
        var topic = $"conferenceproflow.m34c.{Guid.NewGuid():N}";
        var group = $"conferenceproflow.m34c.preview.{Guid.NewGuid():N}";

        using (var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrap
        }).Build())
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }
            });
        }

        using (var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.All
        }).Build())
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = "session-001",
                Value = "{\"status\":\"confirmed\"}"
            }, cancellationToken);
        }

        var options = new AnalyticsKafkaConsumerOptions
        {
            Enabled = true,
            BootstrapServers = bootstrap,
            Topic = topic,
            GroupId = group
        };
        var config = KafkaAnalyticsConsumerPreviewService.CreateConsumerConfig(options);

        var first = ReadOneWithoutCommitting(config, topic);
        var replayed = ReadOneWithoutCommitting(config, topic);

        Assert.Equal(first.Topic, replayed.Topic);
        Assert.Equal(first.Partition, replayed.Partition);
        Assert.Equal(first.Offset, replayed.Offset);
        Assert.Equal("session-001", replayed.Message.Key);
        Assert.Equal("{\"status\":\"confirmed\"}", replayed.Message.Value);
    }

    private static ConsumeResult<string, string> ReadOneWithoutCommitting(
        ConsumerConfig config, string topic)
    {
        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(35);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var record = consumer.Consume(TimeSpan.FromSeconds(1));
            if (record?.Message is not null)
            {
                consumer.Close();
                // No Commit() call. The next consumer re-reads the record.
                return record;
            }
        }

        consumer.Close();
        throw new TimeoutException("Kafka did not deliver the test record within 35 seconds.");
    }
}

using System.Text;
using Confluent.Kafka;
using ConferenceProFlow.Analytics.Worker.Configuration;
using Microsoft.Extensions.Options;

namespace ConferenceProFlow.Analytics.Worker.Messaging.Consumers;

/// <summary>
/// M3.4C: safe, read-only Kafka transport preview. It deliberately does NOT
/// persist events, update statistics, quarantine records or commit offsets.
/// M3.4D/E will connect durable SQL handling BEFORE enabling commits.
/// </summary>
public sealed class KafkaAnalyticsConsumerPreviewService(
    IOptions<AnalyticsKafkaConsumerOptions> options,
    ILogger<KafkaAnalyticsConsumerPreviewService> logger) : BackgroundService
{
    public static ConsumerConfig CreateConsumerConfig(AnalyticsKafkaConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (!options.Enabled || !options.IsValid())
            throw new ArgumentException(
                "The Kafka preview consumer requires Enabled=true, ObserveOnly=true, and valid Kafka settings.",
                nameof(options));

        return new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Consumer.Consume is synchronous. Yield immediately so that host
        // startup and other background services are not blocked by polling.
        await Task.Yield();

        var config = CreateConsumerConfig(options.Value);
        logger.LogWarning(
            "M3.4C PREVIEW ONLY: reading Kafka topic {Topic} with group {GroupId}. " +
            "No events are persisted, no statistics are updated and NO offsets are committed.",
            options.Value.Topic, options.Value.GroupId);

        while (!stoppingToken.IsCancellationRequested)
        {
            IConsumer<string, string>? consumer = null;
            try
            {
                consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe(options.Value.Topic);

                while (!stoppingToken.IsCancellationRequested)
                {
                    var record = consumer.Consume(stoppingToken);
                    if (record?.Message is null)
                        continue;

                    // Do not log the full Kafka payload or its headers. The
                    // preview logs metadata only, not personal information.
                    var payloadBytes = record.Message.Value is null
                        ? 0
                        : Encoding.UTF8.GetByteCount(record.Message.Value);

                    logger.LogInformation(
                        "Kafka preview record: topic={Topic}, partition={Partition}, " +
                        "offset={Offset}, key={Key}, payloadBytes={PayloadBytes}. No offset commit.",
                        record.Topic, record.Partition.Value, record.Offset.Value,
                        record.Message.Key ?? "(null)", payloadBytes);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (KafkaException ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning(ex,
                    "Kafka preview consumer encountered a broker error; recreating the consumer.");
            }
            finally
            {
                if (consumer is not null)
                {
                    try
                    {
                        consumer.Close();
                    }
                    catch (KafkaException ex)
                    {
                        logger.LogDebug(ex, "Kafka preview consumer close failed.");
                    }

                    consumer.Dispose();
                }
            }

            if (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        logger.LogInformation("Kafka preview consumer stopped.");
    }
}

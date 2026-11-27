using System.Diagnostics;
using Confluent.Kafka;
using ConferenceProFlow.Analytics.Worker.Application.Processing;
using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Observability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ConferenceProFlow.Analytics.Worker.Messaging.Consumers;

/// <summary>
/// Single-active-instance baseline. Each consumed record is handled durably
/// in Analytics SQL before its specific Kafka offset is explicitly committed.
/// On ANY SQL/commit failure the consumer is closed and recreated, replaying
/// from the last broker-committed offset after a bounded backoff.
/// </summary>
public sealed class KafkaAnalyticsConsumerService(
    IOptions<AnalyticsKafkaConsumerOptions> options,
    IServiceScopeFactory scopeFactory,
    ILogger<KafkaAnalyticsConsumerService> logger) : BackgroundService
{
    private static readonly TimeSpan RestartDelay = TimeSpan.FromSeconds(3);

    public static ConsumerConfig CreateConsumerConfig(AnalyticsKafkaConsumerOptions settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        if (!settings.IsValidForDurable())
            throw new ArgumentException(
                "Durable Kafka consumer requires Enabled=true, ObserveOnly=false and " +
                "a valid non-preview GroupId.", nameof(settings));

        return new ConsumerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            GroupId = settings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Consume() is synchronous; release host startup before beginning polls.
        await Task.Yield();
        var settings = options.Value;
        var config = CreateConsumerConfig(settings);

        logger.LogInformation(
            "M3.4E durable Analytics consumer starting: topic={Topic}, group={Group}. " +
            "SQL-first processing; explicit Kafka offset commits after durable handling.",
            settings.Topic, settings.GroupId);

        while (!stoppingToken.IsCancellationRequested)
        {
            IConsumer<string, string>? consumer = null;
            try
            {
                consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe(settings.Topic);

                while (!stoppingToken.IsCancellationRequested)
                {
                    var record = consumer.Consume(stoppingToken);
                    if (record?.Message is null)
                        continue; // Partition EOF / no user record.

                    // Each record gets its own span with a bounded lifetime.
                    // M4.2B previously used a using declaration directly in this
                    // loop; that could keep a span alive until the loop exited.
                    using (var activity = KafkaAnalyticsConsumerTelemetry
                        .StartProcessingActivity(record.Message.Headers))
                    {
                        activity?.SetTag("messaging.destination.name", record.Topic);

                        try
                        {
                            // Each record gets a fresh EF Core DbContext and scoped
                            // services. SQL errors propagate rather than being treated
                            // as malformed Kafka payloads.
                            KafkaAnalyticsRecordDisposition disposition;
                            await using (var scope = scopeFactory.CreateAsyncScope())
                            {
                                var handler = scope.ServiceProvider
                                    .GetRequiredService<KafkaAnalyticsRecordHandler>();
                                disposition = await handler.HandleAsync(record, stoppingToken);
                            }

                            // No code path reaches here without a committed SQL
                            // processing transaction, a recognized durable duplicate,
                            // or a committed quarantine insert.
                            // Commit(record) acknowledges the NEXT offset in its partition.
                            consumer.Commit(record);

                            activity?.SetTag(
                                "conferenceproflow.analytics.disposition",
                                disposition.ToString());

                            logger.LogInformation(
                                "Analytics Kafka record handled: topic={Topic}, partition={Partition}, " +
                                "offset={Offset}, disposition={Disposition}; offset committed.",
                                record.Topic, record.Partition.Value, record.Offset.Value, disposition);
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            // Mark telemetry, but preserve existing SQL-first, fail-fast
                            // semantics: rethrow so the consumer session is recreated.
                            activity?.SetStatus(ActivityStatusCode.Error, ex.GetType().Name);
                            throw;
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                // Includes SQL failures, broker errors and offset commit errors.
                // Never Consume() another record on the failed consumer: doing
                // so could advance past an unhandled message in its partition.
                logger.LogError(ex,
                    "Analytics Kafka consumption/SQL handling/offset commit failed. " +
                    "Consumer will restart and replay from last committed offset.");
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
                        logger.LogDebug(ex, "Kafka consumer close failed.");
                    }
                    consumer.Dispose();
                }
            }

            if (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(RestartDelay, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        logger.LogInformation("Durable Analytics Kafka consumer stopped.");
    }
}

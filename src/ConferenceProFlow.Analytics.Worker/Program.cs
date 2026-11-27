using ConferenceProFlow.Analytics.Worker;
using ConferenceProFlow.Analytics.Worker.Application.Processing;
using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Analytics.Worker.Messaging.Consumers;
using ConferenceProFlow.Analytics.Worker.Replay;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using ConferenceProFlow.Analytics.Worker.Observability;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = Host.CreateApplicationBuilder(args);

// M4.2B - OpenTelemetry tracing foundation
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService("conferenceproflow-analytics-worker"))
    .WithTracing(tracing =>
    {
        tracing.AddSource(AnalyticsTelemetry.SourceName);

        if (builder.Environment.IsDevelopment() &&
            builder.Configuration.GetValue<bool>("Observability:ConsoleExporter:Enabled"))
        {
            tracing.AddConsoleExporter();
        }
    });

var replayEnabled = builder.Configuration.GetValue<bool>("AnalyticsReplay:Enabled");
var liveConsumerEnabled = builder.Configuration.GetValue<bool>("AnalyticsKafkaConsumer:Enabled");
var liveConnection = builder.Configuration.GetConnectionString("AnalyticsDb");
string selectedConnection;

if (replayEnabled)
{
    var replay = builder.Configuration.GetSection(AnalyticsReplayOptions.SectionName)
        .Get<AnalyticsReplayOptions>() ?? new AnalyticsReplayOptions();
    var replayConnection = builder.Configuration.GetConnectionString("AnalyticsReplayDb");

    // Must happen before DbContext/consumer creation. No live SQL writes or
    // live-group offset resets are possible from the replay path.
    AnalyticsReplaySafety.ValidateOrThrow(
        replay, liveConnection, replayConnection, liveConsumerEnabled);
    selectedConnection = replayConnection!;
    builder.Services.AddSingleton(replay);
    builder.Services.AddSingleton<KafkaAnalyticsReplayRunner>();
}
else
{
    // M3.4 live behavior is preserved exactly when replay is disabled.
    selectedConnection = liveConnection ?? throw new InvalidOperationException(
        "Missing ConnectionStrings:AnalyticsDb. Configure Analytics Worker User Secrets.");
    if (string.IsNullOrWhiteSpace(selectedConnection))
        throw new InvalidOperationException("ConnectionStrings:AnalyticsDb is empty.");
}

builder.Services.AddDbContext<AnalyticsDbContext>(options =>
    options.UseSqlServer(selectedConnection));
builder.Services.AddScoped<AnalyticsEventProcessingService>();
builder.Services.AddScoped<AnalyticsQuarantineService>();
builder.Services.AddScoped<KafkaAnalyticsRecordHandler>();

if (!replayEnabled)
{
    builder.Services
        .AddOptions<AnalyticsKafkaConsumerOptions>()
        .BindConfiguration(AnalyticsKafkaConsumerOptions.SectionName)
        .Validate(options => !options.Enabled ||
                (options.ObserveOnly ? options.IsValid() : options.IsValidForDurable()),
            "Invalid AnalyticsKafkaConsumer configuration. Preview requires ObserveOnly=true; " +
            "durable processing requires ObserveOnly=false and a non-preview GroupId. " +
            "BootstrapServers, Topic and GroupId must be nonempty.")
        .ValidateOnStart();

    if (!liveConsumerEnabled)
        builder.Services.AddHostedService<AnalyticsWorkerBackgroundService>();
    else if (builder.Configuration.GetValue<bool>("AnalyticsKafkaConsumer:ObserveOnly", true))
        builder.Services.AddHostedService<KafkaAnalyticsConsumerPreviewService>();
    else
        builder.Services.AddHostedService<KafkaAnalyticsConsumerService>();
}

ConferenceProFlow.Analytics.Worker.Observability.JaegerOtlpExporterRegistration.Configure(
    builder.Services, builder.Configuration, builder.Environment);

using var host = builder.Build();
if (replayEnabled)
{
    // One-shot command, not an indefinite hosted service. On any failure the
    // process exits NON-ZERO; it cannot look like a successful replay.
    using var cancellation = new CancellationTokenSource();
    ConsoleCancelEventHandler cancel = (_, e) =>
    {
        e.Cancel = true;
        cancellation.Cancel();
    };
    Console.CancelKeyPress += cancel;
    try
    {
        await host.Services.GetRequiredService<KafkaAnalyticsReplayRunner>()
            .RunAsync(cancellation.Token);
    }
    finally
    {
        Console.CancelKeyPress -= cancel;
    }
}
else
{
    await host.RunAsync();
}

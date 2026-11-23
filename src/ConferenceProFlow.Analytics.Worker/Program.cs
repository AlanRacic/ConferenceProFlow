using ConferenceProFlow.Analytics.Worker;
using ConferenceProFlow.Analytics.Worker.Application.Processing;
using ConferenceProFlow.Analytics.Worker.Configuration;
using ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Analytics.Worker.Messaging.Consumers;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

// Analytics owns this database exclusively; never query Registration/Allocation.
var analyticsConnection = builder.Configuration.GetConnectionString("AnalyticsDb");
if (string.IsNullOrWhiteSpace(analyticsConnection))
{
    throw new InvalidOperationException(
        "Missing ConnectionStrings:AnalyticsDb. Configure Analytics Worker User Secrets.");
}

builder.Services.AddDbContext<AnalyticsDbContext>(options =>
    options.UseSqlServer(analyticsConnection));

builder.Services.AddScoped<AnalyticsEventProcessingService>();
builder.Services.AddScoped<AnalyticsQuarantineService>();
builder.Services.AddScoped<KafkaAnalyticsRecordHandler>();

builder.Services
    .AddOptions<AnalyticsKafkaConsumerOptions>()
    .BindConfiguration(AnalyticsKafkaConsumerOptions.SectionName)
    .Validate(options => !options.Enabled ||
            (options.ObserveOnly ? options.IsValid() : options.IsValidForDurable()),
        "Invalid AnalyticsKafkaConsumer configuration. Preview requires ObserveOnly=true; " +
        "durable processing requires ObserveOnly=false and a non-preview GroupId. " +
        "BootstrapServers, Topic and GroupId must be nonempty.")
    .ValidateOnStart();

if (!builder.Configuration.GetValue<bool>("AnalyticsKafkaConsumer:Enabled"))
{
    builder.Services.AddHostedService<AnalyticsWorkerBackgroundService>();
}
else if (builder.Configuration.GetValue<bool>("AnalyticsKafkaConsumer:ObserveOnly", true))
{
    // Existing M3.4C preview deliberately does NOT write to SQL or commit offsets.
    builder.Services.AddHostedService<KafkaAnalyticsConsumerPreviewService>();
}
else
{
    // M3.4E: one record at a time, SQL-first, then explicit Kafka offset commit.
    builder.Services.AddHostedService<KafkaAnalyticsConsumerService>();
}

await builder.Build().RunAsync();

using ConferenceProFlow.Allocation.Worker;
using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Messaging.Consumers;
using ConferenceProFlow.Allocation.Worker.Messaging.Inbox;
using ConferenceProFlow.Allocation.Worker.Messaging.Outbox;
using ConferenceProFlow.Allocation.Worker.Processing;
using Microsoft.EntityFrameworkCore;
using ConferenceProFlow.Allocation.Worker.Observability;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = Host.CreateApplicationBuilder(args);

// M4.2B - OpenTelemetry tracing foundation
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService("conferenceproflow-allocation-worker"))
    .WithTracing(tracing =>
    {
        tracing.AddSource(AllocationTelemetry.SourceName);

        if (builder.Environment.IsDevelopment() &&
            builder.Configuration.GetValue<bool>("Observability:ConsoleExporter:Enabled"))
        {
            tracing.AddConsoleExporter();
        }
    });

var connectionString = builder.Configuration.GetConnectionString("AllocationDb")
    ?? throw new InvalidOperationException("Connection string 'AllocationDb' is not configured.");

builder.Services.AddDbContext<AllocationDbContext>(options => options.UseSqlServer(connectionString));

builder.Services.AddScoped<InboxReceiveService>();
builder.Services.AddScoped<AllocationProcessingService>();

builder.Services.Configure<AllocationInboxProcessorOptions>(builder.Configuration.GetSection(AllocationInboxProcessorOptions.SectionName));

var rabbitMqConsumerEnabled = builder.Configuration.GetValue<bool>("RabbitMqConsumer:Enabled");
var inboxProcessorEnabled = builder.Configuration.GetValue<bool>("AllocationInboxProcessor:Enabled");
var allocationOutboxPublisherEnabled = builder.Configuration.GetValue<bool>("AllocationOutboxPublisher:Enabled");

if (rabbitMqConsumerEnabled)
    builder.Services.AddHostedService<RegistrationRequestConsumerService>();

if (inboxProcessorEnabled)
    builder.Services.AddHostedService<AllocationInboxProcessorService>();

if (allocationOutboxPublisherEnabled)
    builder.Services.AddHostedService<AllocationOutboxPublisherService>();

if (!rabbitMqConsumerEnabled &&
    !inboxProcessorEnabled &&
    !allocationOutboxPublisherEnabled)
{
    builder.Services.AddHostedService<AllocationBackgroundService>();
}

ConferenceProFlow.Allocation.Worker.Observability.JaegerOtlpExporterRegistration.Configure(
    builder.Services, builder.Configuration, builder.Environment);

await builder.Build().RunAsync();
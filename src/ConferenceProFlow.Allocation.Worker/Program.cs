using ConferenceProFlow.Allocation.Worker;
using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Messaging.Consumers;
using ConferenceProFlow.Allocation.Worker.Messaging.Inbox;
using ConferenceProFlow.Allocation.Worker.Messaging.Outbox;
using ConferenceProFlow.Allocation.Worker.Processing;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

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

await builder.Build().RunAsync();
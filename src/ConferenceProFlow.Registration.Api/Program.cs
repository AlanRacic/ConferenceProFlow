using ConferenceProFlow.Registration.Api.Features.Participants;
using ConferenceProFlow.Registration.Api.Features.Registrations;
using ConferenceProFlow.Registration.Api.Features.Conferences;
using ConferenceProFlow.Registration.Api.Infrastructure.Inbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Messaging.Consumers;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using ConferenceProFlow.Registration.Api.Observability;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddHealthChecks();

// M4.2A - OpenTelemetry HTTP tracing foundation
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService("conferenceproflow-registration-api"))
    .WithTracing(tracing =>
    {
        tracing.AddAspNetCoreInstrumentation();

        // Capture custom Registration API activities.
        tracing.AddSource(RegistrationTelemetry.SourceName);

        if (builder.Environment.IsDevelopment() &&
            builder.Configuration.GetValue<bool>("Observability:ConsoleExporter:Enabled"))
        {
            tracing.AddConsoleExporter();
        }
    });

builder.Services.AddDbContext<RegistrationDbContext>(
    (serviceProvider, options) =>
    {
        var configuration = serviceProvider.GetRequiredService<IConfiguration>();
        var connectionString = configuration.GetConnectionString("RegistrationDb");
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException("Registration database connection string is not configured.");

        options.UseSqlServer(connectionString);
    });

builder.Services.AddScoped<AllocationResultReconciliationService>();

if (builder.Configuration.GetValue<bool>("OutboxPublisher:Enabled"))
    builder.Services.AddHostedService<OutboxPublisherService>();

// Kafka analytics is an independently switchable hosted service. Leave disabled
// until Registration Analytics Outbox migrations and Kafka infrastructure are ready.
if (builder.Configuration.GetValue<bool>("KafkaAnalyticsPublisher:Enabled"))
    builder.Services.AddHostedService<KafkaAnalyticsOutboxPublisherService>();

// Disabled by default. Enable locally after applying the M2.6D1 Registration migration.
if (builder.Configuration.GetValue<bool>("AllocationResultConsumer:Enabled"))
    builder.Services.AddHostedService<AllocationResultConsumerService>();

ConferenceProFlow.Registration.Api.Observability.JaegerOtlpExporterRegistration.Configure(
    builder.Services, builder.Configuration, builder.Environment);

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseHttpsRedirection();
app.MapHealthChecks("/health");
app.MapConferenceEndpoints();
app.MapParticipantEndpoints();
app.MapRegistrationEndpoints();

app.Run();

// Expose Program for integration testing.
public partial class Program { }

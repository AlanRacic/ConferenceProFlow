using ConferenceProFlow.Registration.Api.Features.Participants;
using ConferenceProFlow.Registration.Api.Features.Registrations;
using ConferenceProFlow.Registration.Api.Features.Conferences;
using ConferenceProFlow.Registration.Api.Infrastructure.Inbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddHealthChecks();

builder.Services.AddDbContext<RegistrationDbContext>(
    (serviceProvider, options) =>
    {
        var configuration = serviceProvider.GetRequiredService<IConfiguration>();
        var connectionString = configuration.GetConnectionString("RegistrationDb");
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException("Registration database connection string is not configured.");

        options.UseSqlServer(connectionString);
    });

// Business reconciliation is scoped; the RabbitMQ consumer comes in M2.6D2.
builder.Services.AddScoped<AllocationResultReconciliationService>();

if (builder.Configuration.GetValue<bool>("OutboxPublisher:Enabled"))
    builder.Services.AddHostedService<OutboxPublisherService>();

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
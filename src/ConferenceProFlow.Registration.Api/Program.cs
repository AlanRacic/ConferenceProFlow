using ConferenceProFlow.Registration.Api.Features.Participants;
using ConferenceProFlow.Registration.Api.Features.Registrations;
using ConferenceProFlow.Registration.Api.Features.Conferences;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Register application services.
builder.Services.AddOpenApi();
builder.Services.AddHealthChecks();

builder.Services.AddDbContext<RegistrationDbContext>(
    (serviceProvider, options) =>
    {
        var configuration = serviceProvider.GetRequiredService<IConfiguration>();

        var connectionString = configuration.GetConnectionString("RegistrationDb");

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException("Registration database connection string is not configured.");
        }

        options.UseSqlServer(connectionString);
    });

if (builder.Configuration.GetValue<bool>("OutboxPublisher:Enabled"))
{
    builder.Services.AddHostedService<OutboxPublisherService>();
}

var app = builder.Build();

// Enable OpenAPI documentation in development.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// Configure the HTTP request pipeline.
app.UseHttpsRedirection();

// Expose the application health endpoint.
app.MapHealthChecks("/health");

// Register Conference API endpoints.
app.MapConferenceEndpoints();
app.MapParticipantEndpoints();
app.MapRegistrationEndpoints();

app.Run();

// Expose Program for integration testing.
public partial class Program { }
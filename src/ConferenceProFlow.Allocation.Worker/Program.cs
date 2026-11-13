using ConferenceProFlow.Allocation.Worker;
using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Messaging.Inbox;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

var connectionString =
    builder.Configuration.GetConnectionString("AllocationDb") ?? throw new InvalidOperationException("Connection string 'AllocationDb' is not configured.");

builder.Services.AddDbContext<AllocationDbContext>(options => options.UseSqlServer(connectionString));

builder.Services.AddScoped<InboxReceiveService>();

builder.Services.AddHostedService<AllocationBackgroundService>();

var host = builder.Build();

await host.RunAsync();
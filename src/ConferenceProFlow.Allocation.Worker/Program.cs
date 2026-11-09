using ConferenceProFlow.Allocation.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<AllocationBackgroundService>();

var host = builder.Build();

await host.RunAsync();
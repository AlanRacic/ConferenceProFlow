using Testcontainers.RabbitMq;

namespace ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;

public sealed class AllocationRabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:4.2.9-management").Build();

    public Uri ConnectionUri => new(_container.GetConnectionString());

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task StopBrokerAsync(CancellationToken cancellationToken = default)
    {
        var result = await _container.ExecAsync(new[] { "rabbitmqctl", "stop_app" }, cancellationToken);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"Failed to stop RabbitMQ application: {result.Stderr}");
        }
    }

    public async Task StartBrokerAsync(CancellationToken cancellationToken = default)
    {
        var result = await _container.ExecAsync(new[] { "rabbitmqctl", "start_app" }, cancellationToken);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"Failed to start RabbitMQ application: {result.Stderr}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
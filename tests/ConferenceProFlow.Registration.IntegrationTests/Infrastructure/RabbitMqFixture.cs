using Testcontainers.RabbitMq;

namespace ConferenceProFlow.Registration.IntegrationTests.Infrastructure;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:4.2.9-management").Build();

    public Uri ConnectionUri => new(_container.GetConnectionString());

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
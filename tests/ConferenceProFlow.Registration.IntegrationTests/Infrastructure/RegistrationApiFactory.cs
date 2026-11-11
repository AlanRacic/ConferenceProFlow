using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace ConferenceProFlow.Registration.IntegrationTests.Infrastructure;

public sealed class RegistrationApiFactory : WebApplicationFactory<Program>
{
    private readonly string _connectionString;

    public RegistrationApiFactory(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);

        builder.UseEnvironment("Testing");

        builder.ConfigureAppConfiguration((context, configuration) =>
        {
            configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:RegistrationDb"] = _connectionString,
                    ["OutboxPublisher:Enabled"] = "false"
            });
        });
    }
}
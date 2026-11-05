using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;
using Xunit;

namespace ConferenceProFlow.Registration.IntegrationTests.Infrastructure;

public sealed class SqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
    private string? _connectionString;
    public string ConnectionString =>
    _connectionString ?? throw new InvalidOperationException("SQL Server test container has not been initialized.");

    public async ValueTask InitializeAsync()
    {
        // Start an isolated SQL Server container.
        await _container.StartAsync();

        // Use a dedicated integration test database.
        var connectionBuilder = new SqlConnectionStringBuilder(_container.GetConnectionString())
        {
            InitialCatalog = "ConferenceProFlowRegistrationTests",
            TrustServerCertificate = true
        };

        _connectionString = connectionBuilder.ConnectionString;

        // Apply the application's actual EF Core migrations.
        await using var dbContext = CreateDbContext();

        await dbContext.Database.MigrateAsync();
    }

    public RegistrationDbContext CreateDbContext()
    {
        if (string.IsNullOrWhiteSpace(_connectionString))
        {
            throw new InvalidOperationException("SQL Server test container has not been initialized.");
        }

        var options = new DbContextOptionsBuilder<RegistrationDbContext>()
            .UseSqlServer(_connectionString).Options;

        return new RegistrationDbContext(options);
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
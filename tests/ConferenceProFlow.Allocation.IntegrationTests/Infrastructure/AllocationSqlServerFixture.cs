using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;

namespace ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;

public sealed class AllocationSqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();

    private string? _connectionString;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var connectionBuilder = new SqlConnectionStringBuilder(_container.GetConnectionString())
        {
            InitialCatalog = "ConferenceProFlowAllocationTests",
            TrustServerCertificate = true
        };

        _connectionString = connectionBuilder.ConnectionString;

        await using var dbContext = CreateDbContext();

        await dbContext.Database.MigrateAsync();
    }

    public AllocationDbContext CreateDbContext()
    {
        if (string.IsNullOrWhiteSpace(_connectionString))
        {
            throw new InvalidOperationException("Allocation test database has not been initialized.");
        }

        var options = new DbContextOptionsBuilder<AllocationDbContext>()
            .UseSqlServer(_connectionString)
            .Options;

        return new AllocationDbContext(options);
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
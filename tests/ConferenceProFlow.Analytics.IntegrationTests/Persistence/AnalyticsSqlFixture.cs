using ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;

namespace ConferenceProFlow.Analytics.IntegrationTests.Persistence;

/// <summary>
/// Disposable SQL Server container, separate Analytics test database. Runs the
/// real EF migrations. Never accesses the developer's operational databases.
/// </summary>
public sealed class AnalyticsSqlFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _sql = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();

    private string? _connectionString;

    public async ValueTask InitializeAsync()
    {
        await _sql.StartAsync(TestContext.Current.CancellationToken);
        var connection = new SqlConnectionStringBuilder(_sql.GetConnectionString())
        {
            InitialCatalog = "ConferenceProFlowAnalyticsTests"
        };
        _connectionString = connection.ConnectionString;

        await using var db = CreateDbContext();
        await db.Database.MigrateAsync(TestContext.Current.CancellationToken);
    }

    public AnalyticsDbContext CreateDbContext()
    {
        if (_connectionString is null)
            throw new InvalidOperationException("SQL fixture has not initialized.");

        var options = new DbContextOptionsBuilder<AnalyticsDbContext>()
            .UseSqlServer(_connectionString)
            .Options;
        return new AnalyticsDbContext(options);
    }

    public async ValueTask DisposeAsync() => await _sql.DisposeAsync();
}

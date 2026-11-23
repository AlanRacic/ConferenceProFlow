using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;

namespace ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;

/// <summary>
/// Provides a deterministic EF Core design-time path for a Generic Host worker.
/// Reads the same AnalyticsDb setting as the application, including the worker's
/// own User Secrets. No credentials are hardcoded or committed to source control.
/// </summary>
public sealed class AnalyticsDbContextFactory : IDesignTimeDbContextFactory<AnalyticsDbContext>
{
    public AnalyticsDbContext CreateDbContext(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(AppContext.BaseDirectory, "appsettings.json"), optional: true)
            .AddUserSecrets<AnalyticsDbContextFactory>(optional: true)
            .AddEnvironmentVariables()
            .Build();

        var connectionString = configuration.GetConnectionString("AnalyticsDb");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "Missing ConnectionStrings:AnalyticsDb for EF Core design-time. " +
                "Configure User Secrets on ConferenceProFlow.Analytics.Worker.");
        }

        var options = new DbContextOptionsBuilder<AnalyticsDbContext>()
            .UseSqlServer(connectionString)
            .Options;

        return new AnalyticsDbContext(options);
    }
}

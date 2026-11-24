using ConferenceProFlow.Analytics.Worker.Configuration;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;

namespace ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;

/// <summary>
/// Existing live Analytics design-time behavior is preserved. Explicit
/// '-- --replay' selects the separate, guarded AnalyticsReplayDb target.
/// No new migrations are created: both databases share one EF Core model.
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

        var replayRequested = args.Any(value =>
            string.Equals(value, "--replay", StringComparison.OrdinalIgnoreCase));
        var replayEnabled = configuration.GetValue<bool>("AnalyticsReplay:Enabled");

        string? connectionString;
        if (replayRequested)
        {
            var replay = configuration.GetSection(AnalyticsReplayOptions.SectionName)
                .Get<AnalyticsReplayOptions>() ?? new AnalyticsReplayOptions();

            var liveConnection = configuration.GetConnectionString("AnalyticsDb");
            connectionString = configuration.GetConnectionString("AnalyticsReplayDb");

            AnalyticsReplaySafety.ValidateOrThrow(
                replay,
                liveConnection,
                connectionString,
                configuration.GetValue<bool>("AnalyticsKafkaConsumer:Enabled"));
        }
        else
        {
            // Prevent a forgotten '--replay' from applying migrations to live
            // Analytics while the user has deliberately enabled replay mode.
            if (replayEnabled)
            {
                throw new InvalidOperationException(
                    "Replay is enabled, but EF Core was called without '-- --replay'. " +
                    "Refusing to select the live Analytics database by default.");
            }

            connectionString = configuration.GetConnectionString("AnalyticsDb");
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException(
                    "Missing ConnectionStrings:AnalyticsDb for EF Core design-time. " +
                    "Configure Analytics Worker User Secrets.");
            }
        }

        var options = new DbContextOptionsBuilder<AnalyticsDbContext>()
            .UseSqlServer(connectionString)
            .Options;

        return new AnalyticsDbContext(options);
    }
}

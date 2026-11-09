using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;

public sealed class AllocationDbContextFactory : IDesignTimeDbContextFactory<AllocationDbContext>
{
    public AllocationDbContext CreateDbContext(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<AllocationDbContext>(optional: true)
            .AddEnvironmentVariables()
            .Build();

        var connectionString =
            configuration.GetConnectionString("AllocationDb") ?? throw new InvalidOperationException("Connection string 'AllocationDb' is not configured.");

        var options = new DbContextOptionsBuilder<AllocationDbContext>()
            .UseSqlServer(connectionString)
            .Options;

        return new AllocationDbContext(options);
    }
}
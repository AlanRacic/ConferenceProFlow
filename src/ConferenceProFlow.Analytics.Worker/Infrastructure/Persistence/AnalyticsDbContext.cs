using ConferenceProFlow.Analytics.Worker.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;

/// <summary>
/// Owns the Analytics SQL database ONLY. M3.4B defines schema and constraints;
/// event processing and offset commits are implemented in M3.4C–E.
/// </summary>
public sealed class AnalyticsDbContext(DbContextOptions<AnalyticsDbContext> options)
    : DbContext(options)
{
    public DbSet<ProcessedAnalyticsEvent> ProcessedAnalyticsEvents => Set<ProcessedAnalyticsEvent>();
    public DbSet<SessionRegistrationStat> SessionRegistrationStats => Set<SessionRegistrationStat>();
    public DbSet<QuarantinedAnalyticsEvent> QuarantinedAnalyticsEvents => Set<QuarantinedAnalyticsEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<ProcessedAnalyticsEvent>(entity =>
        {
            entity.ToTable("ProcessedAnalyticsEvents", table =>
            {
                table.HasCheckConstraint(
                    "CK_ProcessedAnalyticsEvents_Status",
                    "[Status] IN (N'confirmed', N'rejected')");
            });

            entity.HasKey(x => x.MessageId);
            entity.Property(x => x.MessageId).ValueGeneratedNever();
            entity.Property(x => x.RegistrationId).IsRequired();
            entity.Property(x => x.SessionId).IsRequired();
            entity.Property(x => x.Status).HasMaxLength(16).IsRequired();
            entity.Property(x => x.OccurredAtUtc).IsRequired();
            entity.Property(x => x.ProcessedAtUtc).IsRequired();

            entity.HasIndex(x => x.RegistrationId).IsUnique();
            entity.HasIndex(x => new { x.SessionId, x.ProcessedAtUtc });
        });

        modelBuilder.Entity<SessionRegistrationStat>(entity =>
        {
            entity.ToTable("SessionRegistrationStats", table =>
            {
                table.HasCheckConstraint(
                    "CK_SessionRegistrationStats_ConfirmedCount", "[ConfirmedCount] >= 0");
                table.HasCheckConstraint(
                    "CK_SessionRegistrationStats_RejectedCount", "[RejectedCount] >= 0");
            });

            entity.HasKey(x => x.SessionId);
            entity.Property(x => x.SessionId).ValueGeneratedNever();
            entity.Property(x => x.ConfirmedCount).IsRequired();
            entity.Property(x => x.RejectedCount).IsRequired();
            entity.Property(x => x.UpdatedAtUtc).IsRequired();
        });

        modelBuilder.Entity<QuarantinedAnalyticsEvent>(entity =>
        {
            entity.ToTable("QuarantinedAnalyticsEvents", table =>
            {
                table.HasCheckConstraint(
                    "CK_QuarantinedAnalyticsEvents_Partition", "[Partition] >= 0");
                table.HasCheckConstraint(
                    "CK_QuarantinedAnalyticsEvents_Offset", "[Offset] >= 0");
            });

            // One quarantine record per Kafka log position. Concurrent retries
            // cannot create duplicate quarantine rows at the same location.
            entity.HasKey(x => new { x.Topic, x.Partition, x.Offset });
            entity.Property(x => x.Topic).HasMaxLength(249).IsRequired();
            entity.Property(x => x.MessageKey).HasMaxLength(256);
            entity.Property(x => x.Payload).HasMaxLength(4096).IsRequired();
            entity.Property(x => x.Reason).HasMaxLength(500).IsRequired();
            entity.Property(x => x.ReceivedAtUtc).IsRequired();
            entity.HasIndex(x => x.ReceivedAtUtc);
        });
    }
}

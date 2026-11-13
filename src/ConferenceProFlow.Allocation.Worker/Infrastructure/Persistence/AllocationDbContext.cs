using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;

public sealed class AllocationDbContext : DbContext
{
    public AllocationDbContext(DbContextOptions<AllocationDbContext> options) : base(options)
    {
    }

    public DbSet<SessionInventory> SessionInventories => Set<SessionInventory>();

    public DbSet<AllocationEntity> Allocations => Set<AllocationEntity>();

    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        ConfigureSessionInventories(modelBuilder);
        ConfigureAllocations(modelBuilder);
        ConfigureInboxMessages(modelBuilder);
    }

    private static void ConfigureSessionInventories(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SessionInventory>(entity =>
        {
            entity.ToTable("SessionInventories", table =>
            {
                table.HasCheckConstraint(
                    "CK_SessionInventories_Capacity",
                    "[Capacity] > 0");

                table.HasCheckConstraint(
                    "CK_SessionInventories_ReservedCount",
                    "[ReservedCount] >= 0 AND [ReservedCount] <= [Capacity]");
            });

            entity.HasKey(x => x.SessionId);

            entity.Property(x => x.SessionId)
                .ValueGeneratedNever();

            entity.Property(x => x.Capacity)
                .IsRequired();

            entity.Property(x => x.ReservedCount)
                .IsRequired();

            entity.Property(x => x.CreatedAtUtc)
                .IsRequired();

            entity.Property(x => x.RowVersion)
                .IsRowVersion();
        });
    }

    private static void ConfigureAllocations(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AllocationEntity>(entity =>
        {
            entity.ToTable("Allocations", table =>
            {
                table.HasCheckConstraint(
                    "CK_Allocations_Status",
                    "[Status] IN (1, 2)");

                table.HasCheckConstraint(
                    "CK_Allocations_RejectionReason",
                    "([Status] = 1 AND [RejectionReason] IS NULL) OR " +
                    "([Status] = 2 AND [RejectionReason] IS NOT NULL)");
            });

            entity.HasKey(x => x.Id);

            entity.Property(x => x.Id)
                .ValueGeneratedNever();

            entity.Property(x => x.RegistrationId)
                .IsRequired();

            entity.Property(x => x.SessionId)
                .IsRequired();

            entity.Property(x => x.Status)
                .HasConversion<int>()
                .IsRequired();

            entity.Property(x => x.RejectionReason)
                .HasMaxLength(500);

            entity.Property(x => x.CreatedAtUtc)
                .IsRequired();

            entity.HasIndex(x => x.RegistrationId)
                .IsUnique();

            entity.HasOne<SessionInventory>()
                .WithMany()
                .HasForeignKey(x => x.SessionId)
                .OnDelete(DeleteBehavior.Restrict);
        });
    }

    private static void ConfigureInboxMessages(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InboxMessage>(entity =>
        {
            entity.ToTable("InboxMessages");

            entity.HasKey(x => x.Id);

            entity.Property(x => x.Id)
                .ValueGeneratedNever();

            entity.Property(x => x.RegistrationId)
                .IsRequired();

            entity.Property(x => x.SessionId)
                .IsRequired();

            entity.Property(x => x.OccurredAtUtc)
                .IsRequired();

            entity.Property(x => x.ReceivedAtUtc)
                .IsRequired();

            entity.Property(x => x.ProcessedAtUtc);

            entity.HasIndex(x => x.RegistrationId)
                .IsUnique()
                .HasDatabaseName("UX_InboxMessages_RegistrationId");

            entity.HasIndex(x => new
            {
                x.ProcessedAtUtc,
                x.ReceivedAtUtc
            })
            .HasDatabaseName("IX_InboxMessages_Processing");
        });
    }
}
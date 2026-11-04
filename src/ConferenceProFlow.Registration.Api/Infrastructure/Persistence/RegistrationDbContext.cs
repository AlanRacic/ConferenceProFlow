using ConferenceProFlow.Registration.Api.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Persistence;

public sealed class RegistrationDbContext : DbContext
{
    public RegistrationDbContext(DbContextOptions<RegistrationDbContext> options) : base(options)
    {
    }

    public DbSet<Conference> Conferences => Set<Conference>();

    public DbSet<ConferenceSession> ConferenceSessions => Set<ConferenceSession>();

    public DbSet<Participant> Participants => Set<Participant>();

    public DbSet<RegistrationEntity> Registrations => Set<RegistrationEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        ConfigureConferences(modelBuilder);
        ConfigureConferenceSessions(modelBuilder);
        ConfigureParticipants(modelBuilder);
        ConfigureRegistrations(modelBuilder);
    }

    private static void ConfigureConferences(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Conference>(entity =>
        {
            entity.ToTable("Conferences");

            entity.HasKey(x => x.Id);

            entity.Property(x => x.Id)
                .ValueGeneratedNever();

            entity.Property(x => x.Name)
                .HasMaxLength(200)
                .IsRequired();

            entity.Property(x => x.StartsAtUtc)
                .IsRequired();

            entity.Property(x => x.EndsAtUtc)
                .IsRequired();
        });
    }

    private static void ConfigureConferenceSessions(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ConferenceSession>(entity =>
        {
            entity.ToTable("ConferenceSessions");

            entity.HasKey(x => x.Id);

            entity.Property(x => x.Id)
                .ValueGeneratedNever();

            entity.Property(x => x.Title)
                .HasMaxLength(200)
                .IsRequired();

            entity.Property(x => x.IsPublished)
                .IsRequired();

            entity.HasOne<Conference>()
                .WithMany()
                .HasForeignKey(x => x.ConferenceId)
                .OnDelete(DeleteBehavior.Restrict);
        });
    }

    private static void ConfigureParticipants(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Participant>(entity =>
        {
            entity.ToTable("Participants");

            entity.HasKey(x => x.Id);

            entity.Property(x => x.Id)
                .ValueGeneratedNever();

            entity.Property(x => x.DisplayName)
                .HasMaxLength(200)
                .IsRequired();

            entity.Property(x => x.Email)
                .HasMaxLength(254)
                .IsRequired();

            entity.Property(x => x.NormalizedEmail)
                .HasMaxLength(254)
                .IsRequired();

            entity.HasIndex(x => x.NormalizedEmail)
                .IsUnique();
        });
    }

    private static void ConfigureRegistrations(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RegistrationEntity>(entity =>
        {
            entity.ToTable("Registrations", table =>
            {
                table.HasCheckConstraint("CK_Registrations_Status", "[Status] IN (0, 1, 2)");
            });

            entity.HasKey(x => x.Id);

            entity.Property(x => x.Id)
                .ValueGeneratedNever();

            entity.Property(x => x.Status)
                .HasConversion<int>()
                .IsRequired();

            entity.Property(x => x.RejectionReason)
                .HasMaxLength(500);

            entity.HasIndex(x => new
            {
                x.ParticipantId,
                x.SessionId
            })
            .IsUnique();

            entity.HasOne<Participant>()
                .WithMany()
                .HasForeignKey(x => x.ParticipantId)
                .OnDelete(DeleteBehavior.Restrict);

            entity.HasOne<ConferenceSession>()
                .WithMany()
                .HasForeignKey(x => x.SessionId)
                .OnDelete(DeleteBehavior.Restrict);
        });
    }
}
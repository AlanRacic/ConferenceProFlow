using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Domain.Enums;
using ConferenceProFlow.Registration.IntegrationTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.IntegrationTests.Persistence;

[Collection("SqlServer")]
public sealed class RegistrationPersistenceTests
{
    private static readonly DateTimeOffset CreatedAt = new(2026, 11, 3, 12, 0, 0, TimeSpan.Zero);
    private readonly SqlServerFixture _fixture;

    public RegistrationPersistenceTests(SqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task SaveRegistration_CanReadFromAnotherDbContext()
    {
        // Arrange
        var (participantId, sessionId) = await CreatePrerequisitesAsync();

        var registration = RegistrationEntity.Create(participantId, sessionId, CreatedAt);

        // Act: Persist the registration.
        await using (var dbContext = _fixture.CreateDbContext())
        {
            dbContext.Registrations.Add(registration);

            await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Assert: Read it using a new DbContext.
        await using var readContext = _fixture.CreateDbContext();

        var savedRegistration = await readContext.Registrations
            .AsNoTracking()
            .SingleAsync(x => x.Id == registration.Id, TestContext.Current.CancellationToken);

        Assert.Equal(registration.Id, savedRegistration.Id);
        Assert.Equal(participantId, savedRegistration.ParticipantId);
        Assert.Equal(sessionId, savedRegistration.SessionId);
        Assert.Equal(RegistrationStatus.Pending, savedRegistration.Status);
    }

    [Fact]
    public async Task SaveDuplicateRegistration_ThrowsDbUpdateException()
    {
        // Arrange
        var (participantId, sessionId) = await CreatePrerequisitesAsync();

        var firstRegistration = RegistrationEntity.Create(participantId, sessionId, CreatedAt);

        var duplicateRegistration = RegistrationEntity.Create(participantId, sessionId, CreatedAt);

        // Save the first registration.
        await using (var firstContext = _fixture.CreateDbContext())
        {
            firstContext.Registrations.Add(firstRegistration);

            await firstContext.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Act: Attempt to save the duplicate.
        await using var secondContext = _fixture.CreateDbContext();

        secondContext.Registrations.Add(duplicateRegistration);

        // Assert: SQL Server rejects the duplicate.
        await Assert.ThrowsAsync<DbUpdateException>(async () => await secondContext.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SaveRegistration_WithInvalidForeignKeys_ThrowsException()
    {
        // Arrange
        var registration = RegistrationEntity.Create(Guid.NewGuid(), Guid.NewGuid(), CreatedAt);

        await using var dbContext = _fixture.CreateDbContext();

        dbContext.Registrations.Add(registration);

        // Act & Assert
        await Assert.ThrowsAsync<DbUpdateException>(async () => await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ConfirmRegistration_PersistsUpdatedStatus()
    {
        // Arrange
        var (participantId, sessionId) = await CreatePrerequisitesAsync();

        var registration = RegistrationEntity.Create(participantId, sessionId, CreatedAt);

        await using (var createContext = _fixture.CreateDbContext())
        {
            createContext.Registrations.Add(registration);

            await createContext.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Act: Load and confirm the registration.
        var confirmedAt = CreatedAt.AddMinutes(1);

        await using (var updateContext = _fixture.CreateDbContext())
        {
            var storedRegistration = await updateContext.Registrations
                .SingleAsync(x => x.Id == registration.Id, TestContext.Current.CancellationToken);

            storedRegistration.Confirm(confirmedAt);

            await updateContext.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Assert: Verify the update in a new DbContext.
        await using var readContext = _fixture.CreateDbContext();

        var updatedRegistration = await readContext.Registrations
            .AsNoTracking()
            .SingleAsync(x => x.Id == registration.Id, TestContext.Current.CancellationToken);

        Assert.Equal(RegistrationStatus.Confirmed, updatedRegistration.Status);

        Assert.Equal(confirmedAt, updatedRegistration.UpdatedAtUtc);
    }

    private async Task<(Guid ParticipantId, Guid SessionId)>CreatePrerequisitesAsync()
    {
        var conferenceStart = new DateTimeOffset(2027, 6, 1, 8, 0, 0, TimeSpan.Zero);

        var conferenceEnd = conferenceStart.AddDays(2);

        var conference = Conference.Create(
            $"Test Conference {Guid.NewGuid():N}",
            conferenceStart,
            conferenceEnd);

        var session = ConferenceSession.Create(
            conference.Id,
            "Distributed Systems in Practice",
            conferenceStart.AddHours(1),
            conferenceStart.AddHours(2));

        var participant = Participant.Create(
            "Test Participant",
            $"participant-{Guid.NewGuid():N}@example.com");

        await using var dbContext = _fixture.CreateDbContext();

        dbContext.Conferences.Add(conference);
        dbContext.ConferenceSessions.Add(session);
        dbContext.Participants.Add(participant);

        await dbContext.SaveChangesAsync();

        return (participant.Id, session.Id);
    }
}
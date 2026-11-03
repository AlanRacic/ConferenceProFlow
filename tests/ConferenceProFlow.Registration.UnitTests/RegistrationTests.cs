using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;
using ConferenceProFlow.Registration.Api.Domain.Enums;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests;

public class RegistrationTests
{
    private static readonly DateTimeOffset CreatedAt = new(2026, 11, 2, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Create_ValidData_ReturnsPendingRegistration()
    {
        // Arrange
        var participantId = Guid.NewGuid();
        var sessionId = Guid.NewGuid();

        // Act
        var registration = RegistrationEntity.Create(participantId, sessionId, CreatedAt);

        // Assert
        Assert.NotEqual(Guid.Empty, registration.Id);
        Assert.Equal(participantId, registration.ParticipantId);
        Assert.Equal(sessionId, registration.SessionId);
        Assert.Equal(RegistrationStatus.Pending, registration.Status);
        Assert.Equal(CreatedAt, registration.CreatedAtUtc);
    }

    [Fact]
    public void Confirm_PendingRegistration_ChangesStatusToConfirmed()
    {
        // Arrange
        var registration = CreateRegistration();
        var confirmedAt = CreatedAt.AddMinutes(1);

        // Act
        registration.Confirm(confirmedAt);

        // Assert
        Assert.Equal(RegistrationStatus.Confirmed, registration.Status);
        Assert.Equal(confirmedAt, registration.UpdatedAtUtc);
    }

    [Fact]
    public void Reject_PendingRegistration_ChangesStatusToRejected()
    {
        // Arrange
        var registration = CreateRegistration();

        // Act
        registration.Reject("Session capacity exceeded.", CreatedAt.AddMinutes(1));

        // Assert
        Assert.Equal(RegistrationStatus.Rejected, registration.Status);
        Assert.Equal("Session capacity exceeded.", registration.RejectionReason);
    }

    [Fact]
    public void Reject_ConfirmedRegistration_ThrowsException()
    {
        // Arrange
        var registration = CreateRegistration();

        registration.Confirm(CreatedAt.AddMinutes(1));

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => registration.Reject("Session capacity exceeded.", CreatedAt.AddMinutes(2)));
    }

    [Fact]
    public void Confirm_AlreadyConfirmedRegistration_DoesNotChangeTimestamp()
    {
        // Arrange
        var registration = CreateRegistration();
        var confirmedAt = CreatedAt.AddMinutes(1);

        registration.Confirm(confirmedAt);

        // Act
        registration.Confirm(CreatedAt.AddMinutes(2));

        // Assert
        Assert.Equal(RegistrationStatus.Confirmed, registration.Status);
        Assert.Equal(confirmedAt, registration.UpdatedAtUtc);
    }

    [Fact]
    public void Create_EmptyParticipantId_ThrowsException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => RegistrationEntity.Create(Guid.Empty, Guid.NewGuid(), CreatedAt));
    }

    private static RegistrationEntity CreateRegistration()
    {
        return RegistrationEntity.Create(Guid.NewGuid(), Guid.NewGuid(), CreatedAt);
    }
}
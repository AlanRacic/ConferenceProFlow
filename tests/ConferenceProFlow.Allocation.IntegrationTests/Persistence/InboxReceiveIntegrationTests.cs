using ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;
using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;
using ConferenceProFlow.Allocation.Worker.Messaging.Inbox;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Allocation.IntegrationTests.Persistence;

[Collection("AllocationSqlServer")]
public sealed class InboxReceiveIntegrationTests
{
    private readonly AllocationSqlServerFixture _fixture;

    public InboxReceiveIntegrationTests(AllocationSqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task NewMessage_IsPersistedSuccessfully()
    {
        // Arrange
        var message = CreateMessage();

        // Act
        var result = await ReceiveOnceAsync(message);

        // Assert
        Assert.Equal(InboxReceiveResult.Stored, result);

        await using var db = _fixture.CreateDbContext();

        var saved = await db.InboxMessages
            .AsNoTracking()
            .SingleAsync(
                x => x.Id == message.MessageId,
                TestContext.Current.CancellationToken);

        Assert.Equal(
            message.RegistrationId,
            saved.RegistrationId);

        Assert.Equal(
            message.SessionId,
            saved.SessionId);

        Assert.Equal(
            message.OccurredAtUtc,
            saved.OccurredAtUtc);

        Assert.Null(saved.ProcessedAtUtc);
    }

    [Fact]
    public async Task SameMessageDeliveredTwice_ReturnsDuplicateMessage()
    {
        // Arrange
        var message = CreateMessage();

        var firstResult = await ReceiveOnceAsync(message);

        // Act
        var secondResult = await ReceiveOnceAsync(message);

        // Assert
        Assert.Equal(InboxReceiveResult.Stored, firstResult);

        Assert.Equal(InboxReceiveResult.DuplicateMessage, secondResult);

        await using var db = _fixture.CreateDbContext();

        var count = await db.InboxMessages
            .CountAsync(
                x => x.RegistrationId == message.RegistrationId,
                TestContext.Current.CancellationToken);

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task DifferentMessageId_SameRegistration_ReturnsDuplicateRegistration()
    {
        // Arrange
        var original = CreateMessage();

        var repeated = original with
        {
            MessageId = Guid.NewGuid()
        };

        await ReceiveOnceAsync(original);

        // Act
        var result = await ReceiveOnceAsync(repeated);

        // Assert
        Assert.Equal(InboxReceiveResult.DuplicateRegistration, result);

        await using var db = _fixture.CreateDbContext();

        var count = await db.InboxMessages
            .CountAsync(
                x => x.RegistrationId == original.RegistrationId,
                TestContext.Current.CancellationToken);

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task SameRegistrationDifferentSession_ReturnsConflict()
    {
        // Arrange
        var original = CreateMessage();

        var conflicting = original with
        {
            MessageId = Guid.NewGuid(),
            SessionId = Guid.NewGuid()
        };

        await ReceiveOnceAsync(original);

        // Act
        var result = await ReceiveOnceAsync(conflicting);

        // Assert
        Assert.Equal(InboxReceiveResult.Conflict, result);

        await using var db = _fixture.CreateDbContext();

        var count = await db.InboxMessages
            .CountAsync(
                x => x.RegistrationId == original.RegistrationId,
                TestContext.Current.CancellationToken);

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task SameMessageIdDifferentRegistration_ReturnsConflict()
    {
        // Arrange
        var original = CreateMessage();

        var conflicting = original with
        {
            RegistrationId = Guid.NewGuid()
        };

        await ReceiveOnceAsync(original);

        // Act
        var result = await ReceiveOnceAsync(conflicting);

        // Assert
        Assert.Equal(InboxReceiveResult.Conflict, result);
    }

    [Fact]
    public async Task ParallelReceipts_SameRegistration_StoreOnlyOneMessage()
    {
        // Arrange
        var original = CreateMessage();

        var repeated = original with
        {
            MessageId = Guid.NewGuid()
        };

        // Act
        // Each call uses a separate DbContext.
        var results = await Task.WhenAll(
            ReceiveOnceAsync(original),
            ReceiveOnceAsync(repeated));

        // Assert
        Assert.Contains(InboxReceiveResult.Stored, results);

        Assert.Contains(InboxReceiveResult.DuplicateRegistration, results);

        await using var db = _fixture.CreateDbContext();

        var count = await db.InboxMessages
            .CountAsync(
                x => x.RegistrationId == original.RegistrationId,
                TestContext.Current.CancellationToken);

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Database_EnforcesUniqueRegistrationId()
    {
        // Arrange
        var original = CreateMessage();

        var duplicate = InboxMessage.Create(
            Guid.NewGuid(),
            original.RegistrationId,
            original.SessionId,
            original.OccurredAtUtc,
            DateTimeOffset.UtcNow);

        await using (var firstDb = _fixture.CreateDbContext())
        {
            firstDb.InboxMessages.Add(
                InboxMessage.Create(
                    original.MessageId,
                    original.RegistrationId,
                    original.SessionId,
                    original.OccurredAtUtc,
                    DateTimeOffset.UtcNow));

            await firstDb.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Act
        await using var secondDb = _fixture.CreateDbContext();

        secondDb.InboxMessages.Add(duplicate);

        // Assert
        await Assert.ThrowsAsync<DbUpdateException>(
            async () =>
            {
                await secondDb.SaveChangesAsync(TestContext.Current.CancellationToken);
            });
    }

    private async Task<InboxReceiveResult> ReceiveOnceAsync(RegistrationRequestedV1 message)
    {
        await using var db = _fixture.CreateDbContext();

        var service = new InboxReceiveService(db);

        return await service.ReceiveAsync(
            message,
            DateTimeOffset.UtcNow,
            TestContext.Current.CancellationToken);
    }

    private static RegistrationRequestedV1 CreateMessage()
    {
        return new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);
    }
}
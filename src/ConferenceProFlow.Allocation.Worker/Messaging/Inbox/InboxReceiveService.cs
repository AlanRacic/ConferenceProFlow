using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Allocation.Worker.Messaging.Inbox;

public sealed class InboxReceiveService
{
    private readonly AllocationDbContext _db;

    public InboxReceiveService(AllocationDbContext db)
    {
        _db = db;
    }

    public async Task<InboxReceiveResult> ReceiveAsync(
        RegistrationRequestedV1 message,
        DateTimeOffset receivedAtUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var inboxMessage = InboxMessage.Create(
            message.MessageId,
            message.RegistrationId,
            message.SessionId,
            message.OccurredAtUtc,
            receivedAtUtc);

        _db.InboxMessages.Add(inboxMessage);

        try
        {
            await _db.SaveChangesAsync(cancellationToken);

            return InboxReceiveResult.Stored;
        }
        catch (DbUpdateException ex)
            when (IsUniqueConstraintViolation(ex))
        {
            // The attempted insert did not succeed.
            // Detach it before inspecting the existing record.
            _db.Entry(inboxMessage).State = EntityState.Detached;

            var existingByMessageId = await _db.InboxMessages
                .AsNoTracking()
                .SingleOrDefaultAsync(
                    x => x.Id == message.MessageId,
                    cancellationToken);

            if (existingByMessageId is not null)
            {
                var sameMessage =
                    existingByMessageId.RegistrationId ==
                        message.RegistrationId &&
                    existingByMessageId.SessionId ==
                        message.SessionId &&
                    existingByMessageId.OccurredAtUtc ==
                        message.OccurredAtUtc;

                return sameMessage
                    ? InboxReceiveResult.DuplicateMessage
                    : InboxReceiveResult.Conflict;
            }

            var existingByRegistrationId = await _db.InboxMessages
                .AsNoTracking()
                .SingleOrDefaultAsync(
                    x => x.RegistrationId == message.RegistrationId,
                    cancellationToken);

            if (existingByRegistrationId is not null)
            {
                return existingByRegistrationId.SessionId ==
                       message.SessionId
                    ? InboxReceiveResult.DuplicateRegistration
                    : InboxReceiveResult.Conflict;
            }

            // A unique constraint failed, but we could not identify an existing matching record.
            // Do not silently acknowledge an unknown outcome.
            throw;
        }
    }

    private static bool IsUniqueConstraintViolation(DbUpdateException exception)
    {
        return exception.InnerException is SqlException
        {
            Number: 2601 or 2627
        };
    }
}
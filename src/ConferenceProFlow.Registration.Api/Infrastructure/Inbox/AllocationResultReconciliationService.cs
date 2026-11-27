using ConferenceProFlow.Registration.Api.Domain.Enums;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using ConferenceProFlow.Registration.Api.Observability;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Inbox;

/// <summary>
/// Atomically records a result receipt and reconciles the Registration status.
/// The RabbitMQ consumer (M2.6D2) will ACK only after this method returns.
/// </summary>
public sealed class AllocationResultReconciliationService
{
    private const int MaxUniqueConflictAttempts = 3;
    private readonly RegistrationDbContext _db;

    public AllocationResultReconciliationService(RegistrationDbContext db) => _db = db;

    public async Task<AllocationResultReconciliationResult> ApplyAsync(
        AllocationDecidedV1 result,
        CancellationToken cancellationToken = default)
    {
        var error = AllocationDecidedV1Validator.GetError(result);
        if (error is not null)
            throw new ArgumentException(error, nameof(result));

        for (var attempt = 1; attempt <= MaxUniqueConflictAttempts; attempt++)
        {
            try
            {
                return await ApplyOnceAsync(result, cancellationToken);
            }
            catch (DbUpdateException ex) when (SqlServerErrors.IsUniqueViolation(ex))
            {
                // A concurrent consumer could have committed this message or registration decision.
                // Discard stale tracked entities before reading the database again;
                // never retry SaveChanges blindly.
                _db.ChangeTracker.Clear();
                if (attempt == MaxUniqueConflictAttempts)
                    throw;
            }
        }

        throw new InvalidOperationException("Unexpected reconciliation retry state.");
    }

    private async Task<AllocationResultReconciliationResult> ApplyOnceAsync(
        AllocationDecidedV1 result,
        CancellationToken cancellationToken)
    {
        // Same transport MessageId but altered data is a conflict, not a duplicate.
        var existingMessage = await _db.AllocationResultInboxMessages
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.Id == result.MessageId, cancellationToken);
        if (existingMessage is not null)
            return existingMessage.Matches(result)
                ? AllocationResultReconciliationResult.DuplicateMessage
                : AllocationResultReconciliationResult.Conflict;

        // One logical Allocation decision is allowed per RegistrationId.
        // Duplicate business decisions are safe even if a different MessageId was used;
        // mismatched allocation IDs or outcomes are conflicts.
        var existingDecisions = await _db.AllocationResultInboxMessages
            .AsNoTracking()
            .Where(x => x.AllocationId == result.AllocationId ||
                        x.RegistrationId == result.RegistrationId)
            .ToListAsync(cancellationToken);
        if (existingDecisions.Count > 0)
        {
            return existingDecisions.Count == 1 &&
                   existingDecisions[0].MatchesBusinessDecision(result)
                ? AllocationResultReconciliationResult.DuplicateDecision
                : AllocationResultReconciliationResult.Conflict;
        }

        var registration = await _db.Registrations
            .SingleOrDefaultAsync(x => x.Id == result.RegistrationId, cancellationToken);
        if (registration is null)
            return AllocationResultReconciliationResult.UnknownRegistration;

        if (registration.SessionId != result.SessionId)
            return AllocationResultReconciliationResult.Conflict;

        var updatedAtUtc = DateTimeOffset.UtcNow;
        if (updatedAtUtc < registration.CreatedAtUtc)
            updatedAtUtc = registration.CreatedAtUtc;

        // Only an actual Pending -> final transition produces an analytics event.
        // Repeated or pre-finalized decisions must never generate a second event.
        var isNewFinalization = registration.Status == RegistrationStatus.Pending;

        if (result.Status == AllocationDecidedV1.ConfirmedStatus)
        {
            if (registration.Status == RegistrationStatus.Rejected)
                return AllocationResultReconciliationResult.Conflict;
            if (registration.Status == RegistrationStatus.Pending)
                registration.Confirm(updatedAtUtc);
        }
        else
        {
            var reason = result.RejectionReason!;
            if (registration.Status == RegistrationStatus.Confirmed ||
                (registration.Status == RegistrationStatus.Rejected &&
                 registration.RejectionReason != reason.Trim()))
            {
                return AllocationResultReconciliationResult.Conflict;
            }
            if (registration.Status == RegistrationStatus.Pending)
                registration.Reject(reason, updatedAtUtc);
        }

        _db.AllocationResultInboxMessages.Add(
            AllocationResultInboxMessage.Create(result, DateTimeOffset.UtcNow));

        if (isNewFinalization)
        {
            // The actual committed Registration status is the analytics source of truth.
            // Persist the complete payload and stable identity once, before Kafka publishing.
            var finalStatus = result.Status == AllocationDecidedV1.ConfirmedStatus
                ? RegistrationFinalizedV1.ConfirmedStatus
                : RegistrationFinalizedV1.RejectedStatus;
            var analyticsEvent = RegistrationFinalizedV1.Create(
                registration.Id, registration.SessionId, finalStatus, updatedAtUtc);

            // Capture the current RabbitMQ consumer span before SaveChanges.
            // This is durable across Kafka-publisher restarts and replay attempts.
            // If tracing is unavailable, the business transaction still succeeds.
            var trace = AnalyticsOutboxTraceContext.Capture();
            _db.AnalyticsOutboxMessages.Add(
                AnalyticsOutboxMessage.FromEvent(
                    analyticsEvent, trace.TraceParent, trace.TraceState));
        }

        // EF Core relational SaveChanges is atomic: status, result Inbox and
        // Analytics Outbox succeed or roll back together. No Kafka call here.
        await _db.SaveChangesAsync(cancellationToken);
        return AllocationResultReconciliationResult.Applied;
    }
}

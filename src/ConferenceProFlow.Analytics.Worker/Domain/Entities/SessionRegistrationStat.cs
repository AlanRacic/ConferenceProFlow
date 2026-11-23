namespace ConferenceProFlow.Analytics.Worker.Domain.Entities;

/// <summary>
/// Materialized confirmed/rejected counts for a single session.
/// M3.4D will implement SQL-atomic increments; EF read/modify/write of these
/// properties alone is NOT concurrency safe.
/// </summary>
public sealed class SessionRegistrationStat
{
    public Guid SessionId { get; set; }
    public long ConfirmedCount { get; set; }
    public long RejectedCount { get; set; }
    public DateTimeOffset UpdatedAtUtc { get; set; }
}

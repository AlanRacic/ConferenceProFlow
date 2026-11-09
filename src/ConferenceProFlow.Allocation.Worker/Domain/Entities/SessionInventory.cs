namespace ConferenceProFlow.Allocation.Worker.Domain.Entities;

public sealed class SessionInventory
{
    private SessionInventory()
    {
        // Required by EF Core.
    }

    public Guid SessionId { get; private set; }

    public int Capacity { get; private set; }

    public int ReservedCount { get; private set; }

    public DateTimeOffset CreatedAtUtc { get; private set; }

    public byte[] RowVersion { get; private set; } = [];

    public static SessionInventory Create(Guid sessionId, int capacity, DateTimeOffset createdAtUtc)
    {
        if (sessionId == Guid.Empty)
        {
            throw new ArgumentException("SessionId must not be empty.", nameof(sessionId));
        }

        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than zero.");
        }

        if (createdAtUtc == default || createdAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("CreatedAtUtc must be a valid UTC timestamp.", nameof(createdAtUtc));
        }

        return new SessionInventory
        {
            SessionId = sessionId,
            Capacity = capacity,
            ReservedCount = 0,
            CreatedAtUtc = createdAtUtc
        };
    }
}
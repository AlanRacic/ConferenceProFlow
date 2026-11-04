namespace ConferenceProFlow.Registration.Api.Domain.Entities;

public sealed class ConferenceSession
{
    private ConferenceSession()
    {
    }

    public Guid Id { get; private set; }

    public Guid ConferenceId { get; private set; }

    public string Title { get; private set; } = string.Empty;

    public DateTimeOffset StartsAtUtc { get; private set; }

    public DateTimeOffset EndsAtUtc { get; private set; }

    public bool IsPublished { get; private set; }

    public static ConferenceSession Create(Guid conferenceId, string title, DateTimeOffset startsAtUtc, DateTimeOffset endsAtUtc)
    {
        if (conferenceId == Guid.Empty)
            throw new ArgumentException("ConferenceId cannot be empty.", nameof(conferenceId));

        if (string.IsNullOrWhiteSpace(title))
            throw new ArgumentException("Session title is required.", nameof(title));

        if (startsAtUtc.Offset != TimeSpan.Zero || endsAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("Session timestamps must use UTC.");
        }

        if (endsAtUtc <= startsAtUtc)
            throw new ArgumentException("Session end time must be after start time.");

        return new ConferenceSession
        {
            Id = Guid.NewGuid(),
            ConferenceId = conferenceId,
            Title = title.Trim(),
            StartsAtUtc = startsAtUtc,
            EndsAtUtc = endsAtUtc,
            IsPublished = false
        };
    }
}
namespace ConferenceProFlow.Registration.Api.Domain.Entities;

public sealed class Conference
{
    private Conference()
    {
    }

    public Guid Id { get; private set; }

    public string Name { get; private set; } = string.Empty;

    public DateTimeOffset StartsAtUtc { get; private set; }

    public DateTimeOffset EndsAtUtc { get; private set; }

    public static Conference Create(string name, DateTimeOffset startsAtUtc, DateTimeOffset endsAtUtc)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Conference name is required.", nameof(name));

        if (startsAtUtc.Offset != TimeSpan.Zero || endsAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("Conference timestamps must use UTC.");
        }

        if (endsAtUtc <= startsAtUtc)
            throw new ArgumentException("Conference end time must be after start time.");

        return new Conference
        {
            Id = Guid.NewGuid(),
            Name = name.Trim(),
            StartsAtUtc = startsAtUtc,
            EndsAtUtc = endsAtUtc
        };
    }
}
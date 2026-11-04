namespace ConferenceProFlow.Registration.Api.Domain.Entities;

public sealed class Participant
{
    private Participant()
    {
    }

    public Guid Id { get; private set; }

    public string DisplayName { get; private set; } = string.Empty;

    public string Email { get; private set; } = string.Empty;

    public string NormalizedEmail { get; private set; } = string.Empty;

    public static Participant Create(string displayName, string email)
    {
        if (string.IsNullOrWhiteSpace(displayName))
            throw new ArgumentException("Participant name is required.", nameof(displayName));

        if (string.IsNullOrWhiteSpace(email))
            throw new ArgumentException("Participant email is required.", nameof(email));

        var normalizedEmail = email.Trim();

        if (normalizedEmail.Length > 254 || !normalizedEmail.Contains('@'))
        {
            throw new ArgumentException("Participant email is invalid.", nameof(email));
        }

        return new Participant
        {
            Id = Guid.NewGuid(),
            DisplayName = displayName.Trim(),
            Email = normalizedEmail,
            NormalizedEmail = normalizedEmail.ToUpperInvariant()
        };
    }
}
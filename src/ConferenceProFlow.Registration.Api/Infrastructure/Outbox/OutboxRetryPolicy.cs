namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

public static class OutboxRetryPolicy
{
    private const int InitialDelaySeconds = 10;

    private const int MaximumDelaySeconds = 300;

    public static TimeSpan GetDelay(int failedAttempts)
    {
        if (failedAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(failedAttempts),
                "Failed attempts must be greater than zero.");
        }

        var exponent = Math.Min(failedAttempts - 1, 5);

        var multiplier = 1 << exponent;

        var delaySeconds = Math.Min(
            InitialDelaySeconds * multiplier,
            MaximumDelaySeconds);

        return TimeSpan.FromSeconds(delaySeconds);
    }
}
namespace ConferenceProFlow.Allocation.Worker.Messaging.Outbox;

public static class AllocationOutboxRetryPolicy
{
    // 10, 20, 40, 80, 160, 300, 300, ... seconds.
    public static TimeSpan GetDelay(int failedAttempts)
    {
        if (failedAttempts < 1)
            throw new ArgumentOutOfRangeException(nameof(failedAttempts));

        var exponent = Math.Min(failedAttempts - 1, 5);
        return TimeSpan.FromSeconds(Math.Min(10 * (1 << exponent), 300));
    }
}
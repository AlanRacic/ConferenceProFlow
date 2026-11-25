using Confluent.Kafka;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

/// <summary>
/// Diagnostic-only classification. It deliberately does not change retry, outbox state,
/// delivery guarantees, or Kafka producer configuration.
/// </summary>
public enum AnalyticsOutboxPublishFailureCategory
{
    PayloadValidation,
    KafkaDelivery,
    Unexpected
}

public static class AnalyticsOutboxPublishAlertPolicy
{
    public const int DefaultAlertAfterAttempts = 5;
    private const int RepeatAlertEveryAttempts = 10;

    public static bool IsValidThreshold(int threshold) => threshold is >= 1 and <= 10000;

    /// <summary>
    /// PayloadValidation identifies deterministic local validation failures.
    /// KafkaDelivery covers broker/client errors, which may be transient OR persistent.
    /// Other failures are Unexpected and must be investigated; do not guess their cause.
    /// </summary>
    public static AnalyticsOutboxPublishFailureCategory Classify(Exception failure, bool payloadValidated)
    {
        ArgumentNullException.ThrowIfNull(failure);
        if (!payloadValidated)
            return AnalyticsOutboxPublishFailureCategory.PayloadValidation;
        if (failure is KafkaException)
            return AnalyticsOutboxPublishFailureCategory.KafkaDelivery;
        return AnalyticsOutboxPublishFailureCategory.Unexpected;
    }

    /// <summary>Persist the category beside the existing bounded LastError text.</summary>
    public static string Describe(AnalyticsOutboxPublishFailureCategory category, Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        return $"[{category}] {failure.GetType().Name}: {failure.Message}";
    }

    /// <summary>
    /// Warn with high severity on the threshold attempt and every 10 subsequent
    /// attempts. Other retries remain visible as normal warning-level log entries.
    /// </summary>
    public static bool ShouldEscalate(int attempts, int alertAfterAttempts)
    {
        if (!IsValidThreshold(alertAfterAttempts))
            throw new ArgumentOutOfRangeException(nameof(alertAfterAttempts));
        return attempts >= alertAfterAttempts &&
               (attempts - alertAfterAttempts) % RepeatAlertEveryAttempts == 0;
    }
}

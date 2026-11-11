using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Outbox;

public sealed class OutboxRetryPolicyTests
{
    [Theory]
    [InlineData(1, 10)]
    [InlineData(2, 20)]
    [InlineData(3, 40)]
    [InlineData(4, 80)]
    [InlineData(5, 160)]
    [InlineData(6, 300)]
    [InlineData(7, 300)]
    public void GetDelay_ReturnsExpectedDelay(int failedAttempts, int expectedSeconds)
    {
        // Act
        var delay = OutboxRetryPolicy.GetDelay(failedAttempts);

        // Assert
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), delay);
    }

    [Fact]
    public void GetDelay_WithZeroAttempts_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => OutboxRetryPolicy.GetDelay(0));
    }
}
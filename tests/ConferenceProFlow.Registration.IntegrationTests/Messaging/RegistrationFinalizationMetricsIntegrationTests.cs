using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Domain.Enums;
using ConferenceProFlow.Registration.Api.Infrastructure.Inbox;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;
using Xunit;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.IntegrationTests.Messaging;

/// <summary>
/// Real SQL Server integration check for the M4.3 business metric boundary.
/// Reuses the existing SQL fixture type; run integration projects sequentially locally.
/// </summary>
public sealed class RegistrationFinalizationMetricsIntegrationTests
    : IClassFixture<ResultReconciliationSqlFixture>
{
    private const string MeterName = "ConferenceProFlow.Registration.Api.Metrics";
    private const string CountName = "conferenceproflow.registration.finalized";
    private const string DurationName = "conferenceproflow.registration.finalization.duration";

    private readonly ResultReconciliationSqlFixture _fixture;

    public RegistrationFinalizationMetricsIntegrationTests(ResultReconciliationSqlFixture fixture)
        => _fixture = fixture;

    [Fact]
    public async Task CommittedDecisions_EmitOneCounterAndDurationPerRegistration_NotForDuplicatesOrConflicts()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var recorded = new ConcurrentQueue<MetricSample>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, activeListener) =>
        {
            if (instrument.Meter.Name == MeterName &&
                (instrument.Name == CountName || instrument.Name == DurationName))
            {
                activeListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            recorded.Enqueue(Sample(instrument.Name, measurement, tags)));
        listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, _) =>
            recorded.Enqueue(Sample(instrument.Name, measurement, tags)));
        listener.Start();

        // Use an already-past timestamp to ensure nonnegative durations, without
        // sleeping or relying on precise wall-clock timing.
        var createdAtUtc = DateTimeOffset.UtcNow.AddSeconds(-20);
        var conferenceStartUtc = new DateTimeOffset(2027, 6, 15, 9, 0, 0, TimeSpan.Zero);
        var conference = Conference.Create("M4.3 Metrics Test", conferenceStartUtc, conferenceStartUtc.AddDays(2));
        var session = ConferenceSession.Create(conference.Id, "Distributed Systems", conferenceStartUtc.AddHours(1), conferenceStartUtc.AddHours(2));
        var confirmedParticipant = Participant.Create("Jordan Lee", $"jordan.lee.{Guid.NewGuid():N}@example.com");
        var rejectedParticipant = Participant.Create("Taylor Morgan", $"taylor.morgan.{Guid.NewGuid():N}@example.com");
        var confirmedRegistration = RegistrationEntity.Create(confirmedParticipant.Id, session.Id, createdAtUtc);
        var rejectedRegistration = RegistrationEntity.Create(rejectedParticipant.Id, session.Id, createdAtUtc);

        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.Conferences.Add(conference);
            seedDb.ConferenceSessions.Add(session);
            seedDb.Participants.AddRange(confirmedParticipant, rejectedParticipant);
            seedDb.Registrations.AddRange(confirmedRegistration, rejectedRegistration);
            await seedDb.SaveChangesAsync(cancellationToken);
        }

        var confirmed = DecisionFor(confirmedRegistration.Id, session.Id, AllocationDecidedV1.ConfirmedStatus);
        var rejected = DecisionFor(rejectedRegistration.Id, session.Id, AllocationDecidedV1.RejectedStatus);

        await using (var db = _fixture.CreateDbContext())
        {
            Assert.Equal(AllocationResultReconciliationResult.Applied,
                await new AllocationResultReconciliationService(db).ApplyAsync(confirmed, cancellationToken));
        }
        await using (var db = _fixture.CreateDbContext())
        {
            Assert.Equal(AllocationResultReconciliationResult.DuplicateMessage,
                await new AllocationResultReconciliationService(db).ApplyAsync(confirmed, cancellationToken));
        }
        await using (var db = _fixture.CreateDbContext())
        {
            Assert.Equal(AllocationResultReconciliationResult.DuplicateDecision,
                await new AllocationResultReconciliationService(db).ApplyAsync(
                    confirmed with { MessageId = Guid.NewGuid() }, cancellationToken));
        }
        await using (var db = _fixture.CreateDbContext())
        {
            Assert.Equal(AllocationResultReconciliationResult.Conflict,
                await new AllocationResultReconciliationService(db).ApplyAsync(
                    DecisionFor(confirmedRegistration.Id, session.Id, AllocationDecidedV1.RejectedStatus),
                    cancellationToken));
        }
        await using (var db = _fixture.CreateDbContext())
        {
            Assert.Equal(AllocationResultReconciliationResult.Applied,
                await new AllocationResultReconciliationService(db).ApplyAsync(rejected, cancellationToken));
        }

        await using (var verify = _fixture.CreateDbContext())
        {
            Assert.Equal(RegistrationStatus.Confirmed,
                (await verify.Registrations.AsNoTracking().SingleAsync(
                    x => x.Id == confirmedRegistration.Id, cancellationToken)).Status);
            Assert.Equal(RegistrationStatus.Rejected,
                (await verify.Registrations.AsNoTracking().SingleAsync(
                    x => x.Id == rejectedRegistration.Id, cancellationToken)).Status);
        }

        var samples = recorded.ToArray();
        Assert.Equal(4, samples.Length);
        Assert.All(samples, sample => Assert.True(sample.OnlyOutcomeTag));

        Assert.Equal(1, samples.Count(x => x.Name == CountName && x.Value == 1 && x.Outcome == "confirmed"));
        Assert.Equal(1, samples.Count(x => x.Name == CountName && x.Value == 1 && x.Outcome == "rejected"));
        Assert.Equal(1, samples.Count(x => x.Name == DurationName && x.Value >= 0 && x.Outcome == "confirmed"));
        Assert.Equal(1, samples.Count(x => x.Name == DurationName && x.Value >= 0 && x.Outcome == "rejected"));
    }

    private static AllocationDecidedV1 DecisionFor(Guid registrationId, Guid sessionId, string status) =>
        new(
            Guid.NewGuid(), Guid.NewGuid(), registrationId, sessionId, status,
            status == AllocationDecidedV1.RejectedStatus ? "Session capacity reached." : null,
            DateTimeOffset.UtcNow);

    private static MetricSample Sample(
        string name, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var hasOnlyOutcomeTag = tags.Length == 1 && tags[0].Key == "outcome";
        var outcome = hasOnlyOutcomeTag ? tags[0].Value as string : null;
        return new MetricSample(name, value, outcome, hasOnlyOutcomeTag);
    }

    private readonly record struct MetricSample(
        string Name, double Value, string? Outcome, bool OnlyOutcomeTag);
}

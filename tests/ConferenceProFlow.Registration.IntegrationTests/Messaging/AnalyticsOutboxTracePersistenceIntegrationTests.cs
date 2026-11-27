using System.Diagnostics;
using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Infrastructure.Inbox;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;
using Xunit;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.IntegrationTests.Messaging;

/// <summary>Verifies real SQL persistence of durable W3C context on finalization.</summary>
public sealed class AnalyticsOutboxTracePersistenceIntegrationTests
    : IClassFixture<ResultReconciliationSqlFixture>
{
    private readonly ResultReconciliationSqlFixture _sql;

    public AnalyticsOutboxTracePersistenceIntegrationTests(
        ResultReconciliationSqlFixture sql) => _sql = sql;

    [Fact]
    public async Task Finalization_PersistsConsumerTraceContextWithAnalyticsOutbox()
    {
        var ct = TestContext.Current.CancellationToken;
        var conferenceStart = new DateTimeOffset(2027, 6, 15, 9, 0, 0, TimeSpan.Zero);
        var conference = Conference.Create(
            "M4.2 trace persistence", conferenceStart, conferenceStart.AddDays(2));
        var session = ConferenceSession.Create(
            conference.Id, "Trace-context persistence",
            conferenceStart.AddHours(1), conferenceStart.AddHours(2));
        var participant = Participant.Create(
            "Taylor Morgan", $"taylor.morgan.{Guid.NewGuid():N}@example.com");
        var registration = RegistrationEntity.Create(
            participant.Id, session.Id, DateTimeOffset.UtcNow);

        await using (var db = _sql.CreateDbContext())
        {
            db.Conferences.Add(conference);
            db.ConferenceSessions.Add(session);
            db.Participants.Add(participant);
            db.Registrations.Add(registration);
            await db.SaveChangesAsync(ct);
        }

        const string name = "ConferenceProFlow.Tests.M42AnalyticsPersistence";
        using var source = new ActivitySource(name);
        using var listener = new ActivityListener
        {
            ShouldListenTo = candidate => candidate.Name == name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        var remoteContext = new ActivityContext(
            ActivityTraceId.CreateFromString("4bf92f3577b34da6a3ce929d0e0e4736"),
            ActivitySpanId.CreateFromString("00f067aa0ba902b7"),
            ActivityTraceFlags.Recorded, "vendor=value", isRemote: true);

        var decision = new AllocationDecidedV1(
            Guid.NewGuid(), Guid.NewGuid(), registration.Id, session.Id,
            AllocationDecidedV1.ConfirmedStatus, null, DateTimeOffset.UtcNow);

        using var activity = source.StartActivity(
            "rabbitmq.allocation-result.process", ActivityKind.Consumer, remoteContext);
        Assert.NotNull(activity);
        await using (var db = _sql.CreateDbContext())
        {
            var result = await new AllocationResultReconciliationService(db)
                .ApplyAsync(decision, ct);
            Assert.Equal(AllocationResultReconciliationResult.Applied, result);
        }

        await using (var verify = _sql.CreateDbContext())
        {
            var saved = await verify.AnalyticsOutboxMessages.AsNoTracking()
                .SingleAsync(x => x.RegistrationId == registration.Id, ct);
            Assert.Equal(activity.Id, saved.TraceParent);
            Assert.Equal(activity.TraceStateString, saved.TraceState);
            Assert.Equal(0, saved.PublishAttempts);
            Assert.Null(saved.PublishedAtUtc);
        }
    }
}

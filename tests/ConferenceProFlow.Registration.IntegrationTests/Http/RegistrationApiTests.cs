using System.Net;
using System.Net.Http.Json;
using ConferenceProFlow.Registration.Api.Domain.Enums;
using ConferenceProFlow.Registration.Api.Features.Conferences;
using ConferenceProFlow.Registration.Api.Features.Participants;
using ConferenceProFlow.Registration.Api.Features.Registrations;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using ConferenceProFlow.Registration.IntegrationTests.Infrastructure;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace ConferenceProFlow.Registration.IntegrationTests.Http;

[Collection("SqlServer")]
public sealed class RegistrationApiTests : IDisposable
{
    private static readonly DateTimeOffset ConferenceStart = new(2027, 6, 1, 8, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset ConferenceEnd = ConferenceStart.AddDays(2);

    private readonly SqlServerFixture _fixture;
    private readonly RegistrationApiFactory _factory;
    private readonly HttpClient _client;

    public RegistrationApiTests(SqlServerFixture fixture)
    {
        _fixture = fixture;

        _factory = new RegistrationApiFactory(fixture.ConnectionString);

        _client = _factory.CreateClient(new WebApplicationFactoryClientOptions
            {
                BaseAddress = new Uri("https://localhost")
            });

        // Safety check: the API must use the Testcontainers database.
        using var scope = _factory.Services.CreateScope();

        var db = scope.ServiceProvider.GetRequiredService<RegistrationDbContext>();

        var expected = new SqlConnectionStringBuilder(fixture.ConnectionString);

        var actual = new SqlConnectionStringBuilder(db.Database.GetConnectionString());

        if (!string.Equals(
                expected.DataSource,
                actual.DataSource,
                StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(
                expected.InitialCatalog,
                actual.InitialCatalog,
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("HTTP tests must use the Testcontainers SQL Server database.");
        }
    }

    [Fact]
    public async Task CreateConference_CanRetrieveCreatedConference()
    {
        var conference = await CreateConferenceAsync();

        using var response = await _client.GetAsync($"/api/conferences/{conference.Id}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var saved = await response.Content.ReadFromJsonAsync<ConferenceResponse>();

        Assert.NotNull(saved);
        Assert.Equal(conference.Id, saved.Id);
        Assert.Equal(conference.Name, saved.Name);

        using var listResponse = await _client.GetAsync("/api/conferences");

        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);

        var conferences = await listResponse.Content.ReadFromJsonAsync<List<ConferenceResponse>>();

        Assert.NotNull(conferences);
        Assert.Contains(conferences, item => item.Id == conference.Id);
    }

    [Fact]
    public async Task CreateConference_WithInvalidDates_ReturnsBadRequest()
    {
        var request = new CreateConferenceRequest(
            "Invalid Conference",
            ConferenceEnd,
            ConferenceStart);

        using var response = await _client.PostAsJsonAsync("/api/conferences", request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task CreateSession_CanRetrieveCreatedSession()
    {
        var conference = await CreateConferenceAsync();

        var session = await CreateSessionAsync(conference.Id);

        using var response = await _client.GetAsync($"/api/conferences/{conference.Id}/sessions/{session.Id}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var saved = await response.Content.ReadFromJsonAsync<SessionResponse>();

        Assert.NotNull(saved);
        Assert.Equal(session.Id, saved.Id);
        Assert.Equal(conference.Id, saved.ConferenceId);
        Assert.False(saved.IsPublished);

        using var listResponse = await _client.GetAsync($"/api/conferences/{conference.Id}/sessions");

        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);

        var sessions = await listResponse.Content.ReadFromJsonAsync<List<SessionResponse>>();

        Assert.NotNull(sessions);
        Assert.Contains(sessions, item => item.Id == session.Id);
    }

    [Fact]
    public async Task CreateSession_OutsideConferenceDates_ReturnsBadRequest()
    {
        var conference = await CreateConferenceAsync();

        var request = new CreateSessionRequest(
            "Invalid Session",
            ConferenceEnd.AddDays(1),
            ConferenceEnd.AddDays(1).AddHours(1));

        using var response = await _client.PostAsJsonAsync($"/api/conferences/{conference.Id}/sessions", request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task CreateParticipant_CanRetrieveCreatedParticipant()
    {
        var participant = await CreateParticipantAsync();

        using var response = await _client.GetAsync($"/api/participants/{participant.Id}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var saved = await response.Content.ReadFromJsonAsync<ParticipantResponse>();

        Assert.NotNull(saved);
        Assert.Equal(participant.Id, saved.Id);
        Assert.Equal(participant.Email, saved.Email);
    }

    [Fact]
    public async Task CreateParticipant_WithDuplicateEmail_ReturnsConflict()
    {
        var participant = await CreateParticipantAsync();

        var duplicate = new CreateParticipantRequest("Duplicate Participant", participant.Email.ToUpperInvariant());

        using var response = await _client.PostAsJsonAsync("/api/participants", duplicate);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
    }

    [Fact]
    public async Task CreateParticipant_WithEmptyName_ReturnsBadRequest()
    {
        var request = new CreateParticipantRequest("", "invalid-name@example.com");

        using var response = await _client.PostAsJsonAsync("/api/participants", request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task CreateRegistration_PersistsPendingStatus()
    {
        var conference = await CreateConferenceAsync();
        var session = await CreateSessionAsync(conference.Id);
        var participant = await CreateParticipantAsync();

        var request = new CreateRegistrationRequest(participant.Id, session.Id);

        using var response = await _client.PostAsJsonAsync("/api/registrations", request);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        var registration = await response.Content.ReadFromJsonAsync<RegistrationResponse>();

        Assert.NotNull(registration);

        Assert.Equal(participant.Id, registration.ParticipantId);
        Assert.Equal(session.Id, registration.SessionId);
        Assert.Equal("Pending", registration.Status);
        Assert.Null(registration.RejectionReason);

        // Verify the registration through the GET endpoint.
        using var getResponse = await _client.GetAsync($"/api/registrations/{registration.Id}");

        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        var retrieved = await getResponse.Content.ReadFromJsonAsync<RegistrationResponse>();

        Assert.NotNull(retrieved);
        Assert.Equal(registration.Id, retrieved.Id);
        Assert.Equal("Pending", retrieved.Status);

        // Verify persistence directly in SQL Server.
        await using var db = _fixture.CreateDbContext();

        var stored = await db.Registrations
            .AsNoTracking()
            .SingleAsync(r => r.Id == registration.Id);

        Assert.Equal(RegistrationStatus.Pending, stored.Status);
    }

    [Fact]
    public async Task CreateRegistration_WithDuplicate_ReturnsConflict()
    {
        var conference = await CreateConferenceAsync();
        var session = await CreateSessionAsync(conference.Id);
        var participant = await CreateParticipantAsync();

        var request = new CreateRegistrationRequest(participant.Id, session.Id);

        using var firstResponse = await _client.PostAsJsonAsync("/api/registrations", request);

        Assert.Equal(HttpStatusCode.Created, firstResponse.StatusCode);

        using var secondResponse = await _client.PostAsJsonAsync("/api/registrations", request);

        Assert.Equal(HttpStatusCode.Conflict, secondResponse.StatusCode);
    }

    [Fact]
    public async Task CreateRegistration_WithUnknownParticipant_ReturnsNotFound()
    {
        var conference = await CreateConferenceAsync();
        var session = await CreateSessionAsync(conference.Id);

        var request = new CreateRegistrationRequest(Guid.NewGuid(), session.Id);

        using var response = await _client.PostAsJsonAsync("/api/registrations", request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task CreateRegistration_WithEmptyParticipantId_ReturnsBadRequest()
    {
        var request = new CreateRegistrationRequest(Guid.Empty, Guid.NewGuid());

        using var response = await _client.PostAsJsonAsync("/api/registrations", request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    private async Task<ConferenceResponse> CreateConferenceAsync()
    {
        var request = new CreateConferenceRequest(
            $"Test Conference {Guid.NewGuid():N}",
            ConferenceStart,
            ConferenceEnd);

        using var response = await _client.PostAsJsonAsync("/api/conferences", request);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        var conference = await response.Content.ReadFromJsonAsync<ConferenceResponse>();

        Assert.NotNull(conference);

        return conference;
    }

    private async Task<SessionResponse> CreateSessionAsync(Guid conferenceId)
    {
        var request = new CreateSessionRequest(
            "Distributed Systems with .NET",
            ConferenceStart.AddHours(1),
            ConferenceStart.AddHours(2));

        using var response = await _client.PostAsJsonAsync(
            $"/api/conferences/{conferenceId}/sessions",
            request);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        var session = await response.Content.ReadFromJsonAsync<SessionResponse>();

        Assert.NotNull(session);

        return session;
    }

    private async Task<ParticipantResponse> CreateParticipantAsync()
    {
        var request = new CreateParticipantRequest(
            "Test Participant",
            $"participant-{Guid.NewGuid():N}@example.com");

        using var response = await _client.PostAsJsonAsync("/api/participants", request);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        var participant = await response.Content.ReadFromJsonAsync<ParticipantResponse>();

        Assert.NotNull(participant);

        return participant;
    }

    public void Dispose()
    {
        _client.Dispose();
        _factory.Dispose();
    }
}
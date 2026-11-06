using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Registration.Api.Features.Conferences;

public static class ConferenceEndpoints
{
    public static IEndpointRouteBuilder MapConferenceEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/conferences").WithTags("Conferences");

        // Create conference
        group.MapPost("", async (CreateConferenceRequest request, RegistrationDbContext db, CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name) || request.Name.Trim().Length > 200)
            {
                return ValidationError("name", "Conference name must contain 1 to 200 characters.");
            }

            if (!IsValidInterval(request.StartsAtUtc, request.EndsAtUtc))
            {
                return ValidationError("dates", "Conference dates must use UTC and end after the start.");
            }

            var conference = Conference.Create(
                request.Name,
                request.StartsAtUtc,
                request.EndsAtUtc);

            db.Conferences.Add(conference);

            await db.SaveChangesAsync(ct);

            var response = new ConferenceResponse(
                conference.Id,
                conference.Name,
                conference.StartsAtUtc,
                conference.EndsAtUtc);

            return Results.Created($"/api/conferences/{conference.Id}", response);
        })
        .WithName("CreateConference");

        // Get all conferences
        group.MapGet("", async (RegistrationDbContext db, CancellationToken ct) =>
        {
            var conferences = await db.Conferences
                .AsNoTracking()
                .OrderBy(c => c.StartsAtUtc)
                .ThenBy(c => c.Name)
                .Select(c => new ConferenceResponse(
                    c.Id,
                    c.Name,
                    c.StartsAtUtc,
                    c.EndsAtUtc))
                .ToListAsync(ct);

            return Results.Ok(conferences);
        })
        .WithName("GetConferences");

        // Get conference by ID
        group.MapGet("/{conferenceId:guid}", async (Guid conferenceId, RegistrationDbContext db, CancellationToken ct) =>
        {
            var conference = await db.Conferences
                .AsNoTracking()
                .Where(c => c.Id == conferenceId)
                .Select(c => new ConferenceResponse(
                    c.Id,
                    c.Name,
                    c.StartsAtUtc,
                    c.EndsAtUtc))
                .SingleOrDefaultAsync(ct);

            return conference is null
                ? Results.NotFound()
                : Results.Ok(conference);
        })
        .WithName("GetConferenceById");

        // Create session
        group.MapPost("/{conferenceId:guid}/sessions", async (
            Guid conferenceId,
            CreateSessionRequest request,
            RegistrationDbContext db,
            CancellationToken ct) =>
        {
            var conference = await db.Conferences
                .AsNoTracking()
                .SingleOrDefaultAsync(
                    c => c.Id == conferenceId,
                    ct);

            if (conference is null)
                return Results.NotFound();

            if (string.IsNullOrWhiteSpace(request.Title) || request.Title.Trim().Length > 200)
            {
                return ValidationError("title", "Session title must contain 1 to 200 characters.");
            }

            if (!IsValidInterval(
                request.StartsAtUtc,
                request.EndsAtUtc))
            {
                return ValidationError("dates", "Session dates must use UTC and end after the start.");
            }

            if (request.StartsAtUtc < conference.StartsAtUtc || request.EndsAtUtc > conference.EndsAtUtc)
            {
                return ValidationError("dates", "Session must be within the conference date range.");
            }

            var session = ConferenceSession.Create(
                conferenceId,
                request.Title,
                request.StartsAtUtc,
                request.EndsAtUtc);

            db.ConferenceSessions.Add(session);

            await db.SaveChangesAsync(ct);

            var response = new SessionResponse(
                session.Id,
                session.ConferenceId,
                session.Title,
                session.StartsAtUtc,
                session.EndsAtUtc,
                session.IsPublished);

            return Results.Created($"/api/conferences/{conferenceId}/sessions/{session.Id}", response);
        })
        .WithName("CreateConferenceSession");

        // Get sessions for a conference
        group.MapGet("/{conferenceId:guid}/sessions", async (
            Guid conferenceId,
            RegistrationDbContext db,
            CancellationToken ct) =>
        {
            var conferenceExists = await db.Conferences
                .AnyAsync(c => c.Id == conferenceId, ct);

            if (!conferenceExists)
                return Results.NotFound();

            var sessions = await db.ConferenceSessions
                .AsNoTracking()
                .Where(s => s.ConferenceId == conferenceId)
                .OrderBy(s => s.StartsAtUtc)
                .Select(s => new SessionResponse(
                    s.Id,
                    s.ConferenceId,
                    s.Title,
                    s.StartsAtUtc,
                    s.EndsAtUtc,
                    s.IsPublished))
                .ToListAsync(ct);

            return Results.Ok(sessions);
        })
        .WithName("GetConferenceSessions");

        // Get one session
        group.MapGet(
            "/{conferenceId:guid}/sessions/{sessionId:guid}",
            async (
                Guid conferenceId,
                Guid sessionId,
                RegistrationDbContext db,
                CancellationToken ct) =>
            {
                var session = await db.ConferenceSessions
                    .AsNoTracking()
                    .Where(s =>
                        s.Id == sessionId &&
                        s.ConferenceId == conferenceId)
                    .Select(s => new SessionResponse(
                        s.Id,
                        s.ConferenceId,
                        s.Title,
                        s.StartsAtUtc,
                        s.EndsAtUtc,
                        s.IsPublished))
                    .SingleOrDefaultAsync(ct);

                return session is null
                    ? Results.NotFound()
                    : Results.Ok(session);
            })
        .WithName("GetConferenceSessionById");

        return app;
    }

    private static bool IsValidInterval(DateTimeOffset start, DateTimeOffset end)
    {
        return start.Offset == TimeSpan.Zero &&
               end.Offset == TimeSpan.Zero &&
               end > start;
    }

    private static IResult ValidationError(string property, string message)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [property] = new[] { message }
            });
    }
}
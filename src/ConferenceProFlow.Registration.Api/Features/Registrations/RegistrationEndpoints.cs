using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using RegistrationEntity = ConferenceProFlow.Registration.Api.Domain.Entities.Registration;

namespace ConferenceProFlow.Registration.Api.Features.Registrations;

public static class RegistrationEndpoints
{
    public static IEndpointRouteBuilder MapRegistrationEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/registrations").WithTags("Registrations");

        // Create registration request
        group.MapPost("", async (CreateRegistrationRequest request, RegistrationDbContext db, CancellationToken ct) =>
        {
            if (request.ParticipantId == Guid.Empty)
            {
                return ValidationError("participantId", "ParticipantId cannot be empty.");
            }

            if (request.SessionId == Guid.Empty)
            {
                return ValidationError("sessionId", "SessionId cannot be empty.");
            }

            var participantExists = await db.Participants
                .AnyAsync(p => p.Id == request.ParticipantId, ct);

            if (!participantExists)
            {
                return Results.NotFound(new
                {
                    message = "Participant not found."
                });
            }

            var sessionExists = await db.ConferenceSessions
                .AnyAsync(s => s.Id == request.SessionId, ct);

            if (!sessionExists)
            {
                return Results.NotFound(new
                {
                    message = "Session not found."
                });
            }

            var registrationExists = await db.Registrations
                .AnyAsync(r => r.ParticipantId == request.ParticipantId && r.SessionId == request.SessionId, ct);

            if (registrationExists)
            {
                return Results.Conflict(new
                {
                    message = "Participant is already registered for this session."
                });
            }

            var registration = RegistrationEntity.Create(
                request.ParticipantId,
                request.SessionId,
                DateTimeOffset.UtcNow);

            db.Registrations.Add(registration);

            try
            {
                await db.SaveChangesAsync(ct);
            }
            catch (DbUpdateException ex)
                when (SqlServerErrors.IsUniqueViolation(ex))
            {
                return Results.Conflict(new
                {
                    message = "Participant is already registered for this session."
                });
            }

            return Results.Created($"/api/registrations/{registration.Id}", ToResponse(registration));
        })
        .WithName("CreateRegistration");

        // Get registration by ID
        group.MapGet("/{registrationId:guid}", async (
            Guid registrationId,
            RegistrationDbContext db,
            CancellationToken ct) =>
        {
            var registration = await db.Registrations
                .AsNoTracking()
                .SingleOrDefaultAsync(r => r.Id == registrationId, ct);

            return registration is null ? Results.NotFound() : Results.Ok(ToResponse(registration));
        })
        .WithName("GetRegistrationById");

        return app;
    }

    private static RegistrationResponse ToResponse(RegistrationEntity registration)
    {
        return new RegistrationResponse(
            registration.Id,
            registration.ParticipantId,
            registration.SessionId,
            registration.Status.ToString(),
            registration.CreatedAtUtc,
            registration.UpdatedAtUtc,
            registration.RejectionReason);
    }

    private static IResult ValidationError(string property, string message)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [property] = new[] { message }
            });
    }
}
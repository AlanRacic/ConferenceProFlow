using ConferenceProFlow.Registration.Api.Domain.Entities;
using ConferenceProFlow.Registration.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Registration.Api.Features.Participants;

public static class ParticipantEndpoints
{
    public static IEndpointRouteBuilder MapParticipantEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/participants").WithTags("Participants");

        // Create participant
        group.MapPost("", async (CreateParticipantRequest request, RegistrationDbContext db, CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(request.DisplayName) || request.DisplayName.Trim().Length > 200)
            {
                return ValidationError("displayName", "Display name must contain 1 to 200 characters.");
            }

            if (string.IsNullOrWhiteSpace(request.Email) || request.Email.Trim().Length > 254 || !request.Email.Contains('@'))
            {
                return ValidationError("email", "A valid email address is required (maximum 254 characters).");
            }

            var participant = Participant.Create(request.DisplayName, request.Email);

            var emailExists = await db.Participants.AnyAsync(p => p.NormalizedEmail == participant.NormalizedEmail, ct);

            if (emailExists)
            {
                return Results.Conflict(new
                {
                    message = "A participant with this email already exists."
                });
            }

            db.Participants.Add(participant);

            try
            {
                await db.SaveChangesAsync(ct);
            }
            catch (DbUpdateException ex)
                when (SqlServerErrors.IsUniqueViolation(ex))
            {
                return Results.Conflict(new
                {
                    message = "A participant with this email already exists."
                });
            }

            var response = new ParticipantResponse(participant.Id, participant.DisplayName, participant.Email);

            return Results.Created($"/api/participants/{participant.Id}", response);
        })
        .WithName("CreateParticipant");

        // Get participant by ID
        group.MapGet("/{participantId:guid}", async (Guid participantId, RegistrationDbContext db, CancellationToken ct) =>
        {
            var participant = await db.Participants
                .AsNoTracking()
                .Where(p => p.Id == participantId)
                .Select(p => new ParticipantResponse(
                    p.Id,
                    p.DisplayName,
                    p.Email))
                .SingleOrDefaultAsync(ct);

            return participant is null ? Results.NotFound() : Results.Ok(participant);
        })
        .WithName("GetParticipantById");

        return app;
    }

    private static IResult ValidationError(string property, string message)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [property] = new[] { message }
            });
    }
}
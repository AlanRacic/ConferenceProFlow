using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Persistence;

internal static class SqlServerErrors
{
    public static bool IsUniqueViolation(DbUpdateException exception)
    {
        return exception.InnerException is SqlException
        {
            Number: 2601 or 2627
        };
    }
}
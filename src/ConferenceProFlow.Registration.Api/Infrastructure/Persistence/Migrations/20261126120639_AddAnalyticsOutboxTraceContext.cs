using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace ConferenceProFlow.Registration.Api.Infrastructure.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class AddAnalyticsOutboxTraceContext : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "TraceParent",
                table: "AnalyticsOutboxMessages",
                type: "nvarchar(55)",
                maxLength: 55,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "TraceState",
                table: "AnalyticsOutboxMessages",
                type: "nvarchar(512)",
                maxLength: 512,
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "TraceParent",
                table: "AnalyticsOutboxMessages");

            migrationBuilder.DropColumn(
                name: "TraceState",
                table: "AnalyticsOutboxMessages");
        }
    }
}

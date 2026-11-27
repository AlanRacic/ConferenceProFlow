using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class AddAllocationInboxTraceContext : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "TraceParent",
                table: "InboxMessages",
                type: "nvarchar(55)",
                maxLength: 55,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "TraceState",
                table: "InboxMessages",
                type: "nvarchar(512)",
                maxLength: 512,
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "TraceParent",
                table: "InboxMessages");

            migrationBuilder.DropColumn(
                name: "TraceState",
                table: "InboxMessages");
        }
    }
}

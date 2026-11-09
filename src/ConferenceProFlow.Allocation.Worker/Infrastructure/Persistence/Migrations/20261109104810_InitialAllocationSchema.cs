using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class InitialAllocationSchema : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "SessionInventories",
                columns: table => new
                {
                    SessionId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    Capacity = table.Column<int>(type: "int", nullable: false),
                    ReservedCount = table.Column<int>(type: "int", nullable: false),
                    CreatedAtUtc = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    RowVersion = table.Column<byte[]>(type: "rowversion", rowVersion: true, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SessionInventories", x => x.SessionId);
                    table.CheckConstraint("CK_SessionInventories_Capacity", "[Capacity] > 0");
                    table.CheckConstraint("CK_SessionInventories_ReservedCount", "[ReservedCount] >= 0 AND [ReservedCount] <= [Capacity]");
                });

            migrationBuilder.CreateTable(
                name: "Allocations",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    RegistrationId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    SessionId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    Status = table.Column<int>(type: "int", nullable: false),
                    RejectionReason = table.Column<string>(type: "nvarchar(500)", maxLength: 500, nullable: true),
                    CreatedAtUtc = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Allocations", x => x.Id);
                    table.CheckConstraint("CK_Allocations_RejectionReason", "([Status] = 1 AND [RejectionReason] IS NULL) OR ([Status] = 2 AND [RejectionReason] IS NOT NULL)");
                    table.CheckConstraint("CK_Allocations_Status", "[Status] IN (1, 2)");
                    table.ForeignKey(
                        name: "FK_Allocations_SessionInventories_SessionId",
                        column: x => x.SessionId,
                        principalTable: "SessionInventories",
                        principalColumn: "SessionId",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Allocations_RegistrationId",
                table: "Allocations",
                column: "RegistrationId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Allocations_SessionId",
                table: "Allocations",
                column: "SessionId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Allocations");

            migrationBuilder.DropTable(
                name: "SessionInventories");
        }
    }
}

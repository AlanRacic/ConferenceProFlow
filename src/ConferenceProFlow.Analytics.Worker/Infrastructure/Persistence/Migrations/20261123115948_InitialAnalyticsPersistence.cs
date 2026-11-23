using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class InitialAnalyticsPersistence : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ProcessedAnalyticsEvents",
                columns: table => new
                {
                    MessageId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    RegistrationId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    SessionId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    Status = table.Column<string>(type: "nvarchar(16)", maxLength: 16, nullable: false),
                    OccurredAtUtc = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    ProcessedAtUtc = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ProcessedAnalyticsEvents", x => x.MessageId);
                    table.CheckConstraint("CK_ProcessedAnalyticsEvents_Status", "[Status] IN (N'confirmed', N'rejected')");
                });

            migrationBuilder.CreateTable(
                name: "QuarantinedAnalyticsEvents",
                columns: table => new
                {
                    Topic = table.Column<string>(type: "nvarchar(249)", maxLength: 249, nullable: false),
                    Partition = table.Column<int>(type: "int", nullable: false),
                    Offset = table.Column<long>(type: "bigint", nullable: false),
                    MessageKey = table.Column<string>(type: "nvarchar(256)", maxLength: 256, nullable: true),
                    Payload = table.Column<string>(type: "nvarchar(max)", maxLength: 4096, nullable: false),
                    Reason = table.Column<string>(type: "nvarchar(500)", maxLength: 500, nullable: false),
                    ReceivedAtUtc = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_QuarantinedAnalyticsEvents", x => new { x.Topic, x.Partition, x.Offset });
                    table.CheckConstraint("CK_QuarantinedAnalyticsEvents_Offset", "[Offset] >= 0");
                    table.CheckConstraint("CK_QuarantinedAnalyticsEvents_Partition", "[Partition] >= 0");
                });

            migrationBuilder.CreateTable(
                name: "SessionRegistrationStats",
                columns: table => new
                {
                    SessionId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    ConfirmedCount = table.Column<long>(type: "bigint", nullable: false),
                    RejectedCount = table.Column<long>(type: "bigint", nullable: false),
                    UpdatedAtUtc = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SessionRegistrationStats", x => x.SessionId);
                    table.CheckConstraint("CK_SessionRegistrationStats_ConfirmedCount", "[ConfirmedCount] >= 0");
                    table.CheckConstraint("CK_SessionRegistrationStats_RejectedCount", "[RejectedCount] >= 0");
                });

            migrationBuilder.CreateIndex(
                name: "IX_ProcessedAnalyticsEvents_RegistrationId",
                table: "ProcessedAnalyticsEvents",
                column: "RegistrationId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_ProcessedAnalyticsEvents_SessionId_ProcessedAtUtc",
                table: "ProcessedAnalyticsEvents",
                columns: new[] { "SessionId", "ProcessedAtUtc" });

            migrationBuilder.CreateIndex(
                name: "IX_QuarantinedAnalyticsEvents_ReceivedAtUtc",
                table: "QuarantinedAnalyticsEvents",
                column: "ReceivedAtUtc");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ProcessedAnalyticsEvents");

            migrationBuilder.DropTable(
                name: "QuarantinedAnalyticsEvents");

            migrationBuilder.DropTable(
                name: "SessionRegistrationStats");
        }
    }
}

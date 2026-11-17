using System.Diagnostics;
using ConferenceProFlow.Allocation.IntegrationTests.Infrastructure;
using ConferenceProFlow.Allocation.Worker.Domain.Entities;
using ConferenceProFlow.Allocation.Worker.Domain.Enums;
using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Processing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.IntegrationTests.Processing;

[Collection("AllocationSqlServer")]
public sealed class AllocationInboxProcessorServiceIntegrationTests
{
    private readonly AllocationSqlServerFixture _fixture;

    public AllocationInboxProcessorServiceIntegrationTests(AllocationSqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task PendingInboxMessage_IsAllocatedAutomatically()
    {
        var token = TestContext.Current.CancellationToken;
        var now = DateTimeOffset.UtcNow;
        var sessionId = Guid.NewGuid();
        var messageId = Guid.NewGuid();
        var registrationId = Guid.NewGuid();

        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.SessionInventories.Add(SessionInventory.Create(sessionId, 1, now));
            seedDb.InboxMessages.Add(InboxMessage.Create(
                messageId, registrationId, sessionId, now, now));
            await seedDb.SaveChangesAsync(token);
        }

        using var host = CreateProcessorHost();
        await host.StartAsync(token);
        try
        {
            await WaitUntilAsync(async () =>
            {
                await using var db = _fixture.CreateDbContext();
                return await db.InboxMessages.AsNoTracking().AnyAsync(
                    x => x.Id == messageId && x.ProcessedAtUtc != null, token);
            }, "Inbox processor did not complete the pending message.");
        }
        finally
        {
            await StopHostAsync(host);
        }

        await using var verificationDb = _fixture.CreateDbContext();
        var allocation = await verificationDb.Allocations.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == registrationId, token);
        var inventory = await verificationDb.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == sessionId, token);

        Assert.Equal(AllocationStatus.Confirmed, allocation.Status);
        Assert.Equal(1, inventory.ReservedCount);
    }

    [Fact]
    public async Task MissingInventory_DoesNotBlockReadyMessage_AndIsProcessedWhenCreated()
    {
        var token = TestContext.Current.CancellationToken;
        var now = DateTimeOffset.UtcNow;
        var missingSessionId = Guid.NewGuid();
        var readySessionId = Guid.NewGuid();
        var waitingMessageId = Guid.NewGuid();
        var readyMessageId = Guid.NewGuid();
        var waitingRegistrationId = Guid.NewGuid();
        var readyRegistrationId = Guid.NewGuid();

        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.SessionInventories.Add(
                SessionInventory.Create(readySessionId, 1, now));
            seedDb.InboxMessages.Add(InboxMessage.Create(
                waitingMessageId, waitingRegistrationId, missingSessionId,
                now.AddMinutes(-2), now.AddMinutes(-2)));
            seedDb.InboxMessages.Add(InboxMessage.Create(
                readyMessageId, readyRegistrationId, readySessionId, now, now));
            await seedDb.SaveChangesAsync(token);
        }

        // BatchSize=1 makes this a direct test of starvation prevention.
        using var host = CreateProcessorHost(batchSize: 1);
        await host.StartAsync(token);
        try
        {
            await WaitUntilAsync(async () =>
            {
                await using var db = _fixture.CreateDbContext();
                return await db.InboxMessages.AsNoTracking().AnyAsync(
                    x => x.Id == readyMessageId && x.ProcessedAtUtc != null, token);
            }, "A waiting message blocked a ready registration.");

            await using (var verifyPending = _fixture.CreateDbContext())
            {
                var waiting = await verifyPending.InboxMessages.AsNoTracking()
                    .SingleAsync(x => x.Id == waitingMessageId, token);

                Assert.Null(waiting.ProcessedAtUtc);
                Assert.False(await verifyPending.Allocations.AnyAsync(
                    x => x.RegistrationId == waitingRegistrationId, token));
            }

            // Provision the missing session after the host is already running.
            await using (var setupDb = _fixture.CreateDbContext())
            {
                setupDb.SessionInventories.Add(
                    SessionInventory.Create(missingSessionId, 1, DateTimeOffset.UtcNow));
                await setupDb.SaveChangesAsync(token);
            }

            await WaitUntilAsync(async () =>
            {
                await using var db = _fixture.CreateDbContext();
                return await db.InboxMessages.AsNoTracking().AnyAsync(
                    x => x.Id == waitingMessageId && x.ProcessedAtUtc != null, token);
            }, "Waiting Inbox message did not resume after inventory provisioning.");
        }
        finally
        {
            await StopHostAsync(host);
        }

        await using var finalDb = _fixture.CreateDbContext();
        Assert.Equal(2, await finalDb.Allocations.CountAsync(
            x => x.RegistrationId == waitingRegistrationId ||
                 x.RegistrationId == readyRegistrationId, token));
    }

    [Fact]
    public async Task BatchProcessing_ProcessesAllMessagesAndRespectsCapacity()
    {
        var token = TestContext.Current.CancellationToken;
        var now = DateTimeOffset.UtcNow;
        var sessionId = Guid.NewGuid();
        var requests = Enumerable.Range(0, 5)
            .Select(_ => (MessageId: Guid.NewGuid(), RegistrationId: Guid.NewGuid())).ToArray();

        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.SessionInventories.Add(SessionInventory.Create(sessionId, 2, now));
            foreach (var request in requests)
            {
                seedDb.InboxMessages.Add(InboxMessage.Create(
                    request.MessageId, request.RegistrationId, sessionId, now, now));
            }
            await seedDb.SaveChangesAsync(token);
        }

        using var host = CreateProcessorHost(batchSize: 2);
        await host.StartAsync(token);
        try
        {
            await WaitUntilAsync(async () =>
            {
                await using var db = _fixture.CreateDbContext();
                return await db.InboxMessages.CountAsync(
                    x => x.SessionId == sessionId && x.ProcessedAtUtc != null, token) == 5;
            }, "Inbox processor did not finish all batches.");
        }
        finally
        {
            await StopHostAsync(host);
        }

        await using var verificationDb = _fixture.CreateDbContext();
        var inventory = await verificationDb.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == sessionId, token);
        var allocations = await verificationDb.Allocations.AsNoTracking()
            .Where(x => x.SessionId == sessionId)
            .ToListAsync(token);

        Assert.Equal(2, inventory.ReservedCount);
        Assert.Equal(5, allocations.Count);
        Assert.Equal(2, allocations.Count(x => x.Status == AllocationStatus.Confirmed));
        Assert.Equal(3, allocations.Count(x => x.Status == AllocationStatus.Rejected));
    }

    [Fact]
    public async Task Restart_ResumesMessagesReceivedWhileProcessorWasStopped()
    {
        var token = TestContext.Current.CancellationToken;
        var now = DateTimeOffset.UtcNow;
        var sessionId = Guid.NewGuid();
        var firstMessageId = Guid.NewGuid();
        var secondMessageId = Guid.NewGuid();
        var firstRegistrationId = Guid.NewGuid();
        var secondRegistrationId = Guid.NewGuid();

        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.SessionInventories.Add(SessionInventory.Create(sessionId, 2, now));
            seedDb.InboxMessages.Add(InboxMessage.Create(
                firstMessageId, firstRegistrationId, sessionId, now, now));
            await seedDb.SaveChangesAsync(token);
        }

        using (var firstHost = CreateProcessorHost())
        {
            await firstHost.StartAsync(token);
            try
            {
                await WaitUntilAsync(async () =>
                {
                    await using var db = _fixture.CreateDbContext();
                    return await db.InboxMessages.AsNoTracking().AnyAsync(
                        x => x.Id == firstMessageId && x.ProcessedAtUtc != null, token);
                }, "First host did not process the initial message.");
            }
            finally
            {
                await StopHostAsync(firstHost);
            }
        }

        // The SQL Inbox persists this request even while the host is offline.
        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.InboxMessages.Add(InboxMessage.Create(
                secondMessageId, secondRegistrationId, sessionId,
                DateTimeOffset.UtcNow, DateTimeOffset.UtcNow));
            await seedDb.SaveChangesAsync(token);
        }

        using (var secondHost = CreateProcessorHost())
        {
            await secondHost.StartAsync(token);
            try
            {
                await WaitUntilAsync(async () =>
                {
                    await using var db = _fixture.CreateDbContext();
                    return await db.InboxMessages.AsNoTracking().AnyAsync(
                        x => x.Id == secondMessageId && x.ProcessedAtUtc != null, token);
                }, "Restarted host did not resume pending Inbox work.");
            }
            finally
            {
                await StopHostAsync(secondHost);
            }
        }

        await using var finalDb = _fixture.CreateDbContext();
        var inventory = await finalDb.SessionInventories.AsNoTracking()
            .SingleAsync(x => x.SessionId == sessionId, token);
        Assert.Equal(2, inventory.ReservedCount);
        Assert.Equal(2, await finalDb.Allocations.CountAsync(
            x => x.SessionId == sessionId, token));
    }

    [Fact]
    public async Task FailedMessage_DoesNotBlockAnotherReadyMessage()
    {
        var token = TestContext.Current.CancellationToken;
        var now = DateTimeOffset.UtcNow;
        var badSessionId = Guid.NewGuid();
        var existingSessionId = Guid.NewGuid();
        var readySessionId = Guid.NewGuid();
        var badMessageId = Guid.NewGuid();
        var readyMessageId = Guid.NewGuid();
        var badRegistrationId = Guid.NewGuid();
        var readyRegistrationId = Guid.NewGuid();

        await using (var seedDb = _fixture.CreateDbContext())
        {
            seedDb.SessionInventories.Add(SessionInventory.Create(badSessionId, 1, now));
            var existingInventory = SessionInventory.Create(existingSessionId, 1, now);
            Assert.True(existingInventory.TryReserveSeat());
            seedDb.SessionInventories.Add(existingInventory);
            seedDb.SessionInventories.Add(SessionInventory.Create(readySessionId, 1, now));

            // This Inbox conflicts with an already allocated, different session.
            seedDb.Allocations.Add(AllocationEntity.CreateConfirmed(
                badRegistrationId, existingSessionId, now));
            seedDb.InboxMessages.Add(InboxMessage.Create(
                badMessageId, badRegistrationId, badSessionId,
                now.AddMinutes(-2), now.AddMinutes(-2)));
            seedDb.InboxMessages.Add(InboxMessage.Create(
                readyMessageId, readyRegistrationId, readySessionId, now, now));
            await seedDb.SaveChangesAsync(token);
        }

        // The failing message is older. A short in-memory cooldown lets the next polling iteration select the good message even at BatchSize=1.
        using var host = CreateProcessorHost(batchSize: 1);
        await host.StartAsync(token);
        try
        {
            await WaitUntilAsync(async () =>
            {
                await using var db = _fixture.CreateDbContext();
                return await db.InboxMessages.AsNoTracking().AnyAsync(
                    x => x.Id == readyMessageId && x.ProcessedAtUtc != null, token);
            }, "A failing Inbox message starved a ready message.");
        }
        finally
        {
            await StopHostAsync(host);
        }

        await using var finalDb = _fixture.CreateDbContext();
        var badInbox = await finalDb.InboxMessages.AsNoTracking()
            .SingleAsync(x => x.Id == badMessageId, token);
        var readyAllocation = await finalDb.Allocations.AsNoTracking()
            .SingleAsync(x => x.RegistrationId == readyRegistrationId, token);

        Assert.Null(badInbox.ProcessedAtUtc);
        Assert.Equal(AllocationStatus.Confirmed, readyAllocation.Status);
        Assert.Equal(1, await finalDb.Allocations.CountAsync(
            x => x.RegistrationId == badRegistrationId, token));
    }

    private IHost CreateProcessorHost(int batchSize = 20)
    {
        using var db = _fixture.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()
            ?? throw new InvalidOperationException("Test SQL connection string is missing.");

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<AllocationDbContext>(
            options => options.UseSqlServer(connectionString));
        builder.Services.AddScoped<AllocationProcessingService>();
        builder.Services.Configure<AllocationInboxProcessorOptions>(options =>
        {
            options.Enabled = true;
            options.PollIntervalSeconds = 1;
            options.BatchSize = batchSize;
            options.FailureRetrySeconds = 10;
        });
        builder.Services.AddHostedService<AllocationInboxProcessorService>();
        return builder.Build();
    }

    private static async Task StopHostAsync(IHost host)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await host.StopAsync(timeout.Token);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string failureMessage)
    {
        var token = TestContext.Current.CancellationToken;
        var stopwatch = Stopwatch.StartNew();
        var timeout = TimeSpan.FromSeconds(30);

        while (stopwatch.Elapsed < timeout)
        {
            token.ThrowIfCancellationRequested();

            if (await condition())
            {
                return;
            }

            await Task.Delay(200, token);
        }

        throw new TimeoutException(failureMessage);
    }
}
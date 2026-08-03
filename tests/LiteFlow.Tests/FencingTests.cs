using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// The zombie case: a worker that was too slow, or paused long enough for its lease to expire, comes back
/// and tries to finish. Its acknowledge is fenced by the lease token, and because that acknowledge is
/// inside the step's transaction, everything the zombie wrote goes with it.
/// </summary>
[Collection("postgres")]
public sealed class FencingTests(PostgresFixture fixture)
{
    [Fact]
    public async Task A_worker_that_lost_its_lease_commits_nothing()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("fence");
        long businessId = TestIds.NextBusiness();

        // The zombie: a short lease and no heartbeat, so its step is guaranteed to outlive its claim.
        await using var zombie = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<LinearWorkflow>(w =>
            {
                w.Concurrency = 1;
                w.Lease = TimeSpan.FromSeconds(3);
                w.RenewLease = false;
            }),
            o =>
            {
                o.MaintenanceInterval = TimeSpan.FromSeconds(1);
                // Generous, so the rounds the zombie loses to its own lease cannot exhaust the step and turn
                // this into a test about dead letters (DeadLetterTests covers that).
                o.MaxStepAttempts = 20;
            });

        // The worker that legitimately takes the step over, with a lease it keeps alive.
        await using var successor = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<LinearWorkflow>(w =>
            {
                w.Concurrency = 1;
                w.Lease = TimeSpan.FromSeconds(30);
            }),
            o =>
            {
                o.MaintenanceInterval = TimeSpan.FromSeconds(1);
                o.MaxStepAttempts = 20;
            });

        var zombieScript = zombie.GetRequiredService<StepScript>();
        var successorScript = successor.GetRequiredService<StepScript>();

        var zombieEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var zombieReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // The two workers write different business rows, so which one committed is unambiguous — no
        // counting, no reliance on how many times either of them was handed the step.
        zombieScript.On("s2", async (ctx, ct) =>
        {
            await StepScript.WriteBusinessAsync(ctx, businessId + 1, "zombie", ct);
            zombieEntered.TrySetResult();
            // Deliberately ignores the token: this models a worker frozen (GC pause, suspended VM), not one
            // being asked to stop. Every attempt of this worker outlives its lease, so it can never commit.
            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);
            zombieReturned.TrySetResult();
            return StepResult.Next();
        });

        successorScript.On("s2", async (ctx, ct) =>
        {
            await StepScript.WriteBusinessAsync(ctx, businessId + 2, "successor", ct);
            return StepResult.Next();
        });

        var handle = await TestHelpers.WithClientAsync(zombie,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        var zombieWorkers = await TestHelpers.StartWorkersAsync(zombie);
        await zombieEntered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        // Only now does the successor come up, so the first attempt is unambiguously the zombie's.
        var successorWorkers = await TestHelpers.StartWorkersAsync(successor);

        // Wait for the zombie's attempt to finish and have its acknowledge refused — that is the behaviour
        // under test — then take the zombie out of the picture.
        //
        // It has to go, and not just because it has made its point: every attempt it makes outlives its lease,
        // so leaving it running means it keeps claiming the step and burning the message's attempt budget. Once
        // that budget is gone the queue dead-letters the message, and the engine (rightly) declares the step
        // definitively failed. Whether the successor gets a turn before that would be a race — so the test does
        // not run one.
        await zombieReturned.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await Task.Delay(TimeSpan.FromSeconds(1));
        await TestHelpers.StopWorkersAsync(zombieWorkers);

        var finished = await TestHelpers.WaitForTerminalAsync(
            successor, handle.WorkflowId, TimeSpan.FromSeconds(90));

        await TestHelpers.StopWorkersAsync(successorWorkers);

        Assert.Equal(WorkflowState.Completed, finished.State);

        // The step really was executed by both workers…
        Assert.True(zombieScript.Entries("s2") >= 1, "the zombie never ran the step");
        Assert.Equal(1, successorScript.Entries("s2"));

        // …and committed exactly once. Everything the zombie wrote went back with its fenced acknowledge,
        // however many times it was handed the step.
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "s2"));
        Assert.Equal(0, await TestDb.BusinessAsync(fixture.ConnectionString, businessId + 1));
        Assert.Equal(1, await TestDb.BusinessAsync(fixture.ConnectionString, businessId + 2));
        Assert.Equal(3, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }
}

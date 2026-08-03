using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// The reason the library exists: a process that dies in the middle of a step leaves the workflow exactly
/// where it was, with none of the step's work applied, and another process picks it up at the same step.
/// </summary>
[Collection("postgres")]
public sealed class ResumeAfterCrashTests(PostgresFixture fixture)
{
    [Fact]
    public async Task An_interrupted_step_leaves_no_trace_and_replays_on_another_worker()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("crash");
        long businessId = TestIds.NextBusiness();

        await using var dying = fixture.BuildProvider(s =>
            s.AddLiteFlowWorkflow<LinearWorkflow>(w => w.Concurrency = 1));

        var script = dying.GetRequiredService<StepScript>();
        var wrote = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var never = new TaskCompletionSource();

        // s2 does half its work, announces it, then hangs until the host is stopped underneath it.
        script.On("s2", async (ctx, ct) =>
        {
            await StepScript.WriteBusinessAsync(ctx, businessId, "written by the doomed attempt", ct);
            wrote.TrySetResult();
            await never.Task.WaitAsync(ct);
            return StepResult.Next();
        });

        var handle = await TestHelpers.WithClientAsync(dying,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState { Tag = "crash" }));

        var workers = await TestHelpers.StartWorkersAsync(dying);
        await wrote.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await TestHelpers.StopWorkersAsync(workers);

        // Nothing the interrupted attempt did survived: not its business write, not its step record.
        Assert.Equal(0, await TestDb.BusinessAsync(fixture.ConnectionString, businessId));
        Assert.Equal(0, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "s2"));
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));

        // And the cursor never moved, so the workflow is still owed exactly that step.
        var interrupted = await TestHelpers.WithClientAsync(dying, c => c.GetAsync(handle.WorkflowId));
        Assert.NotNull(interrupted);
        Assert.Equal(WorkflowState.Running, interrupted.State);
        Assert.Equal(1, interrupted.CurrentStep);
        Assert.Equal("s2", interrupted.CurrentStepName);

        // A fresh process — nothing in common with the first but the database — finishes the job.
        await using var reborn = fixture.BuildProvider(s =>
            s.AddLiteFlowWorkflow<LinearWorkflow>(w => w.Concurrency = 1));
        var revived = reborn.GetRequiredService<StepScript>();

        // Same step, same work — this time nothing interrupts it.
        revived.On("s2", async (ctx, ct) =>
        {
            await StepScript.WriteBusinessAsync(ctx, businessId, "written by the attempt that finished", ct);
            return StepResult.Next();
        });

        var restarted = await TestHelpers.StartWorkersAsync(reborn);
        var finished = await TestHelpers.WaitForTerminalAsync(reborn, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(restarted);

        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.Equal(1, revived.Entries("s2"));
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "s2"));
        Assert.Equal(1, await TestDb.BusinessAsync(fixture.ConnectionString, businessId));
        Assert.Equal(3, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }

    [Fact]
    public async Task The_state_a_step_committed_is_what_the_next_step_reads_after_a_restart()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("crash-state");

        await using var dying = fixture.BuildProvider(s =>
            s.AddLiteFlowWorkflow<LinearWorkflow>(w => w.Concurrency = 1));

        var script = dying.GetRequiredService<StepScript>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var never = new TaskCompletionSource();

        script.On("s1", (ctx, ct) =>
        {
            ctx.State.Tag = "left by s1";
            return Task.FromResult(StepResult.Next());
        });
        script.On("s2", async (ctx, ct) =>
        {
            entered.TrySetResult();
            await never.Task.WaitAsync(ct);
            return StepResult.Next();
        });

        var handle = await TestHelpers.WithClientAsync(dying,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(dying);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await TestHelpers.StopWorkersAsync(workers);

        await using var reborn = fixture.BuildProvider(s =>
            s.AddLiteFlowWorkflow<LinearWorkflow>(w => w.Concurrency = 1));
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        reborn.GetRequiredService<StepScript>().On("s2", (ctx, ct) =>
        {
            seen.TrySetResult(ctx.State.Tag);
            return Task.FromResult(StepResult.Next());
        });

        var restarted = await TestHelpers.StartWorkersAsync(reborn);
        string tag = await seen.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await TestHelpers.WaitForTerminalAsync(reborn, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(restarted);

        // The state bag survived the crash exactly as the last committed step left it.
        Assert.Equal("left by s1", tag);
    }
}

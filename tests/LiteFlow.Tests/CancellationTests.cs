using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// Cancellation is a flag in the database, so it is honoured whatever the instance is doing: waiting to be
/// picked up, waiting on a timer, or halfway through a long step.
/// </summary>
[Collection("postgres")]
public sealed class CancellationTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Cancelling_before_the_first_step_runs_executes_nothing_at_all()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("cancel-early");

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());

        // Delayed start: the message exists but is not claimable yet, so the cancellation lands first.
        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(
                new TestState(), new WorkflowStartOptions { Delay = TimeSpan.FromSeconds(3) }));

        Assert.True(await TestHelpers.WithClientAsync(sp,
            c => c.CancelAsync(handle.WorkflowId, "changed my mind")));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Cancelled, finished.State);
        Assert.Equal("changed my mind", finished.CancelReason);
        Assert.Equal(0, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }

    [Fact]
    public async Task Cancelling_between_two_steps_stops_the_sequence_where_it_is()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("cancel-between");

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());
        var script = sp.GetRequiredService<StepScript>();

        // s1 asks for a pause, so the instance sits in Suspended with nothing running when we cancel.
        script.On("s1", (ctx, ct) => Task.FromResult(StepResult.Suspend(TimeSpan.FromSeconds(30))));

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        await TestHelpers.WaitForStateAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60), WorkflowState.Suspended);

        Assert.True(await TestHelpers.WithClientAsync(sp, c => c.CancelAsync(handle.WorkflowId, "stop there")));

        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Cancelled, finished.State);
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
        Assert.Equal(0, script.Entries("s2"));
    }

    [Fact]
    public async Task Cancelling_during_a_long_step_interrupts_it_and_discards_its_work()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("cancel-running");
        long businessId = TestIds.NextBusiness();

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());
        var script = sp.GetRequiredService<StepScript>();

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var never = new TaskCompletionSource();

        script.On("s2", async (ctx, ct) =>
        {
            await StepScript.WriteBusinessAsync(ctx, businessId, "half done", ct);
            entered.TrySetResult();
            // The step honours the token, which is what lets a cancellation interrupt it mid-flight.
            await never.Task.WaitAsync(ct);
            return StepResult.Next();
        });

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.True(await TestHelpers.WithClientAsync(sp,
            c => c.CancelAsync(handle.WorkflowId, "abort mid-step")));

        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Cancelled, finished.State);
        Assert.Equal("abort mid-step", finished.CancelReason);
        Assert.Equal(1, script.Entries("s2"));

        // The interrupted step committed nothing, and the step after it never started.
        Assert.Equal(0, await TestDb.BusinessAsync(fixture.ConnectionString, businessId));
        Assert.Equal(0, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "s2"));
        Assert.Equal(0, script.Entries("s3"));
    }

    [Fact]
    public async Task Cancelling_a_finished_workflow_changes_nothing()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("cancel-done");

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.False(await TestHelpers.WithClientAsync(sp, c => c.CancelAsync(handle.WorkflowId)));

        var after = await TestHelpers.WithClientAsync(sp, c => c.GetAsync(handle.WorkflowId));
        Assert.NotNull(after);
        Assert.Equal(WorkflowState.Completed, after.State);
    }
}

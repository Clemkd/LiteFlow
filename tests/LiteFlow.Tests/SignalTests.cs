using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// Waiting: on a timer, and on the outside world. A parked instance holds no lease and no worker, and the
/// signal that wakes it is recorded once however many times it is delivered.
/// </summary>
[Collection("postgres")]
public sealed class SignalTests(PostgresFixture fixture)
{
    [Fact]
    public async Task A_suspended_step_continues_after_its_delay()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("suspend");

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());
        var script = sp.GetRequiredService<StepScript>();
        script.On("s1", (ctx, ct) => Task.FromResult(StepResult.Suspend(TimeSpan.FromSeconds(2))));

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);

        var suspended = await TestHelpers.WaitForStateAsync(
            sp, handle.WorkflowId, TimeSpan.FromSeconds(30), WorkflowState.Suspended);
        Assert.NotNull(suspended.ResumeAt);
        Assert.Equal(1, suspended.CurrentStep);
        Assert.Equal(0, script.Entries("s2"));

        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.Equal(3, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }

    [Fact]
    public async Task A_waiting_workflow_resumes_with_the_signal_payload()
    {
        SignalWorkflow.DefinitionName = PostgresFixture.NewName("signal");
        SignalWorkflow.SignalName = "go";
        SignalWorkflow.Timeout = null;

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<SignalWorkflow>());

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<SignalWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);

        var waiting = await TestHelpers.WaitForStateAsync(
            sp, handle.WorkflowId, TimeSpan.FromSeconds(30), WorkflowState.WaitingSignal);
        Assert.Equal("go", waiting.WaitSignal);

        var outcome = await TestHelpers.WithClientAsync(sp,
            c => c.SignalAsync(handle.WorkflowId, "go", new { Reference = "SHIP-1" }));
        Assert.Equal(SignalOutcome.Resumed, outcome);

        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.NotNull(finished.StateJson);
        Assert.Contains("SHIP-1", finished.StateJson);
    }

    [Fact]
    public async Task The_same_signal_delivered_twice_wakes_the_workflow_once()
    {
        SignalWorkflow.DefinitionName = PostgresFixture.NewName("signal-dup");
        SignalWorkflow.SignalName = "go";
        SignalWorkflow.Timeout = null;

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<SignalWorkflow>());
        var script = sp.GetRequiredService<StepScript>();

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<SignalWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        await TestHelpers.WaitForStateAsync(
            sp, handle.WorkflowId, TimeSpan.FromSeconds(30), WorkflowState.WaitingSignal);

        Assert.Equal(SignalOutcome.Resumed,
            await TestHelpers.WithClientAsync(sp, c => c.SignalAsync(handle.WorkflowId, "go", "first")));
        Assert.Equal(SignalOutcome.Duplicate,
            await TestHelpers.WithClientAsync(sp, c => c.SignalAsync(handle.WorkflowId, "go", "second")));

        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.Equal(1, script.Entries("s3"));
        Assert.Equal(2, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }

    [Fact]
    public async Task A_signal_that_arrives_before_the_wait_is_not_lost()
    {
        SignalWorkflow.DefinitionName = PostgresFixture.NewName("signal-early");
        SignalWorkflow.SignalName = "go";
        SignalWorkflow.Timeout = null;

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<SignalWorkflow>());
        var script = sp.GetRequiredService<StepScript>();

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource();
        script.On("s1", async (ctx, ct) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(ct);
            return StepResult.Next();
        });

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<SignalWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        // The signal lands while the workflow is still one step short of waiting for it.
        Assert.Equal(SignalOutcome.Recorded,
            await TestHelpers.WithClientAsync(sp, c => c.SignalAsync(handle.WorkflowId, "go", "early")));

        release.TrySetResult();

        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        // It never parked: the wait was already satisfied when it got there.
        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.Contains("early", finished.StateJson ?? "");
    }

    [Fact]
    public async Task A_wait_that_times_out_fails_the_instance()
    {
        SignalWorkflow.DefinitionName = PostgresFixture.NewName("signal-timeout");
        SignalWorkflow.SignalName = "never-comes";
        SignalWorkflow.Timeout = TimeSpan.FromSeconds(2);

        try
        {
            await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<SignalWorkflow>());

            var handle = await TestHelpers.WithClientAsync(sp,
                c => c.StartAsync<SignalWorkflow, TestState>(new TestState()));

            var workers = await TestHelpers.StartWorkersAsync(sp);

            var waiting = await TestHelpers.WaitForStateAsync(
                sp, handle.WorkflowId, TimeSpan.FromSeconds(30), WorkflowState.WaitingSignal);
            Assert.NotNull(waiting.WaitExpiresAt);

            var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
            await TestHelpers.StopWorkersAsync(workers);

            Assert.Equal(WorkflowState.Failed, finished.State);
            Assert.Contains("never-comes", finished.Error ?? "");
        }
        finally
        {
            SignalWorkflow.Timeout = null;
        }
    }

    [Fact]
    public async Task Signalling_an_unknown_instance_says_so()
    {
        SignalWorkflow.DefinitionName = PostgresFixture.NewName("signal-missing");
        SignalWorkflow.Timeout = null;

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<SignalWorkflow>());

        Assert.Equal(SignalOutcome.NotFound,
            await TestHelpers.WithClientAsync(sp, c => c.SignalAsync(Guid.CreateVersion7(), "go")));
    }
}

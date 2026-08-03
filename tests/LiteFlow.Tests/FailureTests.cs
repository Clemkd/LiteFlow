using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// Ending badly: a step that keeps throwing, a step that refuses outright, and the rollback that follows —
/// which is itself durable, so an interrupted rollback resumes instead of restarting.
/// </summary>
[Collection("postgres")]
public sealed class FailureTests(PostgresFixture fixture)
{
    [Fact]
    public async Task A_step_that_keeps_throwing_fails_the_workflow_and_rolls_it_back_in_reverse_order()
    {
        CompensatingWorkflow.DefinitionName = PostgresFixture.NewName("fail-comp");

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<CompensatingWorkflow>());
        var script = sp.GetRequiredService<StepScript>();

        script.On("c3", (ctx, ct) => throw new InvalidOperationException("the third step never works"));

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<CompensatingWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(90));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Failed, finished.State);
        Assert.Contains("never works", finished.Error ?? "");

        // Two attempts were allowed, both threw, and neither committed anything.
        Assert.Equal(2, script.Entries("c3"));
        Assert.Equal(0, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "c3"));

        // The two completed steps were undone, newest first.
        Assert.Equal(
            ["c2", "c1"],
            await TestDb.CompensationOrderAsync(fixture.ConnectionString, handle.WorkflowId));

        // And the trace shows which step gave up, rather than losing it with the rolled-back attempt.
        var steps = await TestHelpers.WithClientAsync(sp, c => c.GetStepsAsync(handle.WorkflowId));
        var failed = Assert.Single(steps, s => s.StepName == "c3");
        Assert.Equal(StepState.Failed, failed.State);
        Assert.Contains("never works", failed.Error ?? "");
        Assert.Equal(StepState.Compensated, Assert.Single(steps, s => s.StepName == "c1").State);
        Assert.Equal(StepState.Compensated, Assert.Single(steps, s => s.StepName == "c2").State);
    }

    [Fact]
    public async Task A_step_that_refuses_fails_immediately_without_burning_its_attempts()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("fail-refuse");

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());
        var script = sp.GetRequiredService<StepScript>();
        script.On("s2", (ctx, ct) => Task.FromResult(StepResult.Fail("the card was declined")));

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Failed, finished.State);
        Assert.Contains("declined", finished.Error ?? "");
        Assert.Equal(1, script.Entries("s2"));
        Assert.Equal(0, script.Entries("s3"));
    }

    [Fact]
    public async Task An_interrupted_rollback_resumes_where_it_stopped()
    {
        CompensatingWorkflow.DefinitionName = PostgresFixture.NewName("rollback-crash");

        await using var dying = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<CompensatingWorkflow>());
        var script = dying.GetRequiredService<StepScript>();

        script.On("c3", (ctx, ct) => throw new InvalidOperationException("boom"));

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var never = new TaskCompletionSource();
        script.OnCompensate("c2", async (ctx, ct) =>
        {
            entered.TrySetResult();
            await never.Task.WaitAsync(ct);
        });

        var handle = await TestHelpers.WithClientAsync(dying,
            c => c.StartAsync<CompensatingWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(dying);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        // The rollback was interrupted: still compensating, and nothing recorded for c2 yet.
        var interrupted = await TestHelpers.WithClientAsync(dying, c => c.GetAsync(handle.WorkflowId));
        Assert.NotNull(interrupted);
        Assert.Equal(WorkflowState.Compensating, interrupted.State);
        Assert.Empty(await TestDb.CompensationOrderAsync(fixture.ConnectionString, handle.WorkflowId));

        // A new process resumes the rollback rather than restarting it.
        await using var reborn = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<CompensatingWorkflow>());
        var restarted = await TestHelpers.StartWorkersAsync(reborn);
        var finished = await TestHelpers.WaitForTerminalAsync(reborn, handle.WorkflowId, TimeSpan.FromSeconds(90));
        await TestHelpers.StopWorkersAsync(restarted);

        Assert.Equal(WorkflowState.Failed, finished.State);
        Assert.Equal(
            ["c2", "c1"],
            await TestDb.CompensationOrderAsync(fixture.ConnectionString, handle.WorkflowId));
    }

    [Fact]
    public async Task A_failed_workflow_can_be_resumed_at_the_step_that_failed()
    {
        CompensatingWorkflow.DefinitionName = PostgresFixture.NewName("fail-resume");

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<CompensatingWorkflow>());
        var script = sp.GetRequiredService<StepScript>();
        script.On("c3", (ctx, ct) => throw new InvalidOperationException("transient outage"));

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<CompensatingWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var failed = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(90));
        Assert.Equal(WorkflowState.Failed, failed.State);

        // The cause is fixed, and the operator puts the instance back to work.
        script.On("c3", (ctx, ct) => Task.FromResult(StepResult.Next()));
        Assert.True(await TestHelpers.WithClientAsync(sp, c => c.ResumeAsync(handle.WorkflowId)));

        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "c3"));
    }
}

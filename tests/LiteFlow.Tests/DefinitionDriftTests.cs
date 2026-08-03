using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// A deployment changes the sequence while instances are in flight. The engine must never run the step
/// that happens to sit at the old index — it parks the instance instead, and a resume re-anchors it by
/// step name.
/// </summary>
[Collection("postgres")]
public sealed class DefinitionDriftTests(PostgresFixture fixture)
{
    [Fact]
    public async Task An_instance_whose_step_moved_is_parked_and_can_be_re_anchored_by_name()
    {
        DriftBeforeWorkflow.DefinitionName = PostgresFixture.NewName("drift");

        // The version the instance is started on: a, b, c.
        await using var before = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<DriftBeforeWorkflow>(w => w.Concurrency = 1));

        var script = before.GetRequiredService<StepScript>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var never = new TaskCompletionSource();
        script.On("b", async (ctx, ct) =>
        {
            entered.TrySetResult();
            await never.Task.WaitAsync(ct);
            return StepResult.Next();
        });

        var handle = await TestHelpers.WithClientAsync(before,
            c => c.StartAsync<DriftBeforeWorkflow, TestState>(new TestState()));

        var oldWorkers = await TestHelpers.StartWorkersAsync(before);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await TestHelpers.StopWorkersAsync(oldWorkers);

        var stopped = await TestHelpers.WithClientAsync(before, c => c.GetAsync(handle.WorkflowId));
        Assert.NotNull(stopped);
        Assert.Equal(1, stopped.CurrentStep);
        Assert.Equal("b", stopped.CurrentStepName);

        // The new version inserts a step in the middle: index 1 is now 'x', not 'b'.
        await using var after = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<DriftAfterWorkflow>(w => w.Concurrency = 1));
        var newScript = after.GetRequiredService<StepScript>();

        var newWorkers = await TestHelpers.StartWorkersAsync(after);
        var parked = await TestHelpers.WaitForStateAsync(
            after, handle.WorkflowId, TimeSpan.FromSeconds(60), WorkflowState.NeedsAttention);

        // Refused rather than guessed: 'x' was never run in 'b''s place.
        Assert.Contains("'b'", parked.Error ?? "");
        Assert.Equal(0, newScript.Entries("x"));
        Assert.Equal(0, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "x"));

        // The operator decides the new code is right for it; the cursor lands on 'b' wherever it now lives.
        Assert.True(await TestHelpers.WithClientAsync(after, c => c.ResumeAsync(handle.WorkflowId)));

        var finished = await TestHelpers.WaitForTerminalAsync(after, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(newWorkers);

        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "a"));
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "b"));
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "c"));

        // 'x' is behind the cursor the instance was re-anchored to, so it is deliberately not run.
        Assert.Equal(0, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "x"));
    }

    [Fact]
    public void Two_workflows_cannot_share_a_name()
    {
        DriftBeforeWorkflow.DefinitionName = PostgresFixture.NewName("clash");

        Assert.Throws<WorkflowDefinitionException>(() =>
            fixture.BuildProvider(s =>
            {
                s.AddLiteFlowWorkflow<DriftBeforeWorkflow>();
                s.AddLiteFlowWorkflow<DriftAfterWorkflow>();
            }));
    }

    [Fact]
    public void A_definition_with_two_steps_of_the_same_name_is_refused_at_startup()
    {
        Assert.Throws<WorkflowDefinitionException>(() =>
            fixture.BuildProvider(s => s.AddLiteFlowWorkflow<DuplicateStepWorkflow>()));
    }
}

/// <summary>A definition that must not compile at runtime: two steps answer to the same name.</summary>
public sealed class DuplicateStepWorkflow : Workflow<TestState>
{
    public override string Name => "duplicate-steps";

    protected override void Configure(IWorkflowBuilder<TestState> b) => b
        .Step("same", (ctx, ct) => StepScript.Run("same", ctx, ct))
        .Step("same", (ctx, ct) => StepScript.Run("same", ctx, ct));
}

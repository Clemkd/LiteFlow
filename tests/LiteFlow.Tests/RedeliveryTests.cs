using LiteFlow.Internal;
using LiteQueue;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// Redelivery is normal, not exceptional: a queue that guarantees at-least-once delivery will hand the same
/// step out twice sooner or later, and the maintenance sweep offers steps again on purpose. The cursor guard
/// is what makes that harmless — including for the steps that run outside the engine's transaction, where it
/// is the only line of defence.
/// </summary>
[Collection("postgres")]
public sealed class RedeliveryTests(PostgresFixture fixture)
{
    [Fact]
    public async Task A_step_message_delivered_again_after_it_was_applied_is_dropped()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("redeliver");

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());
        var script = sp.GetRequiredService<StepScript>();

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        Assert.Equal(WorkflowState.Completed, finished.State);
        await TestHelpers.StopWorkersAsync(workers);

        // Hand step 0 to the queue a second time, exactly as a redelivery or a sweep would.
        await RedispatchAsync(sp, handle.WorkflowId, stepIndex: 0);

        var again = await TestHelpers.StartWorkersAsync(sp);
        // Give the worker room to claim it and decide what to do with it.
        await Task.Delay(TimeSpan.FromSeconds(3));
        await TestHelpers.StopWorkersAsync(again);

        // It was dropped, not applied: nothing ran a second time and the instance is untouched.
        Assert.Equal(1, script.Entries("s1"));
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "s1"));
        Assert.Equal(3, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));

        var after = await TestHelpers.WithClientAsync(sp, c => c.GetAsync(handle.WorkflowId));
        Assert.NotNull(after);
        Assert.Equal(WorkflowState.Completed, after.State);
    }

    [Fact]
    public async Task A_non_transactional_step_redelivered_after_the_cursor_moved_does_not_advance_it_twice()
    {
        ExternalWorkflow.DefinitionName = PostgresFixture.NewName("external");

        await using var sp = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<ExternalWorkflow>(w => { w.Concurrency = 1; w.ExternalConcurrency = 1; }));
        var script = sp.GetRequiredService<StepScript>();

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<ExternalWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Completed, finished.State);
        // The step outside the transaction did run, on its own queue.
        Assert.Equal(1, script.Entries("ext"));
        Assert.Equal(3, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));

        // Now replay it — the window this path really has, between its own commit and its acknowledge.
        await RedispatchAsync(sp, handle.WorkflowId, stepIndex: 1);

        var again = await TestHelpers.StartWorkersAsync(sp);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await TestHelpers.StopWorkersAsync(again);

        Assert.Equal(1, script.Entries("ext"));
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId, "ext"));
        Assert.Equal(3, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }

    [Fact]
    public async Task A_step_message_for_a_workflow_that_no_longer_exists_is_dropped()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("redeliver-ghost");

        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());

        // Nothing was ever started under this id: the message points at a workflow that is not there.
        await RedispatchAsync(sp, Guid.CreateVersion7(), stepIndex: 0);

        var workers = await TestHelpers.StartWorkersAsync(sp);
        await Task.Delay(TimeSpan.FromSeconds(3));
        await TestHelpers.StopWorkersAsync(workers);

        // Dropped quietly rather than retried forever or dead-lettered: there is nothing to do about it.
        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(LinearWorkflow.DefinitionName));
        Assert.Equal(0, stats.Live);
    }

    /// <summary>
    /// Put a step message back on the queue by hand. Uses the engine's own dispatcher so the message is
    /// byte-for-byte what a redelivery would carry.
    /// </summary>
    private static async Task RedispatchAsync(IServiceProvider services, Guid workflowId, int stepIndex)
    {
        await using var scope = services.CreateAsyncScope();
        var catalog = scope.ServiceProvider.GetRequiredService<WorkflowCatalog>();
        var queue = scope.ServiceProvider.GetRequiredService<ILiteQueueClient>();
        var definition = catalog.Definitions.Single();

        await StepDispatcher.DispatchStepAsync(
            queue.Producer, definition, workflowId, definition.Steps[stepIndex],
            priority: 0, maxAttempts: 3, delay: TimeSpan.Zero, CancellationToken.None);
    }
}

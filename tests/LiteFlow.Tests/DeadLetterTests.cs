using LiteQueue;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// The rule these tests exist for: <b>a step that throws definitively fails its workflow, whichever way the
/// engine finds out</b>. The nominal way is the worker's own catch block. The other ways all end with a message
/// in the queue's dead-letter table and a worker that never got to say anything — a host killed at the last
/// attempt, a connection that died before the verdict could be written, a payload nobody can read.
/// <para>
/// The failure these guard against is specific and was real: the maintenance sweep used to re-dispatch any idle
/// instance blindly, and a dead-lettered message has released its dedup key — so a step that had already
/// exhausted its attempts was handed a brand-new budget, and the workflow carried on past a definitive throw.
/// </para>
/// </summary>
[Collection("postgres")]
public sealed class DeadLetterTests(PostgresFixture fixture)
{
    /// <summary>
    /// The scenario in full: every attempt of the step throws, and its worker dies with it each time (the step
    /// closes the connection under itself, so neither the verdict nor even the queue's own "this attempt
    /// failed" can be written). The message therefore reaches the dead-letter table with nobody having recorded
    /// anything about the workflow — and the workflow must still end up <see cref="WorkflowState.Failed"/>,
    /// rolled back, with the step never executed more times than its configured attempts.
    /// </summary>
    [Fact]
    public async Task A_step_that_throws_until_its_attempts_run_out_fails_the_workflow_even_when_no_worker_reports_it()
    {
        CompensatingWorkflow.DefinitionName = PostgresFixture.NewName("dl-throw");

        var dying = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<CompensatingWorkflow>(w =>
            {
                // Two slots: the first attempt's processor dies with the connection, the second one takes the
                // redelivery. No renewal, so an attempt that killed its worker is recovered by lease expiry
                // rather than lingering forever.
                w.Concurrency = 2;
                w.Lease = TimeSpan.FromSeconds(2);
                w.RenewLease = false;
            }),
            o =>
            {
                o.MaintenanceInterval = TimeSpan.FromSeconds(1);
                o.OrphanGracePeriod = TimeSpan.FromSeconds(2);
            });

        var dyingScript = dying.GetRequiredService<StepScript>();

        // c3 is declared with MaxAttempts(2). Killing the connection before throwing is what makes this a
        // *silent* failure: the savepoint rollback, the verdict and the queue's fail call all die with it.
        dyingScript.On("c3", (ctx, ct) =>
        {
            ctx.Connection.Close();
            throw new InvalidOperationException("the third step never works, and takes its host with it");
        });

        Guid id;
        try
        {
            var handle = await TestHelpers.WithClientAsync(dying,
                c => c.StartAsync<CompensatingWorkflow, TestState>(new TestState()));
            id = handle.WorkflowId;

            await TestHelpers.StartWorkersAsync(dying);

            // Wait for the queue to give up on the step: this is the state a host killed at the last attempt
            // leaves behind.
            Assert.True(
                await TestHelpers.WaitUntilAsync(
                    () => TestDb.DeadLettersAsync(fixture.ConnectionString, id, stepIndex: 2)
                        .ContinueWith(t => t.Result > 0),
                    TimeSpan.FromSeconds(60)),
                "the step's message was never dead-lettered");

            // Exactly the configured attempts, no more: the step is not being retried behind our back.
            Assert.Equal(2, dyingScript.Entries("c3"));
        }
        finally
        {
            await dying.DisposeAsync();
        }

        // A healthy process takes over. c3 is deliberately left unscripted here: if anything re-dispatched it,
        // it would succeed and the workflow would complete — which is exactly the bug being guarded against.
        await using var healthy = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<CompensatingWorkflow>(),
            o =>
            {
                o.MaintenanceInterval = TimeSpan.FromSeconds(1);
                o.OrphanGracePeriod = TimeSpan.FromSeconds(2);
            });
        var healthyScript = healthy.GetRequiredService<StepScript>();

        var workers = await TestHelpers.StartWorkersAsync(healthy);
        var finished = await TestHelpers.WaitForTerminalAsync(healthy, id, TimeSpan.FromSeconds(90));
        await TestHelpers.StopWorkersAsync(workers);

        // The verdict the dead worker never wrote.
        Assert.Equal(WorkflowState.Failed, finished.State);
        Assert.Contains("never works", finished.Error ?? "");

        // The step was never given another chance, by the sweep or by anyone else.
        Assert.Equal(0, healthyScript.Entries("c3"));
        Assert.Equal(0, await TestDb.ExecutionsAsync(fixture.ConnectionString, id, "c3"));

        // And the rollback ran, in reverse, exactly as it would have on the nominal path.
        Assert.Equal(["c2", "c1"], await TestDb.CompensationOrderAsync(fixture.ConnectionString, id));

        var steps = await TestHelpers.WithClientAsync(healthy, c => c.GetStepsAsync(id));
        var failedStep = Assert.Single(steps, s => s.StepName == "c3");
        Assert.Equal(StepState.Failed, failedStep.State);
    }

    /// <summary>
    /// A step that declares itself poison must fail the workflow on the spot: no further attempt, and no
    /// second thoughts from the sweep afterwards.
    /// </summary>
    [Fact]
    public async Task A_step_that_declares_itself_poison_fails_the_workflow_immediately()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("dl-poison");

        await using var sp = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<LinearWorkflow>(),
            o =>
            {
                o.MaintenanceInterval = TimeSpan.FromSeconds(1);
                o.OrphanGracePeriod = TimeSpan.FromSeconds(1);
            });

        var script = sp.GetRequiredService<StepScript>();
        script.On("s2", (ctx, ct) =>
            throw new PoisonMessageException("this payload can never be processed"));

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));

        Assert.Equal(WorkflowState.Failed, finished.State);
        Assert.Contains("never be processed", finished.Error ?? "");
        Assert.Equal(1, script.Entries("s2"));

        // Give the sweep several passes to prove it leaves the verdict alone.
        await Task.Delay(TimeSpan.FromSeconds(4));
        await TestHelpers.StopWorkersAsync(workers);

        var after = await TestHelpers.WithClientAsync(sp, c => c.GetAsync(handle.WorkflowId));
        Assert.NotNull(after);
        Assert.Equal(WorkflowState.Failed, after.State);
        Assert.Equal(1, script.Entries("s2"));
        Assert.Equal(0, script.Entries("s3"));
        Assert.Equal(1, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }

    /// <summary>
    /// The reconciliation rule in isolation: given a dead letter for the step an instance is on, the sweep must
    /// write the verdict and must not put a new message in the queue.
    /// </summary>
    [Fact]
    public async Task The_sweep_never_re_dispatches_a_step_whose_message_was_dead_lettered()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("dl-nodispatch");
        string queue = "wf:" + LinearWorkflow.DefinitionName;

        await using var sp = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<LinearWorkflow>(),
            o =>
            {
                o.MaintenanceInterval = TimeSpan.FromSeconds(1);
                o.OrphanGracePeriod = TimeSpan.FromSeconds(1);
            });

        var script = sp.GetRequiredService<StepScript>();

        // No workers yet, so the first step's message is still sitting in the queue.
        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));
        Assert.Equal(1, await TestDb.PendingMessagesAsync(fixture.ConnectionString, handle.WorkflowId, 0));

        // Replace it with a dead letter: the state left by a worker that exhausted the step and then died.
        await TestDb.DropMessageAsync(fixture.ConnectionString, handle.WorkflowId, 0);
        await TestDb.RecordDeadLetterAsync(
            fixture.ConnectionString, queue, handle.WorkflowId, 0, "the first step gave up for good");

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await Task.Delay(TimeSpan.FromSeconds(3));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Failed, finished.State);
        Assert.Contains("gave up for good", finished.Error ?? "");

        // Nothing was queued and nothing ran — the two ways the old behaviour would have shown itself.
        Assert.Equal(0, await TestDb.PendingMessagesAsync(fixture.ConnectionString, handle.WorkflowId, 0));
        Assert.Equal(0, script.Entries("s1"));
        Assert.Equal(0, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));

        var failedStep = Assert.Single(
            await TestHelpers.WithClientAsync(sp, c => c.GetStepsAsync(handle.WorkflowId)));
        Assert.Equal(StepState.Failed, failedStep.State);
        Assert.Equal("s1", failedStep.StepName);
    }

    /// <summary>
    /// The other half of the rule, and a non-regression guard: the self-healing net must survive the fix. An
    /// instance whose message was genuinely lost — no dead letter to explain it — is still picked back up.
    /// </summary>
    [Fact]
    public async Task The_sweep_re_dispatches_a_step_whose_message_was_genuinely_lost()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("dl-lost");

        await using var sp = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<LinearWorkflow>(),
            o =>
            {
                o.MaintenanceInterval = TimeSpan.FromSeconds(1);
                o.OrphanGracePeriod = TimeSpan.FromSeconds(1);
            });

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState()));

        // Lose the message, and leave no dead letter: nobody ever reported a failure, the work simply vanished.
        await TestDb.DropMessageAsync(fixture.ConnectionString, handle.WorkflowId, 0);
        Assert.Equal(0, await TestDb.PendingMessagesAsync(fixture.ConnectionString, handle.WorkflowId, 0));
        Assert.Equal(0, await TestDb.DeadLettersAsync(fixture.ConnectionString, handle.WorkflowId, 0));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.Equal(3, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }

    /// <summary>
    /// A rollback that cannot finish must be parked, not retried forever and not reported as a rollback that
    /// worked. <see cref="WorkflowState.NeedsAttention"/> is the honest answer.
    /// </summary>
    [Fact]
    public async Task A_dead_lettered_compensation_parks_the_workflow_instead_of_looping()
    {
        CompensatingWorkflow.DefinitionName = PostgresFixture.NewName("dl-comp");
        string queue = "wf:" + CompensatingWorkflow.DefinitionName;

        Guid id;
        await using (var dying = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<CompensatingWorkflow>()))
        {
            var script = dying.GetRequiredService<StepScript>();
            script.On("c3", (ctx, ct) => throw new InvalidOperationException("boom"));

            // Hold the rollback open so the instance is left in Compensating with its message still queued.
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var never = new TaskCompletionSource();
            script.OnCompensate("c2", async (ctx, ct) =>
            {
                entered.TrySetResult();
                await never.Task.WaitAsync(ct);
            });

            var handle = await TestHelpers.WithClientAsync(dying,
                c => c.StartAsync<CompensatingWorkflow, TestState>(new TestState()));
            id = handle.WorkflowId;

            var workers = await TestHelpers.StartWorkersAsync(dying);
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(60));
            await TestHelpers.StopWorkersAsync(workers);
        }

        var compensating = await TestHelpers.WithClientAsync(
            fixture.BuildProvider(s => s.AddLiteFlowWorkflow<CompensatingWorkflow>()),
            c => c.GetAsync(id));
        Assert.NotNull(compensating);
        Assert.Equal(WorkflowState.Compensating, compensating.State);

        // The compensation's message is dead-lettered: its rollback can never complete on its own.
        await TestDb.DropMessageAsync(fixture.ConnectionString, id, 1, compensation: true);
        await TestDb.RecordDeadLetterAsync(
            fixture.ConnectionString, queue, id, 1, "the refund keeps failing", compensation: true);

        await using var healthy = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<CompensatingWorkflow>(),
            o =>
            {
                o.MaintenanceInterval = TimeSpan.FromSeconds(1);
                o.OrphanGracePeriod = TimeSpan.FromSeconds(1);
            });

        var workers2 = await TestHelpers.StartWorkersAsync(healthy);
        var parked = await TestHelpers.WaitForStateAsync(
            healthy, id, TimeSpan.FromSeconds(60), WorkflowState.NeedsAttention);

        // Several more sweeps: the state has to be stable, not a loop.
        await Task.Delay(TimeSpan.FromSeconds(4));
        await TestHelpers.StopWorkersAsync(workers2);

        Assert.Equal(WorkflowState.NeedsAttention, parked.State);
        Assert.Contains("compensation", parked.Error ?? "", StringComparison.OrdinalIgnoreCase);
        Assert.Contains("c2", parked.Error ?? "");

        var after = await TestHelpers.WithClientAsync(healthy, c => c.GetAsync(id));
        Assert.NotNull(after);
        Assert.Equal(WorkflowState.NeedsAttention, after.State);

        // The rollback never completed, and was never faked: c2's compensation committed nothing.
        Assert.Empty(await TestDb.CompensationOrderAsync(fixture.ConnectionString, id));
        Assert.Equal(0, await TestDb.PendingMessagesAsync(fixture.ConnectionString, id, 1, compensation: true));
    }
}

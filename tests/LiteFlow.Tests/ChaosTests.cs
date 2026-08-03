using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// The acceptance test for the whole design: a fleet of workers being stopped, killed and replaced while a
/// few hundred workflows run through them. Two invariants must hold at the end, and neither is
/// timing-dependent: every instance reached a terminal state, and every step of every instance committed
/// exactly once.
/// <para>
/// Half the restarts are graceful (the host is asked to stop, so steps in flight are abandoned and go back
/// to the queue) and half are hard kills: the container is disposed under the running steps, so their
/// connections die mid-transaction and nothing is handed back — exactly what a <c>kill -9</c> does. Those
/// steps come back only because their lease expires and the sweep recovers them.
/// </para>
/// <para>
/// Remove the fenced acknowledge, or move the cursor advance out of the step's transaction, and this test
/// fails with duplicate executions.
/// </para>
/// </summary>
[Collection("postgres")]
public sealed class ChaosTests(PostgresFixture fixture)
{
    private const int Instances = 200;

    private const int Fleet = 3;

    /// <summary>Slows each step just enough that the run outlives the restarts happening to it.</summary>
    private static readonly TimeSpan StepWork = TimeSpan.FromMilliseconds(25);

    [Fact]
    public async Task Under_random_worker_restarts_every_step_still_runs_exactly_once()
    {
        FanWorkflow.DefinitionName = PostgresFixture.NewName("chaos");

        // Fixed seed: a failure has to be reproducible to be worth anything.
        var random = new Random(20260803);
        var fleet = new List<Worker>();
        int graceful = 0;
        int killed = 0;

        try
        {
            for (int i = 0; i < Fleet; i++)
                fleet.Add(await StartWorkerAsync());

            var ids = new List<Guid>(Instances);
            await using (var scope = fleet[0].Provider.CreateAsyncScope())
            {
                var client = scope.ServiceProvider.GetRequiredService<ILiteFlowClient>();
                for (int i = 0; i < Instances; i++)
                {
                    var handle = await client.StartAsync<FanWorkflow, TestState>(
                        new TestState { Tag = $"chaos-{i}" },
                        new WorkflowStartOptions { IdempotencyKey = $"chaos-{FanWorkflow.DefinitionName}-{i}" });
                    ids.Add(handle.WorkflowId);
                }
            }

            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(3);

            // Keep churning the fleet while there is work left, and for a few rounds regardless — the
            // interesting failures happen when a worker dies with a step half-applied.
            while (DateTime.UtcNow < deadline)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(300 + random.Next(400)));

                bool done = await AllTerminalAsync(fleet[0].Provider, ids);
                if (done && graceful + killed >= 6)
                    break;

                // One always stays alive, so progress never depends on the recovery timers alone.
                int victim = random.Next(fleet.Count);
                var target = fleet[victim];
                fleet.RemoveAt(victim);

                if (random.Next(2) == 0)
                {
                    await target.StopGracefullyAsync();
                    graceful++;
                }
                else
                {
                    await target.KillAsync();
                    killed++;
                }

                fleet.Add(await StartWorkerAsync());
            }

            Assert.True(graceful > 0, "the chaos loop never stopped a worker gracefully");
            Assert.True(killed > 0, "the chaos loop never killed a worker");

            // Give the survivors room to finish what the last restart interrupted.
            Assert.True(
                await TestHelpers.WaitUntilAsync(
                    () => AllTerminalAsync(fleet[0].Provider, ids), TimeSpan.FromMinutes(2)),
                await DescribeStragglersAsync(fleet[0].Provider, ids));

            // Invariant 1: every instance finished, and finished well.
            await using (var scope = fleet[0].Provider.CreateAsyncScope())
            {
                var client = scope.ServiceProvider.GetRequiredService<ILiteFlowClient>();
                foreach (var id in ids)
                {
                    var instance = await client.GetAsync(id);
                    Assert.NotNull(instance);
                    Assert.Equal(WorkflowState.Completed, instance.State);
                }
            }

            // Invariant 2: no step of any instance ever committed twice, however many times it ran.
            Assert.Empty(await TestDb.DuplicateExecutionsAsync(fixture.ConnectionString, ids));

            // Invariant 3: nothing was skipped either.
            foreach (var id in ids)
                Assert.Equal(FanWorkflow.Steps.Length, await TestDb.ExecutionsAsync(fixture.ConnectionString, id));
        }
        finally
        {
            foreach (var worker in fleet)
                await worker.KillAsync();
        }
    }

    private async Task<Worker> StartWorkerAsync()
    {
        var provider = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<FanWorkflow>(w => w.Concurrency = 4),
            o =>
            {
                // A killed worker's step comes back only when its lease expires, so the lease and the sweep
                // have to be short enough for the run to make progress between restarts.
                o.StepLease = TimeSpan.FromSeconds(4);
                o.MaintenanceInterval = TimeSpan.FromSeconds(1);
                o.OrphanGracePeriod = TimeSpan.FromSeconds(10);
            });

        var script = provider.GetRequiredService<StepScript>();
        foreach (string step in FanWorkflow.Steps)
        {
            script.On(step, async (ctx, ct) =>
            {
                await Task.Delay(StepWork, ct);
                return StepResult.Next();
            });
        }

        var hosted = await TestHelpers.StartWorkersAsync(provider);
        return new Worker(provider, hosted);
    }

    private static async Task<bool> AllTerminalAsync(IServiceProvider services, List<Guid> ids)
    {
        await using var scope = services.CreateAsyncScope();
        var client = scope.ServiceProvider.GetRequiredService<ILiteFlowClient>();

        foreach (var id in ids)
        {
            var instance = await client.GetAsync(id);
            if (instance is null || !instance.IsTerminal)
                return false;
        }
        return true;
    }

    private static async Task<string> DescribeStragglersAsync(IServiceProvider services, List<Guid> ids)
    {
        await using var scope = services.CreateAsyncScope();
        var client = scope.ServiceProvider.GetRequiredService<ILiteFlowClient>();

        var lines = new List<string>();
        foreach (var id in ids)
        {
            var instance = await client.GetAsync(id);
            if (instance is null || !instance.IsTerminal)
                lines.Add($"{id} → {instance?.State.ToString() ?? "missing"} at step " +
                          $"{instance?.CurrentStepName ?? "?"} ({instance?.Error})");
        }

        return $"{lines.Count} instance(s) never finished: {string.Join("; ", lines.Take(10))}";
    }

    private sealed record Worker(ServiceProvider Provider, List<IHostedService> Hosted)
    {
        /// <summary>A rolling deploy: the host drains, so steps in flight are handed back untouched.</summary>
        public async Task StopGracefullyAsync()
        {
            await TestHelpers.StopWorkersAsync(Hosted);
            await Provider.DisposeAsync();
        }

        /// <summary>
        /// A crash: the container goes away under the running steps, so their connections die mid-transaction
        /// and nobody tells the queue anything. Recovery has to come from the lease alone.
        /// </summary>
        public async Task KillAsync()
        {
            try
            {
                await Provider.DisposeAsync();
            }
            catch (Exception)
            {
                // Disposing a container out from under its own background services is the point of the
                // exercise; whatever it throws on the way down is not the test's business.
            }
        }
    }
}

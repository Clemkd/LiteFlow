using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// Many workers, one shared set of instances: every step of every instance runs once, and the steps of a
/// single instance never overlap.
/// </summary>
[Collection("postgres")]
public sealed class ConcurrencyTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Three_workers_run_each_step_of_each_instance_exactly_once()
    {
        FanWorkflow.DefinitionName = PostgresFixture.NewName("fan");
        const int instances = 24;

        var providers = Enumerable.Range(0, 3)
            .Select(_ => fixture.BuildProvider(s => s.AddLiteFlowWorkflow<FanWorkflow>(w => w.Concurrency = 4)))
            .ToList();

        try
        {
            var ids = new List<Guid>(instances);
            for (int i = 0; i < instances; i++)
            {
                var handle = await TestHelpers.WithClientAsync(providers[0],
                    c => c.StartAsync<FanWorkflow, TestState>(new TestState { Tag = $"i{i}" }));
                ids.Add(handle.WorkflowId);
            }

            var workers = new List<Microsoft.Extensions.Hosting.IHostedService>();
            foreach (var provider in providers)
                workers.AddRange(await TestHelpers.StartWorkersAsync(provider));

            foreach (var id in ids)
            {
                var finished = await TestHelpers.WaitForTerminalAsync(
                    providers[0], id, TimeSpan.FromSeconds(120));
                Assert.Equal(WorkflowState.Completed, finished.State);
            }

            await TestHelpers.StopWorkersAsync(workers);

            Assert.Empty(await TestDb.DuplicateExecutionsAsync(fixture.ConnectionString, ids));

            foreach (var id in ids)
                Assert.Equal(FanWorkflow.Steps.Length, await TestDb.ExecutionsAsync(fixture.ConnectionString, id));
        }
        finally
        {
            foreach (var provider in providers)
                await provider.DisposeAsync();
        }
    }

    [Fact]
    public async Task The_steps_of_one_instance_never_overlap()
    {
        FanWorkflow.DefinitionName = PostgresFixture.NewName("fan-serial");

        await using var sp = fixture.BuildProvider(
            s => s.AddLiteFlowWorkflow<FanWorkflow>(w => w.Concurrency = 4));

        var script = sp.GetRequiredService<StepScript>();
        int inFlight = 0;
        int overlaps = 0;

        foreach (string step in FanWorkflow.Steps)
        {
            script.On(step, async (ctx, ct) =>
            {
                if (Interlocked.Increment(ref inFlight) > 1)
                    Interlocked.Increment(ref overlaps);
                await Task.Delay(40, ct);
                Interlocked.Decrement(ref inFlight);
                return StepResult.Next();
            });
        }

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<FanWorkflow, TestState>(new TestState()));

        var workers = await TestHelpers.StartWorkersAsync(sp);
        var finished = await TestHelpers.WaitForTerminalAsync(sp, handle.WorkflowId, TimeSpan.FromSeconds(60));
        await TestHelpers.StopWorkersAsync(workers);

        Assert.Equal(WorkflowState.Completed, finished.State);
        Assert.Equal(0, overlaps);
        Assert.Equal(4, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }
}

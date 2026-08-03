using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;

namespace LiteFlow.Benchmarks;

/// <summary>
/// What recovering from an incident costs. Two numbers nobody publishes and everybody needs:
/// <list type="bullet">
/// <item>
/// <b>Graceful hand-back.</b> A worker is asked to stop while it holds steps; they go back to the queue
/// without spending an attempt, and another worker takes them immediately. This is the redeploy case, and it
/// should be almost free.
/// </item>
/// <item>
/// <b>Lease recovery.</b> A worker disappears without saying anything. Nothing can happen until its lease
/// expires and the sweep notices, so the floor here is <c>StepLease</c> plus one sweep interval — the two
/// knobs that decide how long a crash costs you.
/// </item>
/// </list>
/// </summary>
[SimpleJob(warmupCount: 0, iterationCount: 3, invocationCount: 1, launchCount: 1)]
public class RecoveryBenchmarks
{
    private const int Instances = 20;

    private BenchEnv _env = null!;

    [GlobalSetup]
    public void Setup()
    {
        BenchWorkflow.DefinitionName = "bench-recovery";
        _env = new BenchEnv();
        // No workers yet: each iteration starts and stops its own, which is the thing being measured.
        _env.StartAsync(concurrency: 4, withWorkers: false).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup() => _env.DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// <summary>
    /// Start work, stop the workers mid-flight, then bring workers back and time how long the remaining steps
    /// take to finish. The steps interrupted by the shutdown were handed back, so this should be dominated by
    /// the work itself rather than by any timer.
    /// </summary>
    [Benchmark(Description = "restart after a graceful stop")]
    public async Task GracefulRestart()
    {
        var ids = await SeedAsync();

        var first = await BenchEnv.StartWorkersAsync(_env.Services);
        await Task.Delay(200);
        await BenchEnv.StopWorkersAsync(first);

        var second = await BenchEnv.StartWorkersAsync(_env.Services);
        try
        {
            if (!await _env.WaitForAllAsync(ids, TimeSpan.FromMinutes(2)))
                throw new InvalidOperationException("The workflows did not finish after the restart.");
        }
        finally
        {
            await BenchEnv.StopWorkersAsync(second);
        }
    }

    /// <summary>
    /// The same run, except the workers vanish instead of stopping: their steps stay leased until the lease
    /// expires. The gap this measures is the real cost of a crash, and the reason
    /// <see cref="LiteFlowOptions.StepLease"/> is worth tuning to how long your steps actually take.
    /// </summary>
    [Benchmark(Description = "recovery through lease expiry")]
    public async Task LeaseRecovery()
    {
        var ids = await SeedAsync();

        // Start workers, let them claim, then abandon them without a shutdown: the claimed steps are now held
        // by nobody, exactly as after a kill -9.
        var doomed = await BenchEnv.StartWorkersAsync(_env.Services);
        await Task.Delay(200);
        _ = doomed; // deliberately never stopped

        var replacement = await BenchEnv.StartWorkersAsync(_env.Services);
        try
        {
            if (!await _env.WaitForAllAsync(ids, TimeSpan.FromMinutes(3)))
                throw new InvalidOperationException("The workflows did not recover from the lease expiry.");
        }
        finally
        {
            await BenchEnv.StopWorkersAsync(replacement);
        }
    }

    private async Task<List<Guid>> SeedAsync()
    {
        var ids = new List<Guid>(Instances);
        for (int i = 0; i < Instances; i++)
        {
            var handle = await _env.WithClientAsync(
                c => c.StartAsync<BenchWorkflow, BenchState>(new BenchState()));
            ids.Add(handle.WorkflowId);
        }
        return ids;
    }
}

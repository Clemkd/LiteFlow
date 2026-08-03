using BenchmarkDotNet.Attributes;

namespace LiteFlow.Benchmarks;

/// <summary>
/// How many steps per second a fleet moves, and what one workflow's end-to-end latency looks like.
/// <para>
/// The two are different questions and are measured separately on purpose. Throughput is what a batch of
/// work costs: it amortises commits across workers, and the number to watch when sizing a fleet. Latency is
/// what a single instance experiences: it is dominated by the round-trip between steps, and no amount of
/// concurrency improves it.
/// </para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3, invocationCount: 1, launchCount: 1)]
public class ThroughputBenchmarks
{
    private BenchEnv _env = null!;

    /// <summary>Workers in the process. The interesting curve is how far the claim path scales before the database does not.</summary>
    [Params(1, 4, 8)]
    public int Concurrency { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        BenchWorkflow.DefinitionName = $"bench-throughput-{Concurrency}";
        _env = new BenchEnv();
        _env.StartAsync(Concurrency, withWorkers: true).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup() => _env.DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// <summary>
    /// 200 instances × 4 steps = 800 durable steps, each its own transaction. Divide the reported time by 800
    /// for the per-step cost, which is the number that matters when sizing.
    /// </summary>
    [Benchmark(Description = "800 steps through the fleet")]
    public async Task Steps()
    {
        var ids = new List<Guid>(200);
        for (int i = 0; i < 200; i++)
        {
            var handle = await _env.WithClientAsync(
                c => c.StartAsync<BenchWorkflow, BenchState>(new BenchState()));
            ids.Add(handle.WorkflowId);
        }

        if (!await _env.WaitForAllAsync(ids, TimeSpan.FromMinutes(2)))
            throw new InvalidOperationException("The fleet did not drain within the benchmark's deadline.");
    }

    /// <summary>
    /// One instance from start to finish, with nothing else in the queue: four steps, four transactions, and
    /// the wake-up latency between them. This is the number a user of a single workflow feels.
    /// </summary>
    [Benchmark(Description = "one instance end to end")]
    public async Task SingleInstanceLatency()
    {
        var handle = await _env.WithClientAsync(
            c => c.StartAsync<BenchWorkflow, BenchState>(new BenchState()));

        if (!await _env.WaitForAllAsync([handle.WorkflowId], TimeSpan.FromSeconds(60)))
            throw new InvalidOperationException("The instance did not finish within the benchmark's deadline.");
    }
}

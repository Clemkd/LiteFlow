using BenchmarkDotNet.Running;
using LiteFlow.Benchmarks;

// Docker is required: every benchmark brings up its own PostgreSQL container so the numbers are comparable
// across machines and nobody has to prepare a database first.
//
//   dotnet run -c Release --project benchmarks/LiteFlow.Benchmarks
//   dotnet run -c Release --project benchmarks/LiteFlow.Benchmarks -- --filter *Throughput*
//   dotnet run -c Release --project benchmarks/LiteFlow.Benchmarks -- --smoke
//
// --smoke runs one iteration of each scenario outside BenchmarkDotNet, to check the harness itself works.
if (args.Contains("--smoke"))
    return await Smoke.RunAsync();

BenchmarkSwitcher
    .FromTypes([typeof(ThroughputBenchmarks), typeof(RecoveryBenchmarks)])
    .Run(args);

return 0;

internal static class Smoke
{
    public static async Task<int> RunAsync()
    {
        Console.WriteLine("Smoke run: 20 instances through a single-worker fleet.");

        BenchWorkflow.DefinitionName = "bench-smoke";
        await using var env = new BenchEnv();
        await env.StartAsync(concurrency: 2, withWorkers: true);

        var started = DateTime.UtcNow;
        var ids = new List<Guid>(20);
        for (int i = 0; i < 20; i++)
        {
            var handle = await env.WithClientAsync(
                c => c.StartAsync<BenchWorkflow, BenchState>(new BenchState()));
            ids.Add(handle.WorkflowId);
        }

        bool drained = await env.WaitForAllAsync(ids, TimeSpan.FromMinutes(1));
        var elapsed = DateTime.UtcNow - started;

        int steps = ids.Count * BenchWorkflow.StepCount;
        Console.WriteLine(drained
            ? $"{steps} steps in {elapsed.TotalMilliseconds:0} ms " +
              $"({steps / Math.Max(0.001, elapsed.TotalSeconds):0} steps/s)."
            : "The fleet did not drain — the harness is not working.");

        return drained ? 0 : 1;
    }
}

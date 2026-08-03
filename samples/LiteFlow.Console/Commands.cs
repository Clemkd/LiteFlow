using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace LiteFlow.SampleConsole;

internal static class Commands
{
    public static int Help()
    {
        Console.WriteLine(
            """
            LiteFlow sample — a five-step order workflow on PostgreSQL.

              demo                       Start one order and run it to the end in this process, then print its trace.
              start   [--count N] [--fast] [--order ID]
                                         Start instances without running them (a worker elsewhere will).
              worker  [--concurrency N]  Run the workers until Ctrl+C. Start several of these at once.
              crash   [--after SECONDS] [--order ID]
                                         Start an order, run a worker, and kill the process mid-step.
                                         Then run 'worker' again to watch it resume at the same step.
              list    [--state NAME] [--live] [--max N]
              show    --id GUID          The instance and its step-by-step trace.
              cancel  --id GUID [--reason TEXT]
              signal  --id GUID [--name shipped] [--payload TEXT]
              resume  --id GUID          Put a failed or parked instance back to work.
              stats                      Counts per state.
              prune                      Archive terminal instances and drop old archive rows.
              reset                      Drop the engine's schemas and the sample table (development only).

            The connection string comes from ConnectionStrings:liteflowdb, then LITEFLOW_CONNECTION,
            then Host=127.0.0.1;Port=5432;Database=liteflow;Username=postgres;Password=postgres.
            """);
        return 0;
    }

    public static async Task<int> DemoAsync(IHost host, CliArgs cli)
    {
        var services = host.Services;
        string orderId = cli.Str("order", $"demo-{DateTime.UtcNow:HHmmss}");

        var handle = await StartOneAsync(services, orderId, fast: true);
        Console.WriteLine($"Started {handle.WorkflowId:D} for order {orderId}.");

        await host.StartAsync();
        try
        {
            // The workflow parks on the 'shipped' wait; signalling it is what lets the demo finish.
            var waiting = await WaitForAsync(services, handle.WorkflowId,
                i => i.State is WorkflowState.WaitingSignal or WorkflowState.Failed
                     or WorkflowState.Cancelled or WorkflowState.Completed,
                TimeSpan.FromSeconds(60));

            if (waiting?.State == WorkflowState.WaitingSignal)
            {
                Console.WriteLine($"Waiting for signal '{waiting.WaitSignal}' — sending it.");
                await WithClientAsync(services, c => c.SignalAsync(handle.WorkflowId, "shipped", "TRK-42"));
            }

            var done = await WaitForAsync(services, handle.WorkflowId, i => i.IsTerminal, TimeSpan.FromSeconds(60));
            Console.WriteLine();
            Console.WriteLine($"Finished as {done?.State}.");
        }
        finally
        {
            await host.StopAsync();
        }

        return await ShowAsync(services, handle.WorkflowId);
    }

    public static async Task<int> StartAsync(IServiceProvider services, CliArgs cli)
    {
        int count = cli.Int("count", 1);
        bool fast = cli.Flag("fast");
        string? order = cli.Flag("order") ? cli.Str("order", "") : null;

        for (int i = 0; i < count; i++)
        {
            string orderId = order is { Length: > 0 }
                ? (count == 1 ? order : $"{order}-{i}")
                : $"order-{Guid.CreateVersion7():N}"[..18];

            var handle = await StartOneAsync(services, orderId, fast);
            Console.WriteLine(
                $"{handle.WorkflowId:D}  {orderId}{(handle.AlreadyExisted ? "  (already existed)" : "")}");
        }

        Console.WriteLine($"{count} instance(s) started. Run 'worker' to process them.");
        return 0;
    }

    public static async Task<int> WorkerAsync(IHost host)
    {
        Console.WriteLine("Workers running. Ctrl+C to stop — a step in flight goes back to the queue untouched.");
        await host.RunAsync();
        return 0;
    }

    /// <summary>
    /// Start an order, run a worker in this process, and kill the process partway through the slow step. The
    /// point is what happens next: run <c>worker</c> again and the workflow resumes at the same step, with
    /// nothing the dead attempt did left behind.
    /// </summary>
    public static async Task<int> CrashAsync(IHost host, CliArgs cli)
    {
        var services = host.Services;
        string orderId = cli.Str("order", $"crash-{DateTime.UtcNow:HHmmss}");
        Crash.ArmAfter(cli.Int("after", 3));

        var handle = await StartOneAsync(services, orderId, fast: false);
        Console.WriteLine($"Started {handle.WorkflowId:D} for order {orderId}.");
        Console.WriteLine("Running a worker — it will be killed in the middle of 'pack-parcel'.");
        Console.WriteLine();

        await host.RunAsync();

        // Only reached if the crash never armed.
        Console.WriteLine("The process was not killed; the crash window was missed.");
        return 1;
    }

    public static async Task<int> ListAsync(IServiceProvider services, CliArgs cli)
    {
        var query = new WorkflowQuery
        {
            Definition = "orders",
            LiveOnly = cli.Flag("live"),
            MaxResults = cli.Int("max", 20),
            State = Enum.TryParse<WorkflowState>(cli.Str("state", ""), ignoreCase: true, out var state)
                ? state
                : null,
        };

        var instances = await WithClientAsync(services, c => c.ListAsync(query));

        Console.WriteLine($"{"id",-38} {"state",-15} {"step",-16} {"updated",-20} error");
        foreach (var i in instances)
        {
            Console.WriteLine(
                $"{i.Id,-38} {i.State,-15} {i.CurrentStep}/{i.StepCount} {i.CurrentStepName,-12} " +
                $"{i.UpdatedAt:yyyy-MM-dd HH:mm:ss}  {Trim(i.Error)}");
        }

        Console.WriteLine($"{instances.Count} instance(s).");
        return 0;
    }

    public static async Task<int> ShowAsync(IServiceProvider services, Guid id)
    {
        var instance = await WithClientAsync(services, c => c.GetAsync(id));
        if (instance is null)
        {
            Console.Error.WriteLine($"No instance {id:D} (it may have been archived).");
            return 1;
        }

        Console.WriteLine($"{instance.Definition} {instance.Id:D}");
        Console.WriteLine($"  state       {instance.State}{(instance.CancelRequested ? " (cancellation requested)" : "")}");
        Console.WriteLine($"  cursor      {instance.CurrentStep}/{instance.StepCount} → {instance.CurrentStepName}");
        Console.WriteLine($"  signature   {instance.Signature}");
        Console.WriteLine($"  created     {instance.CreatedAt:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine($"  updated     {instance.UpdatedAt:yyyy-MM-dd HH:mm:ss}");
        if (instance.ResumeAt is { } resumeAt)
            Console.WriteLine($"  resumes at  {resumeAt:yyyy-MM-dd HH:mm:ss}");
        if (instance.WaitSignal is { } signal)
            Console.WriteLine($"  waiting on  '{signal}'{(instance.WaitExpiresAt is { } e ? $" until {e:yyyy-MM-dd HH:mm:ss}" : "")}");
        if (instance.Error is { } error)
            Console.WriteLine($"  error       {Trim(error)}");
        Console.WriteLine($"  state bag   {Trim(instance.StateJson, 160)}");

        var steps = await WithClientAsync(services, c => c.GetStepsAsync(id));
        Console.WriteLine();
        Console.WriteLine($"  {"#",-3} {"step",-16} {"outcome",-12} {"tries",-6} {"ms",-8} error");
        foreach (var step in steps)
        {
            Console.WriteLine(
                $"  {step.StepIndex,-3} {step.StepName,-16} {step.State,-12} {step.Attempts,-6} " +
                $"{step.DurationMs?.ToString() ?? "-",-8} {Trim(step.Error)}");
        }

        return 0;
    }

    public static async Task<int> CancelAsync(IServiceProvider services, Guid id, CliArgs cli)
    {
        bool cancelled = await WithClientAsync(services,
            c => c.CancelAsync(id, cli.Str("reason", "cancelled from the sample")));
        Console.WriteLine(cancelled
            ? $"Cancellation requested for {id:D}. A worker will honour it, running any compensations."
            : $"{id:D} does not exist or has already finished.");
        return cancelled ? 0 : 1;
    }

    public static async Task<int> SignalAsync(IServiceProvider services, Guid id, CliArgs cli)
    {
        var outcome = await WithClientAsync(services,
            c => c.SignalAsync(id, cli.Str("name", "shipped"), cli.Str("payload", "TRK-1")));
        Console.WriteLine($"{id:D}: {outcome}.");
        return outcome is SignalOutcome.NotFound ? 1 : 0;
    }

    public static async Task<int> ResumeAsync(IServiceProvider services, Guid id)
    {
        bool resumed = await WithClientAsync(services, c => c.ResumeAsync(id));
        Console.WriteLine(resumed
            ? $"{id:D} resumed at the step it stopped on."
            : $"{id:D} is not in a resumable state.");
        return resumed ? 0 : 1;
    }

    public static async Task<int> StatsAsync(IServiceProvider services)
    {
        var stats = await WithClientAsync(services, c => c.GetStatsAsync("orders"));
        Console.WriteLine($"running         {stats.Running}");
        Console.WriteLine($"suspended       {stats.Suspended}");
        Console.WriteLine($"waiting signal  {stats.WaitingSignal}");
        Console.WriteLine($"compensating    {stats.Compensating}");
        Console.WriteLine($"completed       {stats.Completed}");
        Console.WriteLine($"failed          {stats.Failed}");
        Console.WriteLine($"cancelled       {stats.Cancelled}");
        Console.WriteLine($"needs attention {stats.NeedsAttention}");
        Console.WriteLine($"live            {stats.Live}");
        if (stats.OldestLiveAge is { } age)
            Console.WriteLine($"oldest live     {age:hh\\:mm\\:ss}");
        return 0;
    }

    public static async Task<int> PruneAsync(IServiceProvider services)
    {
        long rows = await WithClientAsync(services, c => c.PruneAsync());
        Console.WriteLine($"{rows} row(s) archived or dropped.");
        return 0;
    }

    public static async Task<int> ResetAsync(IServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        await db.Database.ExecuteSqlRawAsync(
            """
            DROP SCHEMA IF EXISTS liteflow CASCADE;
            DROP SCHEMA IF EXISTS litequeue CASCADE;
            DROP TABLE IF EXISTS public.demo_orders;
            """);
        Console.WriteLine("Schemas dropped.");
        return 0;
    }

    private static async Task<WorkflowHandle> StartOneAsync(IServiceProvider services, string orderId, bool fast)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<ILiteFlowClient>();

        await client.InitializeAsync();
        await db.EnsureDemoTableAsync();

        // The order id doubles as the idempotency key: starting the same order twice returns the first
        // instance instead of charging the customer again.
        return await client.StartAsync<OrderWorkflow, OrderState>(
            new OrderState { OrderId = orderId, Amount = 49.90m, Fast = fast },
            new WorkflowStartOptions { IdempotencyKey = orderId, CorrelationId = orderId });
    }

    private static async Task<WorkflowInstance?> WaitForAsync(
        IServiceProvider services, Guid id, Func<WorkflowInstance, bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var instance = await WithClientAsync(services, c => c.GetAsync(id));
            if (instance is not null && predicate(instance))
                return instance;
            await Task.Delay(250);
        }
        return await WithClientAsync(services, c => c.GetAsync(id));
    }

    private static async Task<T> WithClientAsync<T>(IServiceProvider services, Func<ILiteFlowClient, Task<T>> action)
    {
        await using var scope = services.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredService<ILiteFlowClient>());
    }

    private static string Trim(string? value, int max = 60) =>
        value is null ? "" : value.ReplaceLineEndings(" ") is { Length: > 0 } s && s.Length > max
            ? s[..max] + "…"
            : value.ReplaceLineEndings(" ");
}

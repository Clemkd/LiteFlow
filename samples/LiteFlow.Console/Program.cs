using LiteFlow;
using LiteFlow.SampleConsole;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Build a generic host as the DI container and, for the worker commands, as the thing that actually runs the
// workers. Command-line arguments are parsed by CliArgs rather than the configuration binder, so flags like
// "--fast" cannot confuse config parsing.
var builder = Host.CreateApplicationBuilder();

builder.Logging.AddFilter("Microsoft.EntityFrameworkCore", LogLevel.Warning);
builder.Logging.AddFilter("Npgsql", LogLevel.Warning);
builder.Logging.AddSimpleConsole(o => o.SingleLine = true);

// Aspire wiring: OpenTelemetry, health checks, service discovery. Harmless when run standalone, and it is
// what surfaces the liteflow.* meters and the per-step activities in the dashboard.
builder.AddServiceDefaults();

var cli = new CliArgs(args);
string connectionString = ConnectionStringResolver.Resolve(builder.Configuration);

builder.Services.AddDbContext<OrderDbContext>(o => o.UseNpgsql(connectionString));

builder.Services.AddLiteFlow<OrderDbContext>(o =>
{
    o.ConnectionString = connectionString;
    // A step of this sample can take ten seconds, so the lease has to comfortably outlast one.
    o.StepLease = TimeSpan.FromSeconds(30);
    o.MaintenanceInterval = TimeSpan.FromSeconds(10);
    o.CancellationPollInterval = TimeSpan.FromSeconds(2);
});

// Only the commands that are supposed to process work register workers; the others get a client and nothing
// else, so 'list' or 'cancel' never quietly starts draining the queue.
bool runsWorkers = cli.Command is "worker" or "demo" or "crash";
if (runsWorkers)
{
    builder.Services.AddLiteFlowWorkflow<OrderWorkflow>(w =>
    {
        w.Concurrency = cli.Int("concurrency", 2);
        w.ExternalConcurrency = cli.Int("concurrency", 2) * 2;
    });
}
else
{
    // The definition still has to be known: an instance can only be started, inspected or resumed by a
    // process that knows what its steps are.
    builder.Services.AddLiteFlowWorkflow<OrderWorkflow>();
    builder.Services.RemoveAll<IHostedService>();
}

using var host = builder.Build();

try
{
    return cli.Command switch
    {
        "demo" => await Commands.DemoAsync(host, cli),
        "start" or "seed" => await Commands.StartAsync(host.Services, cli),
        "worker" => await Commands.WorkerAsync(host),
        "crash" => await Commands.CrashAsync(host, cli),
        "list" => await Commands.ListAsync(host.Services, cli),
        "show" => cli.Id() is { } showId
            ? await Commands.ShowAsync(host.Services, showId)
            : Fail("show needs --id GUID"),
        "cancel" => cli.Id() is { } cancelId
            ? await Commands.CancelAsync(host.Services, cancelId, cli)
            : Fail("cancel needs --id GUID"),
        "signal" => cli.Id() is { } signalId
            ? await Commands.SignalAsync(host.Services, signalId, cli)
            : Fail("signal needs --id GUID"),
        "resume" => cli.Id() is { } resumeId
            ? await Commands.ResumeAsync(host.Services, resumeId)
            : Fail("resume needs --id GUID"),
        "stats" => await Commands.StatsAsync(host.Services),
        "prune" => await Commands.PruneAsync(host.Services),
        "reset" => await Commands.ResetAsync(host.Services),
        _ => Commands.Help(),
    };
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Error: {ex.Message}");
    return 1;
}

static int Fail(string message)
{
    Console.Error.WriteLine(message);
    return 1;
}

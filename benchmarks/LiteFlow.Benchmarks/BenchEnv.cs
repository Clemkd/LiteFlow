using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Testcontainers.PostgreSql;

namespace LiteFlow.Benchmarks;

/// <summary>The state the benchmark workflows carry. Deliberately small: this measures the engine, not JSON.</summary>
public sealed class BenchState
{
    public int Counter { get; set; }
}

/// <summary>Four steps that do nothing, so the numbers are the engine's own cost per step.</summary>
public sealed class BenchWorkflow : Workflow<BenchState>
{
    public static string DefinitionName = "bench";

    public override string Name => DefinitionName;

    protected override void Configure(IWorkflowBuilder<BenchState> b) => b
        .Step("b1", (ctx, ct) => Advance(ctx))
        .Step("b2", (ctx, ct) => Advance(ctx))
        .Step("b3", (ctx, ct) => Advance(ctx))
        .Step("b4", (ctx, ct) => Advance(ctx));

    public const int StepCount = 4;

    private static Task<StepResult> Advance(IWorkflowStepContext<BenchState> ctx)
    {
        ctx.State.Counter++;
        return Task.FromResult(StepResult.Next());
    }
}

/// <summary>The application's context, so the benchmarks run on the connector real callers use.</summary>
public sealed class BenchDbContext(DbContextOptions<BenchDbContext> options) : DbContext(options);

/// <summary>
/// A PostgreSQL container plus a configured provider, shared by every benchmark in the run.
/// <para>
/// A container rather than a local server: the numbers then mean something on any machine, and nobody has to
/// prepare a database before running them. The cost of that choice is a few seconds of start-up, which
/// BenchmarkDotNet excludes from the measurements anyway.
/// </para>
/// </summary>
public sealed class BenchEnv : IAsyncDisposable
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:18-alpine").Build();
    private ServiceProvider? _provider;
    private List<IHostedService> _workers = [];

    public string ConnectionString { get; private set; } = string.Empty;

    public IServiceProvider Services => _provider
        ?? throw new InvalidOperationException("The environment has not been started.");

    public async Task StartAsync(int concurrency, bool withWorkers)
    {
        await _container.StartAsync();

        // 127.0.0.1 rather than localhost: on a host that resolves localhost to ::1 first, the container's
        // IPv4-only mapping refuses the connection.
        ConnectionString = new NpgsqlConnectionStringBuilder(_container.GetConnectionString())
        {
            Host = "127.0.0.1",
            MaxPoolSize = Math.Max(16, concurrency * 2),
        }.ConnectionString;

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<BenchDbContext>(o => o.UseNpgsql(ConnectionString));
        services.AddLiteFlow<BenchDbContext>(o =>
        {
            o.ConnectionString = ConnectionString;
            o.MaintenanceInterval = TimeSpan.FromSeconds(30);
            o.CancellationPollInterval = TimeSpan.Zero;
            o.EnableNotifications = true;
        });
        services.AddLiteFlowWorkflow<BenchWorkflow>(w => w.Concurrency = concurrency);

        _provider = services.BuildServiceProvider();

        await using (var scope = _provider.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<ILiteFlowClient>().InitializeAsync();

        if (withWorkers)
            _workers = await StartWorkersAsync(_provider);
    }

    public static async Task<List<IHostedService>> StartWorkersAsync(IServiceProvider services)
    {
        var hosted = services.GetServices<IHostedService>().ToList();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);
        return hosted;
    }

    public static async Task StopWorkersAsync(IEnumerable<IHostedService> hosted)
    {
        foreach (var service in hosted)
        {
            try
            {
                await service.StopAsync(CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    public async Task<T> WithClientAsync<T>(Func<ILiteFlowClient, Task<T>> action)
    {
        await using var scope = Services.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredService<ILiteFlowClient>());
    }

    /// <summary>Wait until every one of <paramref name="ids"/> has finished, or the deadline passes.</summary>
    public async Task<bool> WaitForAllAsync(IReadOnlyList<Guid> ids, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await CountTerminalAsync(ids) == ids.Count)
                return true;
            await Task.Delay(20);
        }
        return await CountTerminalAsync(ids) == ids.Count;
    }

    private async Task<int> CountTerminalAsync(IReadOnlyList<Guid> ids)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT count(*) FROM liteflow.workflows WHERE id = ANY(@ids) AND state >= 4";
        cmd.Parameters.AddWithValue("ids", ids.ToArray());
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    public async ValueTask DisposeAsync()
    {
        await StopWorkersAsync(_workers);
        if (_provider is not null)
            await _provider.DisposeAsync();
        await _container.DisposeAsync();
    }
}

using Npgsql;

namespace LiteFlow.Internal;

/// <summary>
/// A connection of the engine's own, outside whatever transaction a step is running in.
/// <para>
/// Three things need it, and all three would be wrong on the step's connection:
/// </para>
/// <list type="bullet">
/// <item>
/// <b>Recording a failed attempt.</b> The step's transaction is about to be rolled back — a trace
/// written inside it would disappear along with the failure it is meant to explain.
/// </item>
/// <item>
/// <b>Failing the workflow on the last attempt.</b> Same reason: the verdict has to outlive the
/// rollback that produced it.
/// </item>
/// <item>
/// <b>The cancellation poll and the maintenance sweep.</b> Both run while steps are executing, and a
/// <see cref="System.Data.Common.DbConnection"/> cannot carry two commands at once.
/// </item>
/// </list>
/// <para>
/// The connection string is taken from <see cref="LiteFlowOptions.ConnectionString"/> when set,
/// otherwise from whatever LiteFlow was registered against (a connection string, or the EF context's).
/// When none can be resolved the engine still works — it just loses the out-of-band diagnostics, and
/// says so once at startup.
/// </para>
/// </summary>
internal sealed class WorkflowSideChannel : IAsyncDisposable
{
    private readonly Func<string?> _resolve;
    private readonly Lock _gate = new();
    private NpgsqlDataSource? _dataSource;
    private bool _resolved;
    private string? _connectionString;

    public WorkflowSideChannel(Func<string?> resolve) => _resolve = resolve;

    /// <summary><c>true</c> when a connection string could be resolved.</summary>
    public bool IsAvailable => ConnectionString is not null;

    private string? ConnectionString
    {
        get
        {
            if (_resolved)
                return _connectionString;

            lock (_gate)
            {
                if (!_resolved)
                {
                    _connectionString = _resolve();
                    _resolved = true;
                }
            }
            return _connectionString;
        }
    }

    /// <summary>Open a connection. The caller disposes it; the pool behind it lives as long as the process.</summary>
    public async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
    {
        var source = DataSource();
        return await source.OpenConnectionAsync(ct);
    }

    /// <summary>Open a connection, or <c>null</c> when the engine has no side channel — for the paths that degrade instead of failing.</summary>
    public async Task<NpgsqlConnection?> TryOpenAsync(CancellationToken ct)
    {
        if (!IsAvailable)
            return null;

        try
        {
            return await OpenAsync(ct);
        }
        catch (NpgsqlException)
        {
            // The database being unreachable is the caller's problem to log, not a reason to lose the
            // failure this connection was opened to record.
            return null;
        }
    }

    private NpgsqlDataSource DataSource()
    {
        if (_dataSource is not null)
            return _dataSource;

        string connectionString = ConnectionString ?? throw new NoSideChannelException();

        lock (_gate)
        {
            // A small dedicated pool: this channel serves the sweep and the odd diagnostic write, and
            // must not be able to starve the pool the steps themselves run on.
            _dataSource ??= new NpgsqlDataSourceBuilder(connectionString).Build();
        }

        return _dataSource;
    }

    public async ValueTask DisposeAsync()
    {
        if (_dataSource is not null)
            await _dataSource.DisposeAsync();
    }
}

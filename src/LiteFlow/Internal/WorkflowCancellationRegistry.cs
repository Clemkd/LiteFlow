using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace LiteFlow.Internal;

/// <summary>
/// Turns "someone asked for this workflow to stop" into a <see cref="CancellationToken"/> the running
/// step actually observes.
/// <para>
/// Cancellation is a row in the database (<c>cancel_requested</c>), which the engine checks before
/// every step — so a cancellation is always honoured between steps, at no cost. That is not enough for
/// a step that runs for minutes: this registry keeps the ids currently executing <i>in this process</i>
/// and asks the database about exactly those, once per tick, in a single round-trip. A workflow
/// cancelled while a long step is in flight then stops within a tick instead of at the end of the step.
/// </para>
/// <para>
/// The poll deliberately covers only local work: an instance running on another host is that host's
/// business, and it is polling too.
/// </para>
/// </summary>
internal sealed class WorkflowCancellationRegistry(
    WorkflowCatalog catalog,
    WorkflowSideChannel sideChannel,
    ILogger<WorkflowCancellationRegistry> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _running = new();

    /// <summary>
    /// Watch an instance for the duration of one step attempt. Dispose the registration when the
    /// attempt ends — the token is only meaningful while a step is actually running.
    /// </summary>
    public Registration Watch(Guid workflowId)
    {
        var cts = new CancellationTokenSource();
        _running[workflowId] = cts;
        return new Registration(this, workflowId, cts);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = catalog.Options.CancellationPollInterval;
        if (interval <= TimeSpan.Zero)
            return;

        if (!sideChannel.IsAvailable)
        {
            logger.LogInformation(
                "LiteFlow cancellation polling is off: no connection of its own is available. " +
                "Cancellation is still honoured between steps.");
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stoppingToken);
                await PollAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // A failed poll only means a cancellation is honoured at the end of the step instead of
                // in the middle of it, so it is never worth taking the process down.
                logger.LogWarning(ex, "LiteFlow cancellation poll failed; retrying next tick.");
            }
        }
    }

    private async Task PollAsync(CancellationToken ct)
    {
        if (_running.IsEmpty)
            return;

        var ids = _running.Keys.ToArray();
        await using var connection = await sideChannel.TryOpenAsync(ct);
        if (connection is null)
            return;

        var cancelled = await WorkflowCommands.SelectCancelledAsync(
            new SqlTarget(connection, null), catalog.Sql, ids, ct);

        foreach (var id in cancelled)
        {
            if (_running.TryGetValue(id, out var cts) && !cts.IsCancellationRequested)
            {
                logger.LogInformation("Workflow {WorkflowId} was cancelled; interrupting its running step.", id);
                await cts.CancelAsync();
            }
        }
    }

    /// <summary>A watch on one instance, live for one step attempt.</summary>
    internal sealed class Registration(
        WorkflowCancellationRegistry registry, Guid workflowId, CancellationTokenSource cts) : IDisposable
    {
        /// <summary>Cancelled when the workflow's cancellation flag is observed by the poll.</summary>
        public CancellationToken Token => cts.Token;

        /// <summary><c>true</c> when the cancellation came from the workflow rather than from the host shutting down.</summary>
        public bool IsCancellationRequested => cts.IsCancellationRequested;

        public void Dispose()
        {
            registry._running.TryRemove(workflowId, out _);
            cts.Dispose();
        }
    }
}

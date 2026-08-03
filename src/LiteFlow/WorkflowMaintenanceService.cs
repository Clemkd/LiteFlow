using LiteFlow.Internal;
using LiteQueue;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace LiteFlow;

/// <summary>
/// The loop that makes the engine self-healing. Everything it does is idempotent and safe to run on
/// every instance of your service at once, which is the point: there is no leader to elect, no cron to
/// install, and nothing an operator has to remember.
/// <list type="number">
/// <item>
/// <b>Due timers.</b> A <see cref="WorkflowState.Suspended"/> instance whose <c>resume_at</c> has passed
/// goes back to <see cref="WorkflowState.Running"/> and its step is dispatched. The delayed message
/// normally does this on its own; this is what covers the case where it did not survive.
/// </item>
/// <item>
/// <b>Expired waits.</b> A wait for a signal that never came fails the instance (and rolls it back if it
/// has compensations), instead of leaving it parked forever.
/// </item>
/// <item>
/// <b>Orphans.</b> A <see cref="WorkflowState.Running"/> instance that has not moved for
/// <see cref="LiteFlowOptions.OrphanGracePeriod"/> gets its current step re-dispatched. Re-dispatch is a
/// no-op while a message for that step still exists — the dedup key collides — so this can run blind and
/// only ever recovers work that was genuinely lost (a dead-lettered message, a queue purge, a crash
/// between the cursor advance and the enqueue on a non-transactional step). After
/// <see cref="LiteFlowOptions.MaxRedispatch"/> tries it parks the instance rather than retrying forever.
/// </item>
/// <item>
/// <b>Retention.</b> Terminal instances move to the archive and archived rows eventually go, so the hot
/// tables stay the size of the work in flight rather than of the history.
/// </item>
/// </list>
/// </summary>
internal sealed class WorkflowMaintenanceService(
    IServiceProvider services,
    WorkflowCatalog catalog,
    WorkflowSideChannel sideChannel,
    ILogger<WorkflowMaintenanceService> logger) : BackgroundService
{
    private const int BatchSize = 200;

    private WorkflowSql Sql => catalog.Sql;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = catalog.Options.MaintenanceInterval;

        if (!sideChannel.IsAvailable)
        {
            logger.LogWarning(
                "LiteFlow maintenance is off: no connection of its own is available. Due timers, expired " +
                "waits and lost steps will not be recovered. Set LiteFlowOptions.ConnectionString.");
            return;
        }

        logger.LogInformation("LiteFlow maintenance started (every {Interval}).", interval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stoppingToken);
                await SweepAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // The sweep is a safety net; a failed pass just means the next one does the work.
                logger.LogWarning(ex, "LiteFlow maintenance pass failed; retrying in {Interval}.", interval);
            }
        }

        logger.LogInformation("LiteFlow maintenance stopped.");
    }

    private async Task SweepAsync(CancellationToken ct)
    {
        await using var connection = await sideChannel.TryOpenAsync(ct);
        if (connection is null)
            return;

        await using var scope = services.CreateAsyncScope();
        var queueClient = scope.ServiceProvider.GetRequiredService<ILiteQueueClient>();

        await WakeDueTimersAsync(connection, queueClient, ct);
        await FinaliseParkedCancellationsAsync(connection, queueClient, ct);
        await ExpireWaitsAsync(connection, queueClient, ct);
        await RecoverOrphansAsync(connection, queueClient, ct);
        await PruneAsync(connection, ct);
    }

    private async Task WakeDueTimersAsync(NpgsqlConnection connection, ILiteQueueClient queueClient, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var target = new SqlTarget(connection, transaction);
        var producer = queueClient.Using(connection, transaction).Producer;

        var due = await WorkflowCommands.DueSuspendedAsync(target, Sql, BatchSize, ct);

        foreach (var instance in due)
            await DispatchAsync(target, producer, instance, ct);

        await transaction.CommitAsync(ct);

        if (due.Count > 0)
            logger.LogInformation("LiteFlow woke {Count} suspended workflow(s) whose timer was due.", due.Count);
    }

    /// <summary>
    /// Cancellations asked for while the instance was parked on a timer or a signal. Nothing is in flight
    /// for those, so no worker would ever notice the request: the sweep hands them their current step, and
    /// the guard at the top of it honours the cancellation (running the compensations, if any).
    /// </summary>
    private async Task FinaliseParkedCancellationsAsync(
        NpgsqlConnection connection, ILiteQueueClient queueClient, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var target = new SqlTarget(connection, transaction);
        var producer = queueClient.Using(connection, transaction).Producer;

        var parked = await WorkflowCommands.CancelledWhileParkedAsync(target, Sql, BatchSize, ct);

        foreach (var instance in parked)
        {
            // Leave the wait behind so the instance is claimable, then let a worker apply the cancellation.
            await WorkflowCommands.AdvanceAsync(
                target, Sql, instance.Id, instance.StepIndex, instance.StepIndex, instance.StepName,
                WorkflowState.Running, null, null, null, null, null, ct);
            await DispatchAsync(target, producer, instance, ct);
        }

        await transaction.CommitAsync(ct);

        if (parked.Count > 0)
            logger.LogInformation(
                "LiteFlow woke {Count} parked workflow(s) so their cancellation could be applied.", parked.Count);
    }

    private async Task ExpireWaitsAsync(NpgsqlConnection connection, ILiteQueueClient queueClient, CancellationToken ct)
    {
        var expired = await WorkflowCommands.DueSignalTimeoutsAsync(
            new SqlTarget(connection, null), Sql, BatchSize, ct);

        foreach (var instance in expired)
        {
            await using var transaction = await connection.BeginTransactionAsync(ct);
            var target = new SqlTarget(connection, transaction);
            var producer = queueClient.Using(connection, transaction).Producer;

            var row = await WorkflowCommands.LoadForUpdateAsync(target, Sql, instance.Id, ct);
            if (row is null || row.State != WorkflowState.WaitingSignal)
            {
                await transaction.RollbackAsync(ct);
                continue;
            }

            string error = $"Timed out waiting for signal '{instance.Signal}'.";

            if (catalog.TryGet(row.Definition, out var definition)
                && await FindCompensableAsync(target, definition!, row, ct) is { } compensable)
            {
                await WorkflowCommands.StartCompensationAsync(target, Sql, row.Id, compensable.Index, error, ct);
                await StepDispatcher.DispatchCompensationAsync(
                    producer, definition!, row.Id, compensable, row.Priority + compensable.Priority,
                    catalog.MaxAttemptsFor(compensable), ct);
            }
            else
            {
                await WorkflowCommands.FinishAsync(
                    target, Sql, row.Id, WorkflowState.Failed, null, error, null, ct);
                WorkflowDiagnostics.WorkflowFinished(row.Definition, WorkflowState.Failed);
            }

            await transaction.CommitAsync(ct);

            logger.LogWarning(
                "Workflow {WorkflowId} timed out waiting for signal '{Signal}'.", row.Id, instance.Signal);
        }
    }

    private async Task RecoverOrphansAsync(NpgsqlConnection connection, ILiteQueueClient queueClient, CancellationToken ct)
    {
        var options = catalog.Options;

        await using var transaction = await connection.BeginTransactionAsync(ct);
        var target = new SqlTarget(connection, transaction);
        var producer = queueClient.Using(connection, transaction).Producer;

        var orphans = await WorkflowCommands.OrphanCandidatesAsync(
            target, Sql, options.OrphanGracePeriod, options.MaxRedispatch, BatchSize, ct);

        int redispatched = 0;
        foreach (var instance in orphans)
        {
            if (await DispatchAsync(target, producer, instance, ct))
                redispatched++;
        }

        int parked = await WorkflowCommands.ExhaustedRedispatchAsync(
            target, Sql, options.OrphanGracePeriod, options.MaxRedispatch,
            $"No step message could be kept in flight after {options.MaxRedispatch} re-dispatches. " +
            "The step is failing before it can report anything, or the queue is dropping its messages.",
            ct);

        await transaction.CommitAsync(ct);

        if (redispatched > 0)
            logger.LogInformation(
                "LiteFlow re-dispatched {Count} workflow(s) whose step was no longer in flight.", redispatched);

        if (parked > 0)
            logger.LogError(
                "LiteFlow parked {Count} workflow(s) that could not be kept in flight; they need attention.",
                parked);
    }

    /// <summary>
    /// Offer an instance's current step to the queue again. Returns <c>false</c> when nothing was queued:
    /// a message for that step was already there (the common case, and the reason this is safe to run
    /// blind), or this process does not host the definition — another one does, and the instance keeps its
    /// place in the sweep.
    /// </summary>
    private async Task<bool> DispatchAsync(
        SqlTarget target, IQueueProducer producer, DispatchTarget instance, CancellationToken ct)
    {
        if (!catalog.TryGet(instance.Definition, out var definition))
        {
            logger.LogDebug(
                "Workflow {WorkflowId} runs '{Definition}', which this process does not host; leaving it to one that does.",
                instance.Id, instance.Definition);
            return false;
        }

        var step = definition!.StepAt(instance.StepIndex);
        if (step is null || !string.Equals(step.Name, instance.StepName, StringComparison.Ordinal))
        {
            // The sequence changed underneath it: parking is the honest outcome, not guessing.
            await WorkflowCommands.MarkNeedsAttentionAsync(
                target, Sql, instance.Id,
                $"Step {instance.StepIndex} ('{instance.StepName}') is no longer where the definition says it is.",
                ct);
            logger.LogError(
                "Workflow {WorkflowId} was parked: step {Index} is no longer '{Step}' in '{Definition}'.",
                instance.Id, instance.StepIndex, instance.StepName, instance.Definition);
            return false;
        }

        var result = await StepDispatcher.DispatchStepAsync(
            producer, definition, instance.Id, step, instance.Priority + step.Priority,
            catalog.MaxAttemptsFor(step), TimeSpan.Zero, ct);

        if (!result.Deduplicated)
            WorkflowDiagnostics.StepRedispatched(definition.Name);

        return !result.Deduplicated;
    }

    private async Task<WorkflowStepDescriptor?> FindCompensableAsync(
        SqlTarget target, WorkflowDefinition definition, WorkflowRow row, CancellationToken ct)
    {
        var steps = await WorkflowCommands.ListStepsAsync(target, Sql, row.Id, ct);

        for (int i = steps.Count - 1; i >= 0; i--)
        {
            var record = steps[i];
            if (record.StepIndex > row.CurrentStep || record.State != StepState.Completed)
                continue;

            var descriptor = definition.StepAt(record.StepIndex);
            if (descriptor is { HasCompensation: true }
                && string.Equals(descriptor.Name, record.StepName, StringComparison.Ordinal))
                return descriptor;
        }

        return null;
    }

    private async Task PruneAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var target = new SqlTarget(connection, null);
        var options = catalog.Options;

        int archived = await WorkflowCommands.ArchiveTerminalAsync(
            target, Sql, options.InstanceRetention, BatchSize, ct);

        int dropped = options.ArchiveRetention > TimeSpan.Zero
            ? await WorkflowCommands.PruneArchiveAsync(target, Sql, options.ArchiveRetention, ct)
            : 0;

        if (archived > 0 || dropped > 0)
            logger.LogDebug(
                "LiteFlow retention: {Archived} instance(s) archived, {Dropped} archived row(s) dropped.",
                archived, dropped);
    }
}

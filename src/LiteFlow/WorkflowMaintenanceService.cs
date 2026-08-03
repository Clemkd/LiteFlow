using LiteFlow.Internal;
using LiteQueue;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace LiteFlow;

/// <summary>
/// The loop that makes the engine self-healing. Everything it does is idempotent and safe to run on every
/// instance of your service at once, which is the point: there is no leader to elect, no cron to install, and
/// nothing an operator has to remember.
/// <list type="number">
/// <item>
/// <b>Due timers.</b> A <see cref="WorkflowState.Suspended"/> instance whose <c>resume_at</c> has passed goes
/// back to <see cref="WorkflowState.Running"/> and its step is dispatched. The delayed message normally does
/// this on its own; this is what covers the case where it did not survive.
/// </item>
/// <item>
/// <b>Parked cancellations.</b> A cancellation asked for while the instance had nothing in flight, which no
/// worker would otherwise notice.
/// </item>
/// <item>
/// <b>Expired waits.</b> A wait for a signal that never came fails the instance (and rolls it back if it has
/// compensations), instead of leaving it parked forever.
/// </item>
/// <item>
/// <b>Reconciliation.</b> For every live instance with no message in flight, the sweep asks the queue why —
/// see <see cref="ReconcileAsync"/>. This is where a step that threw its way through its whole attempt budget
/// on a host that then died gets the verdict its worker never managed to write.
/// </item>
/// <item>
/// <b>Retention.</b> Terminal instances move to the archive and archived rows eventually go, so the hot tables
/// stay the size of the work in flight rather than of the history.
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
                "LiteFlow maintenance is off: no connection of its own is available. Due timers, expired waits " +
                "and steps that failed without being able to report it will not be recovered. Set " +
                "LiteFlowOptions.ConnectionString.");
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
        await ReconcileAsync(connection, queueClient, ct);
        await PruneAsync(connection, ct);
    }

    private async Task WakeDueTimersAsync(
        NpgsqlConnection connection, ILiteQueueClient queueClient, CancellationToken ct)
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
    /// Cancellations asked for while the instance was parked on a timer or a signal. Nothing is in flight for
    /// those, so no worker would ever notice the request: the sweep hands them their current step, and the guard
    /// at the top of it honours the cancellation (running the compensations, if any).
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

    private async Task ExpireWaitsAsync(
        NpgsqlConnection connection, ILiteQueueClient queueClient, CancellationToken ct)
    {
        var expired = await WorkflowCommands.DueSignalTimeoutsAsync(
            new SqlTarget(connection, null), Sql, BatchSize, ct);

        foreach (var instance in expired)
        {
            await using var transaction = await connection.BeginTransactionAsync(ct);
            var target = new SqlTarget(connection, transaction);
            var producer = queueClient.Using(connection, transaction).Producer;

            var row = await WorkflowCommands.LoadForUpdateAsync(target, Sql, instance.Id, ct);
            if (row is null || row.State != WorkflowState.WaitingSignal
                || !catalog.TryGet(row.Definition, out var definition))
            {
                await transaction.RollbackAsync(ct);
                continue;
            }

            string error = $"Timed out waiting for signal '{instance.Signal}'.";
            await WorkflowTermination.TerminateAsync(
                target, Sql, producer, catalog, definition!, row, WorkflowState.Failed, error, null, null, ct);

            await transaction.CommitAsync(ct);

            logger.LogWarning(
                "Workflow {WorkflowId} timed out waiting for signal '{Signal}'.", row.Id, instance.Signal);
        }
    }

    /// <summary>
    /// Reconcile the live instances that have no message in flight against the queue itself.
    /// <para>
    /// There are exactly two explanations, and they call for opposite actions:
    /// </para>
    /// <list type="bullet">
    /// <item>
    /// <b>A dead letter for this step exists.</b> A worker ran the step, it threw through its whole attempt
    /// budget, and the queue gave up on it — but the worker died before it could write the verdict, or could not
    /// write it. The instance is failed here, with the dead letter's error as the cause, and rolled back if it
    /// has compensations. It is <i>never</i> re-dispatched: a dead-lettered message has released its dedup key,
    /// so re-dispatching would hand a step that has definitively failed a brand-new attempt budget, and the
    /// workflow would carry on past a throw it should have died on.
    /// </item>
    /// <item>
    /// <b>No dead letter either.</b> The message was genuinely lost — never enqueued because a host died in the
    /// wrong microsecond, or removed by something outside the engine. This, and only this, is re-dispatched.
    /// </item>
    /// </list>
    /// <para>
    /// A compensation's dead letter is treated differently on purpose: an incomplete rollback is parked in
    /// <see cref="WorkflowState.NeedsAttention"/> rather than being reported as a rollback that succeeded.
    /// </para>
    /// </summary>
    private async Task ReconcileAsync(
        NpgsqlConnection connection, ILiteQueueClient queueClient, CancellationToken ct)
    {
        var options = catalog.Options;

        await using var transaction = await connection.BeginTransactionAsync(ct);
        var target = new SqlTarget(connection, transaction);
        var producer = queueClient.Using(connection, transaction).Producer;

        var candidates = await WorkflowCommands.ReconcileCandidatesAsync(
            target, Sql, options.OrphanGracePeriod, BatchSize, ct);

        int failed = 0;
        int redispatched = 0;
        int parked = 0;

        foreach (var candidate in candidates)
        {
            if (!catalog.TryGet(candidate.Definition, out var definition))
                continue; // another process hosts that definition, and it is sweeping too

            if (candidate.DeadLetterKey is not null)
            {
                if (await ApplyDeadLetterAsync(target, producer, definition!, candidate, ct))
                {
                    if (candidate.DeadLetterIsCompensation)
                        parked++;
                    else
                        failed++;
                }
                continue;
            }

            // A suspended instance is the timer sweep's business: re-dispatching it here would run its step
            // before it is due.
            if (candidate.State == WorkflowState.Suspended || !candidate.Redispatchable)
                continue;

            if (candidate.RedispatchCount >= options.MaxRedispatch)
            {
                await WorkflowCommands.MarkNeedsAttentionAsync(
                    target, Sql, candidate.Id,
                    $"No step message could be kept in flight after {options.MaxRedispatch} re-dispatches, and " +
                    "no dead letter explains it. Something outside the engine is removing its messages.", ct);
                WorkflowDiagnostics.WorkflowFinished(definition!.Name, WorkflowState.NeedsAttention);
                parked++;
                continue;
            }

            await WorkflowCommands.BumpRedispatchAsync(target, Sql, candidate.Id, ct);
            if (await DispatchAsync(target, producer, candidate.ToDispatchTarget(), ct))
                redispatched++;
        }

        await transaction.CommitAsync(ct);

        if (failed > 0)
            logger.LogError(
                "LiteFlow failed {Count} workflow(s) whose step was dead-lettered without being able to report it.",
                failed);

        if (redispatched > 0)
            logger.LogInformation(
                "LiteFlow re-dispatched {Count} workflow(s) whose step message had been lost.", redispatched);

        if (parked > 0)
            logger.LogError("LiteFlow parked {Count} workflow(s) that need attention.", parked);
    }

    /// <summary>
    /// Turn a dead letter into the verdict its worker never wrote — the same verdict, written the same way, so
    /// there is only one definition of "this workflow failed" anywhere in the engine.
    /// </summary>
    private async Task<bool> ApplyDeadLetterAsync(
        SqlTarget target,
        IQueueProducer producer,
        WorkflowDefinition definition,
        ReconcileTarget candidate,
        CancellationToken ct)
    {
        var row = await WorkflowCommands.LoadForUpdateAsync(target, Sql, candidate.Id, ct);
        if (row is null || row.State >= WorkflowState.Completed)
            return false;

        string cause = candidate.DeadLetterError is { Length: > 0 } error
            ? error
            : "The step's message was dead-lettered.";

        if (candidate.DeadLetterIsCompensation)
        {
            int index = candidate.CompensationIndex ?? row.CurrentStep;
            string name = definition.StepAt(index)?.Name ?? index.ToString();

            await WorkflowCommands.MarkNeedsAttentionAsync(
                target, Sql, row.Id,
                $"The compensation of step '{name}' was dead-lettered, so the rollback is incomplete: {cause}",
                ct);
            WorkflowDiagnostics.WorkflowFinished(definition.Name, WorkflowState.NeedsAttention);

            logger.LogError(
                "Workflow {WorkflowId} was parked: the compensation of step {Step} was dead-lettered.",
                row.Id, name);
            return true;
        }

        // Record the step as failed with the queue's own account of why, then end the workflow exactly as its
        // worker would have: rolling back if it has compensations, terminal otherwise.
        await WorkflowCommands.FailStepAsync(
            target, Sql, row.Id, row.CurrentStep, row.CurrentStepName,
            Math.Max(1, candidate.DeadLetterAttempts), null, cause, ct);

        var verdict = row.CancelRequested ? WorkflowState.Cancelled : WorkflowState.Failed;
        var written = await WorkflowTermination.TerminateAsync(
            target, Sql, producer, catalog, definition, row, verdict, cause, null, null, ct);

        logger.LogError(
            "Workflow {WorkflowId} is {State}: step {Step} was dead-lettered after {Attempts} attempt(s) without " +
            "its worker being able to report it.",
            row.Id, written, row.CurrentStepName, candidate.DeadLetterAttempts);
        return true;
    }

    /// <summary>
    /// Offer an instance's current step to the queue again. Returns <c>false</c> when nothing was queued: this
    /// process does not host the definition, or the step is no longer where the definition says it is.
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

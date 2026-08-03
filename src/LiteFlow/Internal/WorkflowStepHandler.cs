using System.Diagnostics;
using LiteQueue;
using LiteQueue.Connectors;
using Microsoft.Extensions.Logging;

namespace LiteFlow.Internal;

/// <summary>
/// Runs one step of one instance, driven by LiteQueue's hosted worker.
/// <para>
/// The whole durability story is the order of operations inside a single transaction — opened by the
/// worker host before this handler is called, committed by it after the fenced acknowledge:
/// </para>
/// <code>
/// BEGIN                                           -- worker host
///   SELECT … FROM workflows WHERE id = … FOR UPDATE  -- nobody else touches this instance
///   guards: cancelled? cursor still here? definition still the same?
///   step.ExecuteAsync(ctx)                           -- caller's writes, same connection
///   UPDATE workflow_steps  … completed
///   UPDATE workflows       … cursor + 1, state
///   INSERT INTO messages   … next step
///   DELETE FROM messages WHERE id = … AND lease_token = …   -- fenced ack, worker host
/// COMMIT
/// </code>
/// <para>
/// Which gives, without any distributed transaction:
/// </para>
/// <list type="bullet">
/// <item><b>Crash before the commit</b> — everything is undone, including the acknowledge. The lease expires and another worker runs the same step again, from the top, on a database that shows no trace of the attempt.</item>
/// <item><b>Crash after the commit</b> — the next step is already queued; another worker picks it up immediately.</item>
/// <item><b>A worker that lost its lease</b> — the acknowledge matches no row, so the host rolls the transaction back and the step's writes go with it. A resurrected zombie cannot double-apply anything.</item>
/// <item><b>A message delivered twice</b> — the cursor guard sees the step is already behind it, and the message is dropped instead of re-running the work.</item>
/// </list>
/// </summary>
internal abstract class WorkflowStepHandlerBase(
    WorkflowCatalog catalog,
    IQueueConnectionSource source,
    ILiteQueueClient queueClient,
    IWorkflowStateSerializer serializer,
    WorkflowSideChannel sideChannel,
    WorkflowCancellationRegistry cancellations,
    WorkflowDbContextAccessor dbContexts,
    IServiceProvider services,
    ILogger logger) : IQueueMessageHandler
{
    private static readonly string DefaultWorkerId = $"{Environment.MachineName}:{Environment.ProcessId}";

    /// <summary>Queue this handler drains.</summary>
    public abstract string Queue { get; }

    /// <summary>The definition it serves.</summary>
    protected abstract WorkflowDefinition Definition { get; }

    /// <summary>
    /// <c>true</c> for the worker that runs the steps covered by the engine's transaction, <c>false</c>
    /// for the one that runs the <see cref="IStepOptions{TState}.NonTransactional"/> steps. Two workers,
    /// two queues: a step waiting on someone else's API must not hold a transaction, and must not
    /// occupy a slot a database-only step could be using.
    /// </summary>
    protected abstract bool Transactional { get; }

    private WorkflowSql Sql => catalog.Sql;

    private string WorkerId => DefaultWorkerId;

    public async Task HandleAsync(QueueMessage message, CancellationToken cancellationToken = default)
    {
        var payload = StepDispatcher.Parse(message);
        var definition = Definition;

        await using var borrowed = await source.AcquireAsync(cancellationToken);
        var target = SqlTarget.From(borrowed);

        // FOR UPDATE only on the transactional path: there, the lock is held until the commit that
        // advances the cursor, which is what serializes the steps of one instance. On the other path
        // there is no transaction to hold it in, and mutual exclusion comes from the lease plus the
        // one-message-per-step dedup key instead.
        var row = Transactional
            ? await WorkflowCommands.LoadForUpdateAsync(target, Sql, payload.WorkflowId, cancellationToken)
            : await WorkflowCommands.LoadAsync(target, Sql, payload.WorkflowId, cancellationToken);

        if (row is null)
        {
            logger.LogDebug(
                "Workflow {WorkflowId} no longer exists (archived or purged); dropping its step message {MessageId}.",
                payload.WorkflowId, message.Id);
            return;
        }

        if (row.State >= WorkflowState.Completed)
        {
            logger.LogDebug(
                "Workflow {WorkflowId} is already {State}; dropping step message {MessageId}.",
                row.Id, row.State, message.Id);
            return;
        }

        if (payload.Purpose == StepPurpose.Compensate)
        {
            await RunCompensationAsync(borrowed, target, definition, row, payload, message, cancellationToken);
            return;
        }

        if (row.CancelRequested)
        {
            logger.LogInformation(
                "Workflow {WorkflowId} was cancelled before step {Step} started; terminating it.",
                row.Id, payload.StepName);
            await TerminateAsync(
                target, queueClient.Producer, definition, row,
                WorkflowState.Cancelled, row.CancelReason, null, cancellationToken);
            return;
        }

        if (row.State is WorkflowState.WaitingSignal or WorkflowState.Compensating)
        {
            // Neither state has a step message of its own in flight, so this one is a leftover.
            logger.LogDebug(
                "Workflow {WorkflowId} is {State}; dropping stale step message {MessageId}.",
                row.Id, row.State, message.Id);
            WorkflowDiagnostics.StepStale(definition.Name, payload.StepName);
            return;
        }

        if (row.CurrentStep != payload.StepIndex)
        {
            // The step already ran and the cursor moved on: this is a redelivery. Dropping it is the
            // whole reason redelivery is harmless.
            logger.LogInformation(
                "Workflow {WorkflowId} is on step {Current} ({CurrentName}); dropping the message for step " +
                "{Stale} ({StaleName}) — it has already been applied.",
                row.Id, row.CurrentStep, row.CurrentStepName, payload.StepIndex, payload.StepName);
            WorkflowDiagnostics.StepStale(definition.Name, payload.StepName);
            return;
        }

        var descriptor = definition.StepAt(row.CurrentStep);

        if (descriptor is null)
        {
            await ParkAsync(target, row,
                $"The definition now has {definition.StepCount} step(s), but this instance is on step " +
                $"{row.CurrentStep} ('{row.CurrentStepName}'). Steps were removed while it was in flight.",
                definition.Name, cancellationToken);
            return;
        }

        if (!string.Equals(descriptor.Name, row.CurrentStepName, StringComparison.Ordinal))
        {
            // The signature check in one sentence: the index still exists, but it no longer holds the
            // step this instance stopped on. Running it would apply the wrong code to real data.
            await ParkAsync(target, row,
                $"Step {row.CurrentStep} is now '{descriptor.Name}' but this instance stopped on " +
                $"'{row.CurrentStepName}' (definition signature {row.Signature} → {definition.Signature}). " +
                "Resume it explicitly once you have decided what should happen to it.",
                definition.Name, cancellationToken);
            return;
        }

        if (descriptor.IsTransactional != Transactional)
        {
            // The step changed sides after a deployment: hand it to the worker that can honour it.
            logger.LogInformation(
                "Step {Step} of workflow {WorkflowId} now runs {Mode}; re-dispatching it to the right queue.",
                descriptor.Name, row.Id, descriptor.IsTransactional ? "transactionally" : "outside a transaction");
            await StepDispatcher.DispatchStepAsync(
                queueClient.Producer, definition, row.Id, descriptor,
                row.Priority + descriptor.Priority, catalog.MaxAttemptsFor(descriptor),
                TimeSpan.Zero, cancellationToken);
            return;
        }

        await RunStepAsync(borrowed, target, definition, row, descriptor, message, cancellationToken);
    }

    private async Task RunStepAsync(
        QueueConnection borrowed,
        SqlTarget target,
        WorkflowDefinition definition,
        WorkflowRow row,
        WorkflowStepDescriptor descriptor,
        QueueMessage message,
        CancellationToken cancellationToken)
    {
        if (message.Attempts > 1)
        {
            logger.LogInformation(
                "Resuming step {Step} of workflow {WorkflowId}: attempt {Attempt}/{Max} " +
                "(the previous one was interrupted or failed).",
                descriptor.Name, row.Id, message.Attempts, message.MaxAttempts);
            WorkflowDiagnostics.StepResumed(definition.Name, descriptor.Name);
        }

        // Reaching a wait step means its signal is already recorded — SignalAsync is the only thing that
        // dispatches it — so the payload is there to be folded into the state.
        WorkflowSignal? signal = descriptor.Kind == StepKind.WaitForSignal && descriptor.SignalName is { } name
            ? await WorkflowCommands.SelectSignalAsync(target, Sql, row.Id, name, cancellationToken)
            : null;

        await WorkflowCommands.UpsertStepStartAsync(
            target, Sql, row.Id, descriptor.Index, descriptor.Name, message.Attempts, WorkerId, cancellationToken);

        var execution = BuildExecution(borrowed, definition, row, descriptor, message, signal);

        using var watch = cancellations.Watch(row.Id);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, watch.Token);
        using var activity = WorkflowDiagnostics.StartStepActivity(
            definition.Name, descriptor.Name, row.Id, descriptor.Index, message.Attempts);

        // The step runs inside a savepoint. Rolling back to it undoes everything the step wrote and leaves
        // the transaction usable — which is what lets a verdict (failed, cancelled) be written in the very
        // transaction that acknowledges the message. Writing it from another connection is not an option:
        // this transaction holds the instance row, so the other one would wait for a step that is waiting
        // for it.
        bool savepoint = false;
        if (target.Transaction is not null)
        {
            await WorkflowCommands.SavepointAsync(target, Sql, cancellationToken);
            savepoint = true;
        }

        var stopwatch = Stopwatch.StartNew();
        StepResult result;

        try
        {
            result = await descriptor.Executor(execution, linked.Token);
            stopwatch.Stop();
        }
        catch (OperationCanceledException) when (watch.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            // Cancelled by the workflow, not by the host stopping and not by a lost lease. The step's
            // half-finished work is discarded, and the cancellation is recorded in its place.
            stopwatch.Stop();
            logger.LogInformation(
                "Step {Step} of workflow {WorkflowId} was interrupted by a cancellation after {Elapsed} ms.",
                descriptor.Name, row.Id, stopwatch.ElapsedMilliseconds);

            await UndoStepAsync(target, savepoint);
            await WorkflowCommands.FailStepAsync(
                target, Sql, row.Id, descriptor.Index, descriptor.Name, message.Attempts, WorkerId,
                "Cancelled while running.", CancellationToken.None);
            await TerminateAsync(
                target, queueClient.Producer, definition, row,
                WorkflowState.Cancelled, row.CancelReason ?? "Cancelled while running.",
                null, CancellationToken.None);
            return;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            WorkflowDiagnostics.StepFailed(definition.Name, descriptor.Name);

            // A payload the code can no longer read, or a step that declared itself poison, will never
            // succeed: no point spending the remaining attempts to prove it.
            bool terminal = ex is PoisonMessageException or WorkflowStateException || message.IsLastAttempt;

            await RecordAttemptAsync(row, descriptor, message, ex);

            if (terminal && await TryRecordFailureAsync(
                    target, savepoint, definition, row, descriptor, message, ex))
            {
                logger.LogError(ex,
                    "Step {Step} of workflow {WorkflowId} failed on attempt {Attempt}/{Max}; the workflow " +
                    "was failed.",
                    descriptor.Name, row.Id, message.Attempts, message.MaxAttempts);
                return;
            }

            logger.LogWarning(ex,
                "Step {Step} of workflow {WorkflowId} failed on attempt {Attempt}/{Max}; it will be retried.",
                descriptor.Name, row.Id, message.Attempts, message.MaxAttempts);

            // Rethrow so the worker host rolls the transaction back and applies the retry policy — the
            // step's writes must not survive a failed attempt.
            throw;
        }

        if (savepoint)
            await WorkflowCommands.ReleaseSavepointAsync(target, Sql, cancellationToken);

        if (Transactional)
        {
            await ApplyResultAsync(
                target, queueClient.Producer, definition, row, descriptor, result, execution,
                (int)stopwatch.ElapsedMilliseconds, cancellationToken);
            return;
        }

        // Non-transactional step: it has already had its effect on the outside world, so the bookkeeping
        // gets a transaction of its own to stay atomic. The acknowledge then happens after that commit,
        // which is precisely the at-least-once window — closed by the cursor guard on redelivery.
        await using var transaction = await source.BeginTransactionAsync(cancellationToken);
        await using var enlisted = await source.AcquireAsync(cancellationToken);
        await ApplyResultAsync(
            SqlTarget.From(enlisted), queueClient.Producer, definition, row, descriptor, result, execution,
            (int)stopwatch.ElapsedMilliseconds, cancellationToken);
        if (transaction is not null)
            await transaction.CommitAsync(cancellationToken);
    }

    private async Task ApplyResultAsync(
        SqlTarget target,
        IQueueProducer producer,
        WorkflowDefinition definition,
        WorkflowRow row,
        WorkflowStepDescriptor descriptor,
        StepResult result,
        StepExecution execution,
        int durationMs,
        CancellationToken cancellationToken)
    {
        switch (result.Kind)
        {
            case StepResultKind.Fail:
                await WorkflowCommands.CompleteStepAsync(
                    target, Sql, row.Id, descriptor.Index, StepState.Failed, durationMs,
                    execution.OutputJson, result.Reason, cancellationToken);
                WorkflowDiagnostics.StepExecuted(definition.Name, descriptor.Name, durationMs, StepState.Failed);
                logger.LogError(
                    "Step {Step} of workflow {WorkflowId} refused to continue: {Reason}",
                    descriptor.Name, row.Id, result.Reason);
                await TerminateAsync(
                    target, producer, definition, row, WorkflowState.Failed, result.Reason,
                    execution.StateJson, cancellationToken);
                return;

            case StepResultKind.Complete:
                await WorkflowCommands.CompleteStepAsync(
                    target, Sql, row.Id, descriptor.Index, StepState.Completed, durationMs,
                    execution.OutputJson, null, cancellationToken);
                WorkflowDiagnostics.StepExecuted(definition.Name, descriptor.Name, durationMs, StepState.Completed);
                await FinishAsync(
                    target, definition, row, WorkflowState.Completed, execution.StateJson, null, cancellationToken);
                return;

            default:
                var stepState = result.Kind == StepResultKind.Skip ? StepState.Skipped : StepState.Completed;
                await WorkflowCommands.CompleteStepAsync(
                    target, Sql, row.Id, descriptor.Index, stepState, durationMs,
                    execution.OutputJson, result.Reason, cancellationToken);
                WorkflowDiagnostics.StepExecuted(definition.Name, descriptor.Name, durationMs, stepState);
                await AdvanceAsync(
                    target, producer, definition, row, descriptor, result, execution, cancellationToken);
                return;
        }
    }

    private async Task AdvanceAsync(
        SqlTarget target,
        IQueueProducer producer,
        WorkflowDefinition definition,
        WorkflowRow row,
        WorkflowStepDescriptor descriptor,
        StepResult result,
        StepExecution execution,
        CancellationToken cancellationToken)
    {
        int nextIndex = descriptor.Index + 1;
        var next = definition.StepAt(nextIndex);

        if (next is null)
        {
            await FinishAsync(
                target, definition, row, WorkflowState.Completed, execution.StateJson, null, cancellationToken);
            return;
        }

        var delay = result.Kind == StepResultKind.Suspend ? result.Delay ?? TimeSpan.Zero : TimeSpan.Zero;

        if (next.Kind == StepKind.WaitForSignal && next.SignalName is { } signalName)
        {
            // A signal that arrived early is already recorded, so the wait is over before it starts.
            var already = await WorkflowCommands.SelectSignalAsync(
                target, Sql, row.Id, signalName, cancellationToken);

            if (already is null)
            {
                int parked = await WorkflowCommands.AdvanceAsync(
                    target, Sql, row.Id, descriptor.Index, nextIndex, next.Name,
                    WorkflowState.WaitingSignal, execution.StateJson, null, signalName,
                    next.SignalTimeout, WorkerId, cancellationToken);

                if (parked == 0)
                    await OnCursorRaceAsync(row, descriptor, cancellationToken);
                else
                    logger.LogInformation(
                        "Workflow {WorkflowId} is waiting for signal '{Signal}'{Timeout}.",
                        row.Id, signalName,
                        next.SignalTimeout is { } t ? $" (timing out in {t})" : " (no timeout)");
                return;
            }

            logger.LogDebug(
                "Signal '{Signal}' had already arrived for workflow {WorkflowId}; not parking it.",
                signalName, row.Id);
        }

        var state = delay > TimeSpan.Zero ? WorkflowState.Suspended : WorkflowState.Running;

        int advanced = await WorkflowCommands.AdvanceAsync(
            target, Sql, row.Id, descriptor.Index, nextIndex, next.Name, state, execution.StateJson,
            delay > TimeSpan.Zero ? delay : null, null, null, WorkerId, cancellationToken);

        if (advanced == 0)
        {
            await OnCursorRaceAsync(row, descriptor, cancellationToken);
            return;
        }

        await StepDispatcher.DispatchStepAsync(
            producer, definition, row.Id, next, row.Priority + next.Priority,
            catalog.MaxAttemptsFor(next), delay, cancellationToken);
    }

    /// <summary>
    /// The cursor moved while this attempt was running. Impossible on the transactional path — the
    /// instance row is locked for the whole attempt — so there it is a bug worth failing loudly for.
    /// On the other path it is the expected outcome of a redelivery, and the work simply stops here.
    /// </summary>
    private Task OnCursorRaceAsync(WorkflowRow row, WorkflowStepDescriptor descriptor, CancellationToken ct)
    {
        if (Transactional)
            throw new InvalidOperationException(
                $"Workflow {row.Id:D}: the cursor moved off step {descriptor.Index} ('{descriptor.Name}') " +
                "while it was running under a row lock. Rolling the attempt back.");

        logger.LogInformation(
            "Workflow {WorkflowId}: the cursor had already moved off step {Step}; the result of this " +
            "non-transactional attempt is dropped.",
            row.Id, descriptor.Name);
        return Task.CompletedTask;
    }

    private async Task RunCompensationAsync(
        QueueConnection borrowed,
        SqlTarget target,
        WorkflowDefinition definition,
        WorkflowRow row,
        StepMessagePayload payload,
        QueueMessage message,
        CancellationToken cancellationToken)
    {
        if (row.State != WorkflowState.Compensating || row.CompensationIndex != payload.StepIndex)
        {
            logger.LogDebug(
                "Workflow {WorkflowId} is not compensating step {Step}; dropping the message.",
                row.Id, payload.StepIndex);
            return;
        }

        var descriptor = definition.StepAt(payload.StepIndex);
        var execution = descriptor is null
            ? null
            : BuildExecution(borrowed, definition, row, descriptor, message, null);

        if (descriptor?.Compensator is { } compensate
            && string.Equals(descriptor.Name, payload.StepName, StringComparison.Ordinal))
        {
            using var watch = cancellations.Watch(row.Id);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, watch.Token);

            bool savepoint = false;
            if (target.Transaction is not null)
            {
                await WorkflowCommands.SavepointAsync(target, Sql, cancellationToken);
                savepoint = true;
            }

            try
            {
                await compensate(execution!, linked.Token);
                if (savepoint)
                    await WorkflowCommands.ReleaseSavepointAsync(target, Sql, cancellationToken);
            }
            catch (Exception ex)
            {
                await RecordAttemptAsync(row, descriptor, message, ex);
                logger.LogError(ex,
                    "Compensation of step {Step} of workflow {WorkflowId} failed on attempt {Attempt}/{Max}.",
                    descriptor.Name, row.Id, message.Attempts, message.MaxAttempts);

                // A rollback that cannot finish is the one outcome worth a human: parking it beats both
                // retrying forever and quietly declaring the workflow rolled back when it is not.
                if (message.IsLastAttempt && savepoint)
                {
                    await UndoStepAsync(target, savepoint);
                    await ParkAsync(target, row,
                        $"The compensation of step '{descriptor.Name}' failed {message.Attempts} times, so the " +
                        "rollback is incomplete: " + ex.Message,
                        definition.Name, CancellationToken.None);
                    return;
                }

                throw;
            }

            await WorkflowCommands.MarkCompensatedAsync(
                target, Sql, row.Id, descriptor.Index, cancellationToken);
            WorkflowDiagnostics.StepCompensated(definition.Name, descriptor.Name);
            logger.LogInformation(
                "Compensated step {Step} of workflow {WorkflowId}.", descriptor.Name, row.Id);
        }
        else
        {
            logger.LogWarning(
                "Workflow {WorkflowId}: no compensation to run for step {Step} ('{Name}'); continuing the rollback.",
                row.Id, payload.StepIndex, payload.StepName);
        }

        var previous = await FindCompensableAsync(
            target, definition, row.Id, payload.StepIndex - 1, cancellationToken);

        if (previous is null)
        {
            var final = row.CancelRequested ? WorkflowState.Cancelled : WorkflowState.Failed;
            await FinishAsync(
                target, definition, row, final, execution?.StateJson, null, cancellationToken);
            logger.LogInformation("Rollback of workflow {WorkflowId} is complete; it is {State}.", row.Id, final);
            return;
        }

        await WorkflowCommands.AdvanceCompensationAsync(
            target, Sql, row.Id, previous.Index, execution?.StateJson, cancellationToken);
        await StepDispatcher.DispatchCompensationAsync(
            queueClient.Producer, definition, row.Id, previous,
            row.Priority + previous.Priority, catalog.MaxAttemptsFor(previous), cancellationToken);
    }

    /// <summary>
    /// Start the rollback, or finish the instance when there is nothing to undo. Shared by every way a
    /// workflow can end badly: a step that refused, a cancellation, an exhausted attempt budget.
    /// </summary>
    private async Task TerminateAsync(
        SqlTarget target,
        IQueueProducer producer,
        WorkflowDefinition definition,
        WorkflowRow row,
        WorkflowState finalState,
        string? error,
        string? stateJson,
        CancellationToken cancellationToken)
    {
        var compensable = await FindCompensableAsync(
            target, definition, row.Id, row.CurrentStep, cancellationToken);

        if (compensable is null)
        {
            await FinishAsync(target, definition, row, finalState, stateJson, error, cancellationToken);
            return;
        }

        await WorkflowCommands.StartCompensationAsync(
            target, Sql, row.Id, compensable.Index, error, cancellationToken);
        await StepDispatcher.DispatchCompensationAsync(
            producer, definition, row.Id, compensable,
            row.Priority + compensable.Priority, catalog.MaxAttemptsFor(compensable), cancellationToken);

        logger.LogInformation(
            "Workflow {WorkflowId} is rolling back, starting at step {Step}.", row.Id, compensable.Name);
    }

    private async Task FinishAsync(
        SqlTarget target,
        WorkflowDefinition definition,
        WorkflowRow row,
        WorkflowState state,
        string? stateJson,
        string? error,
        CancellationToken cancellationToken)
    {
        await WorkflowCommands.FinishAsync(
            target, Sql, row.Id, state, stateJson, error, WorkerId, cancellationToken);
        WorkflowDiagnostics.WorkflowFinished(definition.Name, state);

        logger.Log(
            state == WorkflowState.Completed ? LogLevel.Information : LogLevel.Warning,
            "Workflow {Definition} {WorkflowId} is {State}{Error}.",
            definition.Name, row.Id, state, error is null ? "" : $": {error}");
    }

    /// <summary>Highest completed step at or below <paramref name="maxIndex"/> that can be undone.</summary>
    private async Task<WorkflowStepDescriptor?> FindCompensableAsync(
        SqlTarget target, WorkflowDefinition definition, Guid workflowId, int maxIndex, CancellationToken ct)
    {
        if (maxIndex < 0)
            return null;

        var steps = await WorkflowCommands.ListStepsAsync(target, Sql, workflowId, ct);

        for (int i = steps.Count - 1; i >= 0; i--)
        {
            var record = steps[i];
            if (record.StepIndex > maxIndex || record.State != StepState.Completed)
                continue;

            var descriptor = definition.StepAt(record.StepIndex);
            if (descriptor is { HasCompensation: true }
                && string.Equals(descriptor.Name, record.StepName, StringComparison.Ordinal))
                return descriptor;
        }

        return null;
    }

    private async Task ParkAsync(
        SqlTarget target, WorkflowRow row, string reason, string definitionName, CancellationToken ct)
    {
        logger.LogError(
            "Workflow {WorkflowId} needs attention and will not be resumed automatically: {Reason}",
            row.Id, reason);
        await WorkflowCommands.MarkNeedsAttentionAsync(target, Sql, row.Id, reason, ct);
        WorkflowDiagnostics.WorkflowFinished(definitionName, WorkflowState.NeedsAttention);
    }

    /// <summary>
    /// Discard whatever the step wrote, and hand back a transaction that can still be used. A no-op when
    /// there was no transaction to take a savepoint in — the non-transactional worker, where the step's
    /// effects are the caller's problem by construction.
    /// </summary>
    private async Task UndoStepAsync(SqlTarget target, bool savepoint)
    {
        if (savepoint)
            await WorkflowCommands.RollbackToSavepointAsync(target, Sql, CancellationToken.None);
    }

    /// <summary>
    /// Write the failure verdict — the step marked failed, the workflow failed or rolling back — and return
    /// <c>true</c> when it is durable, in which case the caller returns normally so the worker host commits
    /// it and consumes the message.
    /// <para>
    /// On the transactional path this happens in the step's own transaction, after rolling back to the
    /// savepoint: the verdict then commits with the fenced acknowledge, atomically. On the other path it
    /// gets a transaction of its own. Either way the workflow is never left <see cref="WorkflowState.Running"/>
    /// with a dead-lettered step and nobody looking at it.
    /// </para>
    /// </summary>
    private async Task<bool> TryRecordFailureAsync(
        SqlTarget target,
        bool savepoint,
        WorkflowDefinition definition,
        WorkflowRow row,
        WorkflowStepDescriptor descriptor,
        QueueMessage message,
        Exception exception)
    {
        var verdict = row.CancelRequested ? WorkflowState.Cancelled : WorkflowState.Failed;

        try
        {
            if (Transactional)
            {
                if (!savepoint)
                    return false;

                await UndoStepAsync(target, savepoint);
                await WorkflowCommands.FailStepAsync(
                    target, Sql, row.Id, descriptor.Index, descriptor.Name, message.Attempts, WorkerId,
                    exception.ToString(), CancellationToken.None);
                await TerminateAsync(
                    target, queueClient.Producer, definition, row, verdict, exception.Message, null,
                    CancellationToken.None);
                return true;
            }

            await using var transaction = await source.BeginTransactionAsync(CancellationToken.None);
            await using var enlisted = await source.AcquireAsync(CancellationToken.None);
            var own = SqlTarget.From(enlisted);

            await WorkflowCommands.FailStepAsync(
                own, Sql, row.Id, descriptor.Index, descriptor.Name, message.Attempts, WorkerId,
                exception.ToString(), CancellationToken.None);
            await TerminateAsync(
                own, queueClient.Producer, definition, row, verdict, exception.Message, null,
                CancellationToken.None);

            if (transaction is not null)
                await transaction.CommitAsync(CancellationToken.None);
            return true;
        }
        catch (Exception ex)
        {
            // Never mask the failure being reported: let the message take the retry/dead-letter path, and
            // the maintenance sweep is the backstop for the state that was not written.
            logger.LogWarning(ex,
                "Could not record the failure of step {Step} of workflow {WorkflowId}; falling back to the queue's retry path.",
                descriptor.Name, row.Id);
            return false;
        }
    }

    /// <summary>
    /// Record one failed attempt on a connection of the engine's own, because the step's transaction is
    /// about to be rolled back and would take the explanation with it. Touches only the attempts table, so
    /// it can never contend with the instance row this transaction is holding.
    /// </summary>
    private async Task RecordAttemptAsync(
        WorkflowRow row, WorkflowStepDescriptor descriptor, QueueMessage message, Exception exception)
    {
        if (!catalog.Options.RecordFailedAttempts)
            return;

        try
        {
            await using var connection = await sideChannel.TryOpenAsync(CancellationToken.None);
            if (connection is null)
                return;

            await WorkflowCommands.InsertAttemptAsync(
                new SqlTarget(connection, null), Sql, row.Id, descriptor.Index, descriptor.Name,
                message.Attempts, WorkerId, exception.ToString(), CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex,
                "Could not record the failed attempt of step {Step} of workflow {WorkflowId}.",
                descriptor.Name, row.Id);
        }
    }

    private StepExecution BuildExecution(
        QueueConnection borrowed,
        WorkflowDefinition definition,
        WorkflowRow row,
        WorkflowStepDescriptor descriptor,
        QueueMessage message,
        WorkflowSignal? signal) => new()
        {
            Services = services,
            Serializer = serializer,
            Descriptor = descriptor,
            WorkflowId = row.Id,
            Definition = definition.Name,
            CorrelationId = row.CorrelationId,
            Attempt = message.Attempts,
            MaxAttempts = message.MaxAttempts,
            IsTransactional = Transactional,
            Connection = borrowed.Connection,
            Transaction = borrowed.Transaction,
            DbContext = dbContexts.Resolve(services),
            Signal = signal,
            // The state a step reads is the state the previous one committed; an instance that has not
            // run a step yet reads its input.
            StateJson = row.Context ?? row.Input,
        };
}

/// <summary>
/// Worker for the steps covered by the engine's transaction — the default, and the one that makes a
/// step's writes and its bookkeeping commit as one.
/// </summary>
internal sealed class WorkflowStepHandler<TWorkflow>(
    WorkflowCatalog catalog,
    IQueueConnectionSource source,
    ILiteQueueClient queueClient,
    IWorkflowStateSerializer serializer,
    WorkflowSideChannel sideChannel,
    WorkflowCancellationRegistry cancellations,
    WorkflowDbContextAccessor dbContexts,
    IServiceProvider services,
    ILogger<WorkflowStepHandler<TWorkflow>> logger)
    : WorkflowStepHandlerBase(
        catalog, source, queueClient, serializer, sideChannel, cancellations, dbContexts, services, logger)
    where TWorkflow : Workflow
{
    protected override WorkflowDefinition Definition { get; } = catalog.Require(typeof(TWorkflow));

    protected override bool Transactional => true;

    public override string Queue => Definition.Queue;
}

/// <summary>
/// Worker for the <see cref="IStepOptions{TState}.NonTransactional"/> steps: no transaction is held
/// across the call, and the bookkeeping commits after the fact. Separate queue, separate workers, so a
/// slow external call cannot occupy the capacity the database-only steps need.
/// </summary>
internal sealed class WorkflowExternalStepHandler<TWorkflow>(
    WorkflowCatalog catalog,
    IQueueConnectionSource source,
    ILiteQueueClient queueClient,
    IWorkflowStateSerializer serializer,
    WorkflowSideChannel sideChannel,
    WorkflowCancellationRegistry cancellations,
    WorkflowDbContextAccessor dbContexts,
    IServiceProvider services,
    ILogger<WorkflowExternalStepHandler<TWorkflow>> logger)
    : WorkflowStepHandlerBase(
        catalog, source, queueClient, serializer, sideChannel, cancellations, dbContexts, services, logger)
    where TWorkflow : Workflow
{
    protected override WorkflowDefinition Definition { get; } = catalog.Require(typeof(TWorkflow));

    protected override bool Transactional => false;

    public override string Queue => Definition.NonTransactionalQueue ?? Definition.Queue;
}

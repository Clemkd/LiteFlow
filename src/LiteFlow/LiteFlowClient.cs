using System.Data.Common;
using LiteFlow.Internal;
using LiteQueue;
using LiteQueue.Connectors;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace LiteFlow;

internal sealed class LiteFlowClient(
    WorkflowCatalog catalog,
    IQueueConnectionSource source,
    ILiteQueueClient queueClient,
    IWorkflowStateSerializer serializer,
    ILogger<LiteFlowClient> logger) : ILiteFlowClient
{
    private WorkflowSql Sql => catalog.Sql;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        // The queue schema first: a workflow with nowhere to dispatch its first step is worse than one
        // with no state table.
        await queueClient.InitializeAsync(cancellationToken);
        await catalog.InitializeAsync(source, cancellationToken);
    }

    public Task<WorkflowHandle> StartAsync<TWorkflow, TState>(
        TState state, WorkflowStartOptions? options = null, CancellationToken cancellationToken = default)
        where TWorkflow : Workflow<TState>
        where TState : class
    {
        ArgumentNullException.ThrowIfNull(state);
        return StartCoreAsync(catalog.Require(typeof(TWorkflow)), state, options, cancellationToken);
    }

    public Task<WorkflowHandle> StartAsync(
        string definition, object state, WorkflowStartOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(definition);
        ArgumentNullException.ThrowIfNull(state);

        var compiled = catalog.Require(definition);
        if (!compiled.StateType.IsInstanceOfType(state))
            throw new ArgumentException(
                $"Workflow '{definition}' carries {compiled.StateType.Name}, not {state.GetType().Name}.",
                nameof(state));

        return StartCoreAsync(compiled, state, options, cancellationToken);
    }

    private async Task<WorkflowHandle> StartCoreAsync(
        WorkflowDefinition definition, object state, WorkflowStartOptions? options, CancellationToken ct)
    {
        var opts = options ?? new WorkflowStartOptions();
        // Version 7: time-ordered, so instances inserted together land on neighbouring index pages
        // instead of scattering writes across the whole primary key.
        var id = opts.WorkflowId ?? Guid.CreateVersion7();
        string stateJson = serializer.Serialize(state);

        await InitializeAsync(ct);

        var first = definition.Steps[0];

        // One transaction for the insert and the dispatch: an instance that exists is an instance
        // something is going to pick up.
        await using var transaction = await source.BeginTransactionAsync(ct);
        await using var borrowed = await source.AcquireAsync(ct);
        var target = SqlTarget.From(borrowed);

        var inserted = await WorkflowCommands.InsertAsync(
            target, Sql, id, definition, stateJson, opts, ct);

        if (inserted is null)
        {
            // Nothing was inserted: the idempotency key (or the supplied id) is already taken. That is the
            // expected answer to a retry, not an error.
            Guid existing = id;
            if (opts.IdempotencyKey is { } key)
            {
                existing = await WorkflowCommands.FindByIdempotencyKeyAsync(
                               target, Sql, definition.Name, key, ct) ?? id;
            }

            if (transaction is not null)
                await transaction.CommitAsync(ct);

            logger.LogDebug(
                "Workflow {Definition} was already started as {WorkflowId} (idempotency key '{Key}').",
                definition.Name, existing, opts.IdempotencyKey);

            return new WorkflowHandle(existing, definition.Name, AlreadyExisted: true);
        }

        if (first.Kind == StepKind.WaitForSignal && first.SignalName is { } signalName)
        {
            // A workflow that opens on a wait costs nothing until the signal arrives: park it instead of
            // queueing a message that would only park it a round-trip later.
            await WorkflowCommands.AdvanceAsync(
                target, Sql, id, 0, 0, first.Name, WorkflowState.WaitingSignal, stateJson,
                null, signalName, first.SignalTimeout, null, ct);
        }
        else
        {
            await StepDispatcher.DispatchStepAsync(
                queueClient.Producer, definition, id, first, opts.Priority + first.Priority,
                catalog.MaxAttemptsFor(first), opts.Delay, ct);
        }

        if (transaction is not null)
            await transaction.CommitAsync(ct);

        WorkflowDiagnostics.WorkflowStarted(definition.Name);
        logger.LogInformation(
            "Started workflow {Definition} {WorkflowId} at step {Step}.", definition.Name, id, first.Name);

        return new WorkflowHandle(id, definition.Name, AlreadyExisted: false);
    }

    public async Task<bool> CancelAsync(
        Guid workflowId, string? reason = null, CancellationToken cancellationToken = default)
    {
        await using var transaction = await source.BeginTransactionAsync(cancellationToken);
        await using var borrowed = await source.AcquireAsync(cancellationToken);
        var target = SqlTarget.From(borrowed);

        // Read without locking the instance: a running step holds that row for its whole transaction, and
        // a cancellation that waited for the step it is trying to stop would be useless.
        var row = await WorkflowCommands.LoadAsync(target, Sql, workflowId, cancellationToken);
        if (row is null || row.State >= WorkflowState.Completed)
            return false;

        await WorkflowCommands.RequestCancelAsync(target, Sql, workflowId, reason, cancellationToken);

        // An instance waiting on a signal has no message in flight, so nothing would ever notice the
        // request. Dispatching its current step hands it to a worker that will honour the cancellation — and
        // the dedup key makes this a no-op when a message does exist. A suspended instance already has its
        // (delayed) message, so waking it early is the maintenance sweep's job instead.
        if (row.State is WorkflowState.WaitingSignal)
        {
            if (catalog.TryGet(row.Definition, out var definition)
                && definition!.StepAt(row.CurrentStep) is { } step)
            {
                await StepDispatcher.DispatchStepAsync(
                    queueClient.Producer, definition, workflowId, step, row.Priority + step.Priority,
                    catalog.MaxAttemptsFor(step), TimeSpan.Zero, cancellationToken);
            }
            else
            {
                logger.LogWarning(
                    "Workflow {WorkflowId} ('{Definition}') was marked cancelled, but this process does not " +
                    "host that definition, so its waiting instance cannot be woken from here. Cancel it from " +
                    "a process that registers it, or wait for one to sweep it.",
                    workflowId, row.Definition);
            }
        }

        if (transaction is not null)
            await transaction.CommitAsync(cancellationToken);

        logger.LogInformation(
            "Cancellation requested for workflow {WorkflowId} ({State}){Reason}.",
            workflowId, row.State, reason is null ? "" : $": {reason}");
        return true;
    }

    public async Task<SignalOutcome> SignalAsync(
        Guid workflowId, string name, object? payload = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        await using var transaction = await source.BeginTransactionAsync(cancellationToken);
        await using var borrowed = await source.AcquireAsync(cancellationToken);
        var target = SqlTarget.From(borrowed);

        // No lock on the instance: only the guarded resume below touches it, and it can only match an
        // instance that is parked — which by definition has no step holding it.
        var row = await WorkflowCommands.LoadAsync(target, Sql, workflowId, cancellationToken);
        if (row is null)
            return SignalOutcome.NotFound;
        if (row.State >= WorkflowState.Completed)
            return SignalOutcome.Terminal;

        string? payloadJson = payload is null ? null : serializer.Serialize(payload);
        var received = await WorkflowCommands.InsertSignalAsync(
            target, Sql, workflowId, name, payloadJson, cancellationToken);

        if (received is null)
        {
            if (transaction is not null)
                await transaction.CommitAsync(cancellationToken);
            logger.LogDebug(
                "Signal '{Signal}' for workflow {WorkflowId} had already been delivered; ignoring the duplicate.",
                name, workflowId);
            return SignalOutcome.Duplicate;
        }

        var outcome = SignalOutcome.Recorded;

        if (row.State == WorkflowState.WaitingSignal
            && string.Equals(row.WaitSignal, name, StringComparison.Ordinal))
        {
            var resumed = await WorkflowCommands.ResumeFromSignalAsync(
                target, Sql, workflowId, name, cancellationToken);

            if (resumed is { } position)
            {
                var definition = catalog.Require(row.Definition);
                var step = definition.StepAt(position.CurrentStep)
                           ?? throw new WorkflowDefinitionException(
                               $"Workflow {workflowId:D} waits on step {position.CurrentStep} " +
                               $"('{position.StepName}'), which '{definition.Name}' no longer declares.");

                await StepDispatcher.DispatchStepAsync(
                    queueClient.Producer, definition, workflowId, step, row.Priority + step.Priority,
                    catalog.MaxAttemptsFor(step), TimeSpan.Zero, cancellationToken);

                outcome = SignalOutcome.Resumed;
            }
        }

        if (transaction is not null)
            await transaction.CommitAsync(cancellationToken);

        logger.LogInformation(
            "Signal '{Signal}' delivered to workflow {WorkflowId}: {Outcome}.", name, workflowId, outcome);
        return outcome;
    }

    public async Task<bool> ResumeAsync(Guid workflowId, CancellationToken cancellationToken = default)
    {
        await using var transaction = await source.BeginTransactionAsync(cancellationToken);
        await using var borrowed = await source.AcquireAsync(cancellationToken);
        var target = SqlTarget.From(borrowed);

        var row = await WorkflowCommands.LoadAsync(target, Sql, workflowId, cancellationToken);
        // Only the three "ended badly" states are resumable: a live instance needs no help, and a
        // completed one has nothing left to run.
        if (row is null || row.State <= WorkflowState.Completed)
            return false;

        var definition = catalog.Require(row.Definition);

        // Re-anchor by name: the index the instance stopped at may now hold a different step, which is
        // exactly why it was parked in the first place.
        int index = definition.IndexOf(row.CurrentStepName);
        if (index < 0)
        {
            logger.LogError(
                "Workflow {WorkflowId} cannot be resumed: '{Definition}' no longer declares a step named " +
                "'{Step}'. Add it back, or start a new instance.",
                workflowId, definition.Name, row.CurrentStepName);
            return false;
        }

        var step = definition.Steps[index];

        var resumed = await WorkflowCommands.ResumeAsync(
            target, Sql, workflowId, definition, step, cancellationToken);
        if (resumed is null)
            return false;

        // Drop any cancellation request with it: resuming an instance that is still marked cancelled would
        // only stop it again on its first step.
        await WorkflowCommands.ClearCancellationAsync(target, Sql, workflowId, cancellationToken);

        await StepDispatcher.DispatchStepAsync(
            queueClient.Producer, definition, workflowId, step, row.Priority + step.Priority,
            catalog.MaxAttemptsFor(step), TimeSpan.Zero, cancellationToken);

        if (transaction is not null)
            await transaction.CommitAsync(cancellationToken);

        logger.LogInformation(
            "Workflow {WorkflowId} resumed at step {Step} (was {State}).", workflowId, step.Name, row.State);
        return true;
    }

    public async Task<WorkflowInstance?> GetAsync(
        Guid workflowId, CancellationToken cancellationToken = default)
    {
        await using var borrowed = await source.AcquireAsync(cancellationToken);
        var row = await WorkflowCommands.LoadAsync(
            SqlTarget.From(borrowed), Sql, workflowId, cancellationToken);
        return row?.ToInstance();
    }

    public async Task<IReadOnlyList<WorkflowStepRecord>> GetStepsAsync(
        Guid workflowId, CancellationToken cancellationToken = default)
    {
        await using var borrowed = await source.AcquireAsync(cancellationToken);
        return await WorkflowCommands.ListStepsAsync(
            SqlTarget.From(borrowed), Sql, workflowId, cancellationToken);
    }

    public async Task<IReadOnlyList<WorkflowInstance>> ListAsync(
        WorkflowQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        await using var borrowed = await source.AcquireAsync(cancellationToken);
        return await WorkflowCommands.ListAsync(SqlTarget.From(borrowed), Sql, query, cancellationToken);
    }

    public async Task<WorkflowStats> GetStatsAsync(
        string? definition = null, CancellationToken cancellationToken = default)
    {
        await using var borrowed = await source.AcquireAsync(cancellationToken);
        return await WorkflowCommands.StatsAsync(
            SqlTarget.From(borrowed), Sql, definition, cancellationToken);
    }

    public async Task<long> PruneAsync(CancellationToken cancellationToken = default)
    {
        await using var borrowed = await source.AcquireAsync(cancellationToken);
        var target = SqlTarget.From(borrowed);

        long archived = await WorkflowCommands.ArchiveTerminalAsync(
            target, Sql, catalog.Options.InstanceRetention, 1000, cancellationToken);

        long dropped = catalog.Options.ArchiveRetention > TimeSpan.Zero
            ? await WorkflowCommands.PruneArchiveAsync(
                target, Sql, catalog.Options.ArchiveRetention, cancellationToken)
            : 0;

        return archived + dropped;
    }

    public ILiteFlowClient Using(DbConnection connection, DbTransaction? transaction = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        return new LiteFlowClient(
            catalog,
            new ExistingConnectionQueueSource(connection, transaction),
            queueClient.Using(connection, transaction),
            serializer,
            logger);
    }

    public ILiteFlowClient Using(DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new LiteFlowClient(
            catalog,
            new EfCoreQueueConnectionSource(context),
            queueClient.Using(context),
            serializer,
            logger);
    }
}

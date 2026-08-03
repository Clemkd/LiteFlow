using LiteQueue;

namespace LiteFlow.Internal;

/// <summary>
/// How a workflow ends badly — in one place, because there is more than one way to find out that it has.
/// <para>
/// A step's own worker learns it from the exception it just caught. The reconciliation sweep learns it from a
/// dead letter left behind by a worker that died before it could say anything. Both must reach exactly the same
/// state: the step marked failed, the compensations started if there are any, and the instance terminal — never
/// left running, and never quietly retried.
/// </para>
/// </summary>
internal static class WorkflowTermination
{
    /// <summary>
    /// Highest completed step at or below <paramref name="maxIndex"/> that can be undone, matched by name so a
    /// definition that has changed since cannot make the engine compensate the wrong step.
    /// </summary>
    public static async Task<WorkflowStepDescriptor?> FindCompensableAsync(
        SqlTarget target,
        WorkflowSql sql,
        WorkflowDefinition definition,
        Guid workflowId,
        int maxIndex,
        CancellationToken ct)
    {
        if (maxIndex < 0)
            return null;

        var steps = await WorkflowCommands.ListStepsAsync(target, sql, workflowId, ct);

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

    /// <summary>
    /// End the instance: start the rollback when there is something to undo, otherwise write the terminal state.
    /// Returns the state actually written, which is <see cref="WorkflowState.Compensating"/> when a rollback
    /// began — the caller's <paramref name="finalState"/> is then reached once the rollback finishes.
    /// </summary>
    public static async Task<WorkflowState> TerminateAsync(
        SqlTarget target,
        WorkflowSql sql,
        IQueueProducer producer,
        WorkflowCatalog catalog,
        WorkflowDefinition definition,
        WorkflowRow row,
        WorkflowState finalState,
        string? error,
        string? stateJson,
        string? workerId,
        CancellationToken ct)
    {
        var compensable = await FindCompensableAsync(target, sql, definition, row.Id, row.CurrentStep, ct);

        if (compensable is null)
        {
            await WorkflowCommands.FinishAsync(
                target, sql, row.Id, finalState, stateJson, error, workerId, ct);
            WorkflowDiagnostics.WorkflowFinished(definition.Name, finalState);
            return finalState;
        }

        await WorkflowCommands.StartCompensationAsync(target, sql, row.Id, compensable.Index, error, ct);
        await StepDispatcher.DispatchCompensationAsync(
            producer, definition, row.Id, compensable,
            row.Priority + compensable.Priority, catalog.MaxAttemptsFor(compensable), ct);

        return WorkflowState.Compensating;
    }
}

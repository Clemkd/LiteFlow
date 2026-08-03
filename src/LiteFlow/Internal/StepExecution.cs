using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace LiteFlow.Internal;

/// <summary>Runs one step, typed by the closure the builder captured.</summary>
internal delegate Task<StepResult> StepExecutor(StepExecution execution, CancellationToken cancellationToken);

/// <summary>Undoes one step, typed the same way.</summary>
internal delegate Task StepCompensator(StepExecution execution, CancellationToken cancellationToken);

/// <summary>
/// Everything one attempt needs, passed to the typed closure that knows how to turn it into a
/// <see cref="IWorkflowStepContext{TState}"/>.
/// <para>
/// <see cref="StateJson"/> is deliberately mutable: the closure deserializes it, hands the object to
/// the step, then writes the mutated state back here — the caller (which does not know
/// <c>TState</c>) then persists exactly that string in the step's transaction.
/// </para>
/// </summary>
internal sealed class StepExecution
{
    public required IServiceProvider Services { get; init; }

    public required IWorkflowStateSerializer Serializer { get; init; }

    public required WorkflowStepDescriptor Descriptor { get; init; }

    public required Guid WorkflowId { get; init; }

    public required string Definition { get; init; }

    public string? CorrelationId { get; init; }

    public required int Attempt { get; init; }

    public required int MaxAttempts { get; init; }

    public required bool IsTransactional { get; init; }

    public required DbConnection Connection { get; init; }

    public DbTransaction? Transaction { get; init; }

    public DbContext? DbContext { get; init; }

    public WorkflowSignal? Signal { get; init; }

    /// <summary>The state as stored, on the way in; as the step left it, on the way out.</summary>
    public string? StateJson { get; set; }

    /// <summary>The step's declared output, set by the closure from <see cref="StepResult.Output"/>.</summary>
    public string? OutputJson { get; set; }
}

/// <summary>
/// The context handed to caller code. A thin view over <see cref="StepExecution"/> plus the
/// deserialized state — no behaviour of its own, so a step can be unit-tested against a hand-built
/// instance of it.
/// </summary>
internal sealed class WorkflowStepContext<TState>(StepExecution execution, TState state)
    : IWorkflowStepContext<TState>
    where TState : class
{
    public Guid WorkflowId => execution.WorkflowId;

    public string Definition => execution.Definition;

    public int StepIndex => execution.Descriptor.Index;

    public string StepName => execution.Descriptor.Name;

    public int Attempt => execution.Attempt;

    public int MaxAttempts => execution.MaxAttempts;

    public bool IsLastAttempt => execution.Attempt >= execution.MaxAttempts;

    public string? CorrelationId => execution.CorrelationId;

    public TState State { get; } = state;

    public string IdempotencyKey => StepKeys.Idempotency(execution.WorkflowId, execution.Descriptor.Index);

    public WorkflowSignal? Signal => execution.Signal;

    public bool IsTransactional => execution.IsTransactional;

    public DbConnection Connection => execution.Connection;

    public DbTransaction? Transaction => execution.Transaction;

    public DbContext? DbContext => execution.DbContext;

    public IServiceProvider Services => execution.Services;

    public DbCommand CreateCommand()
    {
        var cmd = execution.Connection.CreateCommand();
        cmd.Transaction = execution.Transaction;
        return cmd;
    }
}

/// <summary>
/// The keys that make redelivery harmless. Both are derived, never stored: the dedup key is what
/// stops a step from being dispatched twice, and the idempotency key is what a caller hands to an
/// external system so <i>its</i> deduplication does the same.
/// </summary>
internal static class StepKeys
{
    public static string Idempotency(Guid workflowId, int stepIndex) => $"{workflowId:N}:{stepIndex}";

    /// <summary>Dedup key of a step message: one pending message per (instance, step), enforced by LiteQueue's unique index.</summary>
    public static string Dispatch(Guid workflowId, int stepIndex) => $"{workflowId:N}:{stepIndex}";

    /// <summary>Dedup key of a compensation message, distinct from the step's own.</summary>
    public static string Compensation(Guid workflowId, int stepIndex) => $"{workflowId:N}:c{stepIndex}";
}

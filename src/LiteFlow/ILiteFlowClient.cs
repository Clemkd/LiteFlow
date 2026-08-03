using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace LiteFlow;

/// <summary>
/// Entry point for starting, steering and inspecting workflows. Scoped: one client is bound to one
/// connector — by default the EF Core <c>DbContext</c> of the current DI scope — and therefore to one
/// connection at a time.
/// <para>
/// Every mutating call runs on that connection, which is what lets you start or cancel a workflow
/// <i>inside your own transaction</i>: the instance appears exactly when your business data does, or
/// not at all.
/// </para>
/// </summary>
public interface ILiteFlowClient
{
    /// <summary>Create the schema, tables and indexes if missing (idempotent, once per process).</summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Start an instance of <typeparamref name="TWorkflow"/> with <paramref name="state"/> as its input
    /// and initial state, and dispatch its first step.
    /// <para>
    /// The insert and the dispatch are one statement pair in one transaction, so an instance can never
    /// exist without something queued to move it — the failure mode that leaves a workflow stuck at step
    /// zero forever.
    /// </para>
    /// <para>
    /// Give <see cref="WorkflowStartOptions.IdempotencyKey"/> the business identity of the work and the
    /// call becomes safe to retry: the second one returns the first instance with
    /// <see cref="WorkflowHandle.AlreadyExisted"/> set.
    /// </para>
    /// </summary>
    Task<WorkflowHandle> StartAsync<TWorkflow, TState>(
        TState state,
        WorkflowStartOptions? options = null,
        CancellationToken cancellationToken = default)
        where TWorkflow : Workflow<TState>
        where TState : class;

    /// <summary>
    /// Start an instance by definition name, for callers that dispatch dynamically (a table of
    /// workflow names, a message type). <paramref name="state"/> must be assignable to the
    /// definition's state type.
    /// </summary>
    Task<WorkflowHandle> StartAsync(
        string definition,
        object state,
        WorkflowStartOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Ask an instance to stop. Cancellation is a flag in the database, checked before every step, so it
    /// is always honoured — even if the instance is currently on another host, or waiting on a timer, or
    /// has not been dispatched yet. A step already running is interrupted through its
    /// <see cref="CancellationToken"/> within
    /// <see cref="LiteFlowOptions.CancellationPollInterval"/>.
    /// <para>
    /// Compensations run if the definition declares any, so cancelling is a rollback, not an abandonment.
    /// </para>
    /// </summary>
    /// <returns><c>false</c> when the instance does not exist or has already finished.</returns>
    Task<bool> CancelAsync(Guid workflowId, string? reason = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deliver an external signal. Recorded once per <paramref name="name"/> and instance, so a partner
    /// that calls your webhook twice wakes the workflow once — and a signal that arrives before the
    /// workflow reaches its <c>WaitForSignal</c> step is kept, not dropped.
    /// </summary>
    Task<SignalOutcome> SignalAsync(
        Guid workflowId, string name, object? payload = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Put a finished-badly instance back to work at the step it stopped on:
    /// <see cref="WorkflowState.Failed"/> once the cause is fixed, or
    /// <see cref="WorkflowState.NeedsAttention"/> once you have decided the current code is right for it.
    /// The cursor is re-anchored by step <i>name</i>, so an instance parked because the sequence changed
    /// resumes on the step it was really on.
    /// </summary>
    /// <returns><c>false</c> when the instance is not in a resumable state, or its step no longer exists.</returns>
    Task<bool> ResumeAsync(Guid workflowId, CancellationToken cancellationToken = default);

    /// <summary>The instance, or <c>null</c> when it does not exist (or has been archived).</summary>
    Task<WorkflowInstance?> GetAsync(Guid workflowId, CancellationToken cancellationToken = default);

    /// <summary>The step-by-step trace of one instance: what ran, how many attempts it took, how long, and what it produced.</summary>
    Task<IReadOnlyList<WorkflowStepRecord>> GetStepsAsync(Guid workflowId, CancellationToken cancellationToken = default);

    /// <summary>Instances matching a filter, most recently touched first.</summary>
    Task<IReadOnlyList<WorkflowInstance>> ListAsync(WorkflowQuery query, CancellationToken cancellationToken = default);

    /// <summary>Counts per state, for one definition or for all of them.</summary>
    Task<WorkflowStats> GetStatsAsync(string? definition = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Move terminal instances older than <see cref="LiteFlowOptions.InstanceRetention"/> to the archive,
    /// and drop archived rows past <see cref="LiteFlowOptions.ArchiveRetention"/>. Runs automatically in
    /// the maintenance loop; call it yourself if you would rather schedule it.
    /// </summary>
    Task<long> PruneAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Bind the engine's operations to a connection you already have — so they join the transaction it is
    /// in. The connection stays yours: LiteFlow neither pools nor closes it.
    /// </summary>
    ILiteFlowClient Using(DbConnection connection, DbTransaction? transaction = null);

    /// <summary>
    /// Bind the engine's operations to the connection of a specific <see cref="DbContext"/>, joining its
    /// current transaction if it has one. Use this when the context you want is not the one the DI scope
    /// would hand out.
    /// </summary>
    ILiteFlowClient Using(DbContext context);
}

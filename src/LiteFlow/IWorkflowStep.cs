namespace LiteFlow;

/// <summary>
/// One step of a workflow: a class the engine resolves from DI (scoped by default, so it gets a
/// fresh <c>DbContext</c> per attempt) and runs exactly once per cursor position.
/// <para>
/// The contract the engine holds up:
/// </para>
/// <list type="bullet">
/// <item>
/// <b>One at a time.</b> The step's message is leased, so no other worker runs this step of this
/// instance concurrently — and the instance row is locked for the duration, so no other step of the
/// same instance runs either.
/// </item>
/// <item>
/// <b>All or nothing</b> (unless the step is declared non-transactional). Writes made through
/// <see cref="IWorkflowStepContext{TState}.DbContext"/> or
/// <see cref="IWorkflowStepContext{TState}.Connection"/> commit in the same transaction as the cursor
/// advance and the dispatch of the next step. A crash before that commit undoes everything, and the
/// step runs again from the top.
/// </item>
/// <item>
/// <b>Rerun after a crash.</b> That is the deal in exchange: a step may be executed more than once if
/// a process dies at the wrong moment. Writes through the context are safe by construction; anything
/// else (an HTTP call, a file, another database) must be idempotent — use
/// <see cref="IWorkflowStepContext{TState}.IdempotencyKey"/>, which is stable across attempts.
/// </item>
/// </list>
/// <para>
/// Throwing means "retry this attempt"; returning <see cref="StepResult.Fail"/> means "this can never
/// succeed". Honour the <see cref="CancellationToken"/>: it is cancelled when the workflow is
/// cancelled <i>and</i> when the lease is lost, and in the second case nothing the step does can be
/// committed any more.
/// </para>
/// </summary>
/// <typeparam name="TState">
/// The workflow's state type — the object passed to <see cref="ILiteFlowClient"/> at start, then
/// carried from step to step and persisted after each one.
/// </typeparam>
public interface IWorkflowStep<TState>
    where TState : class
{
    /// <summary>Do the work of this step.</summary>
    Task<StepResult> ExecuteAsync(IWorkflowStepContext<TState> context, CancellationToken cancellationToken = default);
}

/// <summary>
/// A step that knows how to undo itself. When the workflow fails or is cancelled, the engine runs the
/// compensations of the completed steps in reverse order, each as its own durable message — so a
/// crash during a rollback resumes the rollback instead of restarting it (or worse, abandoning it).
/// <para>
/// A compensation runs under the same transactional rules as a step, and must be idempotent for the
/// same reason: it can be re-delivered.
/// </para>
/// </summary>
public interface ICompensatingWorkflowStep<TState> : IWorkflowStep<TState>
    where TState : class
{
    /// <summary>Undo the effects of a previously completed <see cref="IWorkflowStep{TState}.ExecuteAsync"/>.</summary>
    Task CompensateAsync(IWorkflowStepContext<TState> context, CancellationToken cancellationToken = default);
}

/// <summary>
/// A compensation supplied as its own class, for when undoing a step is a different concern from
/// doing it (<c>ChargeCard</c> / <c>RefundCard</c>). Wire it with
/// <see cref="IStepOptions{TState}.Compensate{TCompensation}"/>.
/// </summary>
public interface IWorkflowCompensation<TState>
    where TState : class
{
    /// <summary>Undo the effects of the step this compensation is attached to.</summary>
    Task CompensateAsync(IWorkflowStepContext<TState> context, CancellationToken cancellationToken = default);
}

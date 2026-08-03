namespace LiteFlow;

/// <summary>
/// Lifecycle of a workflow instance. The numbering is part of the storage contract: the partial
/// indexes in <see cref="WorkflowSchema.CreateScript"/> split live from terminal instances with
/// <c>state &lt; 4</c>, so values must not be reordered.
/// </summary>
public enum WorkflowState
{
    /// <summary>A step is dispatched (queued or being executed) — the normal state of live work.</summary>
    Running = 0,

    /// <summary>
    /// Waiting for a timer: a step returned <see cref="StepResult.Suspend"/>, so the next step's
    /// message is queued with a delay. <c>resume_at</c> says when, and the maintenance sweep
    /// re-dispatches if that message ever went missing.
    /// </summary>
    Suspended = 1,

    /// <summary>
    /// Waiting for an external signal (<see cref="ILiteFlowClient.SignalAsync"/>). Nothing is queued
    /// while in this state — the instance costs nothing until the outside world calls back, which is
    /// what lets a workflow wait for days without holding a worker or a lease.
    /// </summary>
    WaitingSignal = 2,

    /// <summary>
    /// Rolling back: the compensations of the completed steps are running in reverse order, each as
    /// its own message, so a crash mid-rollback resumes the rollback rather than restarting it.
    /// </summary>
    Compensating = 3,

    /// <summary>Every step ran (or a step asked to finish early with <see cref="StepResult.Complete"/>).</summary>
    Completed = 4,

    /// <summary>
    /// A step gave up: it returned <see cref="StepResult.Fail"/>, or exhausted its attempts and was
    /// dead-lettered. The instance and its step rows are kept for diagnosis, and
    /// <see cref="ILiteFlowClient.ResumeAsync"/> can restart it at the failed step once the cause is
    /// fixed.
    /// </summary>
    Failed = 5,

    /// <summary>Cancellation was requested and honoured (after compensation, if any was configured).</summary>
    Cancelled = 6,

    /// <summary>
    /// The engine refused to guess. Reached when the definition changed underneath an instance that
    /// was in flight (the step at the stored cursor is no longer the step the instance was on), so
    /// resuming could run the wrong code against real data. Requires a human decision, then
    /// <see cref="ILiteFlowClient.ResumeAsync"/>.
    /// </summary>
    NeedsAttention = 7,
}

/// <summary>
/// Outcome recorded for one step of one instance. Also part of the storage contract (written as
/// integer literals by the engine's SQL).
/// </summary>
public enum StepState
{
    /// <summary>Claimed and executing. A row left in this state is a step that was interrupted; the next attempt overwrites it.</summary>
    Running = 0,

    /// <summary>Executed successfully.</summary>
    Completed = 1,

    /// <summary>The step declined to do anything (<see cref="StepResult.Skip"/>) and the cursor moved on.</summary>
    Skipped = 2,

    /// <summary>The step failed definitively — attempts exhausted, or <see cref="StepResult.Fail"/>.</summary>
    Failed = 3,

    /// <summary>The step's compensation ran, undoing its effects during a rollback.</summary>
    Compensated = 4,
}

/// <summary>What kind of work a step descriptor represents.</summary>
public enum StepKind
{
    /// <summary>Runs caller code.</summary>
    Execute = 0,

    /// <summary>
    /// Parks the instance until a named signal arrives. Costs nothing while waiting: no queued
    /// message, no lease, no worker.
    /// </summary>
    WaitForSignal = 1,
}

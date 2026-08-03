namespace LiteFlow;

/// <summary>Options for starting an instance.</summary>
public sealed record WorkflowStartOptions
{
    /// <summary>
    /// Idempotency key, unique per definition. Starting twice with the same key returns the first
    /// instance instead of creating a second one — which is what makes a producer retry (a redelivered
    /// message, a double-clicked button, an HTTP retry) safe. Use the business identity of the work:
    /// the order number, not a fresh <c>Guid</c>.
    /// </summary>
    public string? IdempotencyKey { get; init; }

    /// <summary>Free-form correlation id, handed to every step and to the logs.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Claim-order weight of this instance's step messages; higher runs first under load.</summary>
    public int Priority { get; init; }

    /// <summary>Delay before the first step becomes claimable — a scheduled start.</summary>
    public TimeSpan Delay { get; init; }

    /// <summary>Identity of the instance. Generated when left <c>null</c>; supply one to make the start fully idempotent from the caller's side.</summary>
    public Guid? WorkflowId { get; init; }
}

/// <summary>What a start returned.</summary>
/// <param name="WorkflowId">Identity of the instance — the handle for cancel, signal and query.</param>
/// <param name="Definition">Name of the definition.</param>
/// <param name="AlreadyExisted">
/// <c>true</c> when an instance with the same <see cref="WorkflowStartOptions.IdempotencyKey"/> was
/// already there and nothing was created. Not an error: it is the expected answer to a retry.
/// </param>
public sealed record WorkflowHandle(Guid WorkflowId, string Definition, bool AlreadyExisted);

/// <summary>Outcome of publishing a signal.</summary>
public enum SignalOutcome
{
    /// <summary>The instance does not exist (or has already been archived).</summary>
    NotFound = 0,

    /// <summary>The signal was recorded and the instance was resumed.</summary>
    Resumed = 1,

    /// <summary>
    /// The signal was recorded but the instance is not waiting for it yet. It is not lost: when the
    /// workflow reaches the matching <c>WaitForSignal</c> step, it continues immediately.
    /// </summary>
    Recorded = 2,

    /// <summary>The signal had already been delivered; nothing changed (a duplicate publish).</summary>
    Duplicate = 3,

    /// <summary>The instance is already finished, so the signal was dropped.</summary>
    Terminal = 4,
}

/// <summary>A workflow instance as stored.</summary>
public sealed record WorkflowInstance
{
    /// <summary>Identity of the instance.</summary>
    public required Guid Id { get; init; }

    /// <summary>Name of the definition it runs.</summary>
    public required string Definition { get; init; }

    /// <summary>Signature of the definition the instance was started on — compared with the running code on resume.</summary>
    public required string Signature { get; init; }

    /// <summary>Where it is in its lifecycle.</summary>
    public required WorkflowState State { get; init; }

    /// <summary>Index of the step it is on (or stopped on).</summary>
    public required int CurrentStep { get; init; }

    /// <summary>Name of that step — the value that makes the cursor meaningful after a definition edit.</summary>
    public required string CurrentStepName { get; init; }

    /// <summary>Number of steps in the definition it was started on.</summary>
    public required int StepCount { get; init; }

    /// <summary>The state bag as stored (JSON), or the input when no step has committed yet.</summary>
    public string? StateJson { get; init; }

    /// <summary>The input as supplied at start (JSON), kept unchanged for audit.</summary>
    public string? InputJson { get; init; }

    /// <summary>Idempotency key it was started with, if any.</summary>
    public string? IdempotencyKey { get; init; }

    /// <summary>Caller-supplied correlation id, if any.</summary>
    public string? CorrelationId { get; init; }

    /// <summary><c>true</c> once a cancellation has been requested, whether or not it has been honoured yet.</summary>
    public required bool CancelRequested { get; init; }

    /// <summary>Why it was cancelled.</summary>
    public string? CancelReason { get; init; }

    /// <summary>When a <see cref="WorkflowState.Suspended"/> instance is due to continue.</summary>
    public DateTimeOffset? ResumeAt { get; init; }

    /// <summary>Signal a <see cref="WorkflowState.WaitingSignal"/> instance is waiting for.</summary>
    public string? WaitSignal { get; init; }

    /// <summary>When that wait times out, if it does.</summary>
    public DateTimeOffset? WaitExpiresAt { get; init; }

    /// <summary>Error that failed the instance, or the reason it needs attention.</summary>
    public string? Error { get; init; }

    /// <summary>Last worker that touched it — diagnostic only; ownership is enforced by lease tokens.</summary>
    public string? WorkerId { get; init; }

    /// <summary>When it was started.</summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>Last time anything about it changed. Combined with <see cref="State"/>, the signal that spots a stuck instance.</summary>
    public required DateTimeOffset UpdatedAt { get; init; }

    /// <summary>When it reached a terminal state.</summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary><c>true</c> when nothing more will happen without a human or a signal.</summary>
    public bool IsTerminal => State >= WorkflowState.Completed;
}

/// <summary>The trace of one step of one instance.</summary>
public sealed record WorkflowStepRecord
{
    /// <summary>Instance the step belongs to.</summary>
    public required Guid WorkflowId { get; init; }

    /// <summary>Position in the sequence.</summary>
    public required int StepIndex { get; init; }

    /// <summary>Declared name of the step.</summary>
    public required string StepName { get; init; }

    /// <summary>Outcome.</summary>
    public required StepState State { get; init; }

    /// <summary>How many times it was delivered to a worker. Above 1, it was interrupted or it threw.</summary>
    public required int Attempts { get; init; }

    /// <summary>When the current (or last) attempt started.</summary>
    public required DateTimeOffset StartedAt { get; init; }

    /// <summary>When it finished, if it did.</summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>Wall-clock duration of the successful attempt.</summary>
    public int? DurationMs { get; init; }

    /// <summary>The step's declared output (JSON), for audit only.</summary>
    public string? Output { get; init; }

    /// <summary>Error of the last failed attempt.</summary>
    public string? Error { get; init; }

    /// <summary>Worker that ran it — diagnostic only.</summary>
    public string? WorkerId { get; init; }
}

/// <summary>Filter for <see cref="ILiteFlowClient.ListAsync"/>.</summary>
public sealed record WorkflowQuery
{
    /// <summary>Limit to one definition.</summary>
    public string? Definition { get; init; }

    /// <summary>Limit to one state.</summary>
    public WorkflowState? State { get; init; }

    /// <summary>Limit to the instances that are not finished — the usual question during an incident.</summary>
    public bool LiveOnly { get; init; }

    /// <summary>Limit to instances not updated since this instant: how you find the stuck ones.</summary>
    public DateTimeOffset? IdleSince { get; init; }

    /// <summary>Maximum rows returned. Default: 100.</summary>
    public int MaxResults { get; init; } = 100;
}

/// <summary>Counts per state for one definition (or all of them).</summary>
public sealed record WorkflowStats
{
    /// <summary>Definition the counts are for, or <c>null</c> when they cover every definition.</summary>
    public string? Definition { get; init; }

    /// <summary>Instances with a step dispatched.</summary>
    public required long Running { get; init; }

    /// <summary>Instances waiting on a timer.</summary>
    public required long Suspended { get; init; }

    /// <summary>Instances waiting on a signal.</summary>
    public required long WaitingSignal { get; init; }

    /// <summary>Instances rolling back.</summary>
    public required long Compensating { get; init; }

    /// <summary>Instances that finished successfully.</summary>
    public required long Completed { get; init; }

    /// <summary>Instances that failed.</summary>
    public required long Failed { get; init; }

    /// <summary>Instances that were cancelled.</summary>
    public required long Cancelled { get; init; }

    /// <summary>Instances parked for a human decision — the number that should always be zero.</summary>
    public required long NeedsAttention { get; init; }

    /// <summary>Age of the oldest live instance: the real latency signal of the engine.</summary>
    public TimeSpan? OldestLiveAge { get; init; }

    /// <summary>Everything not finished.</summary>
    public long Live => Running + Suspended + WaitingSignal + Compensating;
}

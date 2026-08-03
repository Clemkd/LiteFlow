namespace LiteFlow.Model;

/// <summary>
/// The workflow tables as EF Core entities. Mapped for <b>migrations only</b>
/// (<c>ModelBuilder.AddLiteFlowModel()</c>): the engine itself reads and writes them with hand-written
/// SQL, because its statements use row locks, guarded updates and data-modifying CTEs that no ORM can
/// express — and because it has to run on the caller's connection whatever their model looks like.
/// <para>
/// Consequence worth knowing: these classes exist so your migrations can create the schema. Do not query
/// them expecting the engine's semantics; use <see cref="ILiteFlowClient"/> for that.
/// </para>
/// </summary>
public sealed class WorkflowInstanceEntity
{
    public Guid Id { get; set; }

    public string Definition { get; set; } = string.Empty;

    public string Signature { get; set; } = string.Empty;

    public short State { get; set; }

    public int CurrentStep { get; set; }

    public string CurrentStepName { get; set; } = string.Empty;

    public int StepCount { get; set; }

    public int? CompensationIndex { get; set; }

    public string Input { get; set; } = "{}";

    public string? Context { get; set; }

    public string? IdempotencyKey { get; set; }

    public string? CorrelationId { get; set; }

    public int Priority { get; set; }

    public bool CancelRequested { get; set; }

    public string? CancelReason { get; set; }

    public DateTimeOffset? ResumeAt { get; set; }

    public string? WaitSignal { get; set; }

    public DateTimeOffset? WaitExpiresAt { get; set; }

    public int RedispatchCount { get; set; }

    public string? Error { get; set; }

    public string? WorkerId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }

    public DateTimeOffset? CompletedAt { get; set; }
}

/// <summary>One step of one instance. See <see cref="WorkflowInstanceEntity"/> for why this is mapping-only.</summary>
public sealed class WorkflowStepEntity
{
    public Guid WorkflowId { get; set; }

    public int StepIndex { get; set; }

    public string StepName { get; set; } = string.Empty;

    public short State { get; set; }

    public int Attempts { get; set; }

    public DateTimeOffset StartedAt { get; set; }

    public DateTimeOffset? CompletedAt { get; set; }

    public int? DurationMs { get; set; }

    public string? Output { get; set; }

    public string? Error { get; set; }

    public string? WorkerId { get; set; }
}

/// <summary>One failed attempt, recorded outside the transaction that failed. See <see cref="WorkflowInstanceEntity"/>.</summary>
public sealed class WorkflowStepAttemptEntity
{
    public long Id { get; set; }

    public Guid WorkflowId { get; set; }

    public int StepIndex { get; set; }

    public string StepName { get; set; } = string.Empty;

    public int Attempt { get; set; }

    public DateTimeOffset FailedAt { get; set; }

    public string? WorkerId { get; set; }

    public string? Error { get; set; }
}

/// <summary>An external signal delivered to an instance. See <see cref="WorkflowInstanceEntity"/>.</summary>
public sealed class WorkflowSignalEntity
{
    public Guid WorkflowId { get; set; }

    public string Name { get; set; } = string.Empty;

    public string? Payload { get; set; }

    public DateTimeOffset ReceivedAt { get; set; }
}

/// <summary>A terminal instance moved out of the hot table. See <see cref="WorkflowInstanceEntity"/>.</summary>
public sealed class WorkflowArchiveEntity
{
    public Guid Id { get; set; }

    public string Definition { get; set; } = string.Empty;

    public short State { get; set; }

    public string? Error { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    public DateTimeOffset? CompletedAt { get; set; }

    public DateTimeOffset ArchivedAt { get; set; }

    public string Snapshot { get; set; } = "{}";
}

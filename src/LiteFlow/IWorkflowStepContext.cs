using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace LiteFlow;

/// <summary>
/// Everything a step is given: the instance it belongs to, the state it may read and mutate, and —
/// the reason durability works at all — the very connection and transaction the engine is going to
/// commit its own bookkeeping in.
/// </summary>
/// <typeparam name="TState">The workflow's state type.</typeparam>
public interface IWorkflowStepContext<out TState>
    where TState : class
{
    /// <summary>Identity of the running instance. Stable for its whole life, and the natural correlation id for logs.</summary>
    Guid WorkflowId { get; }

    /// <summary>Name of the workflow definition.</summary>
    string Definition { get; }

    /// <summary>Position of this step in the sequence.</summary>
    int StepIndex { get; }

    /// <summary>Name of this step, as declared in the builder. Stable across definition edits — which is what resume matches on.</summary>
    string StepName { get; }

    /// <summary>
    /// How many times this step has been handed to a worker, including the current attempt (the first
    /// delivery sees <c>1</c>). Above 1, a previous attempt died or threw: assume nothing about what it
    /// left behind, other than what a rolled-back transaction guarantees.
    /// </summary>
    int Attempt { get; }

    /// <summary>Attempts allowed for this step before the workflow fails.</summary>
    int MaxAttempts { get; }

    /// <summary><c>true</c> when failing this attempt fails the workflow — useful to log louder, or to give up cleanly.</summary>
    bool IsLastAttempt { get; }

    /// <summary>Optional caller-supplied correlation id, carried from <see cref="WorkflowStartOptions.CorrelationId"/>.</summary>
    string? CorrelationId { get; }

    /// <summary>
    /// The workflow's state: the object handed to <c>StartAsync</c>, then carried from step to step.
    /// Mutate it freely — it is serialized and persisted in the same transaction that advances the
    /// cursor, so what the next step sees is exactly what this step left, even across a crash and a
    /// different machine.
    /// </summary>
    TState State { get; }

    /// <summary>
    /// A key that is identical for every attempt of this step of this instance, and different for
    /// every other step and instance (<c>{workflowId}:{stepIndex}</c>). Hand it to whatever external
    /// system the step calls — payment providers, mail senders, other APIs — so their own
    /// deduplication turns "at least once" into "once".
    /// </summary>
    string IdempotencyKey { get; }

    /// <summary>
    /// The signal that woke this instance, on the step declared with
    /// <see cref="IWorkflowBuilder{TState}.WaitForSignal"/>; <c>null</c> on every other step.
    /// </summary>
    WorkflowSignal? Signal { get; }

    /// <summary>
    /// <c>true</c> when this step runs inside the engine's transaction — so its writes through
    /// <see cref="DbContext"/> / <see cref="Connection"/> commit atomically with the cursor advance.
    /// <c>false</c> for a step declared <see cref="IStepOptions{TState}.NonTransactional"/>, where the
    /// engine can only promise at-least-once execution.
    /// </summary>
    bool IsTransactional { get; }

    /// <summary>
    /// The connection the engine runs its own SQL on. Writes issued here land in
    /// <see cref="Transaction"/> and commit — or roll back — with the step's bookkeeping.
    /// </summary>
    DbConnection Connection { get; }

    /// <summary>
    /// The transaction the step's writes must join, or <c>null</c> for a non-transactional step (and
    /// for a step running under a caller-owned connection with no transaction open).
    /// </summary>
    DbTransaction? Transaction { get; }

    /// <summary>
    /// The EF Core context the engine borrowed its connection from, when LiteFlow was registered with
    /// <c>AddLiteFlow&lt;TContext&gt;</c>; <c>null</c> when it runs on its own pool. Write through this
    /// context and <c>SaveChangesAsync</c> is part of the step's transaction — no extra ceremony, no
    /// distributed transaction.
    /// </summary>
    DbContext? DbContext { get; }

    /// <summary>
    /// Services of the scope this attempt runs in — the same scope the step class itself was resolved
    /// from. Prefer constructor injection; this is for the rare dynamic resolution.
    /// </summary>
    IServiceProvider Services { get; }

    /// <summary>Create a command already bound to <see cref="Connection"/> and <see cref="Transaction"/>.</summary>
    DbCommand CreateCommand();
}

/// <summary>An external signal delivered to a waiting instance.</summary>
/// <param name="Name">Name the workflow was waiting on.</param>
/// <param name="Payload">Raw JSON payload as it was published, or <c>null</c>.</param>
/// <param name="ReceivedAt">When the signal was recorded (server clock).</param>
public sealed record WorkflowSignal(string Name, string? Payload, DateTimeOffset ReceivedAt)
{
    /// <summary>Deserialize the payload into <typeparamref name="T"/>, or <c>default</c> when there was none.</summary>
    public T? PayloadAs<T>() =>
        Payload is null ? default : System.Text.Json.JsonSerializer.Deserialize<T>(Payload);
}

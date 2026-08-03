using System.Security.Cryptography;
using System.Text;
using LiteFlow.Internal;

namespace LiteFlow;

/// <summary>
/// Non-generic base of every workflow definition. Only <see cref="Workflow{TState}"/> can extend it —
/// the build step it requires is internal on purpose, so a definition always comes with a state type.
/// </summary>
public abstract class Workflow
{
    /// <summary>
    /// Name of the definition: the key instances are stored under, and the queue they are dispatched
    /// on. Defaults to the class name; override it to keep the name stable across a rename, since
    /// running instances reference it.
    /// </summary>
    public virtual string Name => GetType().Name;

    internal abstract WorkflowDefinition BuildDefinition(LiteFlowOptions options);
}

/// <summary>
/// A workflow: a named sequence of steps, plus the type of the state that travels through them.
/// <code>
/// public sealed class OrderWorkflow : Workflow&lt;OrderState&gt;
/// {
///     protected override void Configure(IWorkflowBuilder&lt;OrderState&gt; b) => b
///         .Step&lt;ReserveStock&gt;()
///         .Step&lt;ChargeCard&gt;(s => s.MaxAttempts(3).Compensate&lt;RefundCard&gt;())
///         .Step&lt;SendReceipt&gt;(s => s.NonTransactional())
///         .WaitForSignal("shipped", timeout: TimeSpan.FromDays(2))
///         .Step("close", (ctx, ct) => { ctx.State.Closed = true; return Task.CompletedTask; });
/// }
/// </code>
/// <para>
/// <typeparamref name="TState"/> is both the input and the working state: the object handed to
/// <c>StartAsync</c> is what the first step receives, and whatever each step leaves in it is
/// persisted — in the same transaction as the cursor — for the next one to read. That is the whole
/// resume mechanism: no journal, no replay, one row.
/// </para>
/// </summary>
/// <typeparam name="TState">
/// State type. Must be serializable to JSON by the registered
/// <see cref="IWorkflowStateSerializer"/> (the default is <c>System.Text.Json</c>), and should stay
/// backward-compatible as long as instances of this workflow can still be in flight.
/// </typeparam>
public abstract class Workflow<TState> : Workflow
    where TState : class
{
    /// <summary>Declare the steps, in order.</summary>
    protected abstract void Configure(IWorkflowBuilder<TState> builder);

    internal sealed override WorkflowDefinition BuildDefinition(LiteFlowOptions options)
    {
        var builder = new WorkflowBuilder<TState>();
        Configure(builder);
        return builder.Build(Name, options);
    }
}

/// <summary>
/// A definition compiled once at startup: the step list, the queues it is dispatched on, and the
/// signature that tells a resuming engine whether the code in this process still matches the
/// instances in the database.
/// </summary>
public sealed class WorkflowDefinition
{
    internal WorkflowDefinition(
        string name,
        Type stateType,
        string signature,
        string queue,
        string? nonTransactionalQueue,
        IReadOnlyList<WorkflowStepDescriptor> steps)
    {
        Name = name;
        StateType = stateType;
        Signature = signature;
        Queue = queue;
        NonTransactionalQueue = nonTransactionalQueue;
        Steps = steps;
    }

    /// <summary>Name of the definition.</summary>
    public string Name { get; }

    /// <summary>The state type carried between steps.</summary>
    public Type StateType { get; }

    /// <summary>
    /// Fingerprint of the step list (order, names, kinds). Stored on every instance, and compared on
    /// resume: a mismatch is what makes the engine verify the step name at the cursor rather than
    /// trusting the index blindly.
    /// </summary>
    public string Signature { get; }

    /// <summary>LiteQueue queue the transactional steps of this workflow are dispatched on.</summary>
    public string Queue { get; }

    /// <summary>
    /// Queue the <see cref="IStepOptions{TState}.NonTransactional"/> steps are dispatched on, or
    /// <c>null</c> when the definition has none. Separate on purpose: those steps wait on networks the
    /// database knows nothing about, and must not occupy the workers that move the database-only steps.
    /// </summary>
    public string? NonTransactionalQueue { get; }

    /// <summary>The steps, in execution order.</summary>
    public IReadOnlyList<WorkflowStepDescriptor> Steps { get; }

    /// <summary>Number of steps.</summary>
    public int StepCount => Steps.Count;

    /// <summary>The step at <paramref name="index"/>, or <c>null</c> when the index is past the end (or negative).</summary>
    public WorkflowStepDescriptor? StepAt(int index) =>
        index >= 0 && index < Steps.Count ? Steps[index] : null;

    /// <summary>Index of the step named <paramref name="name"/>, or <c>-1</c>.</summary>
    public int IndexOf(string name)
    {
        for (int i = 0; i < Steps.Count; i++)
            if (string.Equals(Steps[i].Name, name, StringComparison.Ordinal))
                return i;
        return -1;
    }

    /// <summary>The queue a given step is dispatched on.</summary>
    public string QueueFor(WorkflowStepDescriptor step) =>
        step.IsTransactional ? Queue : NonTransactionalQueue ?? Queue;
}

/// <summary>One declared step: what to run, and under which rules.</summary>
public sealed class WorkflowStepDescriptor
{
    internal WorkflowStepDescriptor(
        int index,
        string name,
        StepKind kind,
        bool isTransactional,
        int? maxAttempts,
        int priority,
        string? signalName,
        TimeSpan? signalTimeout,
        Type? stepType,
        Type? compensationType,
        StepExecutor executor,
        StepCompensator? compensator)
    {
        Index = index;
        Name = name;
        Kind = kind;
        IsTransactional = isTransactional;
        MaxAttempts = maxAttempts;
        Priority = priority;
        SignalName = signalName;
        SignalTimeout = signalTimeout;
        StepType = stepType;
        CompensationType = compensationType;
        Executor = executor;
        Compensator = compensator;
    }

    /// <summary>Position in the sequence — the value stored in the instance's cursor.</summary>
    public int Index { get; }

    /// <summary>Name of the step, unique within the definition. The identity a resume is verified against.</summary>
    public string Name { get; }

    /// <summary>Whether the step runs code or waits for a signal.</summary>
    public StepKind Kind { get; }

    /// <summary><c>false</c> for a step declared <see cref="IStepOptions{TState}.NonTransactional"/>.</summary>
    public bool IsTransactional { get; }

    /// <summary>Attempts allowed, or <c>null</c> to use <see cref="LiteFlowOptions.MaxStepAttempts"/>.</summary>
    public int? MaxAttempts { get; }

    /// <summary>Claim-order weight of this step's message.</summary>
    public int Priority { get; }

    /// <summary>Signal this step waits for, for <see cref="StepKind.WaitForSignal"/>.</summary>
    public string? SignalName { get; }

    /// <summary>How long the wait may last before the instance fails, or <c>null</c> for forever.</summary>
    public TimeSpan? SignalTimeout { get; }

    /// <summary>Class implementing the step, when it is not an inline delegate. Registered in DI for you.</summary>
    public Type? StepType { get; }

    /// <summary>Class implementing the compensation, when one was attached as a type.</summary>
    public Type? CompensationType { get; }

    /// <summary><c>true</c> when this step can be undone during a rollback.</summary>
    public bool HasCompensation => Compensator is not null;

    internal StepExecutor Executor { get; }

    internal StepCompensator? Compensator { get; }
}

/// <summary>Thrown when a definition cannot be compiled (no steps, duplicate names, …).</summary>
public sealed class WorkflowDefinitionException(string message) : InvalidOperationException(message);

internal static class WorkflowSignature
{
    /// <summary>
    /// Fingerprint the shape of the sequence — not its implementation. Renaming a class behind a step
    /// whose name is pinned does not change the signature, because nothing about resume depends on the
    /// class; inserting, removing, renaming or reordering a step does.
    /// </summary>
    public static string Compute(string name, IReadOnlyList<WorkflowStepDescriptor> steps)
    {
        var sb = new StringBuilder(name);
        foreach (var step in steps)
            sb.Append('|').Append(step.Index).Append(':').Append(step.Name).Append(':').Append((int)step.Kind);

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()), hash);
        return Convert.ToHexStringLower(hash[..8]);
    }
}

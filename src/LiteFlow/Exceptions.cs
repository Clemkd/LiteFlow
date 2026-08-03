namespace LiteFlow;

/// <summary>
/// Thrown when a workflow name has not been registered in this process. Usually means the
/// <c>AddLiteFlowWorkflow&lt;T&gt;</c> call for it is missing — the engine refuses to guess, because
/// dispatching an instance of an unknown definition would mean running no steps at all and silently
/// stalling it.
/// </summary>
public sealed class WorkflowNotRegisteredException(string definition)
    : InvalidOperationException(
        $"Workflow '{definition}' is not registered in this process. Call AddLiteFlowWorkflow<T>() for it.")
{
    /// <summary>Name that could not be resolved.</summary>
    public string Definition { get; } = definition;
}

/// <summary>
/// Thrown when the stored state of an instance cannot be turned back into its state type — a state
/// class that changed incompatibly while instances were in flight, or a serializer swapped for an
/// incompatible one. Deliberately not retried: no number of attempts fixes a payload the code can no
/// longer read.
/// </summary>
public sealed class WorkflowStateException(Guid workflowId, Type stateType)
    : InvalidOperationException(
        $"Workflow {workflowId:D}: stored state could not be deserialized as {stateType.Name}. " +
        "The state type changed incompatibly, or the registered IWorkflowStateSerializer is not the one that wrote it.")
{
    /// <summary>Instance whose state could not be read.</summary>
    public Guid WorkflowId { get; } = workflowId;

    /// <summary>State type the engine tried to produce.</summary>
    public Type StateType { get; } = stateType;
}

/// <summary>
/// Thrown when an operation needs an independent connection (recording a failed attempt outside the
/// doomed transaction, the cancellation poll, the maintenance sweep) and LiteFlow has none: it was
/// registered on an EF context whose connection string it could not read, and no
/// <see cref="LiteFlowOptions.ConnectionString"/> was set.
/// </summary>
public sealed class NoSideChannelException()
    : InvalidOperationException(
        "LiteFlow needs a connection of its own for out-of-transaction work (attempt diagnostics, " +
        "cancellation polling, maintenance). Set LiteFlowOptions.ConnectionString, or register with " +
        "AddLiteFlow(connectionString).");

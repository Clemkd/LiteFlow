namespace LiteFlow;

/// <summary>
/// Declares the sequence of steps of a workflow. Called once per process, at registration time, so
/// the engine can validate the definition up front (unique step names, at least one step, resolvable
/// types) rather than discovering a mistake on the first instance that hits it.
/// <para>
/// The order in which steps are declared <b>is</b> the storage contract: the cursor of a running
/// instance is an index into this list, and the step name at that index is what a resume is checked
/// against. Appending steps is always safe; inserting, removing or renaming one parks the instances
/// that were in flight in <see cref="WorkflowState.NeedsAttention"/> instead of running the wrong code.
/// </para>
/// </summary>
/// <typeparam name="TState">The workflow's state type.</typeparam>
public interface IWorkflowBuilder<TState>
    where TState : class
{
    /// <summary>
    /// Add a step implemented by a class resolved from DI. The step name defaults to
    /// <typeparamref name="TStep"/>'s name — set it explicitly with
    /// <see cref="IStepOptions{TState}.Named"/> if you ever intend to rename the class without
    /// disturbing the instances in flight.
    /// </summary>
    IWorkflowBuilder<TState> Step<TStep>(Action<IStepOptions<TState>>? configure = null)
        where TStep : class, IWorkflowStep<TState>;

    /// <summary>
    /// Add a step as an inline delegate — the right shape for the two-line steps that would otherwise
    /// be a class of pure ceremony. The name is mandatory here because there is no type to borrow it
    /// from, and it is what resume matches on.
    /// </summary>
    IWorkflowBuilder<TState> Step(
        string name,
        Func<IWorkflowStepContext<TState>, CancellationToken, Task<StepResult>> execute,
        Action<IStepOptions<TState>>? configure = null);

    /// <summary>
    /// Add an inline step that has nothing to decide: returning normally continues with the next step,
    /// throwing retries.
    /// </summary>
    IWorkflowBuilder<TState> Step(
        string name,
        Func<IWorkflowStepContext<TState>, CancellationToken, Task> execute,
        Action<IStepOptions<TState>>? configure = null);

    /// <summary>
    /// Park the instance until <see cref="ILiteFlowClient.SignalAsync"/> delivers
    /// <paramref name="signalName"/>. Nothing is queued while it waits — no lease, no worker, no
    /// polling — so waiting for a human approval or a partner callback for a week costs one row.
    /// <para>
    /// A signal that arrives <i>before</i> the workflow reaches this step is not lost: signals are
    /// recorded per instance, and the step resumes immediately if its signal is already there.
    /// </para>
    /// </summary>
    /// <param name="signalName">Name the caller will signal.</param>
    /// <param name="timeout">
    /// How long to wait before failing the instance with a timeout. <c>null</c> waits forever, which is
    /// a deliberate choice you should make explicitly.
    /// </param>
    /// <param name="stepName">Name of the step in the trace. Defaults to <c>wait:{signalName}</c>.</param>
    IWorkflowBuilder<TState> WaitForSignal(string signalName, TimeSpan? timeout = null, string? stepName = null);

    /// <summary>
    /// Same as <see cref="WaitForSignal(string, TimeSpan?, string?)"/>, but folds the signal's payload
    /// into the state before continuing — which is usually the point of waiting for it. The delegate
    /// runs inside the resuming step's transaction, so the state it produces is committed with the
    /// cursor advance.
    /// </summary>
    IWorkflowBuilder<TState> WaitForSignal(
        string signalName,
        Func<IWorkflowStepContext<TState>, WorkflowSignal, CancellationToken, Task> apply,
        TimeSpan? timeout = null,
        string? stepName = null);
}

/// <summary>Per-step tuning, set from the builder.</summary>
/// <typeparam name="TState">The workflow's state type.</typeparam>
public interface IStepOptions<TState>
    where TState : class
{
    /// <summary>
    /// Override the step's name. The name is the identity used to verify a resume, so pin it when the
    /// class name might change: <c>.Named("charge-card")</c> survives a refactor that the type name
    /// would not.
    /// </summary>
    IStepOptions<TState> Named(string name);

    /// <summary>
    /// Attempts allowed for this step before the workflow fails. <c>null</c> (default) uses
    /// <see cref="LiteFlowOptions.MaxStepAttempts"/>.
    /// </summary>
    IStepOptions<TState> MaxAttempts(int maxAttempts);

    /// <summary>Claim-order weight for this step's message; higher runs first when workers are saturated.</summary>
    IStepOptions<TState> Priority(int priority);

    /// <summary>
    /// Run this step <b>outside</b> the engine's transaction, and say so out loud: its writes are not
    /// covered by the all-or-nothing guarantee, so the engine can only promise it is executed at least
    /// once. Use it for work this database does not own — an HTTP call, a mail, a file — where holding
    /// a transaction open across the network would be the worse of two evils.
    /// <para>
    /// Such a step is dispatched on its own queue and its own workers, so a slow external call cannot
    /// starve the steps that only touch the database. Make it idempotent with
    /// <see cref="IWorkflowStepContext{TState}.IdempotencyKey"/>.
    /// </para>
    /// </summary>
    IStepOptions<TState> NonTransactional();

    /// <summary>
    /// Attach a compensation class, run in reverse order if the workflow later fails or is cancelled.
    /// Not needed when the step itself implements <see cref="ICompensatingWorkflowStep{TState}"/>.
    /// </summary>
    IStepOptions<TState> Compensate<TCompensation>()
        where TCompensation : class, IWorkflowCompensation<TState>;

    /// <summary>Attach an inline compensation.</summary>
    IStepOptions<TState> Compensate(Func<IWorkflowStepContext<TState>, CancellationToken, Task> compensate);
}

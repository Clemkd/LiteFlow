using Microsoft.Extensions.DependencyInjection;

namespace LiteFlow.Internal;

/// <summary>
/// Compiles a <see cref="Workflow{TState}.Configure"/> call into a <see cref="WorkflowDefinition"/>.
/// <para>
/// The generic type only lives here: every step is captured as a closure that knows how to
/// deserialize the state, build the typed context and resolve the step class. The engine then runs
/// steps without ever naming <c>TState</c> — which is what lets one dispatcher serve every workflow
/// in the process.
/// </para>
/// </summary>
internal sealed class WorkflowBuilder<TState> : IWorkflowBuilder<TState>
    where TState : class
{
    private readonly List<PendingStep> _steps = [];

    public IWorkflowBuilder<TState> Step<TStep>(Action<IStepOptions<TState>>? configure = null)
        where TStep : class, IWorkflowStep<TState>
    {
        var options = new StepOptions<TState>(typeof(TStep).Name);
        configure?.Invoke(options);

        StepExecutor executor = async (execution, ct) =>
        {
            var state = Bind(execution, out var context);
            var step = ActivatorUtilities.GetServiceOrCreateInstance<TStep>(execution.Services);
            var result = await step.ExecuteAsync(context, ct).ConfigureAwait(false);
            Persist(execution, state, result);
            return result;
        };

        // A step that implements the compensating interface needs no wiring: the rollback path finds it.
        StepCompensator? compensator = options.Compensator;
        if (compensator is null && typeof(ICompensatingWorkflowStep<TState>).IsAssignableFrom(typeof(TStep)))
        {
            compensator = async (execution, ct) =>
            {
                var state = Bind(execution, out var context);
                var step = (ICompensatingWorkflowStep<TState>)
                    ActivatorUtilities.GetServiceOrCreateInstance<TStep>(execution.Services);
                await step.CompensateAsync(context, ct).ConfigureAwait(false);
                Persist(execution, state, StepResult.Next());
            };
        }

        _steps.Add(new PendingStep(options, StepKind.Execute, executor, compensator, typeof(TStep)));
        return this;
    }

    public IWorkflowBuilder<TState> Step(
        string name,
        Func<IWorkflowStepContext<TState>, CancellationToken, Task<StepResult>> execute,
        Action<IStepOptions<TState>>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(execute);

        var options = new StepOptions<TState>(name);
        configure?.Invoke(options);

        StepExecutor executor = async (execution, ct) =>
        {
            var state = Bind(execution, out var context);
            var result = await execute(context, ct).ConfigureAwait(false);
            Persist(execution, state, result);
            return result;
        };

        _steps.Add(new PendingStep(options, StepKind.Execute, executor, options.Compensator, null));
        return this;
    }

    public IWorkflowBuilder<TState> Step(
        string name,
        Func<IWorkflowStepContext<TState>, CancellationToken, Task> execute,
        Action<IStepOptions<TState>>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(execute);
        return Step(name, async (ctx, ct) =>
        {
            await execute(ctx, ct).ConfigureAwait(false);
            return StepResult.Next();
        }, configure);
    }

    public IWorkflowBuilder<TState> WaitForSignal(
        string signalName, TimeSpan? timeout = null, string? stepName = null) =>
        AddWait(signalName, null, timeout, stepName);

    public IWorkflowBuilder<TState> WaitForSignal(
        string signalName,
        Func<IWorkflowStepContext<TState>, WorkflowSignal, CancellationToken, Task> apply,
        TimeSpan? timeout = null,
        string? stepName = null)
    {
        ArgumentNullException.ThrowIfNull(apply);
        return AddWait(signalName, apply, timeout, stepName);
    }

    private IWorkflowBuilder<TState> AddWait(
        string signalName,
        Func<IWorkflowStepContext<TState>, WorkflowSignal, CancellationToken, Task>? apply,
        TimeSpan? timeout,
        string? stepName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(signalName);
        if (timeout is { } t)
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(t, TimeSpan.Zero, nameof(timeout));

        var options = new StepOptions<TState>(stepName ?? $"wait:{signalName}");

        // Reaching this step means the signal has already been recorded — the instance was parked
        // before the message was dispatched, and only SignalAsync dispatches it. So the step itself
        // does no waiting: it just folds the payload in and moves on.
        StepExecutor executor = async (execution, ct) =>
        {
            var state = Bind(execution, out var context);
            if (apply is not null && execution.Signal is { } signal)
                await apply(context, signal, ct).ConfigureAwait(false);
            var result = StepResult.Next();
            Persist(execution, state, result);
            return result;
        };

        _steps.Add(new PendingStep(
            options with { SignalName = signalName, SignalTimeout = timeout },
            StepKind.WaitForSignal,
            executor,
            null,
            null));
        return this;
    }

    public WorkflowDefinition Build(string name, LiteFlowOptions options)
    {
        if (_steps.Count == 0)
            throw new WorkflowDefinitionException(
                $"Workflow '{name}' declares no step. A definition with an empty sequence has nothing to resume.");

        var descriptors = new List<WorkflowStepDescriptor>(_steps.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);

        for (int i = 0; i < _steps.Count; i++)
        {
            var pending = _steps[i];
            string stepName = pending.Options.Name;

            if (string.IsNullOrWhiteSpace(stepName))
                throw new WorkflowDefinitionException($"Workflow '{name}': the step at index {i} has no name.");

            if (!seen.Add(stepName))
                throw new WorkflowDefinitionException(
                    $"Workflow '{name}' declares two steps named '{stepName}'. Step names are the identity a " +
                    "resume is verified against, so they must be unique — pin one with .Named(\"…\").");

            descriptors.Add(new WorkflowStepDescriptor(
                index: i,
                name: stepName,
                kind: pending.Kind,
                isTransactional: pending.Kind == StepKind.WaitForSignal || pending.Options.Transactional,
                maxAttempts: pending.Options.MaxAttemptsValue,
                priority: pending.Options.PriorityValue,
                signalName: pending.Options.SignalName,
                signalTimeout: pending.Options.SignalTimeout,
                stepType: pending.StepType,
                compensationType: pending.Options.CompensationType,
                executor: pending.Executor,
                compensator: pending.Compensator));
        }

        string queue = options.QueuePrefix + name;
        bool hasExternal = descriptors.Exists(s => !s.IsTransactional);

        return new WorkflowDefinition(
            name,
            typeof(TState),
            WorkflowSignature.Compute(name, descriptors),
            queue,
            hasExternal ? queue + options.NonTransactionalQueueSuffix : null,
            descriptors);
    }

    private static TState Bind(StepExecution execution, out IWorkflowStepContext<TState> context)
    {
        var state = execution.Serializer.Deserialize<TState>(execution.StateJson)
                    ?? throw new WorkflowStateException(execution.WorkflowId, typeof(TState));
        context = new WorkflowStepContext<TState>(execution, state);
        return state;
    }

    private static void Persist(StepExecution execution, TState state, StepResult result)
    {
        execution.StateJson = execution.Serializer.Serialize(state);
        execution.OutputJson = result.Output is null ? null : execution.Serializer.Serialize(result.Output);
    }

    private sealed record PendingStep(
        StepOptions<TState> Options,
        StepKind Kind,
        StepExecutor Executor,
        StepCompensator? Compensator,
        Type? StepType);
}

/// <summary>Mutable collector behind <see cref="IStepOptions{TState}"/>.</summary>
internal sealed record StepOptions<TState> : IStepOptions<TState>
    where TState : class
{
    public StepOptions(string name) => Name = name;

    public string Name { get; private set; }

    public int? MaxAttemptsValue { get; private set; }

    public int PriorityValue { get; private set; }

    public bool Transactional { get; private set; } = true;

    public Type? CompensationType { get; private set; }

    public StepCompensator? Compensator { get; private set; }

    public string? SignalName { get; init; }

    public TimeSpan? SignalTimeout { get; init; }

    public IStepOptions<TState> Named(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
        return this;
    }

    public IStepOptions<TState> MaxAttempts(int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxAttempts);
        MaxAttemptsValue = maxAttempts;
        return this;
    }

    public IStepOptions<TState> Priority(int priority)
    {
        PriorityValue = priority;
        return this;
    }

    public IStepOptions<TState> NonTransactional()
    {
        Transactional = false;
        return this;
    }

    public IStepOptions<TState> Compensate<TCompensation>()
        where TCompensation : class, IWorkflowCompensation<TState>
    {
        CompensationType = typeof(TCompensation);
        Compensator = async (execution, ct) =>
        {
            var state = execution.Serializer.Deserialize<TState>(execution.StateJson)
                        ?? throw new WorkflowStateException(execution.WorkflowId, typeof(TState));
            var context = new WorkflowStepContext<TState>(execution, state);
            var compensation = ActivatorUtilities.GetServiceOrCreateInstance<TCompensation>(execution.Services);
            await compensation.CompensateAsync(context, ct).ConfigureAwait(false);
            execution.StateJson = execution.Serializer.Serialize(state);
        };
        return this;
    }

    public IStepOptions<TState> Compensate(Func<IWorkflowStepContext<TState>, CancellationToken, Task> compensate)
    {
        ArgumentNullException.ThrowIfNull(compensate);
        Compensator = async (execution, ct) =>
        {
            var state = execution.Serializer.Deserialize<TState>(execution.StateJson)
                        ?? throw new WorkflowStateException(execution.WorkflowId, typeof(TState));
            var context = new WorkflowStepContext<TState>(execution, state);
            await compensate(context, ct).ConfigureAwait(false);
            execution.StateJson = execution.Serializer.Serialize(state);
        };
        return this;
    }
}

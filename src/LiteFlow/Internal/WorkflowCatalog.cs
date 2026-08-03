using LiteQueue.Connectors;

namespace LiteFlow.Internal;

/// <summary>
/// Process-wide state shared by every client and worker: the compiled SQL, the definitions registered
/// in this process, and the schema-creation gate.
/// <para>
/// Definitions are compiled at registration time, not on first use. A typo in a step name, a duplicate
/// name or an empty sequence then fails the host's startup — the alternative is discovering it on the
/// first instance that reaches the mistake, in production, with state already committed.
/// </para>
/// </summary>
internal sealed class WorkflowCatalog(LiteFlowOptions options)
{
    private readonly Dictionary<string, WorkflowDefinition> _byName = new(StringComparer.Ordinal);
    private readonly Dictionary<Type, WorkflowDefinition> _byType = [];
    private readonly SemaphoreSlim _initGate = new(1, 1);
    private volatile bool _initialized;

    public LiteFlowOptions Options { get; } = options;

    public WorkflowSql Sql { get; } = new(options.Schema, options.QueueSchema);

    public IReadOnlyCollection<WorkflowDefinition> Definitions => _byName.Values;

    /// <summary>Compile a definition and remember it. Registering the same workflow twice is a no-op.</summary>
    public WorkflowDefinition Register(Workflow workflow)
    {
        ArgumentNullException.ThrowIfNull(workflow);

        var type = workflow.GetType();
        if (_byType.TryGetValue(type, out var known))
            return known;

        var definition = workflow.BuildDefinition(Options);

        if (_byName.ContainsKey(definition.Name))
            throw new WorkflowDefinitionException(
                $"Two workflow classes are registered under the name '{definition.Name}'. Instances are stored " +
                "and dispatched by name, so a name that means two different sequences would let one class " +
                "resume the other's instances — override Name on one of them.");

        _byName[definition.Name] = definition;
        _byType[type] = definition;
        return definition;
    }

    public WorkflowDefinition Require(Type workflowType) =>
        _byType.TryGetValue(workflowType, out var definition)
            ? definition
            : throw new WorkflowNotRegisteredException(workflowType.Name);

    public WorkflowDefinition Require(string name) =>
        _byName.TryGetValue(name, out var definition)
            ? definition
            : throw new WorkflowNotRegisteredException(name);

    public bool TryGet(string name, out WorkflowDefinition? definition) =>
        _byName.TryGetValue(name, out definition);

    /// <summary>Attempts allowed for a step: its own budget, or the engine default.</summary>
    public int MaxAttemptsFor(WorkflowStepDescriptor step) => step.MaxAttempts ?? Options.MaxStepAttempts;

    /// <summary>Create the schema on first use. Concurrent callers (many hosted workers) fold into one.</summary>
    public async Task InitializeAsync(IQueueConnectionSource source, CancellationToken ct)
    {
        if (_initialized || !Options.AutoCreateSchema)
            return;

        await _initGate.WaitAsync(ct);
        try
        {
            if (_initialized)
                return;

            await using var connection = await source.AcquireAsync(ct);
            var target = SqlTarget.From(connection);

            await WorkflowCommands.ExecuteScriptAsync(target, WorkflowSchema.CreateScript(Options.Schema), ct);
            if (Options.ApplyStorageTuning)
                await WorkflowCommands.ExecuteScriptAsync(target, WorkflowSchema.TuningScript(Options.Schema), ct);

            // The reconciliation sweep looks a step up in the queue's dead letters by dedup key; without
            // this index that lookup degrades to a scan of every failure ever recorded.
            await WorkflowCommands.ExecuteScriptAsync(
                target, WorkflowSchema.QueueLookupScript(Options.QueueSchema), ct);

            _initialized = true;
        }
        finally
        {
            _initGate.Release();
        }
    }

    /// <summary>
    /// Forget that the schema was created. Needed when the schema is dropped out from under a running
    /// process (tests, dev tooling), where the cached flag would otherwise skip the recreation.
    /// </summary>
    public void Forget() => _initialized = false;
}

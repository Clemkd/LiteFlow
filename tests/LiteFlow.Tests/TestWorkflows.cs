namespace LiteFlow.Tests;

/// <summary>
/// The definitions the tests run. Each one takes its name from a mutable static that the test sets
/// before building its provider: a fresh name means a fresh queue and a fresh definition, so nothing a
/// previous test left behind can be picked up by a later one. Tests in the <c>postgres</c> collection run
/// sequentially, which is what makes the static safe.
/// </summary>
public sealed class LinearWorkflow : Workflow<TestState>
{
    public static string DefinitionName = "linear";

    public override string Name => DefinitionName;

    protected override void Configure(IWorkflowBuilder<TestState> b) => b
        .Step("s1", (ctx, ct) => StepScript.Run("s1", ctx, ct))
        .Step("s2", (ctx, ct) => StepScript.Run("s2", ctx, ct))
        .Step("s3", (ctx, ct) => StepScript.Run("s3", ctx, ct));
}

/// <summary>Three steps, the first two undoable — the rollback scenarios.</summary>
public sealed class CompensatingWorkflow : Workflow<TestState>
{
    public static string DefinitionName = "comp";

    public override string Name => DefinitionName;

    protected override void Configure(IWorkflowBuilder<TestState> b) => b
        .Step("c1", (ctx, ct) => StepScript.Run("c1", ctx, ct),
            s => s.Compensate((ctx, ct) => StepScript.Compensate("c1", ctx, ct)))
        .Step("c2", (ctx, ct) => StepScript.Run("c2", ctx, ct),
            s => s.Compensate((ctx, ct) => StepScript.Compensate("c2", ctx, ct)))
        .Step("c3", (ctx, ct) => StepScript.Run("c3", ctx, ct), s => s.MaxAttempts(2));
}

/// <summary>A wait in the middle, with the payload folded into the state on the way through.</summary>
public sealed class SignalWorkflow : Workflow<TestState>
{
    public static string DefinitionName = "signal";

    public static TimeSpan? Timeout;

    public static string SignalName = "go";

    public override string Name => DefinitionName;

    protected override void Configure(IWorkflowBuilder<TestState> b) => b
        .Step("s1", (ctx, ct) => StepScript.Run("s1", ctx, ct))
        .WaitForSignal(SignalName, (ctx, signal, ct) =>
        {
            ctx.State.SignalPayload = signal.Payload;
            return Task.CompletedTask;
        }, Timeout)
        .Step("s3", (ctx, ct) => StepScript.Run("s3", ctx, ct));
}

/// <summary>A step that runs outside the engine's transaction — the at-least-once path.</summary>
public sealed class ExternalWorkflow : Workflow<TestState>
{
    public static string DefinitionName = "external";

    public override string Name => DefinitionName;

    protected override void Configure(IWorkflowBuilder<TestState> b) => b
        .Step("s1", (ctx, ct) => StepScript.Run("s1", ctx, ct))
        .Step("ext", (ctx, ct) => StepScript.Run("ext", ctx, ct), s => s.NonTransactional())
        .Step("s3", (ctx, ct) => StepScript.Run("s3", ctx, ct));
}

/// <summary>The sequence an instance was started on, before the code changed under it.</summary>
public sealed class DriftBeforeWorkflow : Workflow<TestState>
{
    public static string DefinitionName = "drift";

    public override string Name => DefinitionName;

    protected override void Configure(IWorkflowBuilder<TestState> b) => b
        .Step("a", (ctx, ct) => StepScript.Run("a", ctx, ct))
        .Step("b", (ctx, ct) => StepScript.Run("b", ctx, ct))
        .Step("c", (ctx, ct) => StepScript.Run("c", ctx, ct));
}

/// <summary>
/// The same definition after a step was inserted in the middle. Index 1 no longer holds <c>b</c>, which
/// is what must park the instances that were in flight instead of running <c>x</c> in their place.
/// </summary>
public sealed class DriftAfterWorkflow : Workflow<TestState>
{
    public override string Name => DriftBeforeWorkflow.DefinitionName;

    protected override void Configure(IWorkflowBuilder<TestState> b) => b
        .Step("a", (ctx, ct) => StepScript.Run("a", ctx, ct))
        .Step("x", (ctx, ct) => StepScript.Run("x", ctx, ct))
        .Step("b", (ctx, ct) => StepScript.Run("b", ctx, ct))
        .Step("c", (ctx, ct) => StepScript.Run("c", ctx, ct));
}

/// <summary>Opens on a wait: the instance must be parked at creation, with nothing queued.</summary>
public sealed class WaitFirstWorkflow : Workflow<TestState>
{
    public static string DefinitionName = "wait-first";

    public override string Name => DefinitionName;

    protected override void Configure(IWorkflowBuilder<TestState> b) => b
        .WaitForSignal("start")
        .Step("after", (ctx, ct) => StepScript.Run("after", ctx, ct));
}

/// <summary>Four steps, used by the concurrency and chaos runs.</summary>
public sealed class FanWorkflow : Workflow<TestState>
{
    public static string DefinitionName = "fan";

    public override string Name => DefinitionName;

    protected override void Configure(IWorkflowBuilder<TestState> b) => b
        .Step("f1", (ctx, ct) => StepScript.Run("f1", ctx, ct))
        .Step("f2", (ctx, ct) => StepScript.Run("f2", ctx, ct))
        .Step("f3", (ctx, ct) => StepScript.Run("f3", ctx, ct))
        .Step("f4", (ctx, ct) => StepScript.Run("f4", ctx, ct));

    /// <summary>Names of the steps, for the invariant checks.</summary>
    public static string[] Steps => ["f1", "f2", "f3", "f4"];
}

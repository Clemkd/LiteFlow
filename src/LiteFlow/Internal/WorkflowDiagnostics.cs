using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LiteFlow.Internal;

/// <summary>
/// The engine's traces and metrics.
/// <para>
/// One counter matters more than the others: <c>liteflow.steps.resumed</c>. It counts the attempts
/// that found a step already started by a previous, dead attempt — in other words, how often the
/// crash-recovery path actually fires. A system that never crashes reads zero; a number that climbs
/// is telling you about hosts being killed mid-step, and it is the only place that shows it.
/// </para>
/// </summary>
internal static class WorkflowDiagnostics
{
    public const string ActivitySourceName = "LiteFlow";

    public const string MeterName = "LiteFlow";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> Started =
        Meter.CreateCounter<long>("liteflow.workflows.started", "{workflow}", "Instances started.");

    private static readonly Counter<long> Completed =
        Meter.CreateCounter<long>("liteflow.workflows.completed", "{workflow}", "Instances that finished successfully.");

    private static readonly Counter<long> Failed =
        Meter.CreateCounter<long>("liteflow.workflows.failed", "{workflow}", "Instances that failed definitively.");

    private static readonly Counter<long> Cancelled =
        Meter.CreateCounter<long>("liteflow.workflows.cancelled", "{workflow}", "Instances that honoured a cancellation.");

    private static readonly Counter<long> NeedsAttention =
        Meter.CreateCounter<long>("liteflow.workflows.needs_attention", "{workflow}",
            "Instances parked for a human decision (definition drift, exhausted re-dispatch).");

    private static readonly Counter<long> StepsExecuted =
        Meter.CreateCounter<long>("liteflow.steps.executed", "{step}", "Step attempts that returned a result.");

    private static readonly Counter<long> StepsFailed =
        Meter.CreateCounter<long>("liteflow.steps.failed", "{step}", "Step attempts that threw.");

    private static readonly Counter<long> StepsResumed =
        Meter.CreateCounter<long>("liteflow.steps.resumed", "{step}",
            "Step attempts that resumed work a previous attempt was interrupted in.");

    private static readonly Counter<long> StepsStale =
        Meter.CreateCounter<long>("liteflow.steps.stale", "{step}",
            "Step messages dropped because the cursor had already moved past them.");

    private static readonly Counter<long> Redispatched =
        Meter.CreateCounter<long>("liteflow.steps.redispatched", "{step}",
            "Steps re-dispatched by the maintenance sweep because no message was in flight.");

    private static readonly Counter<long> Compensated =
        Meter.CreateCounter<long>("liteflow.steps.compensated", "{step}", "Compensations that ran.");

    private static readonly Histogram<double> StepDuration =
        Meter.CreateHistogram<double>("liteflow.step.duration", "ms", "Wall-clock duration of a step attempt.");

    public static void WorkflowStarted(string definition) =>
        Started.Add(1, Tag(definition));

    public static void WorkflowFinished(string definition, WorkflowState state)
    {
        var counter = state switch
        {
            WorkflowState.Completed => Completed,
            WorkflowState.Failed => Failed,
            WorkflowState.Cancelled => Cancelled,
            WorkflowState.NeedsAttention => NeedsAttention,
            _ => null,
        };
        counter?.Add(1, Tag(definition));
    }

    public static void StepExecuted(string definition, string step, double durationMs, StepState state)
    {
        StepsExecuted.Add(1, Tag(definition), Tag("step", step), Tag("outcome", state.ToString()));
        StepDuration.Record(durationMs, Tag(definition), Tag("step", step));
    }

    public static void StepFailed(string definition, string step) =>
        StepsFailed.Add(1, Tag(definition), Tag("step", step));

    public static void StepResumed(string definition, string step) =>
        StepsResumed.Add(1, Tag(definition), Tag("step", step));

    public static void StepStale(string definition, string step) =>
        StepsStale.Add(1, Tag(definition), Tag("step", step));

    public static void StepRedispatched(string definition) =>
        Redispatched.Add(1, Tag(definition));

    public static void StepCompensated(string definition, string step) =>
        Compensated.Add(1, Tag(definition), Tag("step", step));

    public static Activity? StartStepActivity(
        string definition, string stepName, Guid workflowId, int stepIndex, int attempt)
    {
        var activity = ActivitySource.StartActivity($"liteflow.step {definition}/{stepName}", ActivityKind.Consumer);
        if (activity is null)
            return null;

        activity.SetTag("liteflow.definition", definition);
        activity.SetTag("liteflow.workflow_id", workflowId);
        activity.SetTag("liteflow.step_index", stepIndex);
        activity.SetTag("liteflow.step_name", stepName);
        activity.SetTag("liteflow.attempt", attempt);
        return activity;
    }

    private static KeyValuePair<string, object?> Tag(string definition) => new("liteflow.definition", definition);

    private static KeyValuePair<string, object?> Tag(string key, string value) => new(key, value);
}

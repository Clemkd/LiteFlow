using System.Text.Json;
using LiteQueue;

namespace LiteFlow.Internal;

/// <summary>What a step message asks the handler to do.</summary>
internal enum StepPurpose
{
    /// <summary>Run the step at this index.</summary>
    Step = 0,

    /// <summary>Undo the step at this index, as part of a rollback.</summary>
    Compensate = 1,
}

/// <summary>
/// The body of a step message. Deliberately tiny: it carries a pointer, not state. The state lives in
/// the instance row, read under lock at the start of the attempt — so a message can never deliver a
/// stale view of the workflow, however long it sat in the queue.
/// </summary>
internal sealed record StepMessagePayload(Guid WorkflowId, int StepIndex, string StepName, StepPurpose Purpose);

/// <summary>
/// Turns "this instance should run step N" into a LiteQueue message.
/// <para>
/// Everything here runs on the connection the caller passes in, which is the point: dispatching the
/// next step is part of the current step's transaction. Either the step's writes, the cursor advance
/// and the next message all commit, or none of them do — no outbox table, no window where a workflow
/// has advanced but nothing is queued to continue it.
/// </para>
/// </summary>
internal static class StepDispatcher
{
    public static Task<EnqueueResult> DispatchStepAsync(
        IQueueProducer producer,
        WorkflowDefinition definition,
        Guid workflowId,
        WorkflowStepDescriptor step,
        int priority,
        int maxAttempts,
        TimeSpan delay,
        CancellationToken ct) =>
        producer.EnqueueAsync(
            definition.QueueFor(step),
            Build(definition, workflowId, step, StepPurpose.Step, priority, maxAttempts, delay),
            ct);

    public static Task<EnqueueResult> DispatchCompensationAsync(
        IQueueProducer producer,
        WorkflowDefinition definition,
        Guid workflowId,
        WorkflowStepDescriptor step,
        int priority,
        int maxAttempts,
        CancellationToken ct) =>
        producer.EnqueueAsync(
            definition.QueueFor(step),
            Build(definition, workflowId, step, StepPurpose.Compensate, priority, maxAttempts, TimeSpan.Zero),
            ct);

    private static QueueMessageData Build(
        WorkflowDefinition definition,
        Guid workflowId,
        WorkflowStepDescriptor step,
        StepPurpose purpose,
        int priority,
        int maxAttempts,
        TimeSpan delay)
    {
        var payload = new StepMessagePayload(workflowId, step.Index, step.Name, purpose);

        return new QueueMessageData
        {
            Type = $"{definition.Name}/{step.Name}",
            Payload = JsonSerializer.SerializeToUtf8Bytes(payload),
            Priority = priority,
            Delay = delay,
            MaxAttempts = maxAttempts,
            // One pending message per (instance, step, purpose), enforced by LiteQueue's unique partial
            // index. This is what makes re-dispatch idempotent: the maintenance sweep can offer the same
            // step again without ever producing a second copy, and two workers racing to schedule the
            // same successor produce one message between them.
            DedupKey = purpose == StepPurpose.Step
                ? StepKeys.Dispatch(workflowId, step.Index)
                : StepKeys.Compensation(workflowId, step.Index),
        };
    }

    public static StepMessagePayload Parse(QueueMessage message)
    {
        try
        {
            return message.AsJson<StepMessagePayload>()
                   ?? throw new PoisonMessageException(
                       $"Step message {message.Id} on '{message.Queue}' has an empty body.");
        }
        catch (JsonException ex)
        {
            // Unreadable body: no number of retries will fix it, so it goes straight to the dead-letter
            // table instead of burning the instance's attempt budget.
            throw new PoisonMessageException(
                $"Step message {message.Id} on '{message.Queue}' could not be read as a LiteFlow step.", ex);
        }
    }
}

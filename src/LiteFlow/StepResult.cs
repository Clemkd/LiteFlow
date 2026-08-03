namespace LiteFlow;

/// <summary>What a step asked the engine to do next.</summary>
public enum StepResultKind
{
    /// <summary>Move to the next step.</summary>
    Next = 0,

    /// <summary>Move to the next step, and record this one as skipped rather than executed.</summary>
    Skip = 1,

    /// <summary>Move to the next step, but not before a delay.</summary>
    Suspend = 2,

    /// <summary>Finish the workflow successfully now, ignoring the remaining steps.</summary>
    Complete = 3,

    /// <summary>Fail the workflow definitively, without spending the remaining attempts.</summary>
    Fail = 4,
}

/// <summary>
/// The value a step returns to steer the sequence.
/// <para>
/// The distinction that matters: <b>throwing</b> means "this attempt failed, try again" — the engine
/// rolls the step's transaction back and re-delivers it with a backoff until its attempts run out.
/// Returning <see cref="Fail"/> means "this will never work" — the workflow fails immediately, and no
/// attempt is wasted proving it.
/// </para>
/// </summary>
public readonly record struct StepResult
{
    private StepResult(StepResultKind kind, TimeSpan? delay, string? reason, object? output)
    {
        Kind = kind;
        Delay = delay;
        Reason = reason;
        Output = output;
    }

    /// <summary>What the step asked for.</summary>
    public StepResultKind Kind { get; }

    /// <summary>How long to wait before the next step, for <see cref="StepResultKind.Suspend"/>.</summary>
    public TimeSpan? Delay { get; }

    /// <summary>Why the step skipped or failed, recorded on the step row.</summary>
    public string? Reason { get; }

    /// <summary>
    /// Optional value recorded as the step's <c>output</c> (serialized to <c>jsonb</c>). Purely for
    /// audit and support: the engine never reads it back, so a later step must not depend on it —
    /// pass data forward through the state bag instead.
    /// </summary>
    public object? Output { get; }

    /// <summary>Continue with the next step. The default outcome of a step that returns normally.</summary>
    public static StepResult Next(object? output = null) => new(StepResultKind.Next, null, null, output);

    /// <summary>
    /// Nothing to do for this step (a condition was not met). The cursor advances and the step row is
    /// marked <see cref="StepState.Skipped"/>, so the trace shows the decision was taken rather than
    /// leaving a hole.
    /// </summary>
    public static StepResult Skip(string? reason = null) => new(StepResultKind.Skip, null, reason, null);

    /// <summary>
    /// Pause the workflow for <paramref name="delay"/>, then continue with the next step. The
    /// instance holds no lease and no worker while it waits, so a delay of days is as cheap as one of
    /// seconds.
    /// </summary>
    public static StepResult Suspend(TimeSpan delay, object? output = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        return new StepResult(StepResultKind.Suspend, delay, null, output);
    }

    /// <summary>Finish successfully now and skip the remaining steps (an early exit, not a failure).</summary>
    public static StepResult Complete(object? output = null) => new(StepResultKind.Complete, null, null, output);

    /// <summary>
    /// Fail the workflow definitively: no retry, no waiting for the attempt budget to run out.
    /// Compensations of the completed steps run if any are configured.
    /// </summary>
    public static StepResult Fail(string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return new StepResult(StepResultKind.Fail, null, reason, null);
    }

    /// <summary><c>true</c> when this outcome moves the cursor forward.</summary>
    public bool Advances => Kind is StepResultKind.Next or StepResultKind.Skip or StepResultKind.Suspend;
}

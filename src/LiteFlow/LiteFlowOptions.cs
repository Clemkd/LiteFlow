using System.Text.RegularExpressions;

namespace LiteFlow;

/// <summary>Configuration for the LiteFlow services.</summary>
public sealed class LiteFlowOptions
{
    private static readonly Regex IdentifierPattern = new("^[a-z_][a-z0-9_]*$", RegexOptions.Compiled);

    private string _schema = WorkflowSchema.DefaultSchema;

    /// <summary>
    /// PostgreSQL schema holding the workflow tables. Default: <c>liteflow</c>. Must be a plain
    /// lower-case identifier (it is interpolated into the library's hand-written SQL, so it is
    /// validated rather than quoted).
    /// </summary>
    public string Schema
    {
        get => _schema;
        set
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            if (!IdentifierPattern.IsMatch(value))
                throw new ArgumentException(
                    $"Invalid schema name '{value}': expected a lower-case identifier ([a-z_][a-z0-9_]*).",
                    nameof(value));
            _schema = value;
        }
    }

    /// <summary>
    /// When <c>true</c> (default), <see cref="ILiteFlowClient.InitializeAsync"/> creates the schema,
    /// tables and indexes if they are missing (idempotent DDL). Set to <c>false</c> when the workflow
    /// tables are part of your own EF migrations — see <c>ModelBuilder.AddLiteFlowModel()</c>.
    /// </summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// When <c>true</c> (default), initialization also applies the storage tuning that keeps the
    /// instance table fast: a lower <c>fillfactor</c> (room for the cursor update to stay in-place) and
    /// an aggressive autovacuum threshold. An instance row is updated once per step, so dead tuples
    /// accumulate at the rate work flows through — default autovacuum settings let them pile up.
    /// </summary>
    public bool ApplyStorageTuning { get; set; } = true;

    /// <summary>
    /// Prefix of the LiteQueue queue names, one queue per definition
    /// (<c>{prefix}{DefinitionName}</c>). Isolating definitions is what stops a backlog of one workflow
    /// from starving another, and lets each have its own concurrency. Default: <c>wf:</c>.
    /// </summary>
    public string QueuePrefix { get; set; } = "wf:";

    /// <summary>
    /// Suffix of the second queue a definition gets when it declares
    /// <see cref="IStepOptions{TState}.NonTransactional"/> steps. Those steps wait on external systems,
    /// so they are dispatched to their own workers — a slow HTTP call then cannot occupy a slot a
    /// database-only step needs. Default: <c>!io</c>.
    /// </summary>
    public string NonTransactionalQueueSuffix { get; set; } = "!io";

    /// <summary>
    /// PostgreSQL schema holding the step queues. LiteFlow registers and drives LiteQueue itself, so
    /// this is where the dispatch tables live. Default: <c>litequeue</c>.
    /// </summary>
    public string QueueSchema { get; set; } = LiteQueue.QueueSchema.DefaultSchema;

    /// <summary>
    /// Attempts allowed per step before the workflow fails, unless the step overrides it with
    /// <see cref="IStepOptions{TState}.MaxAttempts"/>. Default: 5.
    /// </summary>
    public int MaxStepAttempts { get; set; } = 5;

    /// <summary>
    /// Backoff applied between attempts of a step that threw. Exponential with jitter, because a batch
    /// of steps that fails together (a dependency went down) must not retry together forever.
    /// <para>
    /// It applies to every workflow queue: the retry policy belongs to the dispatcher, not to an
    /// individual step, so this is set once here rather than per step.
    /// </para>
    /// </summary>
    public LiteQueue.RetryBackoff StepRetry { get; set; } = new();

    /// <summary>
    /// When <c>true</c>, dispatching a step emits a <c>NOTIFY</c> and idle workers wait on
    /// <c>LISTEN</c> instead of polling — a workflow then moves from step to step within a round-trip
    /// instead of within a poll interval, and an idle fleet costs no queries at all. Requires a
    /// connection LiteFlow can dedicate to listening, which it has whenever
    /// <see cref="ConnectionString"/> is set or resolvable from the EF context. Default: <c>true</c>.
    /// </summary>
    public bool EnableNotifications { get; set; } = true;

    /// <summary>
    /// How long a worker holds a step before it is considered abandoned. A worker heartbeats to keep
    /// it; if the worker or its host dies, the step becomes claimable again once this elapses — this is
    /// the delay between a crash and the resume, so size it against how long your steps really take.
    /// Default: 30 s.
    /// </summary>
    public TimeSpan StepLease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Interval of the cancellation poll for the instances in flight <i>in this process</i>: one query
    /// per tick, listing the ids this worker holds. Cancellation is always honoured between steps; this
    /// is what also interrupts a step in the middle of a long one. Default: 5 s. Set to
    /// <see cref="TimeSpan.Zero"/> to disable the poll.
    /// </summary>
    public TimeSpan CancellationPollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Interval of the maintenance loop: due timers, signal timeouts, orphan re-dispatch and retention.
    /// Default: 15 s.
    /// </summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// When <c>true</c> (default), registering a workflow also starts the maintenance loop. It is what
    /// makes the system self-healing — due suspensions, timed-out waits and instances whose message
    /// went missing are picked back up without anyone being paged. Safe to run on every instance of
    /// your service.
    /// </summary>
    public bool AutoMaintenance { get; set; } = true;

    /// <summary>
    /// How long a <see cref="WorkflowState.Running"/> instance may go without progress before the
    /// maintenance sweep re-dispatches its current step. Re-dispatch is harmless by construction — the
    /// message's dedup key makes it a no-op while a message for that step still exists — so this only
    /// ever recovers work that was genuinely lost. Default: 5 min.
    /// </summary>
    public TimeSpan OrphanGracePeriod { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How many times the sweep may re-dispatch the same instance before parking it in
    /// <see cref="WorkflowState.NeedsAttention"/>. The stop that keeps a permanently broken step from
    /// being retried forever by the safety net. Default: 5.
    /// </summary>
    public int MaxRedispatch { get; set; } = 5;

    /// <summary>
    /// How long terminal instances stay in the hot table before being moved to
    /// <c>workflow_archive</c>. Default: 7 days.
    /// </summary>
    public TimeSpan InstanceRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>How long archived instances are kept. <see cref="TimeSpan.Zero"/> keeps them forever. Default: 90 days.</summary>
    public TimeSpan ArchiveRetention { get; set; } = TimeSpan.FromDays(90);

    /// <summary>
    /// When <c>true</c> (default), every failed attempt is recorded in <c>workflow_step_attempts</c> on
    /// a connection of its own — the step's own transaction is being rolled back, so a trace written
    /// inside it would vanish with the failure it is meant to explain.
    /// </summary>
    public bool RecordFailedAttempts { get; set; } = true;

    /// <summary>
    /// Connection string LiteFlow uses for the work that cannot run in the step's transaction
    /// (attempt diagnostics, cancellation poll, maintenance sweep). Optional when LiteFlow is
    /// registered with a connection string or when the EF context exposes one — it is picked up
    /// automatically in both cases.
    /// </summary>
    public string? ConnectionString { get; set; }
}

/// <summary>Per-definition worker tuning (see <c>AddLiteFlowWorkflow&lt;T&gt;</c>).</summary>
public sealed record WorkflowWorkerOptions
{
    /// <summary>
    /// How many steps of this definition this process runs at the same time. Each runs in its own DI
    /// scope, so each holds a <c>DbContext</c> and a connection — size it against your connection pool,
    /// not your CPU count. Default: 1.
    /// </summary>
    public int Concurrency { get; set; } = 1;

    /// <summary>
    /// Concurrency for the <see cref="IStepOptions{TState}.NonTransactional"/> steps of this
    /// definition, which run on their own queue and their own workers. Those steps wait on networks
    /// rather than on the database, so they usually deserve a higher number.
    /// <c>null</c> (default) reuses <see cref="Concurrency"/>.
    /// </summary>
    public int? ExternalConcurrency { get; set; }

    /// <summary>Visibility timeout per claimed step. <c>null</c> uses <see cref="LiteFlowOptions.StepLease"/>.</summary>
    public TimeSpan? Lease { get; set; }

    /// <summary>
    /// Keep the lease of a running step alive while it works (default: <c>true</c>). One background query
    /// per tick renews everything this process holds, so a step slower than the lease does not have its
    /// message taken away — and a process that dies stops renewing, which is exactly what hands the step
    /// to someone else.
    /// <para>
    /// Turning it off makes the lease a hard ceiling on step duration. Only useful to force the takeover
    /// path deliberately (that is what the crash-recovery tests do); in production it means any step
    /// slower than <see cref="Lease"/> is executed twice.
    /// </para>
    /// </summary>
    public bool RenewLease { get; set; } = true;

    /// <summary>Identity recorded on claimed instances and steps. <c>null</c> uses machine name + process id.</summary>
    public string? WorkerId { get; set; }
}

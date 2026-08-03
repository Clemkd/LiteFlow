namespace LiteFlow;

/// <summary>
/// The PostgreSQL objects LiteFlow needs, as idempotent DDL. Applied for you by
/// <see cref="ILiteFlowClient.InitializeAsync"/> when <see cref="LiteFlowOptions.AutoCreateSchema"/>
/// is on; exposed here so you can paste it into a migration instead (or check what a
/// <c>ModelBuilder.AddLiteFlowModel()</c> migration is expected to produce).
/// <para>
/// The state values are written as integer literals in the partial index predicates below, so they
/// are part of the storage contract: <see cref="WorkflowState"/> and <see cref="StepState"/> must
/// never be renumbered without a schema version bump.
/// </para>
/// </summary>
public static class WorkflowSchema
{
    /// <summary>Default schema name.</summary>
    public const string DefaultSchema = "liteflow";

    /// <summary>
    /// Version of the DDL below. Recorded in <c>__liteflow_schema_version</c> so a library upgrade can
    /// tell whether the database in front of it is older than the code, and apply what is missing
    /// itself — the point being that nobody has to run a script by hand after a package update.
    /// </summary>
    public const int SchemaVersion = 1;

    /// <summary>
    /// Tables and indexes. Notable choices:
    /// <list type="bullet">
    /// <item>
    /// <b>One row per instance, one row per step.</b> The instance row carries the cursor
    /// (<c>current_step</c>) and the state bag (<c>context</c>); the step rows are the audit trail.
    /// Resuming after a crash is therefore a single-row read, not a journal replay.
    /// </item>
    /// <item>
    /// <b>Partial indexes on live instances only</b> (<c>WHERE state &lt; 4</c>): the indexes the
    /// engine and the maintenance sweep walk are the size of the work in flight, not of the history.
    /// A database that has run ten million workflows dispatches as fast as an empty one.
    /// </item>
    /// <item>
    /// <b>No foreign keys between instances and steps.</b> Step rows are written on the hot path, once
    /// per step, and an FK check would add an index probe plus a parent-row lock to every one of them.
    /// Orphan step rows are impossible anyway: only the engine writes them, and the archive sweep
    /// removes both sides together.
    /// </item>
    /// <item>
    /// <b>State as <c>jsonb</c></b>: queryable for support ("which orders are stuck before payment?")
    /// without the library having to know anything about the caller's types, and large states go to
    /// TOAST instead of bloating the pages the dispatcher reads.
    /// </item>
    /// <item>
    /// <b>No queue table here.</b> Dispatch, leases and retries live in LiteQueue's schema; this one
    /// only holds the durable state a step resumes from.
    /// </item>
    /// </list>
    /// </summary>
    public static string CreateScript(string schema = DefaultSchema) =>
        $"""
         CREATE SCHEMA IF NOT EXISTS {schema};

         CREATE TABLE IF NOT EXISTS {schema}.workflows (
             id                 uuid PRIMARY KEY,
             definition         text NOT NULL,
             signature          text NOT NULL,
             state              smallint NOT NULL DEFAULT 0,
             current_step       integer NOT NULL DEFAULT 0,
             current_step_name  text NOT NULL,
             step_count         integer NOT NULL,
             compensation_index integer,
             input              jsonb NOT NULL,
             context            jsonb,
             idempotency_key    text,
             correlation_id     text,
             priority           integer NOT NULL DEFAULT 0,
             cancel_requested   boolean NOT NULL DEFAULT false,
             cancel_reason      text,
             resume_at          timestamptz,
             wait_signal        text,
             wait_expires_at    timestamptz,
             redispatch_count   integer NOT NULL DEFAULT 0,
             error              text,
             worker_id          text,
             created_at         timestamptz NOT NULL DEFAULT now(),
             updated_at         timestamptz NOT NULL DEFAULT now(),
             completed_at       timestamptz
         );

         -- Starting a workflow twice with the same key is a no-op that returns the first instance:
         -- what makes a producer retry (an HTTP call, a redelivered message) safe.
         CREATE UNIQUE INDEX IF NOT EXISTS ux_workflows_idempotency
             ON {schema}.workflows (definition, idempotency_key)
             WHERE idempotency_key IS NOT NULL;

         -- The supervision and orphan-recovery path: live instances only.
         CREATE INDEX IF NOT EXISTS ix_workflows_live
             ON {schema}.workflows (state, updated_at)
             WHERE state < 4;

         -- The two timer sweeps, each on the rows it can possibly return.
         CREATE INDEX IF NOT EXISTS ix_workflows_resume
             ON {schema}.workflows (resume_at)
             WHERE state = 1;

         CREATE INDEX IF NOT EXISTS ix_workflows_wait
             ON {schema}.workflows (wait_expires_at)
             WHERE state = 2 AND wait_expires_at IS NOT NULL;

         CREATE INDEX IF NOT EXISTS ix_workflows_definition
             ON {schema}.workflows (definition, created_at DESC);

         -- The retention sweep, on terminal rows only.
         CREATE INDEX IF NOT EXISTS ix_workflows_terminal
             ON {schema}.workflows (completed_at)
             WHERE state >= 4;

         CREATE TABLE IF NOT EXISTS {schema}.workflow_steps (
             workflow_id   uuid NOT NULL,
             step_index    integer NOT NULL,
             step_name     text NOT NULL,
             state         smallint NOT NULL DEFAULT 0,
             attempts      integer NOT NULL DEFAULT 0,
             started_at    timestamptz NOT NULL DEFAULT now(),
             completed_at  timestamptz,
             duration_ms   integer,
             output        jsonb,
             error         text,
             worker_id     text,
             PRIMARY KEY (workflow_id, step_index)
         );

         -- Diagnostics for the steps that fought back: one row per failed attempt, written outside
         -- the step's transaction (which is rolled back, taking any in-transaction trace with it).
         CREATE TABLE IF NOT EXISTS {schema}.workflow_step_attempts (
             id           bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
             workflow_id  uuid NOT NULL,
             step_index   integer NOT NULL,
             step_name    text NOT NULL,
             attempt      integer NOT NULL,
             failed_at    timestamptz NOT NULL DEFAULT now(),
             worker_id    text,
             error        text
         );

         CREATE INDEX IF NOT EXISTS ix_step_attempts_workflow
             ON {schema}.workflow_step_attempts (workflow_id, step_index);

         -- Cancellation lives in its own table, and that is a correctness requirement rather than a
         -- normalisation choice: a running step holds a row lock on its instance for the whole of its
         -- transaction, so a cancellation written to that row would block until the step it is trying to
         -- interrupt has finished. Here it is always one unblocked insert.
         CREATE TABLE IF NOT EXISTS {schema}.workflow_cancellations (
             workflow_id   uuid PRIMARY KEY,
             reason        text,
             requested_at  timestamptz NOT NULL DEFAULT now()
         );

         -- One row per (instance, signal name): a signal delivered twice wakes the workflow once.
         CREATE TABLE IF NOT EXISTS {schema}.workflow_signals (
             workflow_id  uuid NOT NULL,
             name         text NOT NULL,
             payload      jsonb,
             received_at  timestamptz NOT NULL DEFAULT now(),
             PRIMARY KEY (workflow_id, name)
         );

         -- Terminal instances are moved here so the hot table stays the size of the work in flight.
         CREATE TABLE IF NOT EXISTS {schema}.workflow_archive (
             id            uuid PRIMARY KEY,
             definition    text NOT NULL,
             state         smallint NOT NULL,
             error         text,
             created_at    timestamptz NOT NULL,
             completed_at  timestamptz,
             archived_at   timestamptz NOT NULL DEFAULT now(),
             snapshot      jsonb NOT NULL
         );

         CREATE INDEX IF NOT EXISTS ix_workflow_archive_at
             ON {schema}.workflow_archive (archived_at);

         CREATE TABLE IF NOT EXISTS {schema}.__liteflow_schema_version (
             version     integer PRIMARY KEY,
             applied_at  timestamptz NOT NULL DEFAULT now()
         );

         INSERT INTO {schema}.__liteflow_schema_version (version)
         VALUES ({SchemaVersion})
         ON CONFLICT (version) DO NOTHING;
         """;

    /// <summary>
    /// One index on the queue's dead-letter table, because the reconciliation sweep asks it a question its own
    /// indexes do not answer: "is there a dead letter for this instance and this step?".
    /// <para>
    /// The lookup key is the step message's dedup key (<c>{instanceId:N}:{stepIndex}</c>), which makes the
    /// match exact — and, with this index, a probe rather than a scan of every failure the system has ever
    /// recorded. LiteFlow owns the queue registration, so the queue schema is its own implementation detail;
    /// the statement is guarded so it does nothing when the queue tables are not there yet.
    /// </para>
    /// </summary>
    public static string QueueLookupScript(string queueSchema = LiteQueue.QueueSchema.DefaultSchema) =>
        $"""
         DO $do$
         BEGIN
             IF to_regclass('{queueSchema}.dead_letters') IS NOT NULL THEN
                 CREATE INDEX IF NOT EXISTS ix_dead_letters_dedup
                     ON {queueSchema}.dead_letters (dedup_key)
                     WHERE dedup_key IS NOT NULL;
             END IF;
         END
         $do$;
         """;

    /// <summary>
    /// Storage tuning for the instance table. An instance row is updated once per step — cursor,
    /// state bag, timestamps — so a workflow of twenty steps produces twenty dead tuples. With stock
    /// autovacuum settings (vacuum at 20% of the table) a busy engine accumulates them faster than
    /// they are reclaimed, and every dispatch then has to step over the debris.
    /// <list type="bullet">
    /// <item><c>fillfactor = 80</c> leaves room on the page for the cursor update to stay in-place (HOT), avoiding index churn on every step.</item>
    /// <item><c>autovacuum_vacuum_scale_factor = 0.02</c> + a low threshold vacuum these tables continuously instead of in rare, painful sweeps.</item>
    /// <item><c>autovacuum_vacuum_cost_delay = 0</c> lets that vacuum run at full speed; both tables are small by design.</item>
    /// </list>
    /// </summary>
    public static string TuningScript(string schema = DefaultSchema) =>
        $"""
         ALTER TABLE {schema}.workflows SET (
             fillfactor = 80,
             autovacuum_vacuum_scale_factor = 0.02,
             autovacuum_vacuum_threshold = 100,
             autovacuum_vacuum_cost_delay = 0,
             autovacuum_analyze_scale_factor = 0.05
         );

         ALTER TABLE {schema}.workflow_steps SET (
             fillfactor = 85,
             autovacuum_vacuum_scale_factor = 0.05,
             autovacuum_vacuum_threshold = 100,
             autovacuum_vacuum_cost_delay = 0
         );
         """;
}

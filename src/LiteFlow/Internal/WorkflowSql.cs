namespace LiteFlow.Internal;

/// <summary>
/// Every statement LiteFlow runs, built once for a given schema.
/// <para>
/// Three rules shape this SQL:
/// </para>
/// <list type="number">
/// <item>
/// <b>The instance row is the lock.</b> A step starts with <c>SELECT … FOR UPDATE</c> on its instance,
/// so two steps of the same workflow can never overlap — whatever the queue, the leases or the number
/// of workers decide. Everything else the step does happens while that row is held, and is released by
/// the same <c>COMMIT</c> that advances the cursor.
/// </item>
/// <item>
/// <b>Guards are predicates, not round-trips.</b> Advancing the cursor is
/// <c>UPDATE … WHERE id = @id AND current_step = @from</c>: a late worker updates zero rows instead of
/// overwriting the work of the one that took over. The check and the write are the same statement, so
/// there is no window between them.
/// </item>
/// <item>
/// <b>Time comes from the server.</b> Timers, timeouts and retention are computed with <c>now()</c>,
/// never from a worker's clock — leases and deadlines stay correct across machines with skewed clocks.
/// </item>
/// </list>
/// </summary>
internal sealed class WorkflowSql
{
    public WorkflowSql(string schema, string queueSchema)
    {
        Schema = schema;
        QueueSchema = queueSchema;

        const string columns =
            """
            w.id, w.definition, w.signature, w.state, w.current_step, w.current_step_name, w.step_count,
            w.compensation_index, w.input::text, w.context::text, w.idempotency_key, w.correlation_id,
            w.priority, w.cancel_requested, w.cancel_reason, w.resume_at, w.wait_signal,
            w.wait_expires_at, w.redispatch_count, w.error, w.worker_id, w.created_at, w.updated_at,
            w.completed_at, c.requested_at, c.reason
            """;

        // Every read of an instance brings its cancellation with it: one round-trip instead of a second
        // query on the hot path, and the guard at the top of every step gets the flag for free.
        string from =
            $"""
             FROM {schema}.workflows w
             LEFT JOIN {schema}.workflow_cancellations c ON c.workflow_id = w.id
             """;

        Columns = columns;

        // DO NOTHING plus a follow-up read rather than DO UPDATE: a second start with the same key must
        // not touch the instance that is already running (its cursor and state are live data).
        Insert =
            $"""
             INSERT INTO {schema}.workflows
                 (id, definition, signature, state, current_step, current_step_name, step_count,
                  input, context, idempotency_key, correlation_id, priority)
             VALUES
                 (@id, @definition, @signature, 0, 0, @step_name, @step_count,
                  @input::jsonb, @input::jsonb, @idempotency_key, @correlation_id, @priority)
             ON CONFLICT DO NOTHING
             RETURNING id
             """;

        SelectByIdempotencyKey =
            $"SELECT id FROM {schema}.workflows WHERE definition = @definition AND idempotency_key = @idempotency_key";

        // FOR UPDATE, not SKIP LOCKED: if another worker holds this instance we must wait for it and then
        // re-read the cursor, because the whole point is that our guard sees its outcome. OF w, so the
        // joined cancellation row is not locked with it — that table has to stay writable at all times.
        LoadForUpdate = $"SELECT {columns} {from} WHERE w.id = @id FOR UPDATE OF w";

        Load = $"SELECT {columns} {from} WHERE w.id = @id";

        // Advance: the cursor moves only if it is still where the caller believes it is.
        Advance =
            $"""
             UPDATE {schema}.workflows SET
                 state = @state,
                 current_step = @to_step,
                 current_step_name = @to_step_name,
                 context = @context::jsonb,
                 resume_at = CASE WHEN @resume_secs IS NULL THEN NULL
                                  ELSE now() + make_interval(secs => @resume_secs) END,
                 wait_signal = @wait_signal,
                 wait_expires_at = CASE WHEN @wait_secs IS NULL THEN NULL
                                        ELSE now() + make_interval(secs => @wait_secs) END,
                 redispatch_count = 0,
                 worker_id = @worker_id,
                 updated_at = now()
             WHERE id = @id AND current_step = @from_step AND state < 4
             """;

        Finish =
            $"""
             UPDATE {schema}.workflows SET
                 state = @state,
                 context = COALESCE(@context::jsonb, context),
                 error = COALESCE(@error, error),
                 resume_at = NULL,
                 wait_signal = NULL,
                 wait_expires_at = NULL,
                 worker_id = @worker_id,
                 updated_at = now(),
                 completed_at = now()
             WHERE id = @id AND state < 4
             """;

        StartCompensation =
            $"""
             UPDATE {schema}.workflows SET
                 state = 3,
                 compensation_index = @index,
                 error = COALESCE(@error, error),
                 resume_at = NULL,
                 wait_signal = NULL,
                 wait_expires_at = NULL,
                 updated_at = now()
             WHERE id = @id AND state < 4
             """;

        AdvanceCompensation =
            $"""
             UPDATE {schema}.workflows SET
                 compensation_index = @index,
                 context = COALESCE(@context::jsonb, context),
                 updated_at = now()
             WHERE id = @id AND state = 3
             """;

        // Never touches the instance row, so it cannot be blocked by the step it is meant to stop.
        InsertCancellation =
            $"""
             INSERT INTO {schema}.workflow_cancellations (workflow_id, reason)
             VALUES (@id, @reason)
             ON CONFLICT (workflow_id) DO NOTHING
             """;

        DeleteCancellation = $"DELETE FROM {schema}.workflow_cancellations WHERE workflow_id = @id";

        // Instances cancelled while parked on a timer or a signal: nothing is in flight for them, so
        // nobody would otherwise notice. The sweep finalises them within one tick.
        CancelledWhileParked =
            $"""
             SELECT w.id, w.definition, w.current_step, w.current_step_name, w.priority
             FROM {schema}.workflows w
             JOIN {schema}.workflow_cancellations c ON c.workflow_id = w.id
             WHERE w.state IN (1, 2)
             ORDER BY c.requested_at
             LIMIT @max
             """;

        // Restart a parked instance at the step it stopped on, with a fresh signature: the operator has
        // decided the current code is the right code for it. The cursor is re-anchored by step *name*,
        // so an instance parked because steps moved lands on the step it was really on, not on whatever
        // its old index now points at.
        Resume =
            $"""
             UPDATE {schema}.workflows SET
                 state = 0,
                 signature = @signature,
                 current_step = @step,
                 current_step_name = @step_name,
                 step_count = @step_count,
                 cancel_requested = false,
                 cancel_reason = NULL,
                 error = NULL,
                 resume_at = NULL,
                 wait_signal = NULL,
                 wait_expires_at = NULL,
                 redispatch_count = 0,
                 compensation_index = NULL,
                 completed_at = NULL,
                 updated_at = now()
             WHERE id = @id AND state >= 4
             RETURNING current_step, current_step_name
             """;

        MarkNeedsAttention =
            $"""
             UPDATE {schema}.workflows SET
                 state = 7,
                 error = @error,
                 updated_at = now(),
                 completed_at = now()
             WHERE id = @id AND state < 4
             """;

        // The cancellation poll: one round-trip for every instance this process holds.
        SelectCancelled =
            $"SELECT workflow_id FROM {schema}.workflow_cancellations WHERE workflow_id = ANY(@ids)";

        UpsertStepStart =
            $"""
             INSERT INTO {schema}.workflow_steps
                 (workflow_id, step_index, step_name, state, attempts, started_at, worker_id)
             VALUES (@id, @step_index, @step_name, 0, @attempts, now(), @worker_id)
             ON CONFLICT (workflow_id, step_index) DO UPDATE SET
                 step_name = EXCLUDED.step_name,
                 state = 0,
                 attempts = EXCLUDED.attempts,
                 started_at = now(),
                 completed_at = NULL,
                 duration_ms = NULL,
                 worker_id = EXCLUDED.worker_id
             """;

        CompleteStep =
            $"""
             UPDATE {schema}.workflow_steps SET
                 state = @state,
                 completed_at = now(),
                 duration_ms = @duration_ms,
                 output = @output::jsonb,
                 error = @error
             WHERE workflow_id = @id AND step_index = @step_index
             """;

        // Upsert, not update: every attempt of a step that keeps throwing is rolled back, taking the
        // "started" row with it — so when the verdict is finally written (from the side connection, after
        // the last attempt) there may be no row to update. Without this, the one step you actually want
        // to see in the trace would be the one missing from it.
        FailStep =
            $"""
             INSERT INTO {schema}.workflow_steps
                 (workflow_id, step_index, step_name, state, attempts, started_at, completed_at, worker_id, error)
             VALUES (@id, @step_index, @step_name, 3, @attempts, now(), now(), @worker_id, @error)
             ON CONFLICT (workflow_id, step_index) DO UPDATE SET
                 state = 3,
                 completed_at = now(),
                 attempts = GREATEST({schema}.workflow_steps.attempts, EXCLUDED.attempts),
                 worker_id = EXCLUDED.worker_id,
                 error = EXCLUDED.error
             """;

        InsertAttempt =
            $"""
             INSERT INTO {schema}.workflow_step_attempts
                 (workflow_id, step_index, step_name, attempt, worker_id, error)
             VALUES (@id, @step_index, @step_name, @attempt, @worker_id, @error)
             """;

        ListSteps =
            $"""
             SELECT workflow_id, step_index, step_name, state, attempts, started_at, completed_at,
                    duration_ms, output::text, error, worker_id
             FROM {schema}.workflow_steps
             WHERE workflow_id = @id
             ORDER BY step_index
             """;

        // Highest completed step at or below the cursor that can be undone — walked one step at a time
        // so a crash in the middle of a rollback simply resumes at the next one.
        SelectCompensable =
            $"""
             SELECT step_index, step_name
             FROM {schema}.workflow_steps
             WHERE workflow_id = @id AND step_index <= @max_index AND state = 1
             ORDER BY step_index DESC
             LIMIT 1
             """;

        MarkCompensated =
            $"""
             UPDATE {schema}.workflow_steps SET state = 4, completed_at = now()
             WHERE workflow_id = @id AND step_index = @step_index
             """;

        InsertSignal =
            $"""
             INSERT INTO {schema}.workflow_signals (workflow_id, name, payload)
             VALUES (@id, @name, @payload::jsonb)
             ON CONFLICT (workflow_id, name) DO NOTHING
             RETURNING received_at
             """;

        SelectSignal =
            $"""
             SELECT name, payload::text, received_at
             FROM {schema}.workflow_signals
             WHERE workflow_id = @id AND name = @name
             """;

        // Woken by a signal: the instance leaves the wait and its step becomes dispatchable again.
        ResumeFromSignal =
            $"""
             UPDATE {schema}.workflows SET
                 state = 0,
                 wait_signal = NULL,
                 wait_expires_at = NULL,
                 redispatch_count = 0,
                 updated_at = now()
             WHERE id = @id AND state = 2 AND wait_signal = @name
             RETURNING current_step, current_step_name
             """;

        DueSuspended =
            $"""
             UPDATE {schema}.workflows SET state = 0, resume_at = NULL, updated_at = now()
             WHERE id IN (
                 SELECT id FROM {schema}.workflows
                 WHERE state = 1 AND resume_at <= now()
                 ORDER BY resume_at
                 LIMIT @max
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING id, definition, current_step, current_step_name, priority
             """;

        DueSignalTimeouts =
            $"""
             SELECT id, definition, current_step, current_step_name, wait_signal
             FROM {schema}.workflows
             WHERE state = 2 AND wait_expires_at IS NOT NULL AND wait_expires_at <= now()
             ORDER BY wait_expires_at
             LIMIT @max
             """;

        // Reconciliation: for every live instance that has *no message in flight*, find out why, by
        // looking at the queue rather than by guessing.
        //
        // An earlier version re-dispatched every idle instance blindly, on the theory that the dedup key
        // made it a no-op when a message still existed. That theory was wrong twice over. A dead-lettered
        // message has *released* its dedup key, so re-dispatching gave a step that had already thrown its
        // way through its whole attempt budget a fresh budget — the workflow carried on after a definitive
        // failure. And an instance whose message was merely queued behind a busy fleet was counted as a
        // failed re-dispatch on every tick, until it was parked while its step had never failed at all.
        //
        // So: the candidate set is exactly the instances with nothing in flight (an index probe on the
        // queue's unique dedup index), and each one is classified by whether a dead letter explains it.
        // A dead letter newer than the instance's last progress is a verdict — never a reason to retry.
        // Anything else is genuinely lost work, and only that is re-dispatched.
        //
        // State 2 (WaitingSignal) is excluded: having no message is its normal condition, not a symptom.
        ReconcileCandidates =
            $"""
             WITH candidate AS (
                 SELECT w.id, w.definition, w.state, w.current_step, w.current_step_name, w.priority,
                        w.compensation_index, w.redispatch_count, w.updated_at
                 FROM {schema}.workflows w
                 WHERE w.state IN (0, 1, 3)
                   AND NOT EXISTS (
                         SELECT 1 FROM {queueSchema}.messages m
                         WHERE m.dedup_key = replace(w.id::text, '-', '') || ':' || w.current_step
                            OR (w.compensation_index IS NOT NULL
                                AND m.dedup_key =
                                    replace(w.id::text, '-', '') || ':c' || w.compensation_index)
                       )
                 ORDER BY w.updated_at
                 LIMIT @max
                 FOR UPDATE SKIP LOCKED
             )
             SELECT DISTINCT ON (c.id)
                    c.id, c.definition, c.state, c.current_step, c.current_step_name, c.priority,
                    c.compensation_index, c.redispatch_count,
                    c.updated_at < now() - make_interval(secs => @grace) AS redispatchable,
                    d.dedup_key, d.error, d.attempts
             FROM candidate c
             LEFT JOIN {queueSchema}.dead_letters d
                    ON (d.dedup_key = replace(c.id::text, '-', '') || ':' || c.current_step
                        OR (c.compensation_index IS NOT NULL
                            AND d.dedup_key =
                                replace(c.id::text, '-', '') || ':c' || c.compensation_index))
                   -- Older than the instance's last progress means it belongs to a previous life of this
                   -- step (it was resumed since), so it is history rather than a verdict.
                   AND d.failed_at > c.updated_at
             ORDER BY c.id, d.failed_at DESC NULLS LAST
             """;

        // Counted only when a message was really put back, so an instance whose step is simply queued
        // behind a busy fleet can never be parked for it.
        BumpRedispatch =
            $"""
             UPDATE {schema}.workflows
             SET redispatch_count = redispatch_count + 1, updated_at = now()
             WHERE id = @id
             RETURNING redispatch_count
             """;

        // Archive keeps a self-contained snapshot: the instance row plus its step trace, so a
        // post-mortem months later needs nothing else.
        ArchiveTerminal =
            $"""
             WITH due AS (
                 SELECT id FROM {schema}.workflows
                 WHERE state >= 4 AND completed_at IS NOT NULL
                   AND completed_at < now() - make_interval(secs => @retention)
                 ORDER BY completed_at
                 LIMIT @max
                 FOR UPDATE SKIP LOCKED
             ), moved AS (
                 INSERT INTO {schema}.workflow_archive
                     (id, definition, state, error, created_at, completed_at, snapshot)
                 SELECT w.id, w.definition, w.state, w.error, w.created_at, w.completed_at,
                        jsonb_build_object(
                            'instance', to_jsonb(w),
                            'steps', COALESCE((
                                SELECT jsonb_agg(to_jsonb(s) ORDER BY s.step_index)
                                FROM {schema}.workflow_steps s WHERE s.workflow_id = w.id
                            ), '[]'::jsonb))
                 FROM {schema}.workflows w
                 JOIN due ON due.id = w.id
                 ON CONFLICT (id) DO NOTHING
                 RETURNING id
             ), cleared_steps AS (
                 DELETE FROM {schema}.workflow_steps WHERE workflow_id IN (SELECT id FROM due)
             ), cleared_attempts AS (
                 DELETE FROM {schema}.workflow_step_attempts WHERE workflow_id IN (SELECT id FROM due)
             ), cleared_signals AS (
                 DELETE FROM {schema}.workflow_signals WHERE workflow_id IN (SELECT id FROM due)
             ), cleared_cancellations AS (
                 DELETE FROM {schema}.workflow_cancellations WHERE workflow_id IN (SELECT id FROM due)
             )
             DELETE FROM {schema}.workflows WHERE id IN (SELECT id FROM due)
             """;

        PruneArchive =
            $"""
             DELETE FROM {schema}.workflow_archive
             WHERE archived_at < now() - make_interval(secs => @retention)
             """;

        Stats =
            $"""
             SELECT
                 count(*) FILTER (WHERE state = 0),
                 count(*) FILTER (WHERE state = 1),
                 count(*) FILTER (WHERE state = 2),
                 count(*) FILTER (WHERE state = 3),
                 count(*) FILTER (WHERE state = 4),
                 count(*) FILTER (WHERE state = 5),
                 count(*) FILTER (WHERE state = 6),
                 count(*) FILTER (WHERE state = 7),
                 min(created_at) FILTER (WHERE state < 4),
                 now()
             FROM {schema}.workflows
             WHERE (@definition IS NULL OR definition = @definition)
             """;

        List =
            $"""
             SELECT {columns} {from}
             WHERE (@definition IS NULL OR w.definition = @definition)
               AND (@state IS NULL OR w.state = @state)
               AND (NOT @live_only OR w.state < 4)
               AND (@idle_since IS NULL OR w.updated_at < @idle_since)
             ORDER BY w.updated_at DESC
             LIMIT @max
             """;

        SchemaVersion = $"SELECT max(version) FROM {schema}.__liteflow_schema_version";

        // The step runs between a savepoint and its release. If it throws, rolling back to the savepoint
        // undoes everything it wrote and leaves the transaction usable — which is what lets the engine
        // record the verdict in the very transaction the fenced acknowledge commits. Without it a verdict
        // could only be written from another connection, and that would deadlock against the row lock
        // this transaction is holding.
        CreateSavepoint = $"SAVEPOINT {StepSavepoint}";
        RollbackToSavepoint = $"ROLLBACK TO SAVEPOINT {StepSavepoint}";
        ReleaseSavepoint = $"RELEASE SAVEPOINT {StepSavepoint}";
    }

    public string Schema { get; }

    /// <summary>Schema holding the step queues, which the reconciliation sweep has to read.</summary>
    public string QueueSchema { get; }

    public string Columns { get; }

    public string Insert { get; }

    public string SelectByIdempotencyKey { get; }

    public string LoadForUpdate { get; }

    public string Load { get; }

    public string Advance { get; }

    public string Finish { get; }

    public string StartCompensation { get; }

    public string AdvanceCompensation { get; }

    public string InsertCancellation { get; }

    public string DeleteCancellation { get; }

    public string CancelledWhileParked { get; }

    public string Resume { get; }

    public string MarkNeedsAttention { get; }

    public string SelectCancelled { get; }

    public string UpsertStepStart { get; }

    public string CompleteStep { get; }

    public string FailStep { get; }

    public string InsertAttempt { get; }

    public string ListSteps { get; }

    public string SelectCompensable { get; }

    public string MarkCompensated { get; }

    public string InsertSignal { get; }

    public string SelectSignal { get; }

    public string ResumeFromSignal { get; }

    public string DueSuspended { get; }

    public string DueSignalTimeouts { get; }

    public string ReconcileCandidates { get; }

    public string BumpRedispatch { get; }

    public string ArchiveTerminal { get; }

    public string PruneArchive { get; }

    public string Stats { get; }

    public string List { get; }

    public string SchemaVersion { get; }

    public string CreateSavepoint { get; }

    public string RollbackToSavepoint { get; }

    public string ReleaseSavepoint { get; }

    /// <summary>Name of the savepoint a step runs inside. Fixed: there is never more than one per transaction.</summary>
    private const string StepSavepoint = "liteflow_step";
}

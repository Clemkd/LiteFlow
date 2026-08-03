using System.Data;
using System.Data.Common;
using LiteQueue.Connectors;

namespace LiteFlow.Internal;

/// <summary>
/// A connection plus the transaction LiteFlow's statements must join. Exists so the same commands
/// serve both paths: the step's transaction (borrowed from the caller's <c>DbContext</c> through
/// LiteQueue's connector) and the engine's own side connection, which by definition must stay outside
/// it.
/// </summary>
internal readonly struct SqlTarget(DbConnection connection, DbTransaction? transaction)
{
    public DbConnection Connection { get; } = connection;

    public DbTransaction? Transaction { get; } = transaction;

    public static SqlTarget From(QueueConnection connection) =>
        new(connection.Connection, connection.Transaction);

    public DbCommand CreateCommand()
    {
        var cmd = Connection.CreateCommand();
        cmd.Transaction = Transaction;
        return cmd;
    }
}

/// <summary>An instance row, as stored.</summary>
internal sealed record WorkflowRow
{
    public required Guid Id { get; init; }

    public required string Definition { get; init; }

    public required string Signature { get; init; }

    public required WorkflowState State { get; init; }

    public required int CurrentStep { get; init; }

    public required string CurrentStepName { get; init; }

    public required int StepCount { get; init; }

    public int? CompensationIndex { get; init; }

    public string? Input { get; init; }

    public string? Context { get; init; }

    public string? IdempotencyKey { get; init; }

    public string? CorrelationId { get; init; }

    public int Priority { get; init; }

    public bool CancelRequested { get; init; }

    public string? CancelReason { get; init; }

    public DateTimeOffset? ResumeAt { get; init; }

    public string? WaitSignal { get; init; }

    public DateTimeOffset? WaitExpiresAt { get; init; }

    public int RedispatchCount { get; init; }

    public string? Error { get; init; }

    public string? WorkerId { get; init; }

    public required DateTimeOffset CreatedAt { get; init; }

    public required DateTimeOffset UpdatedAt { get; init; }

    public DateTimeOffset? CompletedAt { get; init; }

    public WorkflowInstance ToInstance() => new()
    {
        Id = Id,
        Definition = Definition,
        Signature = Signature,
        State = State,
        CurrentStep = CurrentStep,
        CurrentStepName = CurrentStepName,
        StepCount = StepCount,
        StateJson = Context,
        InputJson = Input,
        IdempotencyKey = IdempotencyKey,
        CorrelationId = CorrelationId,
        CancelRequested = CancelRequested,
        CancelReason = CancelReason,
        ResumeAt = ResumeAt,
        WaitSignal = WaitSignal,
        WaitExpiresAt = WaitExpiresAt,
        Error = Error,
        WorkerId = WorkerId,
        CreatedAt = CreatedAt,
        UpdatedAt = UpdatedAt,
        CompletedAt = CompletedAt,
    };
}

/// <summary>An instance whose current step has to be (re)dispatched.</summary>
internal sealed record DispatchTarget(Guid Id, string Definition, int StepIndex, string StepName, int Priority);

/// <summary>An instance whose wait for a signal has expired.</summary>
internal sealed record ExpiredWait(Guid Id, string Definition, int StepIndex, string StepName, string? Signal);

/// <summary>
/// Executes the statements in <see cref="WorkflowSql"/>. Raw ADO on purpose: these are hand-written
/// statements with CTEs, locking clauses and guarded updates, none of which an ORM can express — and
/// the engine has to be able to run on the caller's connection whatever their EF model looks like.
/// </summary>
internal static class WorkflowCommands
{
    public static async Task ExecuteScriptAsync(SqlTarget target, string script, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = script;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<int?> SchemaVersionAsync(SqlTarget target, WorkflowSql sql, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.SchemaVersion;
        var value = await cmd.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToInt32(value);
    }

    public static async Task<Guid?> InsertAsync(
        SqlTarget target, WorkflowSql sql, Guid id, WorkflowDefinition definition,
        string stateJson, WorkflowStartOptions options, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.Insert;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "definition", definition.Name, DbType.String);
        Add(cmd, "signature", definition.Signature, DbType.String);
        Add(cmd, "step_name", definition.Steps[0].Name, DbType.String);
        Add(cmd, "step_count", definition.StepCount, DbType.Int32);
        Add(cmd, "input", stateJson, DbType.String);
        Add(cmd, "idempotency_key", options.IdempotencyKey, DbType.String);
        Add(cmd, "correlation_id", options.CorrelationId, DbType.String);
        Add(cmd, "priority", options.Priority, DbType.Int32);

        var inserted = await cmd.ExecuteScalarAsync(ct);
        return inserted is null or DBNull ? null : (Guid)inserted;
    }

    public static async Task<Guid?> FindByIdempotencyKeyAsync(
        SqlTarget target, WorkflowSql sql, string definition, string idempotencyKey, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.SelectByIdempotencyKey;
        Add(cmd, "definition", definition, DbType.String);
        Add(cmd, "idempotency_key", idempotencyKey, DbType.String);
        var value = await cmd.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : (Guid)value;
    }

    public static Task<WorkflowRow?> LoadForUpdateAsync(
        SqlTarget target, WorkflowSql sql, Guid id, CancellationToken ct) =>
        LoadCoreAsync(target, sql.LoadForUpdate, id, ct);

    public static Task<WorkflowRow?> LoadAsync(
        SqlTarget target, WorkflowSql sql, Guid id, CancellationToken ct) =>
        LoadCoreAsync(target, sql.Load, id, ct);

    private static async Task<WorkflowRow?> LoadCoreAsync(
        SqlTarget target, string statement, Guid id, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = statement;
        Add(cmd, "id", id, DbType.Guid);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadRow(reader) : null;
    }

    public static async Task<int> AdvanceAsync(
        SqlTarget target, WorkflowSql sql, Guid id, int fromStep, int toStep, string toStepName,
        WorkflowState state, string? context, TimeSpan? resumeIn, string? waitSignal,
        TimeSpan? waitFor, string? workerId, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.Advance;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "from_step", fromStep, DbType.Int32);
        Add(cmd, "to_step", toStep, DbType.Int32);
        Add(cmd, "to_step_name", toStepName, DbType.String);
        Add(cmd, "state", (int)state, DbType.Int32);
        Add(cmd, "context", context, DbType.String);
        Add(cmd, "resume_secs", resumeIn?.TotalSeconds, DbType.Double);
        Add(cmd, "wait_signal", waitSignal, DbType.String);
        Add(cmd, "wait_secs", waitFor?.TotalSeconds, DbType.Double);
        Add(cmd, "worker_id", workerId, DbType.String);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<int> FinishAsync(
        SqlTarget target, WorkflowSql sql, Guid id, WorkflowState state, string? context,
        string? error, string? workerId, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.Finish;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "state", (int)state, DbType.Int32);
        Add(cmd, "context", context, DbType.String);
        Add(cmd, "error", error, DbType.String);
        Add(cmd, "worker_id", workerId, DbType.String);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<int> StartCompensationAsync(
        SqlTarget target, WorkflowSql sql, Guid id, int index, string? error, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.StartCompensation;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "index", index, DbType.Int32);
        Add(cmd, "error", error, DbType.String);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<int> AdvanceCompensationAsync(
        SqlTarget target, WorkflowSql sql, Guid id, int? index, string? context, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.AdvanceCompensation;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "index", index, DbType.Int32);
        Add(cmd, "context", context, DbType.String);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<int> RequestCancelAsync(
        SqlTarget target, WorkflowSql sql, Guid id, string? reason, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.InsertCancellation;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "reason", reason, DbType.String);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<int> ClearCancellationAsync(
        SqlTarget target, WorkflowSql sql, Guid id, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.DeleteCancellation;
        Add(cmd, "id", id, DbType.Guid);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<List<DispatchTarget>> CancelledWhileParkedAsync(
        SqlTarget target, WorkflowSql sql, int max, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.CancelledWhileParked;
        Add(cmd, "max", max, DbType.Int32);
        return await ReadTargetsAsync(cmd, ct);
    }

    /// <summary>
    /// Open the window a step runs in. Rolling back to it undoes everything the step wrote while leaving
    /// the transaction usable, which is what lets the verdict of a failed step be written in the same
    /// transaction that acknowledges its message.
    /// </summary>
    public static Task SavepointAsync(SqlTarget target, WorkflowSql sql, CancellationToken ct) =>
        ExecuteAsync(target, sql.CreateSavepoint, ct);

    public static Task RollbackToSavepointAsync(SqlTarget target, WorkflowSql sql, CancellationToken ct) =>
        ExecuteAsync(target, sql.RollbackToSavepoint, ct);

    public static Task ReleaseSavepointAsync(SqlTarget target, WorkflowSql sql, CancellationToken ct) =>
        ExecuteAsync(target, sql.ReleaseSavepoint, ct);

    private static async Task ExecuteAsync(SqlTarget target, string statement, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = statement;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<(int CurrentStep, string StepName)?> ResumeAsync(
        SqlTarget target, WorkflowSql sql, Guid id, WorkflowDefinition definition,
        WorkflowStepDescriptor step, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.Resume;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "signature", definition.Signature, DbType.String);
        Add(cmd, "step", step.Index, DbType.Int32);
        Add(cmd, "step_name", step.Name, DbType.String);
        Add(cmd, "step_count", definition.StepCount, DbType.Int32);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? (reader.GetInt32(0), reader.GetString(1)) : null;
    }

    public static async Task<int> MarkNeedsAttentionAsync(
        SqlTarget target, WorkflowSql sql, Guid id, string error, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.MarkNeedsAttention;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "error", error, DbType.String);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<List<Guid>> SelectCancelledAsync(
        SqlTarget target, WorkflowSql sql, Guid[] ids, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.SelectCancelled;
        AddArray(cmd, "ids", ids);

        var cancelled = new List<Guid>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            cancelled.Add(reader.GetGuid(0));
        return cancelled;
    }

    public static async Task UpsertStepStartAsync(
        SqlTarget target, WorkflowSql sql, Guid id, int stepIndex, string stepName, int attempts,
        string? workerId, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.UpsertStepStart;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "step_index", stepIndex, DbType.Int32);
        Add(cmd, "step_name", stepName, DbType.String);
        Add(cmd, "attempts", attempts, DbType.Int32);
        Add(cmd, "worker_id", workerId, DbType.String);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task CompleteStepAsync(
        SqlTarget target, WorkflowSql sql, Guid id, int stepIndex, StepState state, int durationMs,
        string? output, string? error, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.CompleteStep;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "step_index", stepIndex, DbType.Int32);
        Add(cmd, "state", (int)state, DbType.Int32);
        Add(cmd, "duration_ms", durationMs, DbType.Int32);
        Add(cmd, "output", output, DbType.String);
        Add(cmd, "error", error, DbType.String);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task FailStepAsync(
        SqlTarget target, WorkflowSql sql, Guid id, int stepIndex, string stepName, int attempts,
        string? workerId, string? error, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.FailStep;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "step_index", stepIndex, DbType.Int32);
        Add(cmd, "step_name", stepName, DbType.String);
        Add(cmd, "attempts", attempts, DbType.Int32);
        Add(cmd, "worker_id", workerId, DbType.String);
        Add(cmd, "error", error, DbType.String);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task InsertAttemptAsync(
        SqlTarget target, WorkflowSql sql, Guid id, int stepIndex, string stepName, int attempt,
        string? workerId, string? error, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.InsertAttempt;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "step_index", stepIndex, DbType.Int32);
        Add(cmd, "step_name", stepName, DbType.String);
        Add(cmd, "attempt", attempt, DbType.Int32);
        Add(cmd, "worker_id", workerId, DbType.String);
        Add(cmd, "error", error, DbType.String);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<List<WorkflowStepRecord>> ListStepsAsync(
        SqlTarget target, WorkflowSql sql, Guid id, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.ListSteps;
        Add(cmd, "id", id, DbType.Guid);

        var steps = new List<WorkflowStepRecord>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            steps.Add(new WorkflowStepRecord
            {
                WorkflowId = reader.GetGuid(0),
                StepIndex = reader.GetInt32(1),
                StepName = reader.GetString(2),
                State = (StepState)Convert.ToInt32(reader.GetValue(3)),
                Attempts = reader.GetInt32(4),
                StartedAt = reader.GetFieldValue<DateTimeOffset>(5),
                CompletedAt = reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTimeOffset>(6),
                DurationMs = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                Output = reader.IsDBNull(8) ? null : reader.GetString(8),
                Error = reader.IsDBNull(9) ? null : reader.GetString(9),
                WorkerId = reader.IsDBNull(10) ? null : reader.GetString(10),
            });
        }
        return steps;
    }

    public static async Task<(int StepIndex, string StepName)?> SelectCompensableAsync(
        SqlTarget target, WorkflowSql sql, Guid id, int maxIndex, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.SelectCompensable;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "max_index", maxIndex, DbType.Int32);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? (reader.GetInt32(0), reader.GetString(1)) : null;
    }

    public static async Task MarkCompensatedAsync(
        SqlTarget target, WorkflowSql sql, Guid id, int stepIndex, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.MarkCompensated;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "step_index", stepIndex, DbType.Int32);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<DateTimeOffset?> InsertSignalAsync(
        SqlTarget target, WorkflowSql sql, Guid id, string name, string? payload, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.InsertSignal;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "name", name, DbType.String);
        Add(cmd, "payload", payload, DbType.String);
        var value = await cmd.ExecuteScalarAsync(ct);
        // ExecuteScalar hands back Npgsql's default mapping for timestamptz, which is a UTC DateTime; the
        // implicit conversion keeps the instant and gives it a zero offset.
        return value is null or DBNull ? null : (DateTime)value;
    }

    public static async Task<WorkflowSignal?> SelectSignalAsync(
        SqlTarget target, WorkflowSql sql, Guid id, string name, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.SelectSignal;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "name", name, DbType.String);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;
        return new WorkflowSignal(
            reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.GetFieldValue<DateTimeOffset>(2));
    }

    public static async Task<(int CurrentStep, string StepName)?> ResumeFromSignalAsync(
        SqlTarget target, WorkflowSql sql, Guid id, string name, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.ResumeFromSignal;
        Add(cmd, "id", id, DbType.Guid);
        Add(cmd, "name", name, DbType.String);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? (reader.GetInt32(0), reader.GetString(1)) : null;
    }

    public static async Task<List<DispatchTarget>> DueSuspendedAsync(
        SqlTarget target, WorkflowSql sql, int max, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.DueSuspended;
        Add(cmd, "max", max, DbType.Int32);
        return await ReadTargetsAsync(cmd, ct);
    }

    public static async Task<List<DispatchTarget>> OrphanCandidatesAsync(
        SqlTarget target, WorkflowSql sql, TimeSpan grace, int maxRedispatch, int max, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.OrphanCandidates;
        Add(cmd, "grace", grace.TotalSeconds, DbType.Double);
        Add(cmd, "max_redispatch", maxRedispatch, DbType.Int32);
        Add(cmd, "max", max, DbType.Int32);
        return await ReadTargetsAsync(cmd, ct);
    }

    public static async Task<int> ExhaustedRedispatchAsync(
        SqlTarget target, WorkflowSql sql, TimeSpan grace, int maxRedispatch, string error, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.ExhaustedRedispatch;
        Add(cmd, "grace", grace.TotalSeconds, DbType.Double);
        Add(cmd, "max_redispatch", maxRedispatch, DbType.Int32);
        Add(cmd, "error", error, DbType.String);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<List<ExpiredWait>> DueSignalTimeoutsAsync(
        SqlTarget target, WorkflowSql sql, int max, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.DueSignalTimeouts;
        Add(cmd, "max", max, DbType.Int32);

        var expired = new List<ExpiredWait>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            expired.Add(new ExpiredWait(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        }
        return expired;
    }

    public static async Task<int> ArchiveTerminalAsync(
        SqlTarget target, WorkflowSql sql, TimeSpan retention, int max, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.ArchiveTerminal;
        Add(cmd, "retention", retention.TotalSeconds, DbType.Double);
        Add(cmd, "max", max, DbType.Int32);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<int> PruneArchiveAsync(
        SqlTarget target, WorkflowSql sql, TimeSpan retention, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.PruneArchive;
        Add(cmd, "retention", retention.TotalSeconds, DbType.Double);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<WorkflowStats> StatsAsync(
        SqlTarget target, WorkflowSql sql, string? definition, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.Stats;
        Add(cmd, "definition", definition, DbType.String);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);

        var serverNow = reader.GetFieldValue<DateTimeOffset>(9);
        TimeSpan? oldest = reader.IsDBNull(8)
            ? null
            : serverNow - reader.GetFieldValue<DateTimeOffset>(8);

        return new WorkflowStats
        {
            Definition = definition,
            Running = reader.GetInt64(0),
            Suspended = reader.GetInt64(1),
            WaitingSignal = reader.GetInt64(2),
            Compensating = reader.GetInt64(3),
            Completed = reader.GetInt64(4),
            Failed = reader.GetInt64(5),
            Cancelled = reader.GetInt64(6),
            NeedsAttention = reader.GetInt64(7),
            OldestLiveAge = oldest,
        };
    }

    public static async Task<List<WorkflowInstance>> ListAsync(
        SqlTarget target, WorkflowSql sql, WorkflowQuery query, CancellationToken ct)
    {
        await using var cmd = target.CreateCommand();
        cmd.CommandText = sql.List;
        Add(cmd, "definition", query.Definition, DbType.String);
        Add(cmd, "state", query.State is null ? null : (int)query.State.Value, DbType.Int32);
        Add(cmd, "live_only", query.LiveOnly, DbType.Boolean);
        Add(cmd, "idle_since", query.IdleSince, DbType.DateTimeOffset);
        Add(cmd, "max", Math.Max(1, query.MaxResults), DbType.Int32);

        var instances = new List<WorkflowInstance>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            instances.Add(ReadRow(reader).ToInstance());
        return instances;
    }

    private static async Task<List<DispatchTarget>> ReadTargetsAsync(DbCommand cmd, CancellationToken ct)
    {
        var targets = new List<DispatchTarget>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            targets.Add(new DispatchTarget(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetString(3),
                reader.GetInt32(4)));
        }
        return targets;
    }

    private static WorkflowRow ReadRow(DbDataReader reader) => new()
    {
        Id = reader.GetGuid(0),
        Definition = reader.GetString(1),
        Signature = reader.GetString(2),
        State = (WorkflowState)Convert.ToInt32(reader.GetValue(3)),
        CurrentStep = reader.GetInt32(4),
        CurrentStepName = reader.GetString(5),
        StepCount = reader.GetInt32(6),
        CompensationIndex = reader.IsDBNull(7) ? null : reader.GetInt32(7),
        Input = reader.IsDBNull(8) ? null : reader.GetString(8),
        Context = reader.IsDBNull(9) ? null : reader.GetString(9),
        IdempotencyKey = reader.IsDBNull(10) ? null : reader.GetString(10),
        CorrelationId = reader.IsDBNull(11) ? null : reader.GetString(11),
        Priority = reader.GetInt32(12),
        ResumeAt = reader.IsDBNull(15) ? null : reader.GetFieldValue<DateTimeOffset>(15),
        WaitSignal = reader.IsDBNull(16) ? null : reader.GetString(16),
        WaitExpiresAt = reader.IsDBNull(17) ? null : reader.GetFieldValue<DateTimeOffset>(17),
        RedispatchCount = reader.GetInt32(18),
        Error = reader.IsDBNull(19) ? null : reader.GetString(19),
        WorkerId = reader.IsDBNull(20) ? null : reader.GetString(20),
        CreatedAt = reader.GetFieldValue<DateTimeOffset>(21),
        UpdatedAt = reader.GetFieldValue<DateTimeOffset>(22),
        CompletedAt = reader.IsDBNull(23) ? null : reader.GetFieldValue<DateTimeOffset>(23),
        // The cancellation table is the source of truth while the instance is live: its row can be written
        // at any moment, including while a step holds the instance row locked. The column on the instance is
        // the copy left behind once the cancellation has been honoured.
        CancelRequested = reader.GetBoolean(13) || !reader.IsDBNull(24),
        CancelReason = reader.IsDBNull(25)
            ? (reader.IsDBNull(14) ? null : reader.GetString(14))
            : reader.GetString(25),
    };

    private static void Add(DbCommand cmd, string name, object? value, DbType? type = null)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value ?? DBNull.Value;
        // Null values carry no type information, so nullable parameters state theirs explicitly —
        // otherwise the server cannot resolve the statement's parameter types.
        if (type.HasValue)
            p.DbType = type.Value;
        cmd.Parameters.Add(p);
    }

    private static void AddArray(DbCommand cmd, string name, Array value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }
}

using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace LiteFlow.Tests;

/// <summary>The state that travels through every test workflow.</summary>
public sealed class TestState
{
    public string Tag { get; set; } = string.Empty;

    public int Counter { get; set; }

    public List<string> Trail { get; set; } = [];

    public string? SignalPayload { get; set; }
}

/// <summary>
/// The behaviour of the test workflows, injectable per test.
/// <para>
/// The steps of every test definition are inline delegates that do nothing but call into the script
/// resolved from the step's own scope. One script instance per DI provider means one behaviour per test,
/// with no shared state between them and no need for a new step class per scenario.
/// </para>
/// <para>
/// Every execution records a row in <c>public.step_executions</c> <b>through the step's own connection</b>,
/// so the row exists if and only if the step's transaction committed. Counting those rows is how the
/// tests tell "executed" from "attempted": an interrupted or fenced attempt leaves nothing behind.
/// </para>
/// </summary>
public sealed class StepScript
{
    private readonly ConcurrentDictionary<string, Func<IWorkflowStepContext<TestState>, CancellationToken, Task<StepResult>>> _steps = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Func<IWorkflowStepContext<TestState>, CancellationToken, Task>> _compensations = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _entries = new(StringComparer.Ordinal);

    /// <summary>Every step entry, in order, as <c>step:attempt</c> — including the attempts that were rolled back.</summary>
    public ConcurrentQueue<string> Trail { get; } = new();

    /// <summary>Set what a step does. Without one, a step records itself and continues.</summary>
    public void On(string step, Func<IWorkflowStepContext<TestState>, CancellationToken, Task<StepResult>> behaviour) =>
        _steps[step] = behaviour;

    /// <summary>Set what a compensation does, on top of recording itself.</summary>
    public void OnCompensate(string step, Func<IWorkflowStepContext<TestState>, CancellationToken, Task> behaviour) =>
        _compensations[step] = behaviour;

    /// <summary>How many times a step has been <i>entered</i> — attempts, not commits.</summary>
    public int Entries(string step) => _entries.TryGetValue(step, out int n) ? n : 0;

    /// <summary>Called by the inline steps of the test definitions.</summary>
    public async Task<StepResult> RunAsync(
        string step, IWorkflowStepContext<TestState> context, CancellationToken ct)
    {
        _entries.AddOrUpdate(step, 1, (_, n) => n + 1);
        Trail.Enqueue($"{step}:{context.Attempt}");

        context.State.Counter++;
        context.State.Trail.Add(step);

        await RecordAsync(context, "step_executions", step, context.Attempt, ct);

        return _steps.TryGetValue(step, out var behaviour)
            ? await behaviour(context, ct)
            : StepResult.Next();
    }

    /// <summary>Called by the inline compensations of the test definitions.</summary>
    public async Task CompensateAsync(
        string step, IWorkflowStepContext<TestState> context, CancellationToken ct)
    {
        Trail.Enqueue($"compensate:{step}");
        await RecordAsync(context, "step_compensations", step, null, ct);

        if (_compensations.TryGetValue(step, out var behaviour))
            await behaviour(context, ct);
    }

    /// <summary>
    /// Write a business row on the step's connection. Committed with the step, rolled back with it — the
    /// point the crash tests turn on.
    /// </summary>
    public static async Task WriteBusinessAsync(
        IWorkflowStepContext<TestState> context, long id, string value, CancellationToken ct)
    {
        await using var cmd = context.CreateCommand();
        cmd.CommandText =
            "INSERT INTO public.business_records (id, value) VALUES (@id, @value) ON CONFLICT (id) DO NOTHING";
        Add(cmd, "id", id);
        Add(cmd, "value", value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task RecordAsync(
        IWorkflowStepContext<TestState> context, string table, string step, int? attempt, CancellationToken ct)
    {
        await using var cmd = context.CreateCommand();
        cmd.CommandText = attempt is null
            ? $"INSERT INTO public.{table} (workflow_id, step_name) VALUES (@id, @step)"
            : $"INSERT INTO public.{table} (workflow_id, step_name, attempt) VALUES (@id, @step, @attempt)";
        Add(cmd, "id", context.WorkflowId);
        Add(cmd, "step", step);
        if (attempt is not null)
            Add(cmd, "attempt", attempt.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static void Add(System.Data.Common.DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }

    /// <summary>Shorthand used by the test definitions: <c>Script(ctx).RunAsync(name, ctx, ct)</c>.</summary>
    public static Task<StepResult> Run(string step, IWorkflowStepContext<TestState> ctx, CancellationToken ct) =>
        ctx.Services.GetRequiredService<StepScript>().RunAsync(step, ctx, ct);

    /// <summary>Shorthand for the compensations of the test definitions.</summary>
    public static Task Compensate(string step, IWorkflowStepContext<TestState> ctx, CancellationToken ct) =>
        ctx.Services.GetRequiredService<StepScript>().CompensateAsync(step, ctx, ct);
}

/// <summary>Assertions that look at the rows the steps wrote, rather than at what the engine says it did.</summary>
public static class TestDb
{
    /// <summary>Committed executions of one step of one instance. The exactly-once assertion.</summary>
    public static Task<int> ExecutionsAsync(string connectionString, Guid workflowId, string step) =>
        ScalarAsync(connectionString,
            "SELECT count(*) FROM public.step_executions WHERE workflow_id = @id AND step_name = @step",
            workflowId, step);

    /// <summary>Committed executions of every step of one instance.</summary>
    public static Task<int> ExecutionsAsync(string connectionString, Guid workflowId) =>
        ScalarAsync(connectionString,
            "SELECT count(*) FROM public.step_executions WHERE workflow_id = @id", workflowId, null);

    /// <summary>Business rows with a given id — the "no partial write" assertion.</summary>
    public static async Task<int> BusinessAsync(string connectionString, long id)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT count(*) FROM public.business_records WHERE id = @id";
        cmd.Parameters.AddWithValue("id", id);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    /// <summary>Compensations of one instance, in the order they committed.</summary>
    public static async Task<List<string>> CompensationOrderAsync(string connectionString, Guid workflowId)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "SELECT step_name FROM public.step_compensations WHERE workflow_id = @id ORDER BY seq";
        cmd.Parameters.AddWithValue("id", workflowId);

        var order = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            order.Add(reader.GetString(0));
        return order;
    }

    /// <summary>Instances of a definition whose steps committed more than once — the chaos-test invariant.</summary>
    public static async Task<List<string>> DuplicateExecutionsAsync(string connectionString, IEnumerable<Guid> ids)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            """
            SELECT workflow_id, step_name, count(*)
            FROM public.step_executions
            WHERE workflow_id = ANY(@ids)
            GROUP BY workflow_id, step_name
            HAVING count(*) > 1
            """;
        cmd.Parameters.AddWithValue("ids", ids.ToArray());

        var duplicates = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            duplicates.Add($"{reader.GetGuid(0)}/{reader.GetString(1)} × {reader.GetInt64(2)}");
        return duplicates;
    }

    private static async Task<int> ScalarAsync(
        string connectionString, string sql, Guid workflowId, string? step)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("id", workflowId);
        if (step is not null)
            cmd.Parameters.AddWithValue("step", step);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }
}

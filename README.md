# LiteFlow

**Durable workflows on PostgreSQL.** A workflow is a sequence of steps; every workflow and every step is
tracked in your database; and if the process dies in the middle of a step, the workflow resumes **at that
step**, on a database that shows no trace of the dead attempt.

No workflow server, no cluster, no extra infrastructure. Your PostgreSQL, one background loop, and — the part
that makes the rest work — **a step's own writes and the engine's progress commit in the same transaction**.

```csharp
public sealed class OrderWorkflow : Workflow<OrderState>
{
    protected override void Configure(IWorkflowBuilder<OrderState> b) => b
        .Step<ReserveStock>()
        .Step<ChargeCard>(s => s.MaxAttempts(3))                  // undoable: see ICompensatingWorkflowStep
        .Step<PackParcel>()                                       // slow — crash here and it resumes here
        .WaitForSignal("shipped", timeout: TimeSpan.FromDays(2))   // costs one row while it waits
        .Step<SendReceipt>(s => s.NonTransactional());             // an HTTP call: at-least-once, keyed
}
```

Built on [LiteQueue](https://github.com/Clemkd/LiteQueue) for dispatch, leases and retries.

---

## Why

The usual way to make a multi-step process survive a crash is to write "where am I?" to a table and hope the
writes and the bookkeeping agree. They do not, at exactly the moment it matters: the process dies between the
business write and the progress write, and you are left with a charge and no record of it, or a record and no
charge.

LiteFlow closes that gap by making them the same commit:

```
BEGIN
  SELECT … FROM workflows WHERE id = … FOR UPDATE   -- nobody else touches this instance
  step.ExecuteAsync(ctx)                            -- your writes, on ctx.DbContext
  UPDATE workflows SET current_step = current_step + 1, context = …
  INSERT INTO messages …                            -- the next step
  DELETE FROM messages WHERE id = … AND lease_token = …   -- fenced acknowledge
COMMIT
```

Crash before the `COMMIT` and *everything* is undone, including the acknowledge — so the step is delivered
again and runs from the top on a clean database. Crash after it and the next step is already queued. A worker
that lost its lease finds its acknowledge matches no row, and its writes roll back with it. See
[docs/DESIGN.md](docs/DESIGN.md) for the full argument, the guarantees, and — just as important — the
[non-guarantees](docs/DESIGN.md#5-guarantees-and-non-guarantees).

---

## Quickstart

```bash
# a PostgreSQL to talk to
docker run -d --name liteflow-pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=liteflow postgres:18

# the five-step sample: start one order and run it to the end
dotnet run --project samples/LiteFlow.Console -- demo

# then the interesting one: kill the process in the middle of the slow step…
dotnet run --project samples/LiteFlow.Console -- crash --after 3
# …and watch it resume on the same step, from the top
dotnet run --project samples/LiteFlow.Console -- worker
```

Other sample commands: `start --count 50 --fast`, `list --live`, `show --id <guid>`, `cancel --id <guid>`,
`signal --id <guid> --payload TRK-9`, `resume --id <guid>`, `stats`, `prune`, `reset`.

Or run the whole thing under Aspire — PostgreSQL, pgAdmin and three competing worker replicas:

```bash
dotnet run --project aspire/LiteFlow.AppHost
```

Kill a replica in the dashboard and watch its instances resume on another one.

---

## Wiring

```csharp
builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(connectionString));

builder.Services.AddLiteFlow<AppDbContext>(o =>
{
    o.ConnectionString = connectionString;   // for the sweep and the cancellation poll
    o.StepLease = TimeSpan.FromSeconds(30);  // the delay between a crash and the resume
});

builder.Services.AddLiteFlowWorkflow<OrderWorkflow>(w =>
{
    w.Concurrency = 8;          // steps in flight in this process
    w.ExternalConcurrency = 16; // for the NonTransactional steps, on their own queue
});
```

LiteFlow registers and drives LiteQueue itself — the step queues are an implementation detail it owns — so do
**not** call `AddLiteQueue` as well. Register the same workflow in as many processes as you like: each step is
claimed by exactly one of them, and an instance moves between hosts between steps without noticing.

Without an EF context, `AddLiteFlow(connectionString)` gives the engine its own pool; steps still get a
connection and transaction through `ctx.Connection`, just not a `DbContext`.

The schema is created for you (idempotent DDL, plus the storage tuning a high-churn table needs). To version it
yourself instead, call `modelBuilder.AddLiteFlowModel()` and `modelBuilder.AddLiteQueueModel()` in
`OnModelCreating`, and set `AutoCreateSchema = false`.

---

## Writing a step

```csharp
public sealed class ChargeCard : ICompensatingWorkflowStep<OrderState>
{
    public async Task<StepResult> ExecuteAsync(
        IWorkflowStepContext<OrderState> ctx, CancellationToken ct = default)
    {
        // ctx.DbContext / ctx.Connection / ctx.Transaction — the engine's transaction. Write here and your
        //   changes commit with the cursor advance, or not at all.
        // ctx.State           — the state bag, persisted after every step; what the next step reads.
        // ctx.IdempotencyKey  — stable across every attempt of this step of this instance. Hand it to
        //                       external systems so their deduplication makes a retry harmless.
        // ctx.Attempt / IsLastAttempt — above 1, a previous attempt died or threw.
        // ct                  — cancelled when the workflow is cancelled OR the lease is lost. Observe it:
        //                       once the lease is gone, nothing this step does can be committed.

        var db = (AppDbContext)ctx.DbContext!;
        db.Payments.Add(new Payment { Reference = ctx.IdempotencyKey, Amount = ctx.State.Amount });
        await db.SaveChangesAsync(ct);

        return StepResult.Next();
    }

    // Runs in reverse order if the workflow later fails or is cancelled — as its own durable message, so an
    // interrupted rollback resumes rather than restarting.
    public async Task CompensateAsync(IWorkflowStepContext<OrderState> ctx, CancellationToken ct = default)
        => await RefundAsync(ctx.State, ct);
}
```

A step returns:

| | |
| --- | --- |
| `StepResult.Next()` | continue with the next step |
| `StepResult.Skip(reason)` | nothing to do; recorded as skipped, cursor advances |
| `StepResult.Suspend(delay)` | pause, then continue — no lease and no worker held while waiting |
| `StepResult.Complete()` | finish successfully now, skipping the rest |
| `StepResult.Fail(reason)` | this will never work: fail immediately, no attempts wasted |
| *throwing* | this attempt failed: roll back and retry with backoff, then fail |

---

## Driving workflows

```csharp
var handle = await client.StartAsync<OrderWorkflow, OrderState>(
    new OrderState { OrderId = "A-4711" },
    new WorkflowStartOptions { IdempotencyKey = "A-4711" });   // retry-safe: same key, same instance

await client.SignalAsync(handle.WorkflowId, "shipped", "TRK-9");   // wake a waiting instance
await client.CancelAsync(handle.WorkflowId, "customer changed their mind");  // rolls back if compensations exist
await client.ResumeAsync(handle.WorkflowId);                       // after fixing what made it fail

var instance = await client.GetAsync(handle.WorkflowId);           // state, cursor, error, state bag
var trace = await client.GetStepsAsync(handle.WorkflowId);         // per step: outcome, attempts, duration
var stuck = await client.ListAsync(new WorkflowQuery { LiveOnly = true, IdleSince = threshold });
```

`StartAsync` runs on your connection, so it can be enrolled in your own transaction: the instance appears
exactly when your business data does, or not at all.

---

## Operating it

Nothing to schedule and nothing to elect a leader for: one background loop per process handles due timers,
timed-out waits, cancellations of parked instances, steps whose message went missing, and retention. Every
action it takes is idempotent, so running it everywhere is the intended configuration.

```sql
-- the two queries worth having on a dashboard
SELECT state, count(*) FROM liteflow.workflows GROUP BY state;      -- NeedsAttention should be 0
SELECT * FROM liteflow.workflows WHERE state < 4 ORDER BY updated_at LIMIT 20;   -- what is stuck
```

Metrics under the `LiteFlow` meter; `liteflow.steps.resumed` counts how often crash recovery actually fires,
which nothing else in your stack will tell you. Traces under the `LiteFlow` activity source, one per step
attempt.

---

## Tests

```bash
dotnet test tests/LiteFlow.Tests        # needs Docker: Testcontainers brings up postgres:18-alpine
```

31 tests against a real PostgreSQL, including a chaos run of 200 instances through a fleet that is repeatedly
stopped and hard-killed. They count *committed* effects, so they can tell "ran twice" from "took effect twice".
[docs/DESIGN.md §6](docs/DESIGN.md#6-which-test-protects-which-guarantee) maps every guarantee to the test that
pins it.

## Benchmarks

```bash
dotnet run -c Release --project benchmarks/LiteFlow.Benchmarks -- --smoke   # check the harness
dotnet run -c Release --project benchmarks/LiteFlow.Benchmarks              # throughput + recovery cost
```

## Licence

MIT.

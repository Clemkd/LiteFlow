# LiteFlow — Design, guarantees, and what it deliberately does not do

LiteFlow runs **workflows**: named sequences of steps, stored in PostgreSQL, where every workflow and every
step is tracked. Its single reason to exist is the sentence a durable engine has to be able to say out loud:

> If the process dies in the middle of a step, the workflow resumes **at that step**, on a database that
> shows no trace of the dead attempt.

Everything below is in service of that sentence, and section 5 is honest about where it stops.

It is built on [LiteQueue](https://github.com/Clemkd/LiteQueue): a step is a queue message, and the lease, heartbeat, retry and
fenced-acknowledge machinery is LiteQueue's. LiteFlow adds the notion of a *sequence* — a cursor, a state bag,
and the rules for moving from one step to the next without ever moving twice.

---

## 1. Where it sits

| Feature | Temporal / DTFx | Elsa | Hangfire (continuations) | **LiteFlow** |
| --- | --- | --- | --- | --- |
| Programming model | one async method, replayed from an event journal | visual / DSL activity graph | chained fire-and-forget jobs | **explicit step list, cursor + state bag** |
| Determinism required of your code | yes, strictly | no | no | **no** |
| Infrastructure | server cluster (+ its own database) | database + optional server | database | **your PostgreSQL, nothing else** |
| Resume after a crash | replay the journal | rehydrate the instance | re-run the job | **read one row, run the step again** |
| Step + business writes in one transaction | no (activities are separate) | no | no | **yes, by default** |
| Compensation / saga | manual, in code | activities | no | **per step, in reverse, durable** |
| Waiting for an external event | signals | bookmarks | no | **`WaitForSignal`, costs one row** |
| Branching / parallel steps | yes | yes | limited | **no — linear + suspend (v1)** |
| Versioning in-flight instances | patching API | migrations | n/a | **park in `NeedsAttention`, resume by step name** |
| Operational surface | a cluster to run | a server to run | a dashboard | **one background loop, no leader** |

The trade is deliberate. LiteFlow gives up expressiveness (no branching, no parallel fan-out, no replayed
coroutines) to get two things a replay engine cannot easily give you: **your step's writes and the engine's
progress commit in the same transaction**, and **the whole state of a workflow is one readable row**. When an
incident happens at 3 a.m., `SELECT * FROM liteflow.workflows WHERE state < 4` is the answer, not a journal to
reconstruct.

If you need a graph rather than a sequence, use Elsa or Temporal. If you need one async method with `if` and
`for` in it, use Temporal. If what you actually have is a sequence of steps that must survive a crash without
double-charging anyone, this is smaller and there is much less of it to understand.

---

## 2. Data model (schema `liteflow`)

```
workflows                 one row per instance — the cursor and the state
  id, definition, signature, state, current_step, current_step_name, step_count,
  compensation_index, input jsonb, context jsonb, idempotency_key, correlation_id,
  priority, cancel_requested, cancel_reason, resume_at, wait_signal, wait_expires_at,
  redispatch_count, error, worker_id, created_at, updated_at, completed_at

workflow_steps            one row per (instance, step) — the audit trail
  workflow_id, step_index, step_name, state, attempts, started_at, completed_at,
  duration_ms, output jsonb, error, worker_id

workflow_step_attempts    one row per failed attempt, written outside the failing transaction
workflow_cancellations    one row per cancellation request  (see §4.5 — this table is a correctness device)
workflow_signals          one row per (instance, signal name) — a signal delivered twice arrives once
workflow_archive          terminal instances, with their step trace as a jsonb snapshot
__liteflow_schema_version what the library expects to find, so it can upgrade itself
```

LiteFlow also creates one index outside its own schema: `ix_dead_letters_dedup` on the queue's
`dead_letters (dedup_key)`. The reconciliation sweep (§4.6) asks that table a question its own indexes do not
answer — "is there a dead letter for this instance and this step?" — and without the index that lookup is a
scan of every failure the system has ever recorded. LiteFlow owns the queue registration, so the queue schema
is its implementation detail; the statement is guarded and does nothing if the queue tables are absent.

Three choices worth defending:

- **Partial indexes on live instances only** (`WHERE state < 4`). The indexes the dispatcher and the sweep walk
  are the size of the work in flight, not of the history. A database that has run ten million workflows
  dispatches as fast as an empty one. The state values are therefore part of the storage contract:
  `WorkflowState` must never be renumbered without a schema version bump.
- **No foreign keys between instances and steps.** Step rows are written on the hot path, once per step, and an
  FK check would add an index probe and a parent-row lock to each. Only the engine writes them, and the archive
  sweep removes both sides together.
- **State as `jsonb`**, not a blob. "Which orders are stuck before payment?" is a query, not a support ticket.

The queues themselves live in LiteQueue's schema (`litequeue` by default): one queue per definition
(`wf:OrderWorkflow`), plus a second one (`wf:OrderWorkflow!io`) when the definition declares non-transactional
steps. Isolating definitions is what stops a backlog of one workflow from starving another; isolating the
non-transactional steps is what stops a slow HTTP call from occupying the workers the database-only steps need.

---

## 3. How a step runs

One step is one message, and the whole durability story is the order of operations inside a single
transaction — opened by LiteQueue's worker host before the handler is called, committed by it after the fenced
acknowledge:

```
BEGIN                                                  -- worker host
  SELECT … FROM workflows w
   LEFT JOIN workflow_cancellations c …
   WHERE w.id = @id FOR UPDATE OF w                    -- nobody else touches this instance
  -- guards: terminal? cancelled? cursor still here? step still named what it was?
  INSERT INTO workflow_steps … (state = running, attempts)
  SAVEPOINT liteflow_step
    step.ExecuteAsync(ctx)                             -- caller's writes, same connection
  RELEASE SAVEPOINT liteflow_step
  UPDATE workflow_steps  … completed, duration, output
  UPDATE workflows       … current_step + 1, context, state
     WHERE id = @id AND current_step = @from           -- guarded advance
  INSERT INTO litequeue.messages …                     -- the next step, dedup key {id}:{n+1}
  DELETE FROM litequeue.messages
   WHERE id = @msg AND lease_token = @token            -- fenced ack, worker host
COMMIT
```

### 3.1 The five things that follow

**Crash before the `COMMIT`.** Everything is undone — the step's business writes, the step row, the cursor
advance, the dispatch of the next step, *and the acknowledge*. The message is still leased by a process that
no longer exists, so it stops being renewed; the lease expires, LiteQueue's maintenance returns it to the
queue, and another worker runs the same step again from the top. The delay between the crash and the resume is
`StepLease` + one sweep interval, and that is the only place a crash costs time.

**Crash after the `COMMIT`.** The next step's message is already in the queue, committed atomically with the
cursor. Another worker picks it up immediately. There is no window in which a workflow has advanced but nothing
is scheduled to continue it — the failure mode that leaves a workflow stuck at step 3 forever.

**A worker that lost its lease** (a long GC pause, a suspended VM, a network partition) comes back and finishes
its step. Its acknowledge is fenced by the lease token, so it matches no row — and because that acknowledge is
*inside the step's transaction*, the rollback takes the step's business writes with it. A zombie cannot
double-apply anything, and it does not need to know it is a zombie.

**A message delivered twice** — LiteQueue guarantees at-least-once, and the sweep re-offers steps on purpose —
finds `current_step` already past it. The guard drops the message instead of re-running the work.

**Two workers, one instance.** The instance row is held `FOR UPDATE` for the whole attempt, so the steps of one
workflow are serialised whatever the queue, the leases or the fleet size decide. Combined with the dedup key
(one pending message per `(instance, step)`), there is no path to two concurrent executions of the same step.

### 3.2 Why the savepoint

A step that throws must leave nothing behind, but the engine still has to record *what happened* — and on the
last attempt, that the workflow has failed. Those two requirements pull in opposite directions: the verdict
cannot be written in the transaction that is about to be rolled back, and it cannot be written from another
connection either, because this transaction holds the instance row and the other one would wait for a step
that is waiting for it. (This is not hypothetical: it was the first implementation, and it deadlocked until the
command timed out, leaving the workflow `Running` forever.)

The savepoint resolves it. The step runs between `SAVEPOINT` and `RELEASE`. If it throws,
`ROLLBACK TO SAVEPOINT` discards everything it wrote **and leaves the transaction usable**, so the verdict is
written right there and committed by the same fenced acknowledge that consumes the message. Atomic, one
connection, no second party to wait for.

The same mechanism is what makes a mid-step cancellation clean: the interrupted step's half-finished work is
rolled back to the savepoint before the instance is marked `Cancelled`.

---

## 4. The rest of the lifecycle

### 4.1 Starting

`StartAsync` inserts the instance **and** dispatches its first step in one transaction, on the caller's
connection. So a workflow can be started from inside your own transaction: the instance appears exactly when
your business data does, or not at all. `IdempotencyKey` (use the business identity — the order number, not a
fresh `Guid`) makes the call safe to retry: the second one returns the first instance with `AlreadyExisted`.

A definition that opens on `WaitForSignal` is parked at creation rather than dispatched, so it costs one row
until the outside world calls back.

### 4.2 Suspending and waiting

`StepResult.Suspend(delay)` advances the cursor and dispatches the next step with a delay: the instance holds no
lease and no worker while it waits, so a pause of days is as cheap as one of seconds. `WaitForSignal(name)`
parks the instance with nothing queued at all; `SignalAsync` records the signal (unique per instance and name,
so a partner calling your webhook twice wakes it once) and dispatches the resuming step in the same
transaction. A signal that arrives *before* the workflow reaches its wait is kept, not dropped — the wait is
already satisfied when it gets there.

### 4.3 Failing, and rolling back

Throwing means "this attempt failed": the transaction rolls back and LiteQueue re-delivers with exponential
backoff until the step's attempts run out. Returning `StepResult.Fail(reason)` means "this will never work" and
fails the workflow immediately, without spending the remaining attempts proving it.

**A step that throws definitively fails its workflow — whichever way the engine finds out.** "Definitively"
covers every route: the configured attempts running out, a `PoisonMessageException`, a `WorkflowStateException`,
or anything else that leaves the step unable to succeed. There is no path on which a workflow carries on past a
definitive throw. Three mechanisms deliver that one guarantee:

1. **The worker's own catch block** (the normal case). The step's exception is caught, the savepoint is rolled
   back, and the verdict is written in the same transaction the fenced acknowledge commits — atomic with
   consuming the message.
2. **The verdict cannot be written** — the connection died, the host is going down, the transaction is
   unusable. The worker gives up on reporting and lets the message take the queue's retry path; the message
   eventually reaches the dead-letter table.
3. **Nobody was left to report anything at all** — the host was killed at the last attempt, so the message's
   lease simply expired and the queue dead-lettered it.

In cases 2 and 3 the verdict comes from the reconciliation sweep (§4.6), which reads that dead letter and writes
exactly the same state the worker would have: step `Failed`, and the workflow `Failed` (or `Compensating`, then
`Failed`). The instance is never re-dispatched and never left `Running`.

When a workflow ends badly and the definition declares compensations, the engine walks the completed steps in
reverse, **one durable message per compensation**. So a crash during a rollback resumes the rollback rather
than restarting it or abandoning it half-applied. Each compensation is guarded the same way as a step
(`state = Compensating AND compensation_index = @index`), so redelivery is harmless.

### 4.4 Definition drift

The cursor is an index, but the instance also stores the *name* of the step it is on, and the definition stores
a signature (a hash of the step list). When a worker picks up a step whose index no longer holds the name the
instance stopped on, it refuses: the instance goes to `NeedsAttention` with an explanatory error, and nothing
is executed. Appending steps is always safe. Inserting, removing, renaming or reordering one parks the
instances that were in flight — `ResumeAsync` then re-anchors them **by step name**, so an instance parked
because a step moved resumes on the step it was really on.

### 4.5 Cancelling

Cancellation is a row in `workflow_cancellations`, and it is a separate table for a correctness reason: a
running step holds a lock on its instance row for the whole of its transaction, so a cancellation written to
that row would block until the step it is trying to interrupt had finished. Written to its own table, it is
always one unblocked insert. Every read of an instance joins it, so the guard at the top of every step sees the
flag at no extra cost.

Three paths honour it:

- **Between steps** — always, at no cost: the guard at the top of the next step terminates the instance.
- **During a step** — a poll of the instances *this process* is running (one query per tick, ids it already
  holds) cancels the step's `CancellationToken` within `CancellationPollInterval`.
- **While parked** on a timer or a signal — nothing is in flight, so nobody would notice; the maintenance sweep
  wakes those instances so a worker can apply the cancellation, within one sweep interval.

Compensations run, so cancelling is a rollback rather than an abandonment.

### 4.6 The sweep that makes it maintenance-free

One background loop, safe to run on every instance of your service — every action it takes is idempotent, so
there is no leader to elect and no cron to install:

| Sweep | What it fixes |
| --- | --- |
| due timers | a `Suspended` instance whose delayed message was lost |
| expired waits | a `WaitForSignal` that timed out — fails (and rolls back) the instance |
| parked cancellations | a cancellation asked for while the instance had nothing in flight |
| **reconciliation** | a live instance with **no message in flight** — see below |
| retention | terminal instances to the archive, old archive rows away |

Reconciliation is the part worth reading twice, because getting it wrong breaks the guarantee in §4.3.

The candidate set is every live instance that has **no message in flight** — an index probe against the queue's
own unique dedup index, not a guess. For each candidate the sweep asks the queue *why*, and there are exactly two
answers:

- **A dead letter exists for this step**, newer than the instance's last progress. A worker ran the step, it
  failed through its whole attempt budget, and the queue gave up on it — but no verdict was written. The sweep
  writes it: step `Failed` with the dead letter's error as the cause, then the workflow `Failed` (rolling back
  first if it has compensations). This instance is **never re-dispatched**.
- **No dead letter either.** The message was genuinely lost — never enqueued because a host died in the wrong
  microsecond, or removed by something outside the engine. This, and only this, is re-dispatched, and only after
  `OrphanGracePeriod`. After `MaxRedispatch` re-dispatches that keep vanishing, the instance is parked in
  `NeedsAttention`: that residual case means something outside the engine is eating its messages, which is a
  human's problem.

A dead letter for a *compensation* is treated differently: the instance is parked in `NeedsAttention` rather
than reported as rolled back, because an incomplete rollback must not be dressed up as a complete one.

Two earlier versions of this sweep were wrong, and both are now pinned by tests:

- It re-dispatched every idle instance **blindly**, on the theory that the dedup key made that a no-op whenever a
  message still existed. True for a message in flight; false for a dead-lettered one, which has *released* its
  dedup key. So a step that had already thrown through its entire attempt budget was handed a brand-new budget,
  up to `MaxRedispatch` times — the workflow carried on after a definitive failure, and ended in
  `NeedsAttention` rather than `Failed` even then.
- It counted a re-dispatch for **every** idle candidate, including instances whose step was merely queued behind
  a busy fleet. Under sustained backlog those instances accumulated re-dispatch counts and were eventually
  parked, while their step had never failed at all. The count is now incremented only when a message was really
  put back.

`Compensating` instances are swept the same way, so a rollback whose message dies is no longer stuck in
`Compensating` forever.

---

## 5. Guarantees, and non-guarantees

### What LiteFlow guarantees

1. **One execution of one step of one instance at a time.** Row lock + lease + one-message-per-step dedup key.
2. **A transactional step's effects are applied exactly once.** Its writes, the step record, the cursor advance,
   the dispatch of the next step and the acknowledge are one transaction. Any crash undoes all of it or none.
3. **Resume at the same step, on a clean database.** No partial writes from a dead attempt are ever visible.
4. **A redelivered or stale message is dropped, not re-applied.**
5. **A worker that lost its lease commits nothing.**
6. **The state a step commits is exactly what the next step reads**, across a crash and a different machine.
7. **Starting is idempotent** given an idempotency key, and can be enrolled in the caller's transaction.
8. **Cancellation is always honoured** — between steps unconditionally, during a step within the poll interval.
9. **A rollback is durable**: compensations run in reverse, each resumable after a crash.
10. **A step that throws definitively fails its workflow**, whichever way the engine finds out — the worker's own
    catch block, a dead letter left by a host that died before it could report, or a payload nobody could read. A
    workflow never continues past a definitive throw, and never sits in `Running` with a dead-lettered step.
11. **A changed definition never runs the wrong step.** It parks the instance instead.
12. **No manual maintenance.** Timers, timeouts, lost steps and retention are swept automatically, with no
    leader election.

### What it does not guarantee — read this part

- **External side effects are at-least-once, not exactly-once.** A step declared `NonTransactional` runs outside
  the engine's transaction, and the engine acknowledges its message *after* committing its own bookkeeping.
  Crash in that window and the step runs again. The cursor guard stops the workflow from advancing twice, but
  nothing can un-send an email. Use `ctx.IdempotencyKey` — stable across every attempt of that step of that
  instance — so the receiving system deduplicates. Any step that calls out to a network is in this category
  whether or not it is declared as such: a transaction cannot protect a side effect it does not own.
- **A step that swallows its cancellation token still advances.** If a cancellation is requested mid-step and
  the step catches the `OperationCanceledException` and returns `Next`, the cursor moves; the cancellation is
  honoured at the *next* step. Steps should observe their token.
- **A step slower than its lease is executed more than once** when lease renewal is off
  (`WorkflowWorkerOptions.RenewLease = false`). Only one attempt can ever commit, but the others still ran.
  Renewal is on by default precisely so this does not happen.
- **No branching, no parallel steps, no fan-out.** v1 is a linear sequence plus `Skip`, `Suspend`, `Complete`
  and `Fail`. A step can decide *whether* to act; it cannot decide *what runs next*.
- **A compensation that permanently fails leaves the rollback incomplete.** The instance is parked in
  `NeedsAttention` rather than being declared rolled back — deliberately, because the alternative is a lie.
- **State serialization compatibility is yours.** The state bag is JSON written by a previous version of your
  code. A state class that changes incompatibly makes its in-flight instances unreadable
  (`WorkflowStateException` → the step is dead-lettered and the workflow failed, not silently mangled).
- **Cancelling a suspended instance is not instantaneous**: it waits for the next sweep (default 15 s), because
  its message is delayed and nothing else is watching.
- **Without a connection of its own** (`LiteFlowOptions.ConnectionString`, or an EF context whose connection
  string can be read) the engine loses the maintenance sweep, mid-step cancellation and the failed-attempt
  trace. It says so once, at startup, and keeps working otherwise.
- **`Priority` is claim-order weight, not preemption.** A high-priority instance does not interrupt work in
  progress.

---

## 6. Which test protects which guarantee

Every guarantee above is pinned by a test in `tests/LiteFlow.Tests`. They run against a real PostgreSQL
container (Testcontainers, `postgres:18-alpine`) and count **committed** effects — each step writes a row to
`public.step_executions` through its own connection, so an interrupted or fenced attempt leaves nothing to
count. That is what lets the assertions distinguish "ran twice" from "took effect twice".

| Guarantee | Test |
| --- | --- |
| Resume at the same step, no partial writes (3) | `ResumeAfterCrashTests.An_interrupted_step_leaves_no_trace_and_replays_on_another_worker` |
| State survives a crash intact (6) | `ResumeAfterCrashTests.The_state_a_step_committed_is_what_the_next_step_reads_after_a_restart` |
| A lost lease commits nothing (5) | `FencingTests.A_worker_that_lost_its_lease_commits_nothing` |
| One execution at a time across a fleet (1, 2) | `ConcurrencyTests.Three_workers_run_each_step_of_each_instance_exactly_once` |
| Steps of one instance never overlap (1) | `ConcurrencyTests.The_steps_of_one_instance_never_overlap` |
| Redelivery is dropped (4) | `RedeliveryTests.A_step_message_delivered_again_after_it_was_applied_is_dropped` |
| Non-transactional redelivery does not advance twice (4) | `RedeliveryTests.A_non_transactional_step_redelivered_after_the_cursor_moved_does_not_advance_it_twice` |
| A message for a vanished instance is dropped | `RedeliveryTests.A_step_message_for_a_workflow_that_no_longer_exists_is_dropped` |
| Cancellation before anything ran (8) | `CancellationTests.Cancelling_before_the_first_step_runs_executes_nothing_at_all` |
| Cancellation between steps (8) | `CancellationTests.Cancelling_between_two_steps_stops_the_sequence_where_it_is` |
| Cancellation mid-step discards its work (8) | `CancellationTests.Cancelling_during_a_long_step_interrupts_it_and_discards_its_work` |
| Cancelling a finished workflow is a no-op | `CancellationTests.Cancelling_a_finished_workflow_changes_nothing` |
| Suspend / timer | `SignalTests.A_suspended_step_continues_after_its_delay` |
| Signal delivery and payload | `SignalTests.A_waiting_workflow_resumes_with_the_signal_payload` |
| A duplicate signal wakes it once | `SignalTests.The_same_signal_delivered_twice_wakes_the_workflow_once` |
| An early signal is not lost | `SignalTests.A_signal_that_arrives_before_the_wait_is_not_lost` |
| A wait that times out fails the instance | `SignalTests.A_wait_that_times_out_fails_the_instance` |
| Attempts exhausted → failed + reverse rollback (9) | `FailureTests.A_step_that_keeps_throwing_fails_the_workflow_and_rolls_it_back_in_reverse_order` |
| `Fail` does not burn attempts | `FailureTests.A_step_that_refuses_fails_immediately_without_burning_its_attempts` |
| An interrupted rollback resumes (9) | `FailureTests.An_interrupted_rollback_resumes_where_it_stopped` |
| Attempts exhausted with **no worker left to report it** (10) | `DeadLetterTests.A_step_that_throws_until_its_attempts_run_out_fails_the_workflow_even_when_no_worker_reports_it` |
| A poison step fails immediately and stays failed (10) | `DeadLetterTests.A_step_that_declares_itself_poison_fails_the_workflow_immediately` |
| A dead-lettered step is never re-dispatched (10) | `DeadLetterTests.The_sweep_never_re_dispatches_a_step_whose_message_was_dead_lettered` |
| A genuinely lost message still is re-dispatched (12) | `DeadLetterTests.The_sweep_re_dispatches_a_step_whose_message_was_genuinely_lost` |
| A dead-lettered compensation parks rather than loops | `DeadLetterTests.A_dead_lettered_compensation_parks_the_workflow_instead_of_looping` |
| A failed workflow can be resumed | `FailureTests.A_failed_workflow_can_be_resumed_at_the_step_that_failed` |
| Definition drift parks, resume re-anchors (11) | `DefinitionDriftTests.An_instance_whose_step_moved_is_parked_and_can_be_re_anchored_by_name` |
| Two definitions cannot share a name | `DefinitionDriftTests.Two_workflows_cannot_share_a_name` |
| Duplicate step names are refused at startup | `DefinitionDriftTests.A_definition_with_two_steps_of_the_same_name_is_refused_at_startup` |
| Idempotent start (7) | `StartTests.Starting_twice_with_the_same_key_returns_the_first_instance` |
| Start enrolled in the caller's transaction (7) | `StartTests.A_workflow_started_in_a_rolled_back_transaction_never_existed`, `…in_a_committed_transaction_exists_with_its_business_data` |
| A wait-first definition is parked, not dispatched | `StartTests.A_workflow_that_opens_on_a_wait_is_parked_without_running_anything` |
| An unknown definition is refused | `StartTests.An_unknown_definition_is_refused_rather_than_stalled` |
| **All of the above, under a fleet being killed** | `ChaosTests.Under_random_worker_restarts_every_step_still_runs_exactly_once` |

The chaos test is the acceptance test: 200 instances × 4 steps through a fleet of three workers that are
repeatedly stopped gracefully *and* hard-killed (the DI container disposed under the running steps, so their
connections die mid-transaction and nothing is handed back — recovery has to come from the lease alone). Its
final assertions are not timing-dependent: every instance `Completed`, exactly one committed execution per
`(instance, step)`, and nothing skipped. Remove the fenced acknowledge, or move the cursor advance out of the
step's transaction, and it fails with duplicate executions.

---

## 7. Operating it

- **`StepLease`** is the delay between a crash and the resume. Size it comfortably above your slowest step:
  renewal keeps a longer step alive, but a lease shorter than a step means every takeover re-runs work.
- **`MaintenanceInterval`** (default 15 s) bounds how fast due timers, expired waits and parked cancellations
  are noticed.
- **`OrphanGracePeriod`** (default 5 min) delays only the re-dispatch of instances with *nothing* in flight.
  An instance whose step is running, or whose message is queued, is not a candidate at all, so this no longer has
  to be tuned against your slowest step; it is a margin, not a correctness knob.
- **Supervision**: `GetStatsAsync` per definition, or
  `SELECT state, count(*) FROM liteflow.workflows GROUP BY state`. Two numbers matter: `NeedsAttention` should
  always be zero, and `OldestLiveAge` is the engine's real latency signal.
- **Metrics** (`Meter` `LiteFlow`): `liteflow.workflows.{started,completed,failed,cancelled,needs_attention}`,
  `liteflow.steps.{executed,failed,resumed,stale,redispatched,compensated}` and
  `liteflow.step.duration`. `liteflow.steps.resumed` is the one to graph — it counts how often the
  crash-recovery path actually fires, and nothing else in your stack will tell you that.
- **Traces** (`ActivitySource` `LiteFlow`): one activity per step attempt, tagged with the definition,
  instance, step index and attempt.
- **Retention** is automatic: terminal instances are archived after `InstanceRetention` (7 d) with their step
  trace as a snapshot, and archive rows are dropped after `ArchiveRetention` (90 d).
- **Storage tuning** is applied on initialization (`fillfactor`, aggressive per-table autovacuum). The instance
  table is updated once per step, so it produces dead tuples at the rate work flows through; stock autovacuum
  settings let them pile up and dispatch latency climbs with the bloat. If the workflow tables are in your own
  migrations, `WorkflowSchema.TuningScript` is not generated by EF — add it by hand.

---

## 8. Roadmap

- **Branching and parallel steps.** The cursor becomes a set of active branches, with a join. The reason it is
  not in v1 is that leases, guards and compensation ordering all have to be re-derived per branch.
- **Per-step retry policies.** Backoff is currently per queue (LiteQueue's model), so `StepRetry` is set once
  for the engine. Per-step policies need the queue to accept a backoff at fail time.
- **Intra-step checkpoints.** For steps that are long and internally divisible, a `ctx.CheckpointAsync(state)`
  committed outside the step's transaction so a re-run can skip what it already did.
- **A supervision endpoint.** The query API is there; a minimal ASP.NET dashboard over it is a small addition.
- **A LiteStream bridge**: publish workflow lifecycle events so other services can react without polling.

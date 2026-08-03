using Microsoft.EntityFrameworkCore;

namespace LiteFlow.Model;

/// <summary>
/// Adds the workflow tables to <i>your</i> <c>DbContext</c> model, so they are created and versioned by
/// your own EF migrations instead of by <see cref="LiteFlowOptions.AutoCreateSchema"/>. The production
/// path: schema changes go through the same review and deployment as the rest of your database.
/// </summary>
public static class LiteFlowModelExtensions
{
    /// <summary>
    /// Map the workflow tables into this model. Call from <c>OnModelCreating</c> and turn
    /// <see cref="LiteFlowOptions.AutoCreateSchema"/> off:
    /// <code>
    /// protected override void OnModelCreating(ModelBuilder modelBuilder)
    /// {
    ///     base.OnModelCreating(modelBuilder);
    ///     modelBuilder.AddLiteFlowModel();
    ///     modelBuilder.AddLiteQueueModel();   // the step queues live in LiteQueue's schema
    /// }
    /// </code>
    /// The generated migration will not carry the storage tuning from
    /// <see cref="WorkflowSchema.TuningScript"/> (fillfactor, per-table autovacuum) — EF has no model
    /// concept for those. Add them to the migration by hand with <c>migrationBuilder.Sql(…)</c>; on a busy
    /// engine they matter, because the instance table is updated once per step.
    /// </summary>
    public static ModelBuilder AddLiteFlowModel(
        this ModelBuilder modelBuilder, string schema = WorkflowSchema.DefaultSchema)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);

        modelBuilder.Entity<WorkflowInstanceEntity>(e =>
        {
            e.ToTable("workflows", schema);
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Definition).HasColumnName("definition").IsRequired();
            e.Property(x => x.Signature).HasColumnName("signature").IsRequired();
            e.Property(x => x.State).HasColumnName("state");
            e.Property(x => x.CurrentStep).HasColumnName("current_step");
            e.Property(x => x.CurrentStepName).HasColumnName("current_step_name").IsRequired();
            e.Property(x => x.StepCount).HasColumnName("step_count");
            e.Property(x => x.CompensationIndex).HasColumnName("compensation_index");
            e.Property(x => x.Input).HasColumnName("input").HasColumnType("jsonb").IsRequired();
            e.Property(x => x.Context).HasColumnName("context").HasColumnType("jsonb");
            e.Property(x => x.IdempotencyKey).HasColumnName("idempotency_key");
            e.Property(x => x.CorrelationId).HasColumnName("correlation_id");
            e.Property(x => x.Priority).HasColumnName("priority");
            e.Property(x => x.CancelRequested).HasColumnName("cancel_requested");
            e.Property(x => x.CancelReason).HasColumnName("cancel_reason");
            e.Property(x => x.ResumeAt).HasColumnName("resume_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.WaitSignal).HasColumnName("wait_signal");
            e.Property(x => x.WaitExpiresAt).HasColumnName("wait_expires_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.RedispatchCount).HasColumnName("redispatch_count");
            e.Property(x => x.Error).HasColumnName("error");
            e.Property(x => x.WorkerId).HasColumnName("worker_id");
            e.Property(x => x.CreatedAt).HasColumnName("created_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.UpdatedAt).HasColumnName("updated_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.CompletedAt).HasColumnName("completed_at").HasColumnType("timestamp with time zone");

            e.HasIndex(x => new { x.Definition, x.IdempotencyKey })
                .IsUnique()
                .HasFilter("idempotency_key IS NOT NULL")
                .HasDatabaseName("ux_workflows_idempotency");

            e.HasIndex(x => new { x.State, x.UpdatedAt })
                .HasFilter("state < 4")
                .HasDatabaseName("ix_workflows_live");

            e.HasIndex(x => x.ResumeAt).HasFilter("state = 1").HasDatabaseName("ix_workflows_resume");

            e.HasIndex(x => x.WaitExpiresAt)
                .HasFilter("state = 2 AND wait_expires_at IS NOT NULL")
                .HasDatabaseName("ix_workflows_wait");

            e.HasIndex(x => new { x.Definition, x.CreatedAt }).HasDatabaseName("ix_workflows_definition");

            e.HasIndex(x => x.CompletedAt).HasFilter("state >= 4").HasDatabaseName("ix_workflows_terminal");
        });

        modelBuilder.Entity<WorkflowStepEntity>(e =>
        {
            e.ToTable("workflow_steps", schema);
            e.HasKey(x => new { x.WorkflowId, x.StepIndex });
            e.Property(x => x.WorkflowId).HasColumnName("workflow_id");
            e.Property(x => x.StepIndex).HasColumnName("step_index");
            e.Property(x => x.StepName).HasColumnName("step_name").IsRequired();
            e.Property(x => x.State).HasColumnName("state");
            e.Property(x => x.Attempts).HasColumnName("attempts");
            e.Property(x => x.StartedAt).HasColumnName("started_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.CompletedAt).HasColumnName("completed_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.DurationMs).HasColumnName("duration_ms");
            e.Property(x => x.Output).HasColumnName("output").HasColumnType("jsonb");
            e.Property(x => x.Error).HasColumnName("error");
            e.Property(x => x.WorkerId).HasColumnName("worker_id");
        });

        modelBuilder.Entity<WorkflowStepAttemptEntity>(e =>
        {
            e.ToTable("workflow_step_attempts", schema);
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            e.Property(x => x.WorkflowId).HasColumnName("workflow_id");
            e.Property(x => x.StepIndex).HasColumnName("step_index");
            e.Property(x => x.StepName).HasColumnName("step_name").IsRequired();
            e.Property(x => x.Attempt).HasColumnName("attempt");
            e.Property(x => x.FailedAt).HasColumnName("failed_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.WorkerId).HasColumnName("worker_id");
            e.Property(x => x.Error).HasColumnName("error");
            e.HasIndex(x => new { x.WorkflowId, x.StepIndex }).HasDatabaseName("ix_step_attempts_workflow");
        });

        modelBuilder.Entity<WorkflowSignalEntity>(e =>
        {
            e.ToTable("workflow_signals", schema);
            e.HasKey(x => new { x.WorkflowId, x.Name });
            e.Property(x => x.WorkflowId).HasColumnName("workflow_id");
            e.Property(x => x.Name).HasColumnName("name");
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("jsonb");
            e.Property(x => x.ReceivedAt).HasColumnName("received_at").HasColumnType("timestamp with time zone");
        });

        modelBuilder.Entity<WorkflowArchiveEntity>(e =>
        {
            e.ToTable("workflow_archive", schema);
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Definition).HasColumnName("definition").IsRequired();
            e.Property(x => x.State).HasColumnName("state");
            e.Property(x => x.Error).HasColumnName("error");
            e.Property(x => x.CreatedAt).HasColumnName("created_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.CompletedAt).HasColumnName("completed_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.ArchivedAt).HasColumnName("archived_at").HasColumnType("timestamp with time zone");
            e.Property(x => x.Snapshot).HasColumnName("snapshot").HasColumnType("jsonb").IsRequired();
            e.HasIndex(x => x.ArchivedAt).HasDatabaseName("ix_workflow_archive_at");
        });

        return modelBuilder;
    }
}

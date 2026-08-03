using LiteFlow.Internal;
using LiteQueue;
using LiteQueue.Connectors;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace LiteFlow;

/// <summary>DI wiring for LiteFlow.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Register LiteFlow on top of an existing EF Core context — the default, and the arrangement the
    /// durability guarantee is built on. Steps then run on <typeparamref name="TContext"/>'s connection
    /// and inside its transaction, so a step's writes, the cursor advance and the dispatch of the next
    /// step commit as one.
    /// <para>
    /// <typeparamref name="TContext"/> must be registered (scoped, as <c>AddDbContext</c> does) and must
    /// use the Npgsql provider. Its model does not need to know about the workflow tables; add
    /// <c>modelBuilder.AddLiteFlowModel()</c> only if you want them in your migrations.
    /// </para>
    /// <para>
    /// LiteFlow registers and configures LiteQueue itself — the step queues are an implementation detail
    /// it owns — so do not call <c>AddLiteQueue</c> as well.
    /// </para>
    /// </summary>
    public static IServiceCollection AddLiteFlow<TContext>(
        this IServiceCollection services,
        Action<LiteFlowOptions>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = BuildOptions(configure);

        services.AddSingleton(new WorkflowDbContextAccessor(sp => sp.GetService<TContext>()));

        services.AddLiteQueue<TContext>(q => ApplyQueueOptions(q, options));

        // The side channel borrows the context's connection string: the sweep and the out-of-band
        // diagnostics need a connection of their own, and the caller has already told us where the
        // database is once.
        return AddCore(services, options, sp =>
        {
            if (options.ConnectionString is not null)
                return options.ConnectionString;

            using var scope = sp.CreateScope();
            return scope.ServiceProvider.GetRequiredService<TContext>().Database.GetConnectionString();
        });
    }

    /// <summary>
    /// Register LiteFlow with its own connection pool, for callers with no EF Core context in the picture.
    /// Steps still get a connection and a transaction through
    /// <see cref="IWorkflowStepContext{TState}.Connection"/>, so the all-or-nothing guarantee holds for
    /// anything they write through it — they just do not get a <c>DbContext</c>.
    /// </summary>
    public static IServiceCollection AddLiteFlow(
        this IServiceCollection services,
        string connectionString,
        Action<LiteFlowOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var options = BuildOptions(configure);
        options.ConnectionString ??= connectionString;

        services.TryAddSingleton(WorkflowDbContextAccessor.None);

        services.AddLiteQueue(connectionString, q => ApplyQueueOptions(q, options));

        return AddCore(services, options, _ => options.ConnectionString);
    }

    /// <summary>
    /// Run a workflow definition in this process: compile it, validate it, and start the workers that
    /// execute its steps.
    /// <para>
    /// Register the same workflow in as many processes as you like — that is the point. Each step of each
    /// instance is claimed by exactly one of them, and an instance can move from one host to another
    /// between steps (or after a crash) without noticing.
    /// </para>
    /// <para>
    /// A definition that declares <see cref="IStepOptions{TState}.NonTransactional"/> steps gets a second
    /// worker on its own queue for them, so a step waiting on someone else's API cannot occupy the
    /// capacity the database-only steps need.
    /// </para>
    /// </summary>
    /// <typeparam name="TWorkflow">
    /// The definition. Needs a public parameterless constructor: it is instantiated once at startup to be
    /// compiled, never to run steps (the steps themselves come from DI, per attempt).
    /// </typeparam>
    public static IServiceCollection AddLiteFlowWorkflow<TWorkflow>(
        this IServiceCollection services,
        Action<WorkflowWorkerOptions>? configure = null)
        where TWorkflow : Workflow, new()
    {
        ArgumentNullException.ThrowIfNull(services);

        var catalog = Resolve<WorkflowCatalog>(services)
                      ?? throw new InvalidOperationException(
                          "Call AddLiteFlow(...) before AddLiteFlowWorkflow<T>().");

        // Compiled at startup on purpose: a duplicate step name or an empty sequence fails the host here,
        // not on the first instance that reaches the mistake in production.
        var definition = catalog.Register(new TWorkflow());

        var workerOptions = new WorkflowWorkerOptions();
        configure?.Invoke(workerOptions);

        foreach (var step in definition.Steps)
        {
            if (step.StepType is not null)
                services.TryAddScoped(step.StepType);
            if (step.CompensationType is not null)
                services.TryAddScoped(step.CompensationType);
        }

        services.AddLiteQueueWorker<WorkflowStepHandler<TWorkflow>>(configure: q =>
        {
            q.Concurrency = Math.Max(1, workerOptions.Concurrency);
            q.Lease = workerOptions.Lease ?? catalog.Options.StepLease;
            q.RenewLease = workerOptions.RenewLease;
            q.WorkerId = workerOptions.WorkerId;
            // A step is a unit of work with its own transaction: prefetching would only make a second
            // step hold a lease while waiting behind the first.
            q.PrefetchBatchSize = Math.Max(1, workerOptions.Concurrency);
            q.TransactionalCompletion = true;
        });

        if (definition.NonTransactionalQueue is not null)
        {
            services.AddLiteQueueWorker<WorkflowExternalStepHandler<TWorkflow>>(configure: q =>
            {
                q.Concurrency = Math.Max(1, workerOptions.ExternalConcurrency ?? workerOptions.Concurrency);
                q.Lease = workerOptions.Lease ?? catalog.Options.StepLease;
                q.RenewLease = workerOptions.RenewLease;
                q.WorkerId = workerOptions.WorkerId;
                q.PrefetchBatchSize =
                    Math.Max(1, workerOptions.ExternalConcurrency ?? workerOptions.Concurrency);
                // No transaction is held across a call to someone else's system.
                q.TransactionalCompletion = false;
            });
        }

        if (catalog.Options.AutoMaintenance)
            services.AddLiteFlowMaintenance();

        return services;
    }

    /// <summary>
    /// Run the maintenance loop: due timers, expired waits, lost steps and retention. Idempotent, and
    /// registered automatically by <see cref="AddLiteFlowWorkflow{TWorkflow}"/> unless
    /// <see cref="LiteFlowOptions.AutoMaintenance"/> is turned off. Safe to run on every instance of your
    /// service — every sweep it performs is idempotent, so no leader election is needed.
    /// </summary>
    public static IServiceCollection AddLiteFlowMaintenance(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (services.Any(d => d.ServiceType == typeof(MaintenanceMarker)))
            return services;

        services.AddSingleton<MaintenanceMarker>();
        services.AddSingleton<IHostedService, WorkflowMaintenanceService>();
        return services;
    }

    private static LiteFlowOptions BuildOptions(Action<LiteFlowOptions>? configure)
    {
        var options = new LiteFlowOptions();
        configure?.Invoke(options);
        return options;
    }

    private static void ApplyQueueOptions(QueueOptions queue, LiteFlowOptions options)
    {
        queue.Schema = options.QueueSchema;
        queue.AutoCreateSchema = options.AutoCreateSchema;
        queue.ApplyStorageTuning = options.ApplyStorageTuning;
        queue.LeaseDuration = options.StepLease;
        queue.MaxAttempts = options.MaxStepAttempts;
        queue.Retry = options.StepRetry;
        queue.MaintenanceInterval = options.MaintenanceInterval;
        queue.AutoMaintenance = options.AutoMaintenance;
        // A completed step message has already been recorded in workflow_steps, which is the audit trail
        // that matters — keeping a second copy in the queue's archive would only add churn to the table
        // whose size determines dispatch latency.
        queue.Completion = CompletionMode.Delete;
        queue.EnableNotifications = options.EnableNotifications;
        queue.ListenerConnectionString = options.ConnectionString;
    }

    private static IServiceCollection AddCore(
        IServiceCollection services,
        LiteFlowOptions options,
        Func<IServiceProvider, string?> connectionString)
    {
        services.AddSingleton(options);

        var catalog = Resolve<WorkflowCatalog>(services);
        if (catalog is null)
        {
            catalog = new WorkflowCatalog(options);
            services.AddSingleton(catalog);
        }

        services.TryAddSingleton<IWorkflowStateSerializer>(new JsonWorkflowStateSerializer());

        // Resolved lazily: the connection string may have to be read from a DbContext, which cannot be
        // built while the container is still being configured.
        services.TryAddSingleton(sp => new WorkflowSideChannel(() => connectionString(sp)));

        services.TryAddSingleton<WorkflowCancellationRegistry>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<WorkflowCancellationRegistry>());

        services.TryAddScoped<ILiteFlowClient, LiteFlowClient>();

        return services;
    }

    private static T? Resolve<T>(IServiceCollection services) where T : class =>
        services.FirstOrDefault(d => d.ServiceType == typeof(T))?.ImplementationInstance as T;

    private sealed class MaintenanceMarker;
}

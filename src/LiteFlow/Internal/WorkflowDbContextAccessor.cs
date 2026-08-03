using Microsoft.EntityFrameworkCore;

namespace LiteFlow.Internal;

/// <summary>
/// Finds the <see cref="DbContext"/> whose connection the engine is borrowing, so a step can be handed
/// it directly (<see cref="IWorkflowStepContext{TState}.DbContext"/>) instead of having to know which of
/// the application's contexts happens to be the right one.
/// <para>
/// Captured at registration time by <c>AddLiteFlow&lt;TContext&gt;</c>; resolves to <c>null</c> when
/// LiteFlow runs on its own pool, where there is no context to share.
/// </para>
/// </summary>
internal sealed class WorkflowDbContextAccessor(Func<IServiceProvider, DbContext?> resolve)
{
    public static WorkflowDbContextAccessor None { get; } = new(_ => null);

    public DbContext? Resolve(IServiceProvider services) => resolve(services);
}

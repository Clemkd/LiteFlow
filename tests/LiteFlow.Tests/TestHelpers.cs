using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>Ids nothing else in the run uses, so every assertion counts only its own rows.</summary>
internal static class TestIds
{
    private static long _next;

    public static long NextBusiness() => 1_000_000 + Interlocked.Increment(ref _next) * 100;
}

internal static class TestHelpers
{
    /// <summary>
    /// Run an action with a client from a fresh DI scope — hence a fresh connection. Anything that runs
    /// while workers are working needs its own scope: one connection cannot serve two callers at once.
    /// </summary>
    public static async Task<T> WithClientAsync<T>(IServiceProvider services, Func<ILiteFlowClient, Task<T>> action)
    {
        await using var scope = services.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredService<ILiteFlowClient>());
    }

    public static async Task WithClientAsync(IServiceProvider services, Func<ILiteFlowClient, Task> action)
    {
        await using var scope = services.CreateAsyncScope();
        await action(scope.ServiceProvider.GetRequiredService<ILiteFlowClient>());
    }

    /// <summary>Start every hosted service of a provider — the workers, the sweep, the cancellation poll.</summary>
    public static async Task<List<IHostedService>> StartWorkersAsync(IServiceProvider services)
    {
        var hosted = services.GetServices<IHostedService>().ToList();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);
        return hosted;
    }

    /// <summary>
    /// Stop the workers the way a host being shut down would: the stopping token is cancelled, so a step
    /// in flight sees its <see cref="CancellationToken"/> cancelled, its transaction is rolled back and
    /// its message goes back to the queue without spending an attempt. That is the interruption the
    /// crash-recovery tests exercise.
    /// </summary>
    public static async Task StopWorkersAsync(IEnumerable<IHostedService> hosted)
    {
        foreach (var service in hosted)
        {
            try
            {
                await service.StopAsync(CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                // A worker interrupted mid-step surfaces the cancellation it was told to observe.
            }
        }
    }

    public static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(25);
        }
        return condition();
    }

    public static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return true;
            await Task.Delay(50);
        }
        return await condition();
    }

    /// <summary>Wait for an instance to reach one of <paramref name="states"/>, and return it.</summary>
    public static async Task<WorkflowInstance> WaitForStateAsync(
        IServiceProvider services, Guid workflowId, TimeSpan timeout, params WorkflowState[] states)
    {
        WorkflowInstance? instance = null;

        await WaitUntilAsync(async () =>
        {
            instance = await WithClientAsync(services, c => c.GetAsync(workflowId));
            return instance is not null && Array.IndexOf(states, instance.State) >= 0;
        }, timeout);

        Assert.NotNull(instance);
        Assert.Contains(instance.State, states);
        return instance;
    }

    /// <summary>Wait for an instance to finish, whatever the verdict.</summary>
    public static Task<WorkflowInstance> WaitForTerminalAsync(
        IServiceProvider services, Guid workflowId, TimeSpan timeout) =>
        WaitForStateAsync(services, workflowId, timeout,
            WorkflowState.Completed, WorkflowState.Failed, WorkflowState.Cancelled, WorkflowState.NeedsAttention);
}

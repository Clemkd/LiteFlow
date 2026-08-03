using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LiteFlow.Tests;

/// <summary>
/// Starting an instance: the guarantees a caller gets before any step has run.
/// </summary>
[Collection("postgres")]
public sealed class StartTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Starting_twice_with_the_same_key_returns_the_first_instance()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("start-idem");
        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());

        var options = new WorkflowStartOptions { IdempotencyKey = "order-4711" };

        var first = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState { Tag = "a" }, options));
        var second = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<LinearWorkflow, TestState>(new TestState { Tag = "b" }, options));

        Assert.False(first.AlreadyExisted);
        Assert.True(second.AlreadyExisted);
        Assert.Equal(first.WorkflowId, second.WorkflowId);

        var instances = await TestHelpers.WithClientAsync(sp,
            c => c.ListAsync(new WorkflowQuery { Definition = LinearWorkflow.DefinitionName }));
        Assert.Single(instances);
    }

    [Fact]
    public async Task A_workflow_started_in_a_rolled_back_transaction_never_existed()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("start-tx");
        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());

        Guid id;
        await using (var scope = sp.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<ILiteFlowClient>();
            await client.InitializeAsync();

            // The caller owns the transaction: the engine joins it instead of opening one of its own.
            await using var transaction = await db.Database.BeginTransactionAsync();
            var handle = await client.StartAsync<LinearWorkflow, TestState>(new TestState());
            id = handle.WorkflowId;

            Assert.NotNull(await client.GetAsync(id));
            await transaction.RollbackAsync();
        }

        Assert.Null(await TestHelpers.WithClientAsync(sp, c => c.GetAsync(id)));
    }

    [Fact]
    public async Task A_workflow_started_in_a_committed_transaction_exists_with_its_business_data()
    {
        LinearWorkflow.DefinitionName = PostgresFixture.NewName("start-tx-commit");
        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<LinearWorkflow>());
        long businessId = TestIds.NextBusiness();

        Guid id;
        await using (var scope = sp.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<ILiteFlowClient>();
            await client.InitializeAsync();

            await using var transaction = await db.Database.BeginTransactionAsync();
            db.Records.Add(new BusinessRecord { Id = businessId, Value = "with the workflow" });
            await db.SaveChangesAsync();
            id = (await client.StartAsync<LinearWorkflow, TestState>(new TestState())).WorkflowId;
            await transaction.CommitAsync();
        }

        Assert.NotNull(await TestHelpers.WithClientAsync(sp, c => c.GetAsync(id)));
        Assert.Equal(1, await TestDb.BusinessAsync(fixture.ConnectionString, businessId));
    }

    [Fact]
    public async Task A_workflow_that_opens_on_a_wait_is_parked_without_running_anything()
    {
        WaitFirstWorkflow.DefinitionName = PostgresFixture.NewName("wait-first");
        await using var sp = fixture.BuildProvider(s => s.AddLiteFlowWorkflow<WaitFirstWorkflow>());

        var handle = await TestHelpers.WithClientAsync(sp,
            c => c.StartAsync<WaitFirstWorkflow, TestState>(new TestState()));

        var instance = await TestHelpers.WithClientAsync(sp, c => c.GetAsync(handle.WorkflowId));
        Assert.NotNull(instance);
        Assert.Equal(WorkflowState.WaitingSignal, instance.State);
        Assert.Equal("start", instance.WaitSignal);
        Assert.Equal(0, await TestDb.ExecutionsAsync(fixture.ConnectionString, handle.WorkflowId));
    }

    [Fact]
    public async Task An_unknown_definition_is_refused_rather_than_stalled()
    {
        await using var sp = fixture.BuildProvider(_ => { });

        await Assert.ThrowsAsync<WorkflowNotRegisteredException>(() =>
            TestHelpers.WithClientAsync(sp, c => c.StartAsync("nothing-registered", new TestState())));
    }
}

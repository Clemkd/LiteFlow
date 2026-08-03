using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace LiteFlow.SampleConsole;

/// <summary>The state that travels through the order workflow, persisted after every step.</summary>
public sealed class OrderState
{
    public string OrderId { get; set; } = string.Empty;

    public decimal Amount { get; set; }

    public bool StockReserved { get; set; }

    public string? PaymentReference { get; set; }

    public string? TrackingNumber { get; set; }

    /// <summary>Set to <c>true</c> by <c>--fast</c> so the seeded instances do not each take ten seconds.</summary>
    public bool Fast { get; set; }
}

/// <summary>
/// Five steps that between them show every property worth demonstrating:
/// <list type="bullet">
/// <item><c>reserve-stock</c> and <c>charge-card</c> write business rows through the engine's transaction, so they commit with the cursor.</item>
/// <item><c>charge-card</c> is undoable, so a later failure or a cancellation refunds it.</item>
/// <item><c>pack-parcel</c> is slow — the step the <c>crash</c> command kills the process during.</item>
/// <item><c>wait:shipped</c> parks the instance until an outside system signals it, at no cost.</item>
/// <item><c>send-receipt</c> runs outside the transaction, the way a call to someone else's API has to.</item>
/// </list>
/// </summary>
public sealed class OrderWorkflow : Workflow<OrderState>
{
    public override string Name => "orders";

    protected override void Configure(IWorkflowBuilder<OrderState> b) => b
        .Step<ReserveStock>()
        .Step<ChargeCard>(s => s.Named("charge-card").MaxAttempts(3))
        .Step<PackParcel>(s => s.Named("pack-parcel"))
        .WaitForSignal("shipped", (ctx, signal, ct) =>
        {
            ctx.State.TrackingNumber = signal.PayloadAs<string>() ?? "unknown";
            return Task.CompletedTask;
        }, timeout: TimeSpan.FromDays(2))
        .Step<SendReceipt>(s => s.Named("send-receipt").NonTransactional());
}

/// <summary>Writes the order row. Idempotent through the engine's transaction: a retry sees a clean slate.</summary>
public sealed class ReserveStock(ILogger<ReserveStock> logger) : IWorkflowStep<OrderState>
{
    public async Task<StepResult> ExecuteAsync(
        IWorkflowStepContext<OrderState> ctx, CancellationToken cancellationToken = default)
    {
        var db = ctx.DbContext as OrderDbContext
                 ?? throw new InvalidOperationException("The sample expects the EF connector.");

        var order = await db.Orders.FirstOrDefaultAsync(o => o.Id == ctx.State.OrderId, cancellationToken);
        if (order is null)
        {
            db.Orders.Add(new DemoOrder
            {
                Id = ctx.State.OrderId,
                Status = "reserved",
                Amount = ctx.State.Amount,
            });
            await db.SaveChangesAsync(cancellationToken);
        }

        ctx.State.StockReserved = true;
        logger.LogInformation("Reserved stock for order {OrderId}.", ctx.State.OrderId);
        return StepResult.Next(new { reserved = true });
    }
}

/// <summary>
/// Charges the card, and knows how to refund it. The compensation is what turns a failure further down the
/// sequence into a rollback rather than an inconsistency.
/// </summary>
public sealed class ChargeCard(ILogger<ChargeCard> logger) : ICompensatingWorkflowStep<OrderState>
{
    public async Task<StepResult> ExecuteAsync(
        IWorkflowStepContext<OrderState> ctx, CancellationToken cancellationToken = default)
    {
        // The key an external payment provider would deduplicate on: identical across every attempt of
        // this step of this instance, and different for every other.
        ctx.State.PaymentReference = $"pay-{ctx.IdempotencyKey}";

        var db = (OrderDbContext)ctx.DbContext!;
        var order = await db.Orders.FirstAsync(o => o.Id == ctx.State.OrderId, cancellationToken);
        order.Status = "charged";
        order.Charged = true;
        await db.SaveChangesAsync(cancellationToken);

        logger.LogInformation(
            "Charged {Amount:0.00} for order {OrderId} (reference {Reference}).",
            ctx.State.Amount, ctx.State.OrderId, ctx.State.PaymentReference);
        return StepResult.Next();
    }

    public async Task CompensateAsync(
        IWorkflowStepContext<OrderState> ctx, CancellationToken cancellationToken = default)
    {
        var db = (OrderDbContext)ctx.DbContext!;
        var order = await db.Orders.FirstOrDefaultAsync(o => o.Id == ctx.State.OrderId, cancellationToken);
        if (order is not null)
        {
            order.Charged = false;
            order.Status = "refunded";
            await db.SaveChangesAsync(cancellationToken);
        }

        logger.LogWarning("Refunded order {OrderId} ({Reference}).", ctx.State.OrderId, ctx.State.PaymentReference);
    }
}

/// <summary>
/// The slow step. Long enough to kill the process in the middle of it, which is the whole point of the
/// <c>crash</c> command: on the next run the workflow resumes here, from the top, on a clean database.
/// </summary>
public sealed class PackParcel(ILogger<PackParcel> logger) : IWorkflowStep<OrderState>
{
    public async Task<StepResult> ExecuteAsync(
        IWorkflowStepContext<OrderState> ctx, CancellationToken cancellationToken = default)
    {
        int seconds = ctx.State.Fast ? 1 : 10;
        logger.LogInformation(
            "Packing order {OrderId} — this takes {Seconds} s (attempt {Attempt}).",
            ctx.State.OrderId, seconds, ctx.Attempt);

        for (int i = 0; i < seconds; i++)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            Crash.MaybeNow(ctx, i + 1);
        }

        var db = (OrderDbContext)ctx.DbContext!;
        var order = await db.Orders.FirstAsync(o => o.Id == ctx.State.OrderId, cancellationToken);
        order.Status = "packed";
        await db.SaveChangesAsync(cancellationToken);

        logger.LogInformation("Packed order {OrderId}.", ctx.State.OrderId);
        return StepResult.Next();
    }
}

/// <summary>
/// Runs outside the engine's transaction, because sending a receipt is not this database's business. The
/// engine can only promise it happens at least once, which is why it is keyed by
/// <see cref="IWorkflowStepContext{TState}.IdempotencyKey"/>.
/// </summary>
public sealed class SendReceipt(ILogger<SendReceipt> logger) : IWorkflowStep<OrderState>
{
    public Task<StepResult> ExecuteAsync(
        IWorkflowStepContext<OrderState> ctx, CancellationToken cancellationToken = default)
    {
        logger.LogInformation(
            "Sent the receipt for order {OrderId} (idempotency key {Key}, tracking {Tracking}).",
            ctx.State.OrderId, ctx.IdempotencyKey, ctx.State.TrackingNumber);
        return Task.FromResult(StepResult.Complete());
    }
}

/// <summary>
/// The deliberate crash used by the <c>crash</c> command: <see cref="Environment.FailFast"/> in the middle
/// of the slow step, with no unwinding and no chance for anything to be committed or acknowledged. The next
/// run has nothing but the database to go on — which is exactly the situation the library exists for.
/// </summary>
internal static class Crash
{
    private static int _afterSeconds;

    public static void ArmAfter(int seconds) => _afterSeconds = seconds;

    public static void MaybeNow(IWorkflowStepContext<OrderState> ctx, int elapsedSeconds)
    {
        if (_afterSeconds <= 0 || elapsedSeconds < _afterSeconds)
            return;

        Console.WriteLine();
        Console.WriteLine(
            $"*** Killing the process {elapsedSeconds} s into step '{ctx.StepName}' of {ctx.WorkflowId:D}.");
        Console.WriteLine("*** Nothing this step did will survive. Re-run 'worker' to watch it resume here.");
        Console.Out.Flush();

        Environment.FailFast("LiteFlow sample: deliberate crash mid-step.");
    }
}

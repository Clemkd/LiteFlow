using Microsoft.EntityFrameworkCore;

namespace LiteFlow.SampleConsole;

/// <summary>
/// The application's own context — the connection LiteFlow borrows. A step writing through this context is
/// writing in the same transaction the engine commits its cursor in, which is what makes the sample's order
/// rows and its workflow state impossible to disagree with each other.
/// </summary>
public sealed class OrderDbContext(DbContextOptions<OrderDbContext> options) : DbContext(options)
{
    public DbSet<DemoOrder> Orders => Set<DemoOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DemoOrder>(e =>
        {
            e.ToTable("demo_orders");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.Status).HasColumnName("status").IsRequired();
            e.Property(x => x.Amount).HasColumnName("amount");
            e.Property(x => x.Charged).HasColumnName("charged");
        });

        // The workflow tables are not in this model on purpose: the sample lets
        // LiteFlowOptions.AutoCreateSchema create them. Call modelBuilder.AddLiteFlowModel() instead when you
        // want them versioned by your own migrations.
    }

    /// <summary>
    /// Create the sample's own table. Raw DDL rather than <c>EnsureCreated</c>, which refuses to do anything
    /// once the engine's schema is already there.
    /// </summary>
    public async Task EnsureDemoTableAsync(CancellationToken cancellationToken = default) =>
        await Database.ExecuteSqlRawAsync(
            """
            CREATE TABLE IF NOT EXISTS public.demo_orders (
                id       text PRIMARY KEY,
                status   text NOT NULL,
                amount   numeric(12,2) NOT NULL DEFAULT 0,
                charged  boolean NOT NULL DEFAULT false
            )
            """, cancellationToken);
}

/// <summary>An order, so the sample has business data whose fate can be compared with the workflow's.</summary>
public sealed class DemoOrder
{
    public string Id { get; set; } = string.Empty;

    public string Status { get; set; } = string.Empty;

    public decimal Amount { get; set; }

    public bool Charged { get; set; }
}

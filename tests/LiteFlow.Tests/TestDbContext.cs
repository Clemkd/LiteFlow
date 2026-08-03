using Microsoft.EntityFrameworkCore;

namespace LiteFlow.Tests;

/// <summary>Stands in for the application's own context — the connection the engine borrows.</summary>
public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    public DbSet<BusinessRecord> Records => Set<BusinessRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BusinessRecord>(e =>
        {
            e.ToTable("business_records");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.Value).HasColumnName("value").IsRequired();
        });
    }
}

/// <summary>
/// A business row written by a step, used to prove a step's writes and its bookkeeping commit together —
/// and disappear together when they must.
/// </summary>
public sealed class BusinessRecord
{
    public long Id { get; set; }

    public string Value { get; set; } = string.Empty;
}

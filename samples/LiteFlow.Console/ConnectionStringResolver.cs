using Microsoft.Extensions.Configuration;

namespace LiteFlow.SampleConsole;

internal static class ConnectionStringResolver
{
    // 127.0.0.1 rather than localhost: on a machine where localhost resolves to ::1 first, a container
    // publishing only on the IPv4 loopback refuses the first connection attempt.
    private const string Default =
        "Host=127.0.0.1;Port=5432;Database=liteflow;Username=postgres;Password=postgres";

    /// <summary>
    /// Resolution order:
    /// 1. ConnectionStrings:liteflowdb (injected by Aspire as ConnectionStrings__liteflowdb),
    /// 2. the LITEFLOW_CONNECTION environment variable,
    /// 3. a loopback default for ad-hoc runs.
    /// </summary>
    public static string Resolve(IConfiguration config) =>
        config.GetConnectionString("liteflowdb")
        ?? Environment.GetEnvironmentVariable("LITEFLOW_CONNECTION")
        ?? Default;
}

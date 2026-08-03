namespace LiteFlow.SampleConsole;

/// <summary>Tiny command-line parser: "&lt;command&gt; --key value --flag".</summary>
internal sealed class CliArgs
{
    private readonly Dictionary<string, string?> _opts = new(StringComparer.OrdinalIgnoreCase);

    public CliArgs(string[] args)
    {
        int start;
        if (args.Length == 0 || args[0].StartsWith('-'))
        {
            Command = "help";
            start = 0;
        }
        else
        {
            Command = args[0].ToLowerInvariant();
            start = 1;
        }

        for (int i = start; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--"))
                continue;
            string key = args[i][2..];
            if (i + 1 < args.Length && !args[i + 1].StartsWith("--"))
                _opts[key] = args[++i];
            else
                _opts[key] = null; // boolean flag
        }
    }

    public string Command { get; }

    public string Str(string key, string fallback) =>
        _opts.TryGetValue(key, out var v) && v is not null ? v : fallback;

    public int Int(string key, int fallback) =>
        _opts.TryGetValue(key, out var v) && int.TryParse(v, out var n) ? n : fallback;

    public bool Flag(string key) => _opts.ContainsKey(key);

    public Guid? Id(string key = "id") =>
        _opts.TryGetValue(key, out var v) && Guid.TryParse(v, out var id) ? id : null;
}

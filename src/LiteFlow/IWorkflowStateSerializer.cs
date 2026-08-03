using System.Text.Json;
using System.Text.Json.Serialization;

namespace LiteFlow;

/// <summary>
/// Turns a workflow's state into the <c>jsonb</c> the engine stores, and back. Register your own
/// implementation before <c>AddLiteFlow</c> to override the default (<c>System.Text.Json</c>).
/// <para>
/// Whatever you plug in here becomes a compatibility surface: instances that survive a deployment
/// carry state written by the previous version of the code, so the format has to stay readable.
/// </para>
/// </summary>
public interface IWorkflowStateSerializer
{
    /// <summary>Serialize a state object (or a step output) to JSON.</summary>
    string Serialize(object? value);

    /// <summary>Deserialize state stored for <typeparamref name="T"/>. A <c>null</c> or empty document yields a fresh instance when the type allows it.</summary>
    T? Deserialize<T>(string? json) where T : class;

    /// <summary>Non-generic form, used on the paths where the state type is only known as a <see cref="Type"/>.</summary>
    object? Deserialize(string? json, Type type);
}

/// <summary>
/// Default serializer. Property names are kept as declared and nulls are written: the stored document
/// is meant to be read by a human during an incident, and a round-trip must not silently drop a field
/// a later step depends on.
/// </summary>
public sealed class JsonWorkflowStateSerializer : IWorkflowStateSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>Create the serializer with the default options.</summary>
    public JsonWorkflowStateSerializer()
        : this(new JsonSerializerOptions
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
            NumberHandling = JsonNumberHandling.AllowReadingFromString,
        })
    {
    }

    /// <summary>Create the serializer with caller-supplied options.</summary>
    public JsonWorkflowStateSerializer(JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public string Serialize(object? value) =>
        value is null ? "null" : JsonSerializer.Serialize(value, value.GetType(), _options);

    /// <inheritdoc />
    public T? Deserialize<T>(string? json) where T : class =>
        string.IsNullOrWhiteSpace(json) || json == "null"
            ? Activator.CreateInstance<T>()
            : JsonSerializer.Deserialize<T>(json, _options);

    /// <inheritdoc />
    public object? Deserialize(string? json, Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return string.IsNullOrWhiteSpace(json) || json == "null"
            ? null
            : JsonSerializer.Deserialize(json, type, _options);
    }
}

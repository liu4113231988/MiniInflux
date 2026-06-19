namespace MiniInflux.Net10.Model;

public sealed class Point
{
    public required string Measurement { get; init; }
    public required Dictionary<string, string> Tags { get; init; }
    public required Dictionary<string, FieldValue> Fields { get; init; }
    public required long TimestampNs { get; init; }
}

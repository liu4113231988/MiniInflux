namespace MiniInflux.Net10.Model;

/// <summary>
/// Compact in-memory point used by the write buffer. Replaces the per-point
/// Dictionary&lt;string,string&gt; tags and Dictionary&lt;string,FieldValue&gt; fields
/// with a reference to a batch-shared <see cref="SeriesDescriptor"/> plus a
/// <see cref="Values"/> array aligned to the descriptor's field order.
/// A null entry in <see cref="Values"/> means that field is absent for this point.
/// </summary>
public readonly struct CompactPoint
{
    public long TimestampNs { get; init; }
    public int SeriesIndex { get; init; }
    public FieldValue?[] Values { get; init; }
}

/// <summary>
/// Batch-shared series description: measurement, tag keys/values, and field names/kinds.
/// Many <see cref="CompactPoint"/>s reference one descriptor, so tag/field metadata is
/// stored once per series instead of once per point.
/// </summary>
public sealed class SeriesDescriptor
{
    public string Measurement { get; }
    public string[] TagKeys { get; }
    public string[] TagValues { get; }
    public string[] FieldNames { get; }
    public FieldKind[] FieldKinds { get; }

    public SeriesDescriptor(string measurement, string[] tagKeys, string[] tagValues, string[] fieldNames, FieldKind[] fieldKinds)
    {
        Measurement = measurement;
        TagKeys = tagKeys;
        TagValues = tagValues;
        FieldNames = fieldNames;
        FieldKinds = fieldKinds;
    }
}

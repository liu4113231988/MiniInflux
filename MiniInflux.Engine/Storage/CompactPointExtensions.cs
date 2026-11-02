using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Conversion helpers between <see cref="Point"/> and the compact buffer representation
/// (<see cref="SeriesDescriptor"/> + <see cref="CompactPoint"/>). Used at the boundaries
/// where the engine still needs a full <see cref="Point"/> (schema validation, segment
/// encoding, query output, last-value cache).
/// </summary>
internal static class CompactPointExtensions
{
    public static SeriesDescriptor ToSeriesDescriptor(this Point p)
    {
        var tagKeys = new string[p.Tags.Count];
        var tagVals = new string[p.Tags.Count];
        var i = 0;
        foreach (var t in p.Tags) { tagKeys[i] = t.Key; tagVals[i] = t.Value; i++; }

        var fieldNames = new string[p.Fields.Count];
        var fieldKinds = new FieldKind[p.Fields.Count];
        i = 0;
        foreach (var f in p.Fields) { fieldNames[i] = f.Key; fieldKinds[i] = f.Value.Kind; i++; }

        return new SeriesDescriptor(p.Measurement, tagKeys, tagVals, fieldNames, fieldKinds);
    }

    public static CompactPoint ToCompactPoint(this Point p, int seriesIndex, SeriesDescriptor desc)
    {
        var values = new FieldValue?[desc.FieldNames.Length];
        for (var f = 0; f < desc.FieldNames.Length; f++)
            if (p.Fields.TryGetValue(desc.FieldNames[f], out var v))
                values[f] = v;
        return new CompactPoint { TimestampNs = p.TimestampNs, SeriesIndex = seriesIndex, Values = values };
    }

    public static Point ToPoint(this CompactPoint cp, SeriesDescriptor desc)
    {
        var tags = new Dictionary<string, string>(desc.TagKeys.Length);
        for (var t = 0; t < desc.TagKeys.Length; t++)
            tags[desc.TagKeys[t]] = desc.TagValues[t];

        var fields = new Dictionary<string, FieldValue>(desc.FieldNames.Length);
        for (var f = 0; f < desc.FieldNames.Length; f++)
            if (cp.Values[f].HasValue)
                fields[desc.FieldNames[f]] = cp.Values[f]!.Value;

        return new Point
        {
            Measurement = desc.Measurement,
            Tags = tags,
            Fields = fields,
            TimestampNs = cp.TimestampNs
        };
    }
}


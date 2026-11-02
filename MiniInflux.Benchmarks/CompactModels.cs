using MiniInflux.Net10.Model;

namespace MiniInflux.Benchmarks;

/// <summary>
/// Candidate "batch series descriptor + compact field array" representation.
/// Row-oriented: each point is a struct holding its timestamp, a series index,
/// and a flat FieldValue[] (no per-point Dictionary). Series metadata
/// (measurement + tags) is stored once per unique series in the batch.
/// </summary>
internal sealed class CompactRowBatch
{
    public List<SeriesDescriptor> Series { get; } = new();
    public List<CompactPoint> Points { get; } = new();

    public static CompactRowBatch From(List<Point> source)
    {
        var batch = new CompactRowBatch();
        var seriesIndex = new Dictionary<SeriesKey, int>();

        foreach (var p in source)
        {
            var key = SeriesKey.From(p);
            if (!seriesIndex.TryGetValue(key, out var idx))
            {
                idx = batch.Series.Count;
                seriesIndex[key] = idx;
                batch.Series.Add(new SeriesDescriptor(p.Measurement, p.Tags, p.Fields.Keys.ToArray()));
            }

            // Compact field array: values in the same field-name order as the descriptor.
            var desc = batch.Series[idx];
            var values = new FieldValue[desc.FieldNames.Length];
            for (var i = 0; i < desc.FieldNames.Length; i++)
                values[i] = p.Fields[desc.FieldNames[i]];

            batch.Points.Add(new CompactPoint(p.TimestampNs, idx, values));
        }
        return batch;
    }
}

internal readonly record struct CompactPoint(long TimestampNs, int SeriesIndex, FieldValue[] Values);

internal sealed class SeriesDescriptor
{
    public readonly string Measurement;
    public readonly string[] TagKeys;
    public readonly string[] TagValues;
    public readonly string[] FieldNames;

    public SeriesDescriptor(string measurement, Dictionary<string, string> tags, string[] fieldNames)
    {
        Measurement = measurement;
        TagKeys = tags.Keys.ToArray();
        TagValues = tags.Values.ToArray();
        FieldNames = fieldNames;
    }
}

/// <summary>
/// Columnar variant: per-series typed arrays. Timestamps and each field value
/// are stored in flat arrays indexed by point order within the series. This
/// removes per-point FieldValue[] allocation as well.
/// </summary>
internal sealed class CompactColumnarBatch
{
    public List<ColumnarSeries> Series { get; } = new();

    public static CompactColumnarBatch From(List<Point> source)
    {
        var batch = new CompactColumnarBatch();
        var index = new Dictionary<SeriesKey, ColumnarSeries>();

        foreach (var p in source)
        {
            var key = SeriesKey.From(p);
            if (!index.TryGetValue(key, out var cs))
            {
                cs = new ColumnarSeries(p.Measurement, p.Tags, p.Fields.Keys.ToArray(), p.Fields.Count);
                index[key] = cs;
                batch.Series.Add(cs);
            }
            cs.Add(p);
        }
        return batch;
    }
}

internal sealed class ColumnarSeries
{
    public readonly string Measurement;
    public readonly string[] TagKeys;
    public readonly string[] TagValues;
    public readonly string[] FieldNames;
    public readonly FieldKind[] FieldKinds;
    public List<long> Timestamps { get; } = new();
    // One parallel value list per field, stored as the raw underlying type to avoid
    // FieldValue struct overhead on the hot path.
    public readonly List<double>[] FloatValues;
    public readonly List<long>[] IntValues;
    public readonly List<bool>[] BoolValues;
    public readonly List<string>[] StringValues;

    public ColumnarSeries(string measurement, Dictionary<string, string> tags, string[] fieldNames, int fieldCount)
    {
        Measurement = measurement;
        TagKeys = tags.Keys.ToArray();
        TagValues = tags.Values.ToArray();
        FieldNames = fieldNames;
        FieldKinds = new FieldKind[fieldCount];
        FloatValues = new List<double>[fieldCount];
        IntValues = new List<long>[fieldCount];
        BoolValues = new List<bool>[fieldCount];
        StringValues = new List<string>[fieldCount];
    }

    public void Add(Point p)
    {
        Timestamps.Add(p.TimestampNs);
        for (var i = 0; i < FieldNames.Length; i++)
        {
            var fv = p.Fields[FieldNames[i]];
            FieldKinds[i] = fv.Kind;
            switch (fv.Kind)
            {
                case FieldKind.Float:
                    (FloatValues[i] ??= new List<double>()).Add(fv.Float);
                    break;
                case FieldKind.Integer:
                    (IntValues[i] ??= new List<long>()).Add(fv.Integer);
                    break;
                case FieldKind.Boolean:
                    (BoolValues[i] ??= new List<bool>()).Add(fv.Boolean);
                    break;
                case FieldKind.String:
                    (StringValues[i] ??= new List<string>()).Add(fv.String!);
                    break;
            }
        }
    }
}

using System.IO.Compression;
using System.Text;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

public static class SegmentWriter
{
    private const uint Magic = 0x4D545344;
    private const byte FormatVersion = 4;
    private const uint MetadataMagic = 0x4D455441;
    private const uint MetadataFooterMagic = 0x4D455446;

    /// <summary>
    /// Write segment atomically: write to .tmp, fsync, rename to .seg.
    /// Format v4: [magic:4][version:1][columnCount:4][columns...][metadata...][metadataOffset:8][metadataLength:4][metadataFooterMagic:4][crc32:4]
    /// </summary>
    public static List<SegmentColumnMeta> WriteSegment(string path, IEnumerable<Point> points)
        => WriteSegment(path, points.Select(point => (point, SeriesKey.From(point))));

    public static List<SegmentColumnMeta> WriteSegment(string path, IEnumerable<(Point Point, SeriesKey SeriesKey)> points)
        => WriteColumns(path, BuildColumns(points));

    internal static List<SegmentColumn> BuildColumns(IEnumerable<(Point Point, SeriesKey SeriesKey)> points)
    {
        using var timing = WriteDiagnostics.Measure(WriteDiagnostics.Stage.SegmentColumns);
        var builders = new Dictionary<(string Measurement, string TagsCanonical, string Field), ColumnBuilder>();
        foreach (var (point, series) in points)
        {
            foreach (var field in point.Fields)
            {
                var key = (series.Measurement, series.TagsCanonical, field.Key);
                if (!builders.TryGetValue(key, out var builder))
                {
                    builder = new ColumnBuilder(series.Measurement, series.TagsCanonical, field.Key, field.Value.Kind);
                    builders[key] = builder;
                }
                builder.Add(point.TimestampNs, field.Value);
            }
        }

        var columns = new List<SegmentColumn>(builders.Count);
        foreach (var builder in builders.Values)
            columns.Add(builder.ToColumn());
        return columns;
    }

    private sealed class ColumnBuilder(string measurement, string tagsCanonical, string field, FieldKind kind)
    {
        private readonly List<long> _timestamps = [];
        private readonly List<FieldValue> _values = [];
        private bool _sorted = true;

        public void Add(long timestamp, FieldValue value)
        {
            if (_timestamps.Count > 0 && timestamp == _timestamps[^1])
            {
                _values[^1] = value;
                return;
            }
            if (_timestamps.Count > 0 && timestamp < _timestamps[^1])
                _sorted = false;
            _timestamps.Add(timestamp);
            _values.Add(value);
        }

        public SegmentColumn ToColumn()
        {
            if (!_sorted && HasManyDuplicateTimestamps())
            {
                // Deduplicate in append order before sorting unique timestamps. This avoids
                // tree-node allocation and sorting every occurrence in duplicate-heavy input.
                var latest = new Dictionary<long, FieldValue>();
                for (var i = 0; i < _timestamps.Count; i++)
                    latest[_timestamps[i]] = _values[i];
                var timestamps = latest.Keys.ToArray();
                Array.Sort(timestamps);
                _timestamps.Clear();
                _values.Clear();
                foreach (var timestamp in timestamps)
                {
                    _timestamps.Add(timestamp);
                    _values.Add(latest[timestamp]);
                }
            }
            else if (!_sorted)
            {
                SortByOriginalIndex();
            }

            return new SegmentColumn(
                measurement,
                tagsCanonical,
                field,
                kind,
                _timestamps[0],
                _timestamps[^1],
                _timestamps,
                _values);
        }

        private bool HasManyDuplicateTimestamps()
        {
            // A bounded, evenly spaced sample only selects the algorithm. Both algorithms
            // fully deduplicate and preserve append order for ties, regardless of this estimate.
            var count = Math.Min(128, _timestamps.Count);
            var sample = new HashSet<long>(count);
            for (var i = 0; i < count; i++)
                sample.Add(_timestamps[(int)((long)i * _timestamps.Count / count)]);
            return sample.Count < count * 3 / 4;
        }

        private void SortByOriginalIndex()
        {
            var order = new int[_timestamps.Count];
            for (var i = 0; i < order.Length; i++) order[i] = i;
            Array.Sort(order, (left, right) =>
            {
                var comparison = _timestamps[left].CompareTo(_timestamps[right]);
                return comparison != 0 ? comparison : left.CompareTo(right);
            });

            // Apply permutation cycles to the existing lists, using only the index array.
            for (var i = 0; i < order.Length; i++)
            {
                if (order[i] == i) continue;
                var timestamp = _timestamps[i];
                var value = _values[i];
                var current = i;
                while (order[current] != i)
                {
                    var next = order[current];
                    _timestamps[current] = _timestamps[next];
                    _values[current] = _values[next];
                    order[current] = current;
                    current = next;
                }
                _timestamps[current] = timestamp;
                _values[current] = value;
                order[current] = current;
            }

            var written = 0;
            for (var i = 0; i < _timestamps.Count; i++)
            {
                if (written > 0 && _timestamps[i] == _timestamps[written - 1])
                    _values[written - 1] = _values[i];
                else
                {
                    _timestamps[written] = _timestamps[i];
                    _values[written++] = _values[i];
                }
            }
            _timestamps.RemoveRange(written, _timestamps.Count - written);
            _values.RemoveRange(written, _values.Count - written);
        }
    }

    /// <summary>
    /// Write the columns and return the column metadata that was just persisted. Callers can
    /// register it directly instead of re-opening the file to parse the footer back.
    /// </summary>
    public static List<SegmentColumnMeta> WriteColumns(string path, IReadOnlyList<SegmentColumn> columns)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var tmpPath = path + ".tmp";

        var metas = new List<SegmentColumnMeta>(columns.Count);
        using (var fs = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.Read))
        using (var ms = new MemoryStream())
        {
            using (var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
            {
                bw.Write(Magic);
                bw.Write(FormatVersion);
                bw.Write(columns.Count);
                foreach (var column in columns)
                {
                    var kind = column.Kind;
                    var ts = column.Timestamps;
                    var vals = column.Values;
                    TimestampEncodedBlock timestampBlock;
                    using (WriteDiagnostics.Measure(WriteDiagnostics.Stage.TimestampEncode))
                        timestampBlock = CompressionCodec.EncodeTimestampsAdaptive(ts);
                    ValueEncodedBlock valueBlock;
                    using (WriteDiagnostics.Measure(WriteDiagnostics.Stage.ValueEncode))
                        valueBlock = CompressionCodec.EncodeValuesAdaptive(kind, vals);
                    WriteString(bw, column.Measurement);
                    WriteString(bw, column.TagsCanonical);
                    WriteString(bw, column.Field);
                    bw.Write((byte)kind);
                    bw.Write(ts[0]); bw.Write(ts[^1]); bw.Write(ts.Count);
                    bw.Write((byte)timestampBlock.Codec);
                    bw.Write((byte)timestampBlock.Compression);
                    bw.Write((byte)valueBlock.Codec);
                    bw.Write((byte)valueBlock.Compression);
                    bw.Write(timestampBlock.Payload.Length); bw.Write(timestampBlock.Payload);
                    bw.Write(valueBlock.Payload.Length); bw.Write(valueBlock.Payload);
                    // Block stats
                    var stats = ComputeStats(kind, vals);
                    bw.Write(stats.Min); bw.Write(stats.Max); bw.Write(stats.Sum); bw.Write(stats.Count);
                    metas.Add(new SegmentColumnMeta(
                        column.Measurement, column.TagsCanonical, column.Field, kind,
                        ts[0], ts[^1], ts.Count, new BlockStats(stats.Min, stats.Max, stats.Sum, stats.Count),
                        timestampBlock.Codec, valueBlock.Codec, timestampBlock.Compression, valueBlock.Compression));
                }

                var metadataOffset = ms.Position;
                bw.Write(MetadataMagic);
                bw.Write(metas.Count);
                foreach (var meta in metas)
                    WriteMetadata(bw, meta);
                var metadataLength = checked((int)(ms.Position - metadataOffset));
                bw.Write(metadataOffset);
                bw.Write(metadataLength);
                bw.Write(MetadataFooterMagic);
            }
            // Write the MemoryStream's backing buffer directly: ToArray() would copy the whole
            // segment a second time (a large LOH allocation for big flushes).
            using var persistTiming = WriteDiagnostics.Measure(WriteDiagnostics.Stage.SegmentPersist);
            var buffer = ms.GetBuffer();
            var length = checked((int)ms.Length);
            fs.Write(buffer, 0, length);
            var crc = Crc32.Compute(buffer.AsSpan(0, length));
            var crcBytes = new byte[4];
            BitConverter.TryWriteBytes(crcBytes, crc);
            fs.Write(crcBytes);
            fs.Flush(true);
        }
        if (File.Exists(path)) File.Delete(path);
        File.Move(tmpPath, path);
        return metas;
    }

    private static (double Min, double Max, double Sum, int Count) ComputeStats(FieldKind kind, IReadOnlyList<FieldValue> vals)
    {
        if (vals.Count == 0) return (0, 0, 0, 0);
        double min = 0, max = 0, sum = 0;
        if (kind == FieldKind.Float || kind == FieldKind.Integer)
        {
            var first = kind == FieldKind.Float ? vals[0].Float : vals[0].Integer;
            min = first; max = first;
            foreach (var v in vals) { var d = kind == FieldKind.Float ? v.Float : v.Integer; if (d < min) min = d; if (d > max) max = d; sum += d; }
        }
        else if (kind == FieldKind.Boolean)
        {
            var trueCount = 0;
            foreach (var value in vals)
                if (value.Boolean)
                    trueCount++;
            min = trueCount > 0 ? 0 : 1;
            max = trueCount > 0 ? 1 : 0;
            sum = trueCount;
        }
        return (min, max, sum, vals.Count);
    }

    private static void WriteString(BinaryWriter bw, string v)
    { var b = Encoding.UTF8.GetBytes(v); bw.Write(b.Length); bw.Write(b); }

    private static void WriteMetadata(BinaryWriter bw, SegmentColumnMeta meta)
    {
        WriteString(bw, meta.Measurement);
        WriteString(bw, meta.TagsCanonical);
        WriteString(bw, meta.Field);
        bw.Write((byte)meta.Kind);
        bw.Write(meta.MinTime);
        bw.Write(meta.MaxTime);
        bw.Write(meta.PointCount);
        bw.Write((byte)meta.TimestampCodec);
        bw.Write((byte)meta.TimestampCompression);
        bw.Write((byte)meta.ValueCodec);
        bw.Write((byte)meta.ValueCompression);
        var stats = meta.Stats ?? new BlockStats(0, 0, 0, 0);
        bw.Write(stats.Min);
        bw.Write(stats.Max);
        bw.Write(stats.Sum);
        bw.Write(stats.Count);
    }
}

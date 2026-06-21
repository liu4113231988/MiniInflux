using System.IO.Compression;
using System.Text;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

public static class SegmentWriter
{
    private const uint Magic = 0x4D545344;
    private const byte FormatVersion = 3;

    /// <summary>
    /// Write segment atomically: write to .tmp, fsync, rename to .seg.
    /// Format v3: [magic:4][version:1][columnCount:4][columns...][crc32:4]
    /// </summary>
    public static void WriteSegment(string path, IEnumerable<Point> points)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var tmpPath = path + ".tmp";
        var grouped = points
            .SelectMany(p => p.Fields.Select(f => new { Series = SeriesKey.From(p), Field = f.Key, Value = f.Value, p.TimestampNs }))
            .GroupBy(x => (x.Series, x.Field)).ToList();

        using (var fs = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.Read))
        using (var ms = new MemoryStream())
        {
            using (var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
            {
                bw.Write(Magic);
                bw.Write(FormatVersion);
                bw.Write(grouped.Count);
                foreach (var g in grouped)
                {
                    var o = g.OrderBy(x => x.TimestampNs).ToList();
                    var kind = o[0].Value.Kind;
                    var ts = o.Select(x => x.TimestampNs).ToArray();
                    var vals = o.Select(x => x.Value).ToArray();
                    var timestampBlock = CompressionCodec.EncodeTimestampsAdaptive(ts);
                    var valueBlock = CompressionCodec.EncodeValuesAdaptive(kind, vals);
                    WriteString(bw, g.Key.Series.Measurement);
                    WriteString(bw, g.Key.Series.TagsCanonical);
                    WriteString(bw, g.Key.Field);
                    bw.Write((byte)kind);
                    bw.Write(ts[0]); bw.Write(ts[^1]); bw.Write(ts.Length);
                    bw.Write((byte)timestampBlock.Codec);
                    bw.Write((byte)timestampBlock.Compression);
                    bw.Write((byte)valueBlock.Codec);
                    bw.Write((byte)valueBlock.Compression);
                    bw.Write(timestampBlock.Payload.Length); bw.Write(timestampBlock.Payload);
                    bw.Write(valueBlock.Payload.Length); bw.Write(valueBlock.Payload);
                    // Block stats
                    var stats = ComputeStats(kind, vals);
                    bw.Write(stats.Min); bw.Write(stats.Max); bw.Write(stats.Sum); bw.Write(stats.Count);
                }
            }
            var data = ms.ToArray();
            fs.Write(data);
            var crc = Crc32.Compute(data);
            var crcBytes = new byte[4];
            BitConverter.TryWriteBytes(crcBytes, crc);
            fs.Write(crcBytes);
            fs.Flush(true);
        }
        if (File.Exists(path)) File.Delete(path);
        File.Move(tmpPath, path);
    }

    private static (double Min, double Max, double Sum, int Count) ComputeStats(FieldKind kind, FieldValue[] vals)
    {
        if (vals.Length == 0) return (0, 0, 0, 0);
        double min = 0, max = 0, sum = 0;
        if (kind == FieldKind.Float || kind == FieldKind.Integer)
        {
            var first = kind == FieldKind.Float ? vals[0].Float : vals[0].Integer;
            min = first; max = first;
            foreach (var v in vals) { var d = kind == FieldKind.Float ? v.Float : v.Integer; if (d < min) min = d; if (d > max) max = d; sum += d; }
        }
        else if (kind == FieldKind.Boolean)
        { int tc = vals.Count(v => v.Boolean); min = tc > 0 ? 0 : 1; max = tc > 0 ? 1 : 0; sum = tc; }
        return (min, max, sum, vals.Length);
    }

    private static void WriteString(BinaryWriter bw, string v)
    { var b = Encoding.UTF8.GetBytes(v); bw.Write(b.Length); bw.Write(b); }
}

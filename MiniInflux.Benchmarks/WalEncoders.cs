using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using MiniInflux.Net10.Model;

namespace MiniInflux.Benchmarks;

/// <summary>
/// Mirrors the current WalManager text payload encoding (db\trp\tline-protocol\n)
/// so its throughput/size reflect the real production format without going through
/// the file lock.
/// </summary>
internal static class WalTextEncoder
{
    public static void WritePayload(PooledWriter w, string db, string rp, List<Point> points)
    {
        w.WriteUtf8(db);
        w.WriteByte((byte)'\t');
        w.WriteUtf8(rp);
        w.WriteByte((byte)'\t');
        foreach (var p in points)
        {
            w.WriteUtf8(p.Measurement);
            foreach (var tag in p.Tags)
            {
                w.WriteByte((byte)',');
                w.WriteUtf8(tag.Key);
                w.WriteByte((byte)'=');
                w.WriteUtf8(tag.Value);
            }
            w.WriteByte((byte)' ');
            var first = true;
            foreach (var f in p.Fields)
            {
                if (!first) w.WriteByte((byte)',');
                first = false;
                w.WriteUtf8(f.Key);
                w.WriteByte((byte)'=');
                WriteFieldValue(w, f.Value);
            }
            w.WriteByte((byte)' ');
            w.WriteInt64(p.TimestampNs);
            w.WriteByte((byte)'\n');
        }
    }

    private static void WriteFieldValue(PooledWriter w, FieldValue v)
    {
        switch (v.Kind)
        {
            case FieldKind.Integer:
                w.WriteInt64(v.Integer);
                w.WriteByte((byte)'i');
                break;
            case FieldKind.Float:
                w.WriteDoubleText(v.Float);
                break;
            case FieldKind.Boolean:
                w.WriteBytes(v.Boolean ? "true"u8 : "false"u8);
                break;
            case FieldKind.String:
                w.WriteByte((byte)'"');
                w.WriteUtf8(v.String!);
                w.WriteByte((byte)'"');
                break;
        }
    }
}

/// <summary>
/// Candidate binary WAL format. Record layout (no CRC here; the real record
/// frame adds [length:4][crc32:4] which is identical for both formats):
///
///   db: u16 len + utf8 bytes
///   rp: u16 len + utf8 bytes
///   series_table:
///     u16 series_count
///     per series:
///       measurement: u16 len + utf8
///       tag_count: u8
///       per tag: key(u16+utf8), value(u16+utf8)
///       field_count: u8
///       per field: name(u16+utf8), kind(u8)
///   point_count: u32
///   per point:
///     series_index: u16
///     timestamp: varint (zigzag)
///     per field in series order:
///       float: 8 bytes (little-endian)
///       integer: varint (zigzag)
///       bool: 1 byte
///       string: u16 len + utf8
///
/// Design goals vs text: shared series table (no repeated measurement/tags/field
/// names per point), fixed-width float encoding (no G-style round-trip text),
/// varint timestamps/ints.
/// </summary>
internal static class WalBinaryEncoder
{
    public static void WritePayload(PooledWriter w, string db, string rp, List<Point> points)
    {
        w.WriteString16(db);
        w.WriteString16(rp);

        // Build the series table in encounter order.
        var seriesList = new List<(SeriesKey Key, Point Sample)>();
        var seriesIndex = new Dictionary<SeriesKey, int>();
        var pointSeries = new int[points.Count];
        for (var i = 0; i < points.Count; i++)
        {
            var key = SeriesKey.From(points[i]);
            if (!seriesIndex.TryGetValue(key, out var idx))
            {
                idx = seriesList.Count;
                seriesIndex[key] = idx;
                seriesList.Add((key, points[i]));
            }
            pointSeries[i] = idx;
        }

        w.WriteUInt16((ushort)seriesList.Count);
        foreach (var (_, sample) in seriesList)
        {
            w.WriteString16(sample.Measurement);
            w.WriteByte((byte)sample.Tags.Count);
            foreach (var t in sample.Tags)
            {
                w.WriteString16(t.Key);
                w.WriteString16(t.Value);
            }
            w.WriteByte((byte)sample.Fields.Count);
            foreach (var f in sample.Fields)
            {
                w.WriteString16(f.Key);
                w.WriteByte((byte)f.Value.Kind);
            }
        }

        w.WriteUInt32((uint)points.Count);
        for (var i = 0; i < points.Count; i++)
        {
            var p = points[i];
            w.WriteUInt16((ushort)pointSeries[i]);
            w.WriteVarintZigzag(p.TimestampNs);
            var series = seriesList[pointSeries[i]].Sample;
            foreach (var f in series.Fields)
            {
                var v = p.Fields[f.Key];
                switch (v.Kind)
                {
                    case FieldKind.Float: w.WriteDouble(v.Float); break;
                    case FieldKind.Integer: w.WriteVarintZigzag(v.Integer); break;
                    case FieldKind.Boolean: w.WriteByte(v.Boolean ? (byte)1 : (byte)0); break;
                    case FieldKind.String: w.WriteString16(v.String!); break;
                }
            }
        }
    }

    public static List<Point> ReadPayload(byte[] payload)
    {
        var span = payload.AsSpan();
        var pos = 0;
        var db = ReadString16(span, ref pos);
        var rp = ReadString16(span, ref pos);
        var seriesCount = BinaryPrimitives.ReadUInt16LittleEndian(span[pos..]); pos += 2;

        var series = new (string Measurement, string[] TagKeys, string[] TagVals, string[] FieldNames, FieldKind[] Kinds)[seriesCount];
        for (var s = 0; s < seriesCount; s++)
        {
            var meas = ReadString16(span, ref pos);
            var tagCount = span[pos++];
            var tk = new string[tagCount]; var tv = new string[tagCount];
            for (var t = 0; t < tagCount; t++) { tk[t] = ReadString16(span, ref pos); tv[t] = ReadString16(span, ref pos); }
            var fcount = span[pos++];
            var fn = new string[fcount]; var fk = new FieldKind[fcount];
            for (var f = 0; f < fcount; f++) { fn[f] = ReadString16(span, ref pos); fk[f] = (FieldKind)span[pos++]; }
            series[s] = (meas, tk, tv, fn, fk);
        }

        var pointCount = BinaryPrimitives.ReadUInt32LittleEndian(span[pos..]); pos += 4;
        var result = new List<Point>((int)pointCount);
        for (var i = 0; i < pointCount; i++)
        {
            var sidx = BinaryPrimitives.ReadUInt16LittleEndian(span[pos..]); pos += 2;
            var ts = ReadVarintZigzag(span, ref pos);
            var s = series[sidx];
            var tags = new Dictionary<string, string>(s.TagKeys.Length);
            for (var t = 0; t < s.TagKeys.Length; t++) tags[s.TagKeys[t]] = s.TagVals[t];
            var fields = new Dictionary<string, FieldValue>(s.FieldNames.Length);
            for (var f = 0; f < s.FieldNames.Length; f++)
            {
                FieldValue fv = s.Kinds[f] switch
                {
                    FieldKind.Float => FieldValue.FromDouble(BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(pos, 8)).Tap(ref pos, 8)),
                    FieldKind.Integer => FieldValue.FromInteger(ReadVarintZigzag(span, ref pos)),
                    FieldKind.Boolean => FieldValue.FromBoolean(span[pos++] == 1),
                    FieldKind.String => FieldValue.FromString(ReadString16(span, ref pos)),
                    _ => default
                };
                fields[s.FieldNames[f]] = fv;
            }
            result.Add(new Point { Measurement = s.Measurement, Tags = tags, Fields = fields, TimestampNs = ts });
        }
        return result;
    }

    private static string ReadString16(ReadOnlySpan<byte> span, ref int pos)
    {
        var len = BinaryPrimitives.ReadUInt16LittleEndian(span[pos..]); pos += 2;
        var s = Encoding.UTF8.GetString(span.Slice(pos, len)); pos += len;
        return s;
    }

    private static long ReadVarintZigzag(ReadOnlySpan<byte> span, ref int pos)
    {
        ulong result = 0; int shift = 0; byte b;
        do
        {
            b = span[pos++];
            result |= (ulong)(b & 0x7f) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return (long)((result >> 1) ^ (~(result & 1) + 1));
    }
}

file static class SpanExtensions
{
    public static double Tap(this double value, ref int pos, int bytes) { pos += bytes; return value; }
}

/// <summary>
/// Minimal ArrayPool-backed buffer writer used by the benchmark encoders.
/// Decoupled from the production WalManager.PooledBufferWriter so this project
/// stays independent of internal production types.
/// </summary>
internal sealed class PooledWriter(int initialCapacity) : IDisposable
{
    private byte[] _buf = ArrayPool<byte>.Shared.Rent(Math.Max(initialCapacity, 64));
    private int _pos;

    public int Written => _pos;
    public ReadOnlySpan<byte> WrittenSpan => _buf.AsSpan(0, _pos);

    private Span<byte> Ensure(int n)
    {
        var need = _pos + n;
        if (need <= _buf.Length) return _buf.AsSpan(_pos);
        var next = ArrayPool<byte>.Shared.Rent(Math.Max(need, _buf.Length * 2));
        Buffer.BlockCopy(_buf, 0, next, 0, _pos);
        ArrayPool<byte>.Shared.Return(_buf);
        _buf = next;
        return _buf.AsSpan(_pos);
    }

    public void WriteByte(byte b) { Ensure(1)[0] = b; _pos++; }
    public void WriteBytes(ReadOnlySpan<byte> b) { b.CopyTo(Ensure(b.Length)); _pos += b.Length; }
    public void WriteUtf8(string s) { _pos += Encoding.UTF8.GetBytes(s, Ensure(s.Length * 3)); }
    public void WriteUInt16(ushort v) { BinaryPrimitives.WriteUInt16LittleEndian(Ensure(2), v); _pos += 2; }
    public void WriteUInt32(uint v) { BinaryPrimitives.WriteUInt32LittleEndian(Ensure(4), v); _pos += 4; }
    public void WriteDouble(double v) { BinaryPrimitives.WriteDoubleLittleEndian(Ensure(8), v); _pos += 8; }
    public void WriteDoubleText(double v)
    {
        var span = Ensure(32);
        System.Buffers.Text.Utf8Formatter.TryFormat(v, span, out var written);
        _pos += written;
    }
    public void WriteInt64(long v)
    {
        var span = Ensure(20);
        System.Buffers.Text.Utf8Formatter.TryFormat(v, span, out var written);
        _pos += written;
    }
    public void WriteString16(string s)
    {
        WriteUInt16((ushort)Encoding.UTF8.GetByteCount(s));
        WriteUtf8(s);
    }
    public void WriteVarintZigzag(long v)
    {
        var zz = (ulong)((v << 1) ^ (v >> 63));
        var span = Ensure(10);
        var n = 0;
        while (zz >= 0x80) { span[n++] = (byte)(zz | 0x80); zz >>= 7; }
        span[n++] = (byte)zz;
        _pos += n;
    }

    public void Dispose() => ArrayPool<byte>.Shared.Return(_buf);
}

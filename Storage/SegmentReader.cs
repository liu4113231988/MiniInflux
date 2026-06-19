using System.IO.Compression;
using System.Text;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

public sealed record BlockStats(double Min, double Max, double Sum, int Count);

public sealed record SegmentColumn(
    string Measurement, string TagsCanonical, string Field, FieldKind Kind,
    long MinTime, long MaxTime, List<long> Timestamps, List<FieldValue> Values,
    BlockStats? Stats = null);

public sealed record SegmentColumnMeta(
    string Measurement, string TagsCanonical, string Field, FieldKind Kind,
    long MinTime, long MaxTime, int PointCount, BlockStats? Stats = null);

public static class SegmentReader
{
    private const uint Magic = 0x4D545344;

    public static List<SegmentColumn> ReadSegment(string path)
    {
        return ReadSegment(path, null);
    }

    /// <summary>
    /// Read a segment, optionally filtering to specific fields (projection pushdown).
    /// When requestedFields is non-null, only matching columns are decompressed.
    /// </summary>
    public static List<SegmentColumn> ReadSegment(string path, HashSet<string>? requestedFields)
    {
        var allBytes = File.ReadAllBytes(path);
        if (allBytes.Length < 8) throw new InvalidDataException("segment file too small");
        var dataBytes = allBytes.AsSpan(0, allBytes.Length - 4);
        var storedCrc = BitConverter.ToUInt32(allBytes, allBytes.Length - 4);
        if (storedCrc != Crc32.Compute(dataBytes.ToArray()))
            throw new InvalidDataException("segment CRC mismatch");

        var result = new List<SegmentColumn>();
        using var ms = new MemoryStream(dataBytes.ToArray());
        using var br = new BinaryReader(ms, Encoding.UTF8);
        if (br.ReadUInt32() != Magic) throw new InvalidDataException("invalid segment magic");

        var (version, count) = ReadVersionAndCount(br, ms);

        for (int i = 0; i < count; i++)
        {
            var m = ReadString(br); var tags = ReadString(br); var f = ReadString(br);
            var k = (FieldKind)br.ReadByte();
            var min = br.ReadInt64(); var max = br.ReadInt64();
            br.ReadInt32(); // point count (unused)

            // Projection pushdown: skip reading compressed data for unneeded columns
            if (requestedFields != null && !requestedFields.Contains(f))
            {
                var skipTl = br.ReadInt32(); ms.Position += skipTl;
                var skipVl = br.ReadInt32(); ms.Position += skipVl;
                if (version >= 2) { ms.Position += 28; } // 3 doubles + 1 int
                continue;
            }

            var tl = br.ReadInt32(); var tb = br.ReadBytes(tl);
            var vl = br.ReadInt32(); var vb = br.ReadBytes(vl);
            BlockStats? stats = null;
            if (version >= 2) { stats = new BlockStats(br.ReadDouble(), br.ReadDouble(), br.ReadDouble(), br.ReadInt32()); }

            result.Add(new SegmentColumn(m, tags, f, k, min, max,
                CompressionCodec.DecodeTimestamps(UnBrotli(tb)),
                CompressionCodec.DecodeValues(k, UnBrotli(vb)), stats));
        }
        return result;
    }

    public static List<SegmentColumnMeta> ReadMetadata(string path)
    {
        var allBytes = File.ReadAllBytes(path);
        if (allBytes.Length < 8) throw new InvalidDataException("segment file too small");
        var dataBytes = allBytes.AsSpan(0, allBytes.Length - 4);
        if (BitConverter.ToUInt32(allBytes, allBytes.Length - 4) != Crc32.Compute(dataBytes.ToArray()))
            throw new InvalidDataException("segment CRC mismatch");

        var result = new List<SegmentColumnMeta>();
        using var ms = new MemoryStream(dataBytes.ToArray());
        using var br = new BinaryReader(ms, Encoding.UTF8);
        if (br.ReadUInt32() != Magic) throw new InvalidDataException("invalid segment magic");

        var (version, count) = ReadVersionAndCount(br, ms);

        for (int i = 0; i < count; i++)
        {
            var m = ReadString(br); var tags = ReadString(br); var f = ReadString(br);
            var k = (FieldKind)br.ReadByte();
            var min = br.ReadInt64(); var max = br.ReadInt64(); var pc = br.ReadInt32();
            var tl = br.ReadInt32(); ms.Position += tl;
            var vl = br.ReadInt32(); ms.Position += vl;
            BlockStats? stats = null;
            if (version >= 2) { stats = new BlockStats(br.ReadDouble(), br.ReadDouble(), br.ReadDouble(), br.ReadInt32()); }
            result.Add(new SegmentColumnMeta(m, tags, f, k, min, max, pc, stats));
        }
        return result;
    }

    private static (byte Version, int Count) ReadVersionAndCount(BinaryReader br, MemoryStream ms)
    {
        var nextBytes = br.ReadBytes(5);
        if (nextBytes[0] == 2)
            return (2, BitConverter.ToInt32(nextBytes, 1));
        // v1: no version byte, first 4 bytes are columnCount, 5th byte belongs to first column
        ms.Position -= 1;
        return (1, BitConverter.ToInt32(nextBytes, 0));
    }

    private static byte[] UnBrotli(byte[] input)
    { using var src = new MemoryStream(input); using var bs = new BrotliStream(src, CompressionMode.Decompress); using var o = new MemoryStream(); bs.CopyTo(o); return o.ToArray(); }

    private static string ReadString(BinaryReader br)
    { int len = br.ReadInt32(); return Encoding.UTF8.GetString(br.ReadBytes(len)); }
}

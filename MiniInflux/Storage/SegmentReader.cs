using System.Text;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

public sealed record BlockStats(double Min, double Max, double Sum, int Count);

public sealed record SegmentColumn(
    string Measurement, string TagsCanonical, string Field, FieldKind Kind,
    long MinTime, long MaxTime, List<long> Timestamps, List<FieldValue> Values,
    BlockStats? Stats = null,
    TimestampCodecKind TimestampCodec = TimestampCodecKind.DeltaOfDeltaVarint,
    ValueCodecKind ValueCodec = ValueCodecKind.Legacy,
    BlockCompressionKind TimestampCompression = BlockCompressionKind.Brotli,
    BlockCompressionKind ValueCompression = BlockCompressionKind.Brotli);

public sealed record SegmentColumnMeta(
    string Measurement, string TagsCanonical, string Field, FieldKind Kind,
    long MinTime, long MaxTime, int PointCount, BlockStats? Stats = null,
    TimestampCodecKind TimestampCodec = TimestampCodecKind.DeltaOfDeltaVarint,
    ValueCodecKind ValueCodec = ValueCodecKind.Legacy,
    BlockCompressionKind TimestampCompression = BlockCompressionKind.Brotli,
    BlockCompressionKind ValueCompression = BlockCompressionKind.Brotli);

/// <summary>
/// Lightweight column read that contains only decoded timestamps (no field values).
/// Used by the fast count path to avoid decoding value blocks.
/// </summary>
public sealed record SegmentTimestampColumn(
    string Measurement, string TagsCanonical, string Field, FieldKind Kind,
    long MinTime, long MaxTime, List<long> Timestamps);

public sealed record SegmentMetadataReadResult(List<SegmentColumnMeta> Metadata, bool UsedFooter);

public static class SegmentReader
{
    private const uint Magic = 0x4D545344;
    private const uint MetadataMagic = 0x4D455441;
    private const uint MetadataFooterMagic = 0x4D455446;
    private const int MetadataFooterSize = 16;

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
        return ReadSegment(path, requestedFields, null, null, null, null);
    }

    public static List<SegmentColumn> ReadSegment(
        string path,
        HashSet<string>? requestedFields,
        string? measurement,
        long? minTimeNs,
        long? maxTimeNs,
        HashSet<string>? allowedTagsCanonical)
    {
        var allBytes = ReadAllBytesShared(path);
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

            // Projection and predicate pushdown: skip reading compressed data for irrelevant columns.
            if (!ShouldReadColumn(requestedFields, measurement, minTimeNs, maxTimeNs, allowedTagsCanonical, m, tags, f, min, max))
            {
                SkipColumnPayload(version, br, ms);
                continue;
            }

            var codecs = ReadCodecInfo(version, br);
            var tl = br.ReadInt32(); var tb = br.ReadBytes(tl);
            var vl = br.ReadInt32(); var vb = br.ReadBytes(vl);
            BlockStats? stats = null;
            if (version >= 2) { stats = new BlockStats(br.ReadDouble(), br.ReadDouble(), br.ReadDouble(), br.ReadInt32()); }

            result.Add(new SegmentColumn(m, tags, f, k, min, max,
                CompressionCodec.DecodeTimestamps(codecs.TimestampCodec, codecs.TimestampCompression, tb),
                CompressionCodec.DecodeValues(k, codecs.ValueCodec, codecs.ValueCompression, vb), stats,
                codecs.TimestampCodec, codecs.ValueCodec, codecs.TimestampCompression, codecs.ValueCompression));
        }
        return result;
    }

    /// <summary>
    /// Read only timestamp columns from a segment, skipping the expensive value block decoding.
    /// This is used by the fast count path where field values are not needed.
    /// </summary>
    public static List<SegmentTimestampColumn> ReadSegmentTimestampsOnly(
        string path,
        HashSet<string>? requestedFields,
        string? measurement,
        long? minTimeNs,
        long? maxTimeNs,
        HashSet<string>? allowedTagsCanonical)
    {
        var allBytes = ReadAllBytesShared(path);
        if (allBytes.Length < 8) throw new InvalidDataException("segment file too small");
        var dataBytes = allBytes.AsSpan(0, allBytes.Length - 4);
        var storedCrc = BitConverter.ToUInt32(allBytes, allBytes.Length - 4);
        if (storedCrc != Crc32.Compute(dataBytes.ToArray()))
            throw new InvalidDataException("segment CRC mismatch");

        var result = new List<SegmentTimestampColumn>();
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

            // Projection and predicate pushdown: skip reading compressed data for irrelevant columns.
            if (!ShouldReadColumn(requestedFields, measurement, minTimeNs, maxTimeNs, allowedTagsCanonical, m, tags, f, min, max))
            {
                SkipColumnPayload(version, br, ms);
                continue;
            }

            var codecs = ReadCodecInfo(version, br);
            var tl = br.ReadInt32(); var tb = br.ReadBytes(tl);
            // Skip value block instead of decoding it.
            var vl = br.ReadInt32(); ms.Position += vl;
            // Skip stats block if present.
            if (version >= 2) ms.Position += 28; // 3 doubles + 1 int

            result.Add(new SegmentTimestampColumn(m, tags, f, k, min, max,
                CompressionCodec.DecodeTimestamps(codecs.TimestampCodec, codecs.TimestampCompression, tb)));
        }
        return result;
    }

    public static List<SegmentColumnMeta> ReadMetadata(string path)
    {
        return ReadMetadataWithInfo(path).Metadata;
    }

    public static SegmentMetadataReadResult ReadMetadataWithInfo(string path)
    {
        if (TryReadFooterMetadata(path, out var metadata))
            return new SegmentMetadataReadResult(metadata, true);

        var allBytes = ReadAllBytesShared(path);
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
            var codecs = ReadCodecInfo(version, br);
            var tl = br.ReadInt32(); ms.Position += tl;
            var vl = br.ReadInt32(); ms.Position += vl;
            BlockStats? stats = null;
            if (version >= 2) { stats = new BlockStats(br.ReadDouble(), br.ReadDouble(), br.ReadDouble(), br.ReadInt32()); }
            result.Add(new SegmentColumnMeta(m, tags, f, k, min, max, pc, stats,
                codecs.TimestampCodec, codecs.ValueCodec, codecs.TimestampCompression, codecs.ValueCompression));
        }
        return new SegmentMetadataReadResult(result, false);
    }

    private static bool TryReadFooterMetadata(string path, out List<SegmentColumnMeta> metadata)
    {
        metadata = [];
        var length = new FileInfo(path).Length;
        if (length < 4 + MetadataFooterSize + 4)
            return false;

        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        fs.Position = length - 4 - MetadataFooterSize;
        Span<byte> footer = stackalloc byte[MetadataFooterSize];
        if (fs.Read(footer) != MetadataFooterSize)
            return false;
        var metadataOffset = BitConverter.ToInt64(footer[..8]);
        var metadataLength = BitConverter.ToInt32(footer.Slice(8, 4));
        var footerMagic = BitConverter.ToUInt32(footer.Slice(12, 4));
        if (footerMagic != MetadataFooterMagic || metadataOffset <= 0 || metadataLength <= 8)
            return false;
        if (metadataOffset + metadataLength > length - 4 - MetadataFooterSize)
            return false;

        var block = new byte[metadataLength];
        fs.Position = metadataOffset;
        if (fs.Read(block, 0, block.Length) != block.Length)
            return false;

        using var ms = new MemoryStream(block);
        using var br = new BinaryReader(ms, Encoding.UTF8);
        if (br.ReadUInt32() != MetadataMagic)
            return false;

        var count = br.ReadInt32();
        var result = new List<SegmentColumnMeta>(count);
        for (var i = 0; i < count; i++)
            result.Add(ReadMetadataEntry(br));
        metadata = result;
        return true;
    }

    private static byte[] ReadAllBytesShared(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var bytes = new byte[stream.Length];
        stream.ReadExactly(bytes);
        return bytes;
    }

    private static (TimestampCodecKind TimestampCodec, BlockCompressionKind TimestampCompression, ValueCodecKind ValueCodec, BlockCompressionKind ValueCompression) ReadCodecInfo(byte version, BinaryReader br)
    {
        if (version < 3)
            return (TimestampCodecKind.DeltaOfDeltaVarint, BlockCompressionKind.Brotli, ValueCodecKind.Legacy, BlockCompressionKind.Brotli);

        return (
            (TimestampCodecKind)br.ReadByte(),
            (BlockCompressionKind)br.ReadByte(),
            (ValueCodecKind)br.ReadByte(),
            (BlockCompressionKind)br.ReadByte());
    }

    private static (byte Version, int Count) ReadVersionAndCount(BinaryReader br, MemoryStream ms)
    {
        var nextBytes = br.ReadBytes(5);
        if (nextBytes[0] is >= 2 and <= 4)
            return (nextBytes[0], BitConverter.ToInt32(nextBytes, 1));
        // v1: no version byte, first 4 bytes are columnCount, 5th byte belongs to first column
        ms.Position -= 1;
        return (1, BitConverter.ToInt32(nextBytes, 0));
    }

    private static bool ShouldReadColumn(
        HashSet<string>? requestedFields,
        string? measurement,
        long? minTimeNs,
        long? maxTimeNs,
        HashSet<string>? allowedTagsCanonical,
        string columnMeasurement,
        string tagsCanonical,
        string field,
        long columnMinTimeNs,
        long columnMaxTimeNs)
    {
        if (requestedFields != null && !requestedFields.Contains(field))
            return false;
        if (measurement != null && !string.Equals(columnMeasurement, measurement, StringComparison.Ordinal))
            return false;
        if (minTimeNs.HasValue && columnMaxTimeNs < minTimeNs.Value)
            return false;
        if (maxTimeNs.HasValue && columnMinTimeNs > maxTimeNs.Value)
            return false;
        if (allowedTagsCanonical != null && !allowedTagsCanonical.Contains(tagsCanonical))
            return false;
        return true;
    }

    private static void SkipColumnPayload(byte version, BinaryReader br, MemoryStream ms)
    {
        if (version >= 3)
            ms.Position += 4; // timestamp/value codec + compression ids
        var skipTl = br.ReadInt32();
        ms.Position += skipTl;
        var skipVl = br.ReadInt32();
        ms.Position += skipVl;
        if (version >= 2)
            ms.Position += 28; // 3 doubles + 1 int
    }

    private static string ReadString(BinaryReader br)
    { int len = br.ReadInt32(); return Encoding.UTF8.GetString(br.ReadBytes(len)); }

    private static SegmentColumnMeta ReadMetadataEntry(BinaryReader br)
    {
        var measurement = ReadString(br);
        var tags = ReadString(br);
        var field = ReadString(br);
        var kind = (FieldKind)br.ReadByte();
        var min = br.ReadInt64();
        var max = br.ReadInt64();
        var count = br.ReadInt32();
        var timestampCodec = (TimestampCodecKind)br.ReadByte();
        var timestampCompression = (BlockCompressionKind)br.ReadByte();
        var valueCodec = (ValueCodecKind)br.ReadByte();
        var valueCompression = (BlockCompressionKind)br.ReadByte();
        var stats = new BlockStats(br.ReadDouble(), br.ReadDouble(), br.ReadDouble(), br.ReadInt32());
        return new SegmentColumnMeta(measurement, tags, field, kind, min, max, count, stats,
            timestampCodec, valueCodec, timestampCompression, valueCompression);
    }
}

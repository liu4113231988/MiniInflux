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

    // Segment files are immutable (atomic .tmp -> .seg rename), so a successful CRC check only
    // needs to happen once per (path, length, mtime); previously every query re-verified the
    // whole file for segments below the streaming threshold.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, (long Length, DateTime LastWriteUtc)> s_crcVerified = new(StringComparer.Ordinal);

    // Decoded column block cache. Repeated dashboard-style queries used to re-run Brotli
    // decompression + delta/Gorilla decoding for the same columns on every single query; segment
    // files are immutable, so decoded blocks can be shared safely. Bounded by a byte budget with
    // LRU eviction.
    private const long MaxDecodedBlockCacheBytes = 128L * 1024 * 1024;
    private static readonly object s_blockCacheLock = new();
    private static readonly Dictionary<(string Path, long Length, int Ordinal), LinkedListNode<CachedColumn>> s_blockCache = [];
    private static readonly LinkedList<CachedColumn> s_blockLru = new();
    private static long s_blockCacheBytes;

    private sealed class CachedColumn((string Path, long Length, int Ordinal) key, List<long> timestamps, List<FieldValue>? values, long bytes)
    {
        public readonly (string Path, long Length, int Ordinal) Key = key;
        public readonly List<long> Timestamps = timestamps;
        public List<FieldValue>? Values = values;
        public long Bytes = bytes;
    }

    private static long EstimateColumnBytes(int pointCount, bool hasValues) =>
        256 + pointCount * 8L + (hasValues ? pointCount * 32L : 0);

    private static (List<long>? Timestamps, List<FieldValue>? Values) GetCachedColumn(string path, long length, int ordinal)
    {
        lock (s_blockCacheLock)
        {
            if (!s_blockCache.TryGetValue((path, length, ordinal), out var node))
                return (null, null);
            s_blockLru.Remove(node);
            s_blockLru.AddFirst(node);
            return (node.Value.Timestamps, node.Value.Values);
        }
    }

    private static void CacheColumn(string path, long length, int ordinal, List<long> timestamps, List<FieldValue>? values)
    {
        var key = (path, length, ordinal);
        lock (s_blockCacheLock)
        {
            if (s_blockCache.TryGetValue(key, out var existing))
            {
                // Upgrade a timestamps-only entry with decoded values.
                if (existing.Value.Values == null && values != null)
                {
                    existing.Value.Values = values;
                    var newBytes = EstimateColumnBytes(timestamps.Count, hasValues: true);
                    s_blockCacheBytes += newBytes - existing.Value.Bytes;
                    existing.Value.Bytes = newBytes;
                }
                s_blockLru.Remove(existing);
                s_blockLru.AddFirst(existing);
            }
            else
            {
                var bytes = EstimateColumnBytes(timestamps.Count, values != null);
                var node = new LinkedListNode<CachedColumn>(new CachedColumn(key, timestamps, values, bytes));
                s_blockCache[key] = node;
                s_blockLru.AddFirst(node);
                s_blockCacheBytes += bytes;
            }

            while (s_blockCacheBytes > MaxDecodedBlockCacheBytes && s_blockLru.Last != null)
            {
                var last = s_blockLru.Last;
                s_blockLru.RemoveLast();
                s_blockCache.Remove(last.Value.Key);
                s_blockCacheBytes -= last.Value.Bytes;
            }
        }
    }

    /// <summary>
    /// Segments larger than this are read through a buffered <see cref="FileStream"/> instead of being
    /// slurped into one big <c>byte[]</c>. Compacted L2 segments can reach hundreds of megabytes, and
    /// with up to 8 segments read in parallel the old full-file buffering alone could pin multiple GB
    /// of memory per query. Streaming lets skipped column payloads never enter memory at all.
    /// </summary>
    private const long StreamingReadThresholdBytes = 8L * 1024 * 1024;

    /// <summary>
    /// Buffer size used for the streaming segment reads.
    /// </summary>
    private const int StreamingBufferBytes = 128 * 1024;

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
        var result = new List<SegmentColumn>();
        using var ms = OpenSegmentForSequentialRead(path, out var dataLength, out var fileLength);
        using var br = new BinaryReader(ms, Encoding.UTF8);
        if (br.ReadUInt32() != Magic) throw new InvalidDataException("invalid segment magic");

        var (version, count) = ReadVersionAndCount(br, ms);

        for (int i = 0; i < count; i++)
        {
            // The metadata block follows the columns, so never decode past the column area.
            if (ms.Position >= dataLength) break;

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

            result.Add(ReadColumnBody(br, ms, version, path, fileLength, i, m, tags, f, k, min, max));
        }
        return result;
    }

    /// <summary>
    /// Read only the columns for which <paramref name="columnSelector"/> returns true, skipping all
    /// other payloads in a single sequential pass. Used by compaction to decode exactly one output
    /// batch's worth of columns per pass instead of materializing whole segments.
    /// </summary>
    public static List<SegmentColumn> ReadSegmentSelected(string path, Func<string, string, string, bool> columnSelector)
    {
        var result = new List<SegmentColumn>();
        using var ms = OpenSegmentForSequentialRead(path, out var dataLength, out var fileLength);
        using var br = new BinaryReader(ms, Encoding.UTF8);
        if (br.ReadUInt32() != Magic) throw new InvalidDataException("invalid segment magic");

        var (version, count) = ReadVersionAndCount(br, ms);

        for (int i = 0; i < count; i++)
        {
            if (ms.Position >= dataLength) break;

            var m = ReadString(br); var tags = ReadString(br); var f = ReadString(br);
            var k = (FieldKind)br.ReadByte();
            var min = br.ReadInt64(); var max = br.ReadInt64();
            br.ReadInt32(); // point count (unused)

            if (!columnSelector(m, tags, f))
            {
                SkipColumnPayload(version, br, ms);
                continue;
            }

            result.Add(ReadColumnBody(br, ms, version, path, fileLength, i, m, tags, f, k, min, max));
        }
        return result;
    }

    /// <summary>Shared decode of one selected column's payload (codecs, blocks, cache, stats).</summary>
    private static SegmentColumn ReadColumnBody(
        BinaryReader br, Stream ms, byte version, string path, long fileLength, int ordinal,
        string m, string tags, string f, FieldKind k, long min, long max)
    {
        var codecs = ReadCodecInfo(version, br);
        var (cachedTimestamps, cachedValues) = GetCachedColumn(path, fileLength, ordinal);

        var tl = br.ReadInt32();
        byte[]? tb = null;
        if (cachedTimestamps != null) SkipBytes(ms, tl);
        else tb = br.ReadBytes(tl);

        var vl = br.ReadInt32();
        byte[]? vb = null;
        if (cachedValues != null) SkipBytes(ms, vl);
        else vb = br.ReadBytes(vl);

        BlockStats? stats = null;
        if (version >= 2) { stats = new BlockStats(br.ReadDouble(), br.ReadDouble(), br.ReadDouble(), br.ReadInt32()); }

        var timestamps = cachedTimestamps
            ?? CompressionCodec.DecodeTimestamps(codecs.TimestampCodec, codecs.TimestampCompression, tb!);
        var values = cachedValues
            ?? CompressionCodec.DecodeValues(k, codecs.ValueCodec, codecs.ValueCompression, vb!);
        if (tb != null || vb != null)
            CacheColumn(path, fileLength, ordinal, timestamps, values);

        return new SegmentColumn(m, tags, f, k, min, max,
            timestamps, values, stats,
            codecs.TimestampCodec, codecs.ValueCodec, codecs.TimestampCompression, codecs.ValueCompression);
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
        var result = new List<SegmentTimestampColumn>();
        using var ms = OpenSegmentForSequentialRead(path, out var dataLength, out var fileLength);
        using var br = new BinaryReader(ms, Encoding.UTF8);
        if (br.ReadUInt32() != Magic) throw new InvalidDataException("invalid segment magic");

        var (version, count) = ReadVersionAndCount(br, ms);

        for (int i = 0; i < count; i++)
        {
            // The metadata block follows the columns, so never decode past the column area.
            if (ms.Position >= dataLength) break;

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
            var (cachedTimestamps, _) = GetCachedColumn(path, fileLength, i);

            var tl = br.ReadInt32();
            byte[]? tb = null;
            if (cachedTimestamps != null) SkipBytes(ms, tl);
            else tb = br.ReadBytes(tl);
            // Skip value block instead of decoding it.
            var vl = br.ReadInt32(); SkipBytes(ms, vl);
            // Skip stats block if present.
            if (version >= 2) SkipBytes(ms, 28); // 3 doubles + 1 int

            var timestamps = cachedTimestamps
                ?? CompressionCodec.DecodeTimestamps(codecs.TimestampCodec, codecs.TimestampCompression, tb!);
            if (tb != null)
                CacheColumn(path, fileLength, i, timestamps, values: null);

            result.Add(new SegmentTimestampColumn(m, tags, f, k, min, max, timestamps));
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
        var dataLength = allBytes.Length - 4;
        if (BitConverter.ToUInt32(allBytes, dataLength) != Crc32.Compute(allBytes.AsSpan(0, dataLength)))
            throw new InvalidDataException("segment CRC mismatch");

        var result = new List<SegmentColumnMeta>();
        using var ms = new MemoryStream(allBytes, 0, dataLength, writable: false);
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

    /// <summary>
    /// Open a segment's column area for sequential reading.
    /// <para>
    /// Small segments keep the original behaviour: the whole file is buffered and its CRC verified up
    /// front. Large segments are streamed through a buffered <see cref="FileStream"/>; their trailing
    /// CRC is verified once per (path, length, mtime) with a chunked sequential pass (see
    /// <see cref="VerifyLargeSegmentCrc"/>), so column skipping still works after the first read.
    /// </para>
    /// </summary>
    private static Stream OpenSegmentForSequentialRead(string path, out long dataLength, out long fileLength)
    {
        var info = new FileInfo(path);
        fileLength = info.Length;
        if (fileLength < 8) throw new InvalidDataException("segment file too small");

        if (fileLength > StreamingReadThresholdBytes)
        {
            dataLength = fileLength - 4;
            VerifyLargeSegmentCrc(path, info, fileLength);
            return new FileStream(path, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete, StreamingBufferBytes, FileOptions.SequentialScan);
        }

        var allBytes = ReadAllBytesShared(path);
        if (allBytes.Length < 8) throw new InvalidDataException("segment file too small");
        dataLength = allBytes.Length - 4;
        // Segments are immutable: verify the CRC once per (path, length, mtime) instead of on
        // every query that reads the file.
        if (!(s_crcVerified.TryGetValue(path, out var stamp)
              && stamp.Length == allBytes.Length && stamp.LastWriteUtc == info.LastWriteTimeUtc))
        {
            var storedCrc = BitConverter.ToUInt32(allBytes, (int)dataLength);
            if (storedCrc != Crc32.Compute(allBytes.AsSpan(0, (int)dataLength)))
                throw new InvalidDataException("segment CRC mismatch");
            s_crcVerified[path] = (allBytes.Length, info.LastWriteTimeUtc);
        }
        return new MemoryStream(allBytes, 0, (int)dataLength, writable: false);
    }

    /// <summary>
    /// Large segments are streamed with column skipping, so a corrupt payload may otherwise never be
    /// read and silently return wrong results. Verify the trailing CRC once per (path, length, mtime)
    /// with a chunked sequential pass; later reads of the same immutable file skip it entirely.
    /// </summary>
    private static void VerifyLargeSegmentCrc(string path, FileInfo info, long fileLength)
    {
        if (s_crcVerified.TryGetValue(path, out var stamp)
            && stamp.Length == fileLength && stamp.LastWriteUtc == info.LastWriteTimeUtc)
            return;

        var dataLength = fileLength - 4;
        var crc = IncrementalCrc32.Create();
        var buffer = new byte[StreamingBufferBytes];
        uint storedCrc;
        using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete, StreamingBufferBytes, FileOptions.SequentialScan))
        {
            long remaining = dataLength;
            while (remaining > 0)
            {
                var chunk = (int)Math.Min(buffer.Length, remaining);
                stream.ReadExactly(buffer, 0, chunk);
                crc.Append(buffer.AsSpan(0, chunk));
                remaining -= chunk;
            }
            stream.ReadExactly(buffer, 0, 4);
            storedCrc = BitConverter.ToUInt32(buffer, 0);
        }

        if (storedCrc != crc.GetResult())
            throw new InvalidDataException("segment CRC mismatch");
        s_crcVerified[path] = (fileLength, info.LastWriteTimeUtc);
    }

    /// <summary>
    /// Advance a segment stream by <paramref name="count"/> bytes. <see cref="MemoryStream"/> and
    /// <see cref="FileStream"/> are both seekable, so this is a cheap position adjustment; for the
    /// streaming case it also means the skipped payload is never materialized.
    /// </summary>
    private static void SkipBytes(Stream stream, long count)
    {
        if (count <= 0) return;
        stream.Seek(count, SeekOrigin.Current);
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

    private static (byte Version, int Count) ReadVersionAndCount(BinaryReader br, Stream ms)
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

    private static void SkipColumnPayload(byte version, BinaryReader br, Stream ms)
    {
        if (version >= 3)
            SkipBytes(ms, 4); // timestamp/value codec + compression ids
        var skipTl = br.ReadInt32();
        SkipBytes(ms, skipTl);
        var skipVl = br.ReadInt32();
        SkipBytes(ms, skipVl);
        if (version >= 2)
            SkipBytes(ms, 28); // 3 doubles + 1 int
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

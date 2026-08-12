using System.IO.Compression;
using System.Text;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class SegmentTests : IDisposable
{
    private readonly string _testDir;

    public SegmentTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public void WriteSegment_ThenReadSegment_RoundtripsCorrectly()
    {
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.5) } },
                TimestampNs = 1000_000_000
            },
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(2.5) } },
                TimestampNs = 2000_000_000
            }
        };

        var segPath = Path.Combine(_testDir, "test.seg");
        SegmentWriter.WriteSegment(segPath, points);

        var columns = SegmentReader.ReadSegment(segPath);

        Assert.Single(columns);
        Assert.Equal("cpu", columns[0].Measurement);
        Assert.Equal("host=server01", columns[0].TagsCanonical);
        Assert.Equal("value", columns[0].Field);
        Assert.Equal(FieldKind.Float, columns[0].Kind);
        Assert.Equal(2, columns[0].Timestamps.Count);
    }

    [Fact]
    public void WriteSegment_CreatesAtomicFile()
    {
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string>(),
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.5) } },
                TimestampNs = 1000_000_000
            }
        };

        var segPath = Path.Combine(_testDir, "test.seg");
        SegmentWriter.WriteSegment(segPath, points);

        Assert.True(File.Exists(segPath));
        Assert.False(File.Exists(segPath + ".tmp")); // tmp file should be renamed
    }

    [Fact]
    public void ReadSegment_CorruptedFile_ThrowsException()
    {
        var segPath = Path.Combine(_testDir, "corrupt.seg");
        File.WriteAllBytes(segPath, new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });

        Assert.Throws<InvalidDataException>(() => SegmentReader.ReadSegment(segPath));
    }

    [Fact]
    public void WriteSegment_MultipleMeasurements_AllWrittenCorrectly()
    {
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.5) } },
                TimestampNs = 1000_000_000
            },
            new Point
            {
                Measurement = "mem",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "used", FieldValue.FromInteger(1024) } },
                TimestampNs = 1000_000_000
            }
        };

        var segPath = Path.Combine(_testDir, "multi.seg");
        SegmentWriter.WriteSegment(segPath, points);

        var columns = SegmentReader.ReadSegment(segPath);

        Assert.Equal(2, columns.Count);
        Assert.Contains(columns, c => c.Measurement == "cpu");
        Assert.Contains(columns, c => c.Measurement == "mem");
    }

    [Fact]
    public void WriteSegment_V3Metadata_ExposesSelectedCodecs()
    {
        var points = Enumerable.Range(0, 256)
            .Select(i => new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(100 + (i / 16) * 0.5) },
                TimestampNs = 1_000_000_000 + i * 1_000_000
            })
            .ToList();

        var segPath = Path.Combine(_testDir, "v3.seg");
        SegmentWriter.WriteSegment(segPath, points);

        var meta = Assert.Single(SegmentReader.ReadMetadata(segPath));
        var column = Assert.Single(SegmentReader.ReadSegment(segPath));

        Assert.Equal(TimestampCodecKind.Gorilla, meta.TimestampCodec);
        Assert.Equal(ValueCodecKind.Gorilla, meta.ValueCodec);
        Assert.Equal(meta.TimestampCodec, column.TimestampCodec);
        Assert.Equal(meta.ValueCodec, column.ValueCodec);
    }

    [Fact]
    public void ReadMetadata_V4Footer_DoesNotRequireColumnPayload()
    {
        var points = Enumerable.Range(0, 128)
            .Select(i => new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(i) },
                TimestampNs = 1_000 + i
            })
            .ToList();

        var segPath = Path.Combine(_testDir, "metadata-footer.seg");
        SegmentWriter.WriteSegment(segPath, points);
        using (var fs = new FileStream(segPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
        {
            fs.Position = 32;
            fs.WriteByte(0xff);
        }

        var meta = Assert.Single(SegmentReader.ReadMetadata(segPath));

        Assert.Equal("cpu", meta.Measurement);
        Assert.Equal("host=server01", meta.TagsCanonical);
        Assert.Equal("value", meta.Field);
        Assert.Equal(128, meta.PointCount);
        Assert.Throws<InvalidDataException>(() => SegmentReader.ReadSegment(segPath));
    }

    [Fact]
    public void ReadSegment_WithColumnPredicatePushdown_SkipsIrrelevantColumnsBeforeDecode()
    {
        var points = new List<Point>
        {
            new()
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(1.5) },
                TimestampNs = 1_000
            },
            new()
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server02" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(2.5) },
                TimestampNs = 2_000
            },
            new()
            {
                Measurement = "mem",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["used"] = FieldValue.FromInteger(1024) },
                TimestampNs = 3_000
            }
        };

        var segPath = Path.Combine(_testDir, "predicate-pushdown.seg");
        SegmentWriter.WriteSegment(segPath, points);

        var columns = SegmentReader.ReadSegment(
            segPath,
            requestedFields: ["value"],
            measurement: "cpu",
            minTimeNs: 1_500,
            maxTimeNs: 2_500,
            allowedTagsCanonical: ["host=server02"]);

        var column = Assert.Single(columns);
        Assert.Equal("cpu", column.Measurement);
        Assert.Equal("host=server02", column.TagsCanonical);
        Assert.Equal("value", column.Field);
        Assert.Single(column.Timestamps);
        Assert.Equal(2_000, column.Timestamps[0]);
        Assert.Equal(2.5, column.Values[0].Float, 10);
    }

    [Fact]
    public void ReadLegacyV2Segment_RemainsCompatible()
    {
        var points = new List<Point>
        {
            new()
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(1.25) },
                TimestampNs = 1_000
            },
            new()
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(2.5) },
                TimestampNs = 2_000
            }
        };

        var segPath = Path.Combine(_testDir, "legacy-v2.seg");
        WriteLegacyV2Segment(segPath, points);

        var column = Assert.Single(SegmentReader.ReadSegment(segPath));
        var meta = Assert.Single(SegmentReader.ReadMetadata(segPath));

        Assert.Equal(TimestampCodecKind.DeltaOfDeltaVarint, column.TimestampCodec);
        Assert.Equal(ValueCodecKind.Legacy, column.ValueCodec);
        Assert.Equal(2, meta.PointCount);
        Assert.Equal(1.25, column.Values[0].Float, 10);
        Assert.Equal(2.5, column.Values[1].Float, 10);
    }

    private static void WriteLegacyV2Segment(string path, List<Point> points)
    {
        const uint magic = 0x4D545344;
        const byte version = 2;

        var grouped = points
            .SelectMany(p => p.Fields.Select(f => new { Series = SeriesKey.From(p), Field = f.Key, Value = f.Value, p.TimestampNs }))
            .GroupBy(x => (x.Series, x.Field))
            .ToList();

        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        using var ms = new MemoryStream();
        using (var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(magic);
            bw.Write(version);
            bw.Write(grouped.Count);
            foreach (var group in grouped)
            {
                var ordered = group.OrderBy(x => x.TimestampNs).ToList();
                var kind = ordered[0].Value.Kind;
                var timestamps = ordered.Select(x => x.TimestampNs).ToArray();
                var values = ordered.Select(x => x.Value).ToArray();
                var timestampBytes = CompressLegacy(CompressionCodec.EncodeTimestamps(timestamps));
                var valueBytes = CompressLegacy(CompressionCodec.EncodeValues(kind, values));

                WriteString(bw, group.Key.Series.Measurement);
                WriteString(bw, group.Key.Series.TagsCanonical);
                WriteString(bw, group.Key.Field);
                bw.Write((byte)kind);
                bw.Write(timestamps[0]);
                bw.Write(timestamps[^1]);
                bw.Write(timestamps.Length);
                bw.Write(timestampBytes.Length);
                bw.Write(timestampBytes);
                bw.Write(valueBytes.Length);
                bw.Write(valueBytes);
                bw.Write(1.25);
                bw.Write(2.5);
                bw.Write(3.75);
                bw.Write(values.Length);
            }
        }

        var data = ms.ToArray();
        fs.Write(data);
        var crcBytes = new byte[4];
        BitConverter.TryWriteBytes(crcBytes, Crc32.Compute(data));
        fs.Write(crcBytes);
    }

    private static byte[] CompressLegacy(byte[] input)
    {
        using var ms = new MemoryStream();
        using (var brotli = new BrotliStream(ms, CompressionLevel.SmallestSize, leaveOpen: true))
            brotli.Write(input);
        return ms.ToArray();
    }

    private static void WriteString(BinaryWriter bw, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        bw.Write(bytes.Length);
        bw.Write(bytes);
    }

    [Fact]
    public async Task Flush_SplitsIntoFilesBoundedByMaxSegmentFileBytes()
    {
        // ponytail: regression for the segment-size cap. With a small cap and far more data than it,
        // FlushLocked must shard the flush into multiple .seg files, none exceeding the cap (within a
        // tolerance for estimation/compression variance). Data must be merged into big files rather
        // than scattered across many tiny ones.
        const long maxSegBytes = 32L * 1024;
        using var engine = new TsdbEngine(_testDir, flushThreshold: 100, compactionIntervalMs: 0, maxSegmentFileBytes: maxSegBytes);

        var pts = new List<Point>();
        for (int i = 0; i < 5000; i++)
        {
            pts.Add(new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(i) }, { "load", FieldValue.FromDouble(i * 0.5) } },
                TimestampNs = (i + 1) * 1_000_000_000L
            });
        }
        await engine.WriteAsync("db", "autogen", pts);
        engine.FlushAll();

        var segFiles = Directory.GetFiles(_testDir, "*.seg", SearchOption.AllDirectories);
        Assert.NotEmpty(segFiles);
        // Should have been split into multiple files (data far exceeds a single cap).
        Assert.True(segFiles.Length >= 2, $"expected multiple segment files, got {segFiles.Length}");
        foreach (var f in segFiles)
        {
            var len = new FileInfo(f).Length;
            Assert.True(len <= maxSegBytes * 2,
                $"segment file {Path.GetFileName(f)} is {len} bytes, exceeds cap tolerance {maxSegBytes * 2}");
        }
    }

    [Fact]
    public async Task Flush_TailMergesInsteadOfTrailingSmallFile()
    {
        // ponytail: regression for the tail-merge rule. With a fill ratio below 1, data that only
        // slightly exceeds the cap must NOT be split into one big file plus a tiny trailing .seg
        // (which would inflate file count x random IO on HDD/network storage). Instead the remainder
        // is packed into the current file. Write ~1.4x the cap.
        const long maxSegBytes = 64L * 1024;
        const double fillRatio = 0.5;
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1_000_000, compactionIntervalMs: 0,
            maxSegmentFileBytes: maxSegBytes, segmentFillRatio: fillRatio);

        // Size points so the columnar on-disk payload lands a bit above 1x cap (≈1.4x). With the
        // columnar estimate (8B ts + 8B/field) each point is small, so we need many points to exceed
        // the 64KB cap; the point is to confirm a single large flush does NOT scatter many tiny files.
        long totalBudget = (long)(maxSegBytes * 1.4);
        var pts = new List<Point>();
        int i = 0;
        long acc = 0;
        while (acc < totalBudget)
        {
            var p = new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(i) }, { "load", FieldValue.FromDouble(i * 0.5) } },
                TimestampNs = (i + 1) * 1_000_000_000L
            };
            pts.Add(p);
            // columnar estimate: 8 (ts) + 2 * 8 (two double fields) + header amortized below.
            acc += 8L + 2L * 8L;
            i++;
        }
        await engine.WriteAsync("db", "autogen", pts);
        engine.FlushAll();

        var segFiles = Directory.GetFiles(_testDir, "*.seg", SearchOption.AllDirectories);
        Assert.NotEmpty(segFiles);
        var lengths = segFiles.Select(f => new FileInfo(f).Length).Order().ToList();
        // ponytail: a single large flush that is only ~1.4x the configured cap must stay as 1 file
        // (tail-merged). With columnar compression the real payload is far smaller than the cap, so the
        // splitter must NOT chop it into one big file plus a tiny trailing .seg — that would inflate the
        // file count x random IO on HDD/network storage. The point is file *count*, not absolute bytes.
        Assert.True(segFiles.Length == 1,
            $"tail-merge failed: expected 1 segment file, got {segFiles.Length} lengths=[{string.Join(",", lengths)}]");
        // The single file must still respect the hard cap (no unbounded growth).
        Assert.True(lengths[0] <= maxSegBytes * 2, $"segment {lengths[0]} exceeds cap tolerance");
    }
}

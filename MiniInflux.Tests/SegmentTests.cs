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
}

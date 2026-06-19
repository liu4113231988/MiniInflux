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
}

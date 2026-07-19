using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class DuplicatePointTests : IDisposable
{
    private readonly string _testDir;
    private readonly TsdbEngine _engine;

    public DuplicatePointTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_dup_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _engine = new TsdbEngine(_testDir, flushThreshold: 100, compactionIntervalMs: 0);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public async Task WriteDuplicatePoints_SameTimestamp_MergesFields()
    {
        // Write two points with same measurement+tags+timestamp but different fields
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
                Fields = new Dictionary<string, FieldValue> { { "temp", FieldValue.FromDouble(42.0) } },
                TimestampNs = 1000_000_000
            }
        };

        await _engine.WriteAsync("testdb", "autogen", points);

        var result = _engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        Assert.Single(result); // Should be merged into one point
        Assert.Equal(1000_000_000, result[0].TimestampNs);
        Assert.Equal(2, result[0].Fields.Count); // Both fields present
        Assert.Equal(1.5, result[0].Fields["value"].Float);
        Assert.Equal(42.0, result[0].Fields["temp"].Float);
    }

    [Fact]
    public async Task WriteDuplicatePoints_SameField_LastWriteWins()
    {
        // Write two points with same measurement+tags+timestamp+field - last value wins
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
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(9.9) } },
                TimestampNs = 1000_000_000
            }
        };

        await _engine.WriteAsync("testdb", "autogen", points);

        var result = _engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        Assert.Single(result);
        Assert.Equal(9.9, result[0].Fields["value"].Float); // Last write wins
    }

    [Fact]
    public async Task WriteDuplicatePoint_AfterFlush_OverwritesExistingValue()
    {
        await _engine.WriteAsync("testdb", "autogen", [Point(1.5, 1000_000_000)]);
        _engine.FlushAll();
        await _engine.WriteAsync("testdb", "autogen", [Point(9.9, 1000_000_000)]);

        var read = _engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        var streamed = _engine.EnumeratePoints("testdb", "autogen", "cpu", null, null).ToList();

        Assert.Single(read);
        Assert.Equal(9.9, read[0].Fields["value"].Float);
        Assert.Single(streamed);
        Assert.Equal(9.9, streamed[0].Fields["value"].Float);
    }

    [Fact]
    public async Task WriteDuplicatePoint_AfterFlush_DescendingReadUsesNewerValue()
    {
        // Regression: when a point is flushed to a segment, then a new value for the same
        // timestamp arrives in the buffer, the descending read path must return the newer
        // buffer value instead of overwriting it with the older segment value.
        await _engine.WriteAsync("testdb", "autogen", [Point(1.5, 1000_000_000)]);
        _engine.FlushAll();
        await _engine.WriteAsync("testdb", "autogen", [Point(9.9, 1000_000_000)]);

        // Use the streaming descending path (TryReadSeriesDescending), which is where the bug manifested.
        var descending = _engine.TryReadSeriesDescending("testdb", "autogen", "cpu", "host=server01", null, null);
        Assert.NotNull(descending);
        var dp = Assert.Single(descending.Points);
        Assert.Equal(9.9, dp.Fields["value"].Float);

        // The QueryExecutor descending path must also return the newer value.
        var response = await new QueryExecutor().ExecuteAsync(_engine, "testdb", "SELECT value FROM cpu");
        var series = Assert.Single(response.Results[0].Series!);
        var row = Assert.Single(series.Values);
        Assert.Equal(9.9, row[^1]);
    }

    [Fact]
    public async Task WriteOutOfOrderDuplicateAcrossBatches_FlushesOnlyNewestValue()
    {
        await _engine.WriteAsync("testdb", "autogen", [Point(2, 2_000_000_000)]);
        await _engine.WriteAsync("testdb", "autogen", [Point(1, 1_000_000_000)]);
        await _engine.WriteAsync("testdb", "autogen", [Point(99, 2_000_000_000)]);
        _engine.FlushAll();

        var segment = Assert.Single(Directory.GetFiles(Path.Combine(_testDir, "db"), "*.seg", SearchOption.AllDirectories));
        var column = Assert.Single(SegmentReader.ReadSegment(segment));
        Assert.Equal([1_000_000_000, 2_000_000_000], column.Timestamps);
        Assert.Equal([1.0, 99.0], column.Values.Select(value => value.AsDouble()));
    }

    [Fact]
    public async Task WriteDuplicatePoints_DifferentTimestamps_NotMerged()
    {
        // Points with different timestamps should NOT be merged
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

        await _engine.WriteAsync("testdb", "autogen", points);

        var result = _engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        Assert.Equal(2, result.Count); // Two separate points
    }

    [Fact]
    public async Task WriteDuplicatePoints_DifferentTags_NotMerged()
    {
        // Points with different tags should NOT be merged
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
                Tags = new Dictionary<string, string> { { "host", "server02" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(2.5) } },
                TimestampNs = 1000_000_000
            }
        };

        await _engine.WriteAsync("testdb", "autogen", points);

        var result = _engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        Assert.Equal(2, result.Count);
    }

    private static Point Point(double value, long timestampNs) => new()
    {
        Measurement = "cpu",
        Tags = new Dictionary<string, string> { { "host", "server01" } },
        Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(value) } },
        TimestampNs = timestampNs
    };
}

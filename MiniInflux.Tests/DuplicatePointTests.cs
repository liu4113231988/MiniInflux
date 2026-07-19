using MiniInflux.Net10.Model;
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
        _engine = new TsdbEngine(_testDir, flushThreshold: 100);
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

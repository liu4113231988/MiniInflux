using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class IndexTests : IDisposable
{
    private readonly string _testDir;
    private readonly TsdbEngine _engine;

    public IndexTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_idx_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _engine = new TsdbEngine(_testDir, flushThreshold: 1000);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public void SeriesIndex_TracksSeriesAfterWrite()
    {
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" }, { "region", "us-east" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.0) } },
                TimestampNs = 1000_000_000
            },
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server02" }, { "region", "us-west" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(2.0) } },
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

        _engine.WriteAsync("testdb", "autogen", points).Wait();

        // Check series index via Manifest
        var cpuSeries = _engine.Meta.GetSeries("testdb", "cpu");
        Assert.Equal(2, cpuSeries.Count);

        var memSeries = _engine.Meta.GetSeries("testdb", "mem");
        Assert.Single(memSeries);

        var allSeries = _engine.Meta.GetSeries("testdb", null);
        Assert.Equal(3, allSeries.Count);
    }

    [Fact]
    public void TagIndex_TracksTagKeysAndValues()
    {
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" }, { "region", "us-east" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.0) } },
                TimestampNs = 1000_000_000
            },
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server02" }, { "region", "us-west" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(2.0) } },
                TimestampNs = 1000_000_000
            }
        };

        _engine.WriteAsync("testdb", "autogen", points).Wait();

        // Check tag index
        var hostValues = _engine.Meta.GetTagValues("testdb", "cpu", "host");
        Assert.Equal(2, hostValues.Count);
        Assert.Contains(hostValues, x => x.Key == "host" && x.Value == "server01");
        Assert.Contains(hostValues, x => x.Key == "host" && x.Value == "server02");

        var regionValues = _engine.Meta.GetTagValues("testdb", "cpu", "region");
        Assert.Equal(2, regionValues.Count);
        Assert.Contains(regionValues, x => x.Key == "region" && x.Value == "us-east");
        Assert.Contains(regionValues, x => x.Key == "region" && x.Value == "us-west");
    }

    [Fact]
    public void ListTagValues_UsesIndex()
    {
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.0) } },
                TimestampNs = 1000_000_000
            },
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server02" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(2.0) } },
                TimestampNs = 1000_000_000
            }
        };

        _engine.WriteAsync("testdb", "autogen", points).Wait();

        var tagValues = _engine.ListTagValues("testdb", "cpu", "host");
        Assert.Equal(2, tagValues.Count);
    }

    [Fact]
    public void DropMeasurement_RemovesIndexEntries()
    {
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.0) } },
                TimestampNs = 1000_000_000
            }
        };

        _engine.WriteAsync("testdb", "autogen", points).Wait();

        // Verify index exists
        Assert.NotEmpty(_engine.Meta.GetSeries("testdb", "cpu"));

        // Drop measurement
        _engine.DropMeasurement("testdb", "cpu");

        // Verify index is cleared
        Assert.Empty(_engine.Meta.GetSeries("testdb", "cpu"));
        Assert.Empty(_engine.Meta.GetTagValues("testdb", "cpu", "host"));
    }

    [Fact]
    public void ProjectionPushdown_OnlyReadsRequestedFields()
    {
        // Write points with multiple fields
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue>
                {
                    { "value", FieldValue.FromDouble(1.0) },
                    { "temp", FieldValue.FromDouble(42.0) },
                    { "load", FieldValue.FromDouble(0.5) }
                },
                TimestampNs = 1000_000_000
            }
        };

        _engine.WriteAsync("testdb", "autogen", points).Wait();

        // Read with field filter - only "value"
        var requestedFields = new HashSet<string>(StringComparer.Ordinal) { "value" };
        var result = _engine.ReadAllPoints("testdb", "autogen", "cpu", null, null, requestedFields);

        Assert.Single(result);
        Assert.Single(result[0].Fields); // Only "value" should be read
        Assert.True(result[0].Fields.ContainsKey("value"));
    }
}

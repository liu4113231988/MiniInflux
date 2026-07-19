using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class AggregationTests : IDisposable
{
    private readonly string _testDir;
    private readonly TsdbEngine _engine;
    private readonly QueryExecutor _executor;

    public AggregationTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_agg_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _engine = new TsdbEngine(_testDir, flushThreshold: 100);
        _executor = new QueryExecutor();
        SeedData();
    }

    private void SeedData()
    {
        var points = new List<Point>();
        // 10 points with values 1..10 at 1-second intervals
        for (int i = 1; i <= 10; i++)
        {
            points.Add(new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(i) } },
                TimestampNs = i * 1_000_000_000L
            });
        }
        _engine.WriteAsync("testdb", "autogen", points).Wait();
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public async Task Spread_ReturnsMaxMinusMin()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SELECT spread(value) FROM cpu GROUP BY time(100s)");
        Assert.Null(result.Results[0].Error);
        var series = result.Results[0].Series!;
        Assert.True(series.Count > 0);
        // spread(1..10) = 10 - 1 = 9
        var val = series[0].Values[0][1];
        Assert.NotNull(val);
        Assert.Equal(9.0, Convert.ToDouble(val));
    }

    [Fact]
    public async Task Stddev_ReturnsValidResult()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SELECT stddev(value) FROM cpu GROUP BY time(100s)");
        Assert.Null(result.Results[0].Error);
        var series = result.Results[0].Series!;
        Assert.True(series.Count > 0);
        var val = Convert.ToDouble(series[0].Values[0][1]);
        // stddev(1..10) ≈ 3.0277
        Assert.True(val > 3.0 && val < 3.1, $"stddev was {val}");
    }

    [Fact]
    public async Task Median_ReturnsMiddleValue()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SELECT median(value) FROM cpu GROUP BY time(100s)");
        Assert.Null(result.Results[0].Error);
        var series = result.Results[0].Series!;
        Assert.True(series.Count > 0);
        var val = Convert.ToDouble(series[0].Values[0][1]);
        // median(1..10) = 5.5 (interpolated between 5 and 6)
        Assert.Equal(5.5, val);
    }

    [Fact]
    public async Task Percentile_ReturnsCorrectValue()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SELECT percentile(value, 90) FROM cpu GROUP BY time(100s)");
        Assert.Null(result.Results[0].Error);
        var series = result.Results[0].Series!;
        Assert.True(series.Count > 0);
        var val = Convert.ToDouble(series[0].Values[0][1]);
        // 90th percentile of 1..10 = 9.1 (interpolated)
        Assert.True(val > 9.0 && val < 10.0, $"percentile(90) was {val}");
    }

    [Fact]
    public async Task CountStar_ReturnsPointCount()
    {
        _engine.FlushAll();

        var result = await _executor.ExecuteAsync(_engine, "testdb", "SELECT count(*) FROM cpu");

        Assert.Null(result.Results[0].Error);
        var row = Assert.Single(result.Results[0].Series![0].Values);
        Assert.Equal(10, Convert.ToInt32(row[1]));
    }

    [Fact]
    public void ParsePercentile_WithSecondArg()
    {
        var q = InfluxQlParser.Parse("SELECT percentile(value, 95) FROM cpu");
        Assert.Equal(QueryKind.Select, q.Kind);
        Assert.Single(q.Select);
        Assert.Equal("percentile", q.Select[0].Func);
        Assert.Equal("value", q.Select[0].Field);
        Assert.Equal(95, q.Select[0].Param);
    }

    [Fact]
    public void ParseMovingAverage_WithSecondArg()
    {
        var q = InfluxQlParser.Parse("SELECT moving_average(value, 5) FROM cpu GROUP BY time(1m)");
        Assert.Equal(QueryKind.Select, q.Kind);
        Assert.Single(q.Select);
        Assert.Equal("moving_average", q.Select[0].Func);
        Assert.Equal("value", q.Select[0].Field);
        Assert.Equal(5, q.Select[0].Param);
    }

    [Fact]
    public void ParseMultipleFunctions_WithCommas()
    {
        var q = InfluxQlParser.Parse("SELECT mean(value), stddev(value), spread(value) FROM cpu GROUP BY time(1m)");
        Assert.Equal(3, q.Select.Count);
        Assert.Equal("mean", q.Select[0].Func);
        Assert.Equal("stddev", q.Select[1].Func);
        Assert.Equal("spread", q.Select[2].Func);
    }
}

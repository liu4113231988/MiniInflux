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

        var outcome = _executor.ExecuteWithReport(_engine, "testdb", "SELECT count(*) FROM cpu");
        var result = outcome.Response;

        Assert.Null(result.Results[0].Error);
        Assert.True(outcome.Report.UsedAggregatePushdown);
        var row = Assert.Single(result.Results[0].Series![0].Values);
        Assert.Equal(10, Convert.ToInt32(row[1]));
    }

    [Fact]
    public async Task CountStar_BypassesPointMaterializationLimit()
    {
        var result = await new QueryExecutor(maxQueryPoints: 1).ExecuteAsync(_engine, "testdb", "SELECT count(*) FROM cpu");

        Assert.Null(result.Results[0].Error);
        Assert.Equal(10, Convert.ToInt32(Assert.Single(result.Results[0].Series![0].Values)[1]));
    }

    [Fact]
    public async Task CountStar_WithMultipleFields_UsesAggregatePushdownPerField()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"miniinflux_countstar_{Guid.NewGuid():N}");
        try
        {
            using var engine = new TsdbEngine(dir, flushThreshold: 1);
            await engine.WriteAsync("testdb", "autogen",
            [
                new Point
                {
                    Measurement = "mem",
                    Tags = new Dictionary<string, string> { ["host"] = "server01" },
                    Fields = new Dictionary<string, FieldValue>
                    {
                        ["free"] = FieldValue.FromDouble(1),
                        ["used"] = FieldValue.FromDouble(2)
                    },
                    TimestampNs = 1
                },
                new Point
                {
                    Measurement = "mem",
                    Tags = new Dictionary<string, string> { ["host"] = "server01" },
                    Fields = new Dictionary<string, FieldValue> { ["free"] = FieldValue.FromDouble(3) },
                    TimestampNs = 2
                }
            ]);
            engine.FlushAll();

            var outcome = new QueryExecutor(maxQueryPoints: 1).ExecuteWithReport(engine, "testdb", "SELECT count(*) FROM mem");

            Assert.Null(outcome.Response.Results[0].Error);
            Assert.True(outcome.Report.UsedAggregatePushdown);
            var series = outcome.Response.Results[0].Series![0];
            var row = Assert.Single(series.Values);
            Assert.Equal(["time", "count_free", "count_used"], series.Columns);
            Assert.Equal(2, Convert.ToInt32(row[1]));
            Assert.Equal(1, Convert.ToInt32(row[2]));
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
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

    [Fact]
    public async Task CountStar_WithOverlappingSegments_UsesFastCountFallback()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"miniinflux_fastcount_{Guid.NewGuid():N}");
        try
        {
            using var engine = new TsdbEngine(dir, flushThreshold: 10000);

            // Write first batch: timestamps 1-5s
            var points1 = new List<Point>();
            for (int i = 1; i <= 5; i++)
            {
                points1.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { { "host", "s1" } },
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(i) } },
                    TimestampNs = i * 1_000_000_000L
                });
            }
            await engine.WriteAsync("testdb", "autogen", points1);
            engine.FlushAll(); // Segment 1: [1s, 5s]

            // Write second batch: timestamps 3-7s (overlaps with segment 1)
            var points2 = new List<Point>();
            for (int i = 3; i <= 7; i++)
            {
                points2.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { { "host", "s1" } },
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(i * 10) } },
                    TimestampNs = i * 1_000_000_000L
                });
            }
            await engine.WriteAsync("testdb", "autogen", points2);
            engine.FlushAll(); // Segment 2: [3s, 7s], overlaps with segment 1

            // count(*) metadata pushdown should fail due to overlapping segments,
            // then the fast count fallback (timestamp-only reads) should be used.
            var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT count(*) FROM cpu");

            Assert.Null(outcome.Response.Results[0].Error);
            Assert.True(outcome.Report.UsedAggregatePushdown);
            var series = outcome.Response.Results[0].Series![0];
            var row = Assert.Single(series.Values);
            // Unique timestamps: 1, 2, 3, 4, 5, 6, 7 = 7 points (deduplicated)
            Assert.Equal(7, Convert.ToInt32(row[1]));
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }

    [Fact]
    public async Task CountStar_WithBufferedPointsOverlappingSegment_UsesFastCountFallback()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"miniinflux_fastcount2_{Guid.NewGuid():N}");
        try
        {
            using var engine = new TsdbEngine(dir, flushThreshold: 10000);

            // Write and flush: timestamps 1-5s
            var points1 = new List<Point>();
            for (int i = 1; i <= 5; i++)
            {
                points1.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { { "host", "s1" } },
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(i) } },
                    TimestampNs = i * 1_000_000_000L
                });
            }
            await engine.WriteAsync("testdb", "autogen", points1);
            engine.FlushAll(); // Segment: [1s, 5s]

            // Write but DON'T flush: timestamps 3-7s (overlaps with segment)
            var points2 = new List<Point>();
            for (int i = 3; i <= 7; i++)
            {
                points2.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { { "host", "s1" } },
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(i * 10) } },
                    TimestampNs = i * 1_000_000_000L
                });
            }
            await engine.WriteAsync("testdb", "autogen", points2);
            // Don't flush! Points stay in buffer, overlapping with segment time range.

            var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT count(*) FROM cpu");

            Assert.Null(outcome.Response.Results[0].Error);
            Assert.True(outcome.Report.UsedAggregatePushdown);
            var series = outcome.Response.Results[0].Series![0];
            var row = Assert.Single(series.Values);
            // Unique timestamps: 1, 2, 3, 4, 5, 6, 7 = 7 (buffered points overwrite segment, dedup)
            Assert.Equal(7, Convert.ToInt32(row[1]));
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }

    [Fact]
    public async Task CountField_WithOverlappingSegments_UsesFastCountFallback()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"miniinflux_fastcount3_{Guid.NewGuid():N}");
        try
        {
            using var engine = new TsdbEngine(dir, flushThreshold: 10000);

            var points1 = new List<Point>();
            for (int i = 1; i <= 5; i++)
            {
                points1.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { { "host", "s1" } },
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(i) } },
                    TimestampNs = i * 1_000_000_000L
                });
            }
            await engine.WriteAsync("testdb", "autogen", points1);
            engine.FlushAll();

            var points2 = new List<Point>();
            for (int i = 3; i <= 7; i++)
            {
                points2.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { { "host", "s1" } },
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(i * 10) } },
                    TimestampNs = i * 1_000_000_000L
                });
            }
            await engine.WriteAsync("testdb", "autogen", points2);
            engine.FlushAll();

            // count(value) uses the same overlap fallback.
            var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT count(value) FROM cpu");

            Assert.Null(outcome.Response.Results[0].Error);
            Assert.True(outcome.Report.UsedAggregatePushdown);
            var series = outcome.Response.Results[0].Series![0];
            var row = Assert.Single(series.Values);
            Assert.Equal(7, Convert.ToInt32(row[1]));
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }

    [Fact]
    public async Task CountField_FastCountFallback_AppliesTombstones()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"miniinflux_fastcount_delete_{Guid.NewGuid():N}");
        try
        {
            using var engine = new TsdbEngine(dir, flushThreshold: 10000);
            await engine.WriteAsync("testdb", "autogen", Enumerable.Range(1, 5).Select(i => new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "s1" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(i) },
                TimestampNs = i
            }).ToList());
            engine.FlushAll();
            await engine.WriteAsync("testdb", "autogen", Enumerable.Range(3, 5).Select(i => new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "s1" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(i) },
                TimestampNs = i
            }).ToList());
            engine.FlushAll();
            engine.DeleteFromMeasurement("testdb", "cpu", minTime: 4, maxTime: 4);

            var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT count(value) FROM cpu");

            Assert.Null(outcome.Response.Results[0].Error);
            Assert.True(outcome.Report.UsedAggregatePushdown);
            var row = Assert.Single(outcome.Response.Results[0].Series![0].Values);
            Assert.Equal(6, Convert.ToInt32(row[1]));
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }

    [Fact]
    public async Task CountStar_WithMultipleFieldsAndOverlaps_CountsPerField()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"miniinflux_fastcount4_{Guid.NewGuid():N}");
        try
        {
            using var engine = new TsdbEngine(dir, flushThreshold: 10000);

            // Points 1-5: both "free" and "used" fields
            var points1 = new List<Point>();
            for (int i = 1; i <= 5; i++)
            {
                points1.Add(new Point
                {
                    Measurement = "mem",
                    Tags = new Dictionary<string, string> { { "host", "s1" } },
                    Fields = new Dictionary<string, FieldValue>
                    {
                        { "free", FieldValue.FromDouble(i) },
                        { "used", FieldValue.FromDouble(i * 2) }
                    },
                    TimestampNs = i * 1_000_000_000L
                });
            }
            await engine.WriteAsync("testdb", "autogen", points1);
            engine.FlushAll();

            // Points 3-7: only "free" field (overlapping timestamps)
            var points2 = new List<Point>();
            for (int i = 3; i <= 7; i++)
            {
                points2.Add(new Point
                {
                    Measurement = "mem",
                    Tags = new Dictionary<string, string> { { "host", "s1" } },
                    Fields = new Dictionary<string, FieldValue> { { "free", FieldValue.FromDouble(i * 10) } },
                    TimestampNs = i * 1_000_000_000L
                });
            }
            await engine.WriteAsync("testdb", "autogen", points2);
            engine.FlushAll();

            var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT count(*) FROM mem");

            Assert.Null(outcome.Response.Results[0].Error);
            Assert.True(outcome.Report.UsedAggregatePushdown);
            var series = outcome.Response.Results[0].Series![0];
            var row = Assert.Single(series.Values);
            Assert.Equal(["time", "count_free", "count_used"], series.Columns);
            // free: timestamps 1-7 = 7 (all points have "free")
            Assert.Equal(7, Convert.ToInt32(row[1]));
            // used: timestamps 1-5 = 5 (only first batch has "used")
            Assert.Equal(5, Convert.ToInt32(row[2]));
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }
}

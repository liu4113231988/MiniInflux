using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class MediumPriorityFixTests : IDisposable
{
    private readonly string _testDir;

    public MediumPriorityFixTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_medium_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public void Parser_ParsesShowSeriesAndOffset()
    {
        var show = InfluxQlParser.Parse("SHOW SERIES FROM cpu");
        Assert.Equal(QueryKind.ShowSeries, show.Kind);
        Assert.Equal("cpu", show.Measurement);

        var select = InfluxQlParser.Parse("SELECT value FROM cpu OFFSET 2 LIMIT 5");
        Assert.Equal(2, select.Offset);
        Assert.Equal(5, select.Limit);
    }

    [Fact]
    public async Task ShowSeriesAndCardinality_ReturnExpectedValues()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1, "server01", 1),
            Point("cpu", "value", 2, "server02", 2),
            Point("mem", "used", 3, "server01", 3)
        ]);
        var executor = new QueryExecutor();

        var series = await executor.ExecuteAsync(engine, "testdb", "SHOW SERIES FROM cpu");
        Assert.Equal(2, series.Results[0].Series![0].Values.Count);

        var cardinality = await executor.ExecuteAsync(engine, "testdb", "SHOW SERIES CARDINALITY FROM cpu");
        Assert.Equal(2, cardinality.Results[0].Series![0].Values[0][0]);

        var measurements = await executor.ExecuteAsync(engine, "testdb", "SHOW MEASUREMENT CARDINALITY");
        Assert.Equal(2, measurements.Results[0].Series![0].Values[0][0]);
    }

    [Fact]
    public async Task Offset_SkipsRows()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1, "server01", 1),
            Point("cpu", "value", 2, "server01", 2),
            Point("cpu", "value", 3, "server01", 3)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT value FROM cpu OFFSET 1 LIMIT 1");

        var row = Assert.Single(response.Results[0].Series![0].Values);
        Assert.Equal(2.0, row[^1]);
    }

    [Fact]
    public async Task FillLinear_InterpolatesMissingBucket()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 10, "server01", 0),
            Point("cpu", "value", 30, "server01", 120)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT mean(value) FROM cpu WHERE time >= 0 AND time <= 120000000000 GROUP BY time(1m) fill(linear)");

        var values = response.Results[0].Series![0].Values;
        Assert.Equal(3, values.Count);
        Assert.Equal(20.0, values[1][1]);
    }

    [Fact]
    public async Task SequenceFunctions_ReturnPerPointRows()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 10, "server01", 1),
            Point("cpu", "value", 20, "server01", 2),
            Point("cpu", "value", 35, "server01", 3)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT difference(value),cumulative_sum(value) FROM cpu");

        var rows = response.Results[0].Series![0].Values;
        Assert.Equal(3, rows.Count);
        Assert.Equal(10.0, rows[1][1]);
        Assert.Equal(30.0, rows[1][2]);
    }

    [Fact]
    public async Task OrderByDescAndSeriesWindow_AppliesToGroupedResults()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 10, "server01", 0),
            Point("cpu", "value", 20, "server01", 60),
            Point("cpu", "value", 30, "server02", 0),
            Point("cpu", "value", 40, "server02", 60)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT mean(value) FROM cpu WHERE time >= 0 AND time <= 60000000000 GROUP BY time(1m),host ORDER BY time DESC SLIMIT 1 SOFFSET 1");

        var series = Assert.Single(response.Results[0].Series!);
        Assert.Equal("server02", series.Tags!["host"]);
        Assert.Equal(2, series.Values.Count);
        Assert.True(string.CompareOrdinal(series.Values[0][0]?.ToString(), series.Values[1][0]?.ToString()) > 0);
    }

    [Fact]
    public async Task DerivativeWithUnitAndIntegral_ReturnExpectedValues()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 10, "server01", 0),
            Point("cpu", "value", 20, "server01", 30),
            Point("cpu", "value", 50, "server01", 60)
        ]);

        var derivative = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT derivative(value, 1m) FROM cpu ORDER BY time DESC");
        var derivativeRows = derivative.Results[0].Series![0].Values;
        Assert.Equal(2, derivativeRows.Count);
        Assert.Equal(60.0, derivativeRows[0][1]);
        Assert.Equal(20.0, derivativeRows[1][1]);

        var integral = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT integral(value, 1s) FROM cpu");
        var integralRows = integral.Results[0].Series![0].Values;
        Assert.Equal(2, integralRows.Count);
        Assert.Equal(450.0, integralRows[0][1]);
        Assert.Equal(1500.0, integralRows[1][1]);
    }

    [Fact]
    public async Task TopAndBottom_ReturnRankedPoints()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 10, "server01", 1),
            Point("cpu", "value", 30, "server01", 2),
            Point("cpu", "value", 20, "server01", 3)
        ]);

        var top = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT top(value, 2) FROM cpu");
        var topRows = top.Results[0].Series![0].Values;
        Assert.Equal(2, topRows.Count);
        Assert.Equal(30.0, topRows[0][1]);
        Assert.Equal(20.0, topRows[1][1]);

        var bottom = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT bottom(value, 1) FROM cpu");
        var bottomRow = Assert.Single(bottom.Results[0].Series![0].Values);
        Assert.Equal(10.0, bottomRow[1]);
    }

    [Fact]
    public async Task GroupByTag_AllowsTopAndBottomFunctions()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 10, "server01", 1),
            Point("cpu", "value", 30, "server01", 2),
            Point("cpu", "value", 20, "server01", 3),
            Point("cpu", "value", 5, "server02", 1),
            Point("cpu", "value", 15, "server02", 2),
            Point("cpu", "value", 25, "server02", 3)
        ]);

        var top = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT top(value, 2) FROM cpu GROUP BY host");
        Assert.Null(top.Results[0].Error);
        Assert.Equal(2, top.Results[0].Series!.Count);
        var server01Top = Assert.Single(top.Results[0].Series!, s => s.Tags!["host"] == "server01");
        var server02Top = Assert.Single(top.Results[0].Series!, s => s.Tags!["host"] == "server02");
        Assert.Equal(2, server01Top.Values.Count);
        Assert.Equal(30.0, server01Top.Values[0][1]);
        Assert.Equal(20.0, server01Top.Values[1][1]);
        Assert.Equal(2, server02Top.Values.Count);
        Assert.Equal(15.0, server02Top.Values[0][1]);
        Assert.Equal(25.0, server02Top.Values[1][1]);

        var bottom = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT bottom(value, 1) FROM cpu GROUP BY host");
        Assert.Null(bottom.Results[0].Error);
        var server01Bottom = Assert.Single(bottom.Results[0].Series!, s => s.Tags!["host"] == "server01");
        var server02Bottom = Assert.Single(bottom.Results[0].Series!, s => s.Tags!["host"] == "server02");
        Assert.Single(server01Bottom.Values);
        Assert.Equal(10.0, server01Bottom.Values[0][1]);
        Assert.Single(server02Bottom.Values);
        Assert.Equal(5.0, server02Bottom.Values[0][1]);
    }

    [Fact]
    public async Task GroupByTime_AllowsSampleFunction()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1, "server01", 1),
            Point("cpu", "value", 2, "server01", 2),
            Point("cpu", "value", 3, "server01", 3),
            Point("cpu", "value", 10, "server01", 61),
            Point("cpu", "value", 20, "server01", 62),
            Point("cpu", "value", 30, "server01", 63)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(
            engine,
            "testdb",
            "SELECT sample(value, 2) FROM cpu WHERE time >= 0 AND time <= 120000000000 GROUP BY time(1m)");

        Assert.Null(response.Results[0].Error);
        var series = Assert.Single(response.Results[0].Series!);
        Assert.Equal(4, series.Values.Count);
        Assert.Equal(1.0, series.Values[0][1]);
        Assert.Equal(3.0, series.Values[1][1]);
        Assert.Equal(10.0, series.Values[2][1]);
        Assert.Equal(30.0, series.Values[3][1]);
    }

    [Fact]
    public async Task DropSeries_RemovesOnlyMatchingSeries()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1, "server01", 1),
            Point("cpu", "value", 2, "server02", 2)
        ]);

        await new QueryExecutor().ExecuteAsync(engine, "testdb", "DROP SERIES FROM cpu WHERE host='server01'");

        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        var point = Assert.Single(points);
        Assert.Equal("server02", point.Tags["host"]);
    }

    [Fact]
    public async Task QueryPointLimit_ReturnsError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1, "server01", 1),
            Point("cpu", "value", 2, "server01", 2)
        ]);

        var response = await new QueryExecutor(maxQueryPoints: 1).ExecuteAsync(engine, "testdb", "SELECT value FROM cpu");

        Assert.Contains("query point limit", response.Results[0].Error);
    }

    [Fact]
    public async Task BufferLimit_RejectsWrites()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000, maxBufferPoints: 1);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", "value", 1, "server01", 1)]);

        await Assert.ThrowsAsync<MemoryLimitExceededException>(() =>
            engine.WriteAsync("testdb", "autogen", [Point("cpu", "value", 2, "server01", 2)]));
    }

    private static Point Point(string measurement, string field, double value, string host, long seconds) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { [field] = FieldValue.FromDouble(value) },
        TimestampNs = seconds * 1_000_000_000
    };
}

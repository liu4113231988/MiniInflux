using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Regression tests for the P1 query-semantics items in docs/todo-202610.md:
/// GROUP BY with raw/DISTINCT items, count(distinct) in grouped paths, row-expanding
/// functions under GROUP BY, fill/alias/SLIMIT/INTO edge cases and the LVC raw schema.
/// </summary>
public class P1QuerySemanticsTests : IDisposable
{
    private readonly string _testDir;

    public P1QuerySemanticsTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p1q_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    // ---- P1-1 GROUP BY with non-aggregate items ----

    [Fact]
    public async Task RawSelect_GroupByTag_ReturnsFieldColumnsPerSeries()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 2, "server01", 2_000_000_000L),
            PointAt("cpu", 3, "server02", 1_000_000_000L)
        ]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT value FROM cpu GROUP BY host");

        Assert.Null(result.Results[0].Error);
        var series = result.Results[0].Series!;
        Assert.Equal(2, series.Count);
        Assert.All(series, s => Assert.Equal(["time", "value"], s.Columns));
        var server01 = Assert.Single(series, s => s.Tags!["host"] == "server01");
        Assert.Equal(2, server01.Values.Count);
        Assert.Equal([1.0, 2.0], server01.Values.Select(row => Convert.ToDouble(row[1])).ToArray());
    }

    [Fact]
    public async Task RawSelect_GroupByAllTags_ReturnsFieldColumnsPerSeries()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 2, "server02", 1_000_000_000L)
        ]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT * FROM cpu GROUP BY *");

        Assert.Null(result.Results[0].Error);
        var series = result.Results[0].Series!;
        Assert.Equal(2, series.Count);
        Assert.All(series, s => Assert.Equal(["time", "value"], s.Columns));
    }

    [Fact]
    public async Task RawSelect_GroupByTime_ReturnsExplicitError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT value FROM cpu GROUP BY time(1s)");

        Assert.Contains("GROUP BY requires at least one aggregate function", result.Results[0].Error);
        Assert.Null(result.Results[0].Series);
    }

    [Fact]
    public async Task MixedAggregateAndRawSelect_ReturnsExplicitError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT value, mean(value) FROM cpu GROUP BY host");

        Assert.Contains("mixing aggregate and non-aggregate queries is not supported", result.Results[0].Error);
    }

    [Fact]
    public async Task MultipleDistinctSelect_ReturnsExplicitError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT DISTINCT value, DISTINCT host FROM cpu");

        Assert.Contains("DISTINCT() queries must select a single field", result.Results[0].Error);
    }

    [Fact]
    public async Task DistinctWithGroupByTime_ReturnsExplicitError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT DISTINCT value FROM cpu GROUP BY time(1s)");

        Assert.Contains("DISTINCT() cannot be combined with GROUP BY time()", result.Results[0].Error);
    }

    // ---- P1-2 count(DISTINCT field) in grouped paths ----

    [Fact]
    public async Task CountDistinct_GroupedByTag_MatchesUngroupedDistinctCount()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 1, "server01", 2_000_000_000L),
            PointAt("cpu", 2, "server01", 3_000_000_000L)
        ]);

        var ungrouped = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT count(distinct value) FROM cpu");
        var grouped = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT count(distinct value) FROM cpu GROUP BY host");

        Assert.Null(ungrouped.Results[0].Error);
        Assert.Null(grouped.Results[0].Error);
        Assert.Equal(2, Convert.ToInt32(ungrouped.Results[0].Series![0].Values[0][1]));
        Assert.Equal(2, Convert.ToInt32(Assert.Single(grouped.Results[0].Series!).Values[0][1]));
    }

    // ---- P1-3 row-expanding functions under GROUP BY ----

    [Fact]
    public async Task Difference_GroupedByTag_KeepsPerPointRows()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 3, "server01", 2_000_000_000L),
            PointAt("cpu", 6, "server01", 3_000_000_000L)
        ]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT difference(value) FROM cpu GROUP BY host");

        Assert.Null(result.Results[0].Error);
        var rows = Assert.Single(result.Results[0].Series!).Values;
        Assert.Equal([2.0, 3.0], rows.Select(row => Convert.ToDouble(row[1])).ToArray());
    }

    [Fact]
    public async Task Elapsed_GroupedByTag_ReturnsDeltasInsteadOfNull()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 3, "server01", 3_000_000_000L)
        ]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT elapsed(value) FROM cpu GROUP BY host");

        Assert.Null(result.Results[0].Error);
        var rows = Assert.Single(result.Results[0].Series!).Values;
        Assert.Equal(2.0, Convert.ToDouble(Assert.Single(rows)[1]));
    }

    // ---- P1-4 fill, alias, SLIMIT, count(*) and SELECT INTO ----

    [Fact]
    public async Task FillZero_ParsesZeroWordAndSpacedClause()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 5, "server01", 1_000_000_000L),
            PointAt("cpu", 7, "server01", 3_000_000_000L)
        ]);

        foreach (var query in new[]
        {
            "SELECT mean(value) FROM cpu WHERE time >= 0 AND time <= 3999999999 GROUP BY time(1s) fill(zero)",
            "SELECT mean(value) FROM cpu WHERE time >= 0 AND time <= 3999999999 GROUP BY time(1s) fill (0)"
        })
        {
            var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", query);
            Assert.Null(result.Results[0].Error);
            var rows = Assert.Single(result.Results[0].Series!).Values;
            Assert.Equal(4, rows.Count);
            Assert.Equal(0, Convert.ToInt32(rows[0][1]));
            Assert.Equal(5.0, Convert.ToDouble(rows[1][1]));
            Assert.Equal(0, Convert.ToInt32(rows[2][1]));
            Assert.Equal(7.0, Convert.ToDouble(rows[3][1]));
        }
    }

    [Fact]
    public async Task FillPrevious_ContinuesAcrossBucketWithNullAggregate()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 5, "server01", 1_000_000_000L),
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["other"] = FieldValue.FromDouble(1) },
                TimestampNs = 2_000_000_000L
            }
        ]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT mean(value) FROM cpu WHERE time >= 0 AND time <= 3999999999 GROUP BY time(1s) fill(previous)");

        Assert.Null(result.Results[0].Error);
        var rows = Assert.Single(result.Results[0].Series!).Values;
        Assert.Equal(4, rows.Count);
        Assert.Null(rows[0][1]);
        Assert.Equal(5.0, Convert.ToDouble(rows[1][1]));
        Assert.Null(rows[2][1]);
        Assert.Equal(5.0, Convert.ToDouble(rows[3][1]));
    }

    [Fact]
    public async Task RawSelect_WithAlias_UsesAliasColumnAndKeepsValue()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 42, "server01", 1_000_000_000L)]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT value AS v FROM cpu");

        Assert.Null(result.Results[0].Error);
        var series = Assert.Single(result.Results[0].Series!);
        Assert.Contains("v", series.Columns);
        Assert.DoesNotContain("value", series.Columns);
        Assert.Equal(42.0, Convert.ToDouble(series.Values[0][series.Columns.IndexOf("v")]));
    }

    [Fact]
    public async Task SeriesWindow_AppliesToRawGroupedAndTimeOnlyAggregates()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 2, "server02", 1_000_000_000L),
            PointAt("cpu", 3, "server03", 1_000_000_000L)
        ]);

        var rawGrouped = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT value FROM cpu GROUP BY host SLIMIT 1 SOFFSET 1");
        Assert.Null(rawGrouped.Results[0].Error);
        var rawSeries = Assert.Single(rawGrouped.Results[0].Series!);
        Assert.Equal("server02", rawSeries.Tags!["host"]);

        var aggregate = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT count(value) FROM cpu GROUP BY time(1s) SLIMIT 0");
        Assert.Null(aggregate.Results[0].Error);
        Assert.Empty(aggregate.Results[0].Series!);
    }

    [Fact]
    public async Task CountStar_GroupedByTag_CountsEachFieldInsteadOfZero()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
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
                TimestampNs = 1_000_000_000L
            },
            new Point
            {
                Measurement = "mem",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["free"] = FieldValue.FromDouble(3) },
                TimestampNs = 2_000_000_000L
            }
        ]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT count(*) FROM mem GROUP BY host");

        Assert.Null(result.Results[0].Error);
        var series = Assert.Single(result.Results[0].Series!);
        Assert.Equal(["time", "count_free", "count_used"], series.Columns);
        var row = Assert.Single(series.Values);
        Assert.Equal(2, Convert.ToInt32(row[1]));
        Assert.Equal(1, Convert.ToInt32(row[2]));
    }

    [Fact]
    public async Task SelectInto_ReturnsWrittenStatsAndUsesTargetDefaultRetentionPolicy()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("archive");
        engine.Meta.CreateRetentionPolicy("archive", "daily", durationNs: 0, isDefault: true);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 2, "server01", 2_000_000_000L)
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT value INTO archive.cpu FROM cpu");

        Assert.Null(outcome.Response.Results[0].Error);
        var series = Assert.Single(outcome.Response.Results[0].Series!);
        Assert.Equal("result", series.Name);
        Assert.Equal(["time", "written"], series.Columns);
        Assert.Equal(2, Convert.ToInt32(series.Values[0][1]));

        Assert.Equal(2, engine.ReadAllPoints("archive", "daily", "cpu", null, null).Count);
        Assert.Empty(engine.ReadAllPoints("archive", "autogen", "cpu", null, null));
    }

    // ---- P1-5 Last Value Cache raw schema ----

    [Fact]
    public async Task RawLastViaCache_ReturnsSameColumnsAsScan()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 2, "server02", 2_000_000_000L)
        ]);

        var cached = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT * FROM cpu ORDER BY time DESC LIMIT 1");
        // A field predicate disables the LVC path, forcing the regular scan.
        var scanned = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT * FROM cpu WHERE value > 0 ORDER BY time DESC LIMIT 1");

        Assert.True(cached.Report.UsedLastValueCache);
        Assert.False(scanned.Report.UsedLastValueCache);
        Assert.Null(cached.Response.Results[0].Error);
        Assert.Null(scanned.Response.Results[0].Error);
        var cachedSeries = Assert.Single(cached.Response.Results[0].Series!);
        var scannedSeries = Assert.Single(scanned.Response.Results[0].Series!);
        Assert.Equal(scannedSeries.Columns, cachedSeries.Columns);
        Assert.Equal(2.0, Convert.ToDouble(cachedSeries.Values[0][cachedSeries.Columns.IndexOf("value")]));
    }

    private static Point PointAt(string measurement, double value, string host, long timestampNs) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };
}

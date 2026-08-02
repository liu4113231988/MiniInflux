using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class RecentCommitRegressionTests : IDisposable
{
    private readonly string _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_recent_{Guid.NewGuid():N}");

    public RecentCommitRegressionTests() => Directory.CreateDirectory(_testDir);

    public void Dispose() => Directory.Delete(_testDir, recursive: true);

    [Fact]
    public void ParseWhere_ParenthesizedOr_PreservesCommonTimeFilter()
    {
        var query = InfluxQlParser.Parse("SELECT value FROM cpu WHERE time >= 1s AND (host='a' OR host='b')");

        Assert.Equal(1_000_000_000, query.MinTimeNs);
        Assert.True(query.HasOrFilters);
        Assert.Equal(2, query.OrTagFilterGroups.Count);
    }

    [Fact]
    public async Task Delete_WithOr_DeletesOnlyMatchingSeries()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("db", "autogen",
        [
            Point("a", 1, 1),
            Point("b", 2, 2),
            Point("c", 3, 3)
        ]);

        await new QueryExecutor().ExecuteAsync(engine, "db", "DELETE FROM cpu WHERE host='a' OR host='b'");

        var remaining = Assert.Single(engine.ReadAllPoints("db", "autogen", "cpu", null, null));
        Assert.Equal("c", remaining.Tags["host"]);
    }

    [Fact]
    public async Task Distinct_AppliesBeforeLimit_AndCountDistinctBypassesCountPushdown()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 2);
        await engine.WriteAsync("db", "autogen",
        [
            Point("a", 1, 1),
            Point("a", 1, 2),
            Point("a", 2, 3),
            Point("a", 3, 4)
        ]);
        engine.FlushAll();
        var executor = new QueryExecutor();

        var distinct = await executor.ExecuteAsync(engine, "db", "SELECT DISTINCT(value) FROM cpu LIMIT 2");
        Assert.Equal([1.0, 2.0], distinct.Results[0].Series![0].Values.Select(row => row[^1]).ToArray());

        var count = executor.ExecuteWithReport(engine, "db", "SELECT COUNT(DISTINCT value) FROM cpu");
        Assert.Equal(3L, count.Response.Results[0].Series![0].Values[0][1]);
        Assert.False(count.Report.UsedAggregatePushdown);
    }

    [Fact]
    public async Task CountDistinct_SupportsStringFields()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("db", "autogen",
        [
            StringPoint("x", 1),
            StringPoint("x", 2),
            StringPoint("y", 3)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "db", "SELECT COUNT(DISTINCT status) FROM events");

        Assert.Equal(2L, response.Results[0].Series![0].Values[0][1]);
    }

    [Fact]
    public async Task GroupByTagDescendingLimit_AppliesPerRequestedTagGroup()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 2);
        await engine.WriteAsync("db", "autogen",
        [
            Point("a", 1, 1, "east"),
            Point("a", 2, 2, "west"),
            Point("b", 3, 3, "east"),
            Point("b", 4, 4, "west")
        ]);
        engine.FlushAll();

        var response = await new QueryExecutor().ExecuteAsync(engine, "db",
            "SELECT * FROM cpu WHERE host='a' OR host='b' GROUP BY host ORDER BY time DESC LIMIT 1");

        Assert.Equal(2, response.Results[0].Series!.Count);
        Assert.All(response.Results[0].Series!, series => Assert.Single(series.Values));
        Assert.Equal(["a", "b"], response.Results[0].Series!.Select(series => series.Tags!["host"]).Order().ToArray());
    }

    [Fact]
    public async Task ShowFilters_UseRegexIntentAndAssociatedSeries()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("db", "autogen",
        [
            Point("a", 1, 1, "east", "cpu"),
            Point("b", 2, 2, "west", "cpu"),
            Point("c", 3, 3, "east", "cpu_total")
        ]);
        var executor = new QueryExecutor();

        var measurements = await executor.ExecuteAsync(engine, "db", "SHOW MEASUREMENTS WITH MEASUREMENT =~ /^cpu$/");
        Assert.Equal("cpu", Assert.Single(measurements.Results[0].Series![0].Values)[0]);

        var values = await executor.ExecuteAsync(engine, "db", "SHOW TAG VALUES FROM cpu WITH KEY = host WHERE region='east'");
        Assert.Equal("a", Assert.Single(values.Results[0].Series![0].Values)[1]);
    }

    [Fact]
    public async Task ExplainAndMathFunctions_ReturnWithoutRecursionAndTransformEveryPoint()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("db", "autogen", [Point("a", -2, 1), Point("a", 3, 2), Point("a", 1, 3)]);
        var executor = new QueryExecutor();

        var explain = await executor.ExecuteAsync(engine, "db", "EXPLAIN SELECT value FROM cpu");
        Assert.Null(explain.Results[0].Error);
        Assert.Contains(explain.Results[0].Series![0].Values, row => Equals(row[0], "scanned_points") && Equals(row[1], 0L));

        var abs = await executor.ExecuteAsync(engine, "db", "SELECT ABS(value) FROM cpu");
        Assert.Equal([2.0, 3.0, 1.0], abs.Results[0].Series![0].Values.Select(row => row[1]).ToArray());

        var difference = await executor.ExecuteAsync(engine, "db", "SELECT NON_NEGATIVE_DIFFERENCE(value) FROM cpu");
        Assert.Equal(5.0, Assert.Single(difference.Results[0].Series![0].Values)[1]);
    }

    [Fact]
    public async Task Compaction_DropsMetadataForDeletedSegments()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1);
        for (var i = 0; i < 12; i++)
            await engine.WriteAsync("db", "autogen", [Point("a", i, i + 1)]);
        engine.FlushAll();
        var before = engine.GetMetadataCacheStats().CachedCount;

        Assert.True(engine.CompactNow() > 0);

        Assert.True(before >= 12);
        Assert.Equal(0, engine.GetMetadataCacheStats().CachedCount);
    }

    [Fact]
    public async Task KillQuery_CancelsRunningQuery()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: int.MaxValue, rpCheckIntervalMs: 0,
            flushIntervalMs: 0, compactionIntervalMs: 0);
        var points = Enumerable.Range(0, 200_000).Select(i => Point("a", i, i + 1)).ToList();
        await engine.WriteAsync("db", "autogen", points);
        var executor = new QueryExecutor();
        const string queryText = "SELECT ABS(value) FROM cpu";
        var running = Task.Factory.StartNew(() => executor.ExecuteWithReport(engine, "db", queryText),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        long? queryId = null;
        for (var attempt = 0; attempt < 100 && !running.IsCompleted; attempt++)
        {
            var active = await executor.ExecuteAsync(engine, "db", "SHOW QUERIES");
            queryId = active.Results[0].Series![0].Values
                .Where(row => Equals(row[1], queryText))
                .Select(row => (long?)row[0])
                .FirstOrDefault();
            if (queryId.HasValue) break;
            await Task.Yield();
        }

        Assert.NotNull(queryId);
        await executor.ExecuteAsync(engine, "db", $"KILL QUERY {queryId}");
        Assert.True((await running).Report.Canceled);
    }

    private static Point Point(string host, double value, long seconds, string? region = null, string measurement = "cpu") => new()
    {
        Measurement = measurement,
        Tags = region == null
            ? new Dictionary<string, string> { ["host"] = host }
            : new Dictionary<string, string> { ["host"] = host, ["region"] = region },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = seconds * 1_000_000_000
    };

    private static Point StringPoint(string value, long seconds) => new()
    {
        Measurement = "events",
        Tags = [],
        Fields = new Dictionary<string, FieldValue> { ["status"] = FieldValue.FromString(value) },
        TimestampNs = seconds * 1_000_000_000
    };
}

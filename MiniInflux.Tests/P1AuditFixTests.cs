using System.Text;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Regression tests for the 2026-10 audit P1 fixes:
/// C3 — unbounded subquery recursion (StackOverflowException crashes the process).
/// C5 — fill() materialized all bucket rows before the response-row limit could reject them.
/// C4 (revised) — a configured MaxResponseRows of 0 ("unlimited") collapsed to Take(0) on the
/// buffered query paths, silently returning no rows.
/// </summary>
public class P1AuditFixTests : IDisposable
{
    private readonly string _testDir;

    public P1AuditFixTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p1fix_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    private static Point PointAt(double value, long timestampNs) => new()
    {
        Measurement = "cpu",
        Tags = new Dictionary<string, string> { ["host"] = "server01" },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };

    private static string BuildNestedSubquery(int depth)
    {
        var query = new StringBuilder("SELECT max(value) FROM cpu");
        for (var i = 0; i < depth; i++)
        {
            query.Insert(0, "SELECT max(value) FROM (");
            query.Append(')');
        }
        return query.ToString();
    }

    [Fact]
    public async Task DeeplyNestedSubquery_ReturnsExplicitErrorInsteadOfCrashing()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        await engine.WriteAsync("testdb", "autogen", [PointAt(1, 1_000_000_000L)]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            BuildNestedSubquery(QueryPermission.MaxSubqueryDepth + 5));

        var error = Assert.Single(response.Results).Error;
        Assert.Contains("subquery nesting exceeds the maximum depth", error);
    }

    [Fact]
    public async Task NestedSubquery_WithinDepthLimit_StillExecutes()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        for (var i = 1; i <= 3; i++)
            await engine.WriteAsync("testdb", "autogen", [PointAt(i, i * 1_000_000_000L)]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT mean(max_value) FROM (SELECT max(value) FROM (SELECT value FROM cpu) GROUP BY time(10s)) GROUP BY time(20s)");

        Assert.Null(response.Results[0].Error);
        var row = Assert.Single(Assert.Single(response.Results[0].Series!).Values);
        Assert.Equal(3.0, Convert.ToDouble(row[1])); // max(3 points)=3, mean over one 20s bucket=3
    }

    [Fact]
    public async Task Fill_WithTinyStepOverWideRange_ErrorsBeforeMaterializingBuckets()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        await engine.WriteAsync("testdb", "autogen", [PointAt(1, 1_000_000_000L)]);
        engine.FlushAll();

        // 1ns buckets across an hour would materialize 3.6 trillion rows per series before the
        // response-row limit could reject them; the fill budget must reject the query first.
        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT mean(value) FROM cpu WHERE time >= 1000000000 AND time <= 3700000000000 GROUP BY time(1ns) fill(previous)");

        var error = Assert.Single(response.Results).Error;
        Assert.Contains("response row limit", error);
    }

    [Fact]
    public async Task Fill_WithReasonableStep_StillFillsBuckets()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        await engine.WriteAsync("testdb", "autogen", [PointAt(1, 1_000_000_000L)]);
        engine.FlushAll();

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb",
            "SELECT mean(value) FROM cpu WHERE time >= 1000000000 AND time <= 31000000000 GROUP BY time(10s) fill(previous)");

        Assert.Null(response.Results[0].Error);
        var series = Assert.Single(response.Results[0].Series!);
        Assert.Equal(4, series.Values.Count); // buckets at 1s, 11s, 21s, 31s
    }

    [Fact]
    public async Task MaxResponseRowsZero_MeansUnlimited_ReturnsAllRows()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        for (var i = 1; i <= 3; i++)
            await engine.WriteAsync("testdb", "autogen", [PointAt(i, i * 1_000_000_000L)]);
        engine.FlushAll();

        // Segments => buffered (non-streaming) query path. A 0 cap means unlimited, not Take(0).
        var response = await new QueryExecutor(maxResponseRows: 0).ExecuteAsync(engine, "testdb", "SELECT value FROM cpu");

        Assert.Null(response.Results[0].Error);
        var series = Assert.Single(response.Results[0].Series!);
        Assert.Equal(3, series.Values.Count);
    }

    [Fact]
    public async Task MaxResponseRowsZero_MeansUnlimited_GroupedQueryReturnsAllRows()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        for (var i = 1; i <= 3; i++)
            await engine.WriteAsync("testdb", "autogen", [PointAt(i, i * 1_000_000_000L)]);
        engine.FlushAll();

        var response = await new QueryExecutor(maxResponseRows: 0).ExecuteAsync(engine, "testdb",
            "SELECT mean(value) FROM cpu GROUP BY time(1s)");

        Assert.Null(response.Results[0].Error);
        var series = Assert.Single(response.Results[0].Series!);
        Assert.Equal(3, series.Values.Count); // one bucket per point
    }

    [Fact]
    public async Task MaxResponseRowsCap_TruncatesWithoutError()
    {
        // Documents the project's chosen semantics for the cap: rows beyond the configured
        // MaxResponseRows are truncated (the existing, tested behaviour) — not an error.
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        for (var i = 1; i <= 3; i++)
            await engine.WriteAsync("testdb", "autogen", [PointAt(i, i * 1_000_000_000L)]);
        engine.FlushAll();

        var response = await new QueryExecutor(maxResponseRows: 2).ExecuteAsync(engine, "testdb", "SELECT value FROM cpu");

        Assert.Null(response.Results[0].Error);
        var series = Assert.Single(response.Results[0].Series!);
        Assert.Equal(2, series.Values.Count);
    }

    [Fact]
    public async Task ExplicitLimit_SmallerThanCap_StillApplies()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        for (var i = 1; i <= 3; i++)
            await engine.WriteAsync("testdb", "autogen", [PointAt(i, i * 1_000_000_000L)]);
        engine.FlushAll();

        var response = await new QueryExecutor(maxResponseRows: 100).ExecuteAsync(engine, "testdb", "SELECT value FROM cpu LIMIT 2");

        Assert.Null(response.Results[0].Error);
        var series = Assert.Single(response.Results[0].Series!);
        Assert.Equal(2, series.Values.Count);
        Assert.Equal(1.0, Convert.ToDouble(series.Values[0][series.Columns.IndexOf("value")]));
        Assert.Equal(2.0, Convert.ToDouble(series.Values[1][series.Columns.IndexOf("value")]));
    }
}

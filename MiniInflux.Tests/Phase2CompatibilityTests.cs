using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Phase-2 query compatibility regressions: SHOW TAG VALUES WITH KEY regex,
/// SHOW SERIES / SHOW TAG KEYS WITH MEASUREMENT filters, parenthesized boolean
/// WHERE groups, string field equality, and exp/log/log2/ln math functions.
/// </summary>
public class Phase2CompatibilityTests : IDisposable
{
    private readonly string _testDir;
    private readonly TsdbEngine _engine;
    private readonly QueryExecutor _executor;

    public Phase2CompatibilityTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p2_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _engine = new TsdbEngine(_testDir, flushThreshold: 100);
        _executor = new QueryExecutor();
        SeedData();
    }

    private void SeedData()
    {
        var points = new List<Point>();
        // cpu measurement: 6 points, hosts server01/server02, region cn, values 1..6,
        // string field name = alpha for even i, beta for odd i.
        for (int i = 1; i <= 6; i++)
        {
            var host = i <= 3 ? "server01" : "server02";
            points.Add(new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", host }, { "region", "cn" } },
                Fields = new Dictionary<string, FieldValue>
                {
                    { "value", FieldValue.FromDouble(i) },
                    { "name", FieldValue.FromString(i % 2 == 0 ? "alpha" : "beta") }
                },
                TimestampNs = i * 1_000_000_000L
            });
        }
        // mem measurement with a distinct tag so WITH MEASUREMENT filtering is observable.
        points.Add(new Point
        {
            Measurement = "mem",
            Tags = new Dictionary<string, string> { { "host", "server01" } },
            Fields = new Dictionary<string, FieldValue> { { "used", FieldValue.FromDouble(0.5) } },
            TimestampNs = 1_000_000_000L
        });
        _engine.WriteAsync("testdb", "autogen", points).Wait();
        _engine.FlushAll();
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    static List<List<object?>> Rows(QueryResponse response)
    {
        Assert.Null(response.Results[0].Error);
        return response.Results[0].Series![0].Values;
    }

    // ---- 2.1 SHOW TAG VALUES WITH KEY =~ /regex/ ----

    [Fact]
    public async Task ShowTagValues_KeyRegex_ReturnsMatchingKeysAndValues()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SHOW TAG VALUES FROM cpu WITH KEY =~ /h.*/");
        var rows = Rows(result);
        Assert.Equal(2, rows.Count); // host=server01, host=server02
        Assert.All(rows, row => Assert.Equal("host", row[0]));
        Assert.Equal(["server01", "server02"], rows.Select(r => r[1]).Order().ToList());
    }

    [Fact]
    public async Task ShowTagValues_KeyRegex_NoMatch_ReturnsEmpty()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SHOW TAG VALUES FROM cpu WITH KEY =~ /zzz.*/");
        Assert.Empty(Rows(result));
    }

    [Fact]
    public async Task ShowTagValues_KeyExact_StillWorks()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SHOW TAG VALUES FROM cpu WITH KEY = host");
        var rows = Rows(result);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal("host", row[0]));
    }

    // ---- 2.2 SHOW SERIES / SHOW TAG KEYS WITH MEASUREMENT ----

    [Fact]
    public async Task ShowSeries_WithMeasurementRegex_FiltersMeasurements()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SHOW SERIES WITH MEASUREMENT =~ /cp.*/");
        var rows = Rows(result);
        Assert.NotEmpty(rows);
        Assert.All(rows, row => Assert.StartsWith("cpu,", (string)row[0]!));
    }

    [Fact]
    public async Task ShowSeries_WithMeasurementExact_FiltersMeasurements()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SHOW SERIES WITH MEASUREMENT = 'mem'");
        var rows = Rows(result);
        var single = Assert.Single(rows);
        Assert.StartsWith("mem,", (string)single[0]!);
    }

    [Fact]
    public async Task ShowSeries_FromMeasurement_IgnoresFilterAndKeepsBehavior()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SHOW SERIES FROM cpu");
        var rows = Rows(result);
        Assert.Equal(2, rows.Count); // two cpu series
        Assert.All(rows, row => Assert.StartsWith("cpu,", (string)row[0]!));
    }

    [Fact]
    public async Task ShowTagKeys_WithMeasurementRegex_UnionsFilteredMeasurements()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SHOW TAG KEYS WITH MEASUREMENT =~ /cp.*/");
        var keys = Rows(result).Select(r => (string)r[0]!).ToList();
        Assert.Contains("host", keys);
        Assert.Contains("region", keys);
        Assert.DoesNotContain("unused_key", keys);
    }

    [Fact]
    public async Task ShowTagKeys_FromMeasurement_StillWorks()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SHOW TAG KEYS FROM cpu");
        var keys = Rows(result).Select(r => (string)r[0]!).ToList();
        Assert.Equal(["host", "region"], keys.Order().ToList());
    }

    // ---- 2.3 WHERE 括号布尔分组 ----

    [Fact]
    public async Task Select_ParenthesizedOrGroup_AndCondition_AppliesBothBranches()
    {
        // Discriminating case: second OR branch matches nothing; only server01 rows survive.
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SELECT * FROM cpu WHERE (host='server01' OR region='us') AND region='cn'");
        var rows = Rows(result);
        Assert.Equal(3, rows.Count); // server01 has 3 points
        Assert.All(rows, row => Assert.Equal("server01", row[1]));
    }

    [Fact]
    public async Task Select_ParenthesizedOrGroup_MatchesEitherBranch()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SELECT * FROM cpu WHERE (host='server01' OR host='server02')");
        Assert.Equal(6, Rows(result).Count);
    }

    // ---- 2.4 字符串字段等值过滤 ----

    [Fact]
    public async Task Select_StringFieldEquality_OnFlushedSegments()
    {
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SELECT * FROM cpu WHERE name = 'alpha'");
        var rows = Rows(result);
        Assert.Equal(3, rows.Count); // even i values are alpha
        // Columns: time, host, region, name, value
        Assert.All(rows, row => Assert.Equal("alpha", row[3]));
    }

    [Fact]
    public async Task Select_StringFieldEquality_InBufferOnlyPath()
    {
        // Explicit dispose (not `using`) so the engine is fully flushed BEFORE the
        // finally block removes its data directory.
        var engine = new TsdbEngine(_testDir + "_buffer", flushThreshold: 1_000_000);
        try
        {
            var points = new List<Point>();
            for (int i = 1; i <= 4; i++)
            {
                points.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { { "host", "h1" } },
                    Fields = new Dictionary<string, FieldValue>
                    {
                        { "value", FieldValue.FromDouble(i) },
                        { "name", FieldValue.FromString(i % 2 == 0 ? "alpha" : "beta") }
                    },
                    TimestampNs = i * 1_000_000_000L
                });
            }
            await engine.WriteAsync("bufdb", "autogen", points);

            var executor = new QueryExecutor();
            var result = await executor.ExecuteAsync(engine, "bufdb",
                "SELECT * FROM cpu WHERE name = 'alpha'");
            var rows = Rows(result);
            Assert.Equal(2, rows.Count);
            // Columns: time, host, name, value
            Assert.All(rows, row => Assert.Equal("alpha", row[2]));
        }
        finally
        {
            engine.Dispose();
            if (Directory.Exists(_testDir + "_buffer"))
                Directory.Delete(_testDir + "_buffer", true);
        }
    }

    // ---- 2.5 EXP / LOG / LOG2 / LN ----

    [Fact]
    public async Task Math_NoGroupBy_ProducesPerPointRows()
    {
        // Without GROUP BY, math transforms emit one row per point (same convention as
        // difference/derivative in this codebase).
        var exp = Rows(await _executor.ExecuteAsync(_engine, "testdb", "SELECT exp(value) FROM cpu"));
        Assert.Equal(6, exp.Count);
        Assert.Equal(Math.Exp(1), Convert.ToDouble(exp[0][1]), 9);
        Assert.Equal(Math.Exp(6), Convert.ToDouble(exp[^1][1]), 9);

        var log = Rows(await _executor.ExecuteAsync(_engine, "testdb", "SELECT log(value) FROM cpu"));
        Assert.Equal(Math.Log(6), Convert.ToDouble(log[^1][1]), 9);

        var log2 = Rows(await _executor.ExecuteAsync(_engine, "testdb", "SELECT log2(value) FROM cpu"));
        Assert.Equal(Math.Log2(6), Convert.ToDouble(log2[^1][1]), 9);

        var ln = Rows(await _executor.ExecuteAsync(_engine, "testdb", "SELECT ln(value) FROM cpu"));
        Assert.Equal(Math.Log(6), Convert.ToDouble(ln[^1][1]), 9);
    }

    [Fact]
    public async Task Math_GroupByTime_ReturnsScalarPerBucket()
    {
        // With GROUP BY time, math functions collapse to a per-bucket scalar using the
        // bucket's last value — matching the existing abs/sqrt behavior.
        var result = await _executor.ExecuteAsync(_engine, "testdb",
            "SELECT exp(value) FROM cpu GROUP BY time(10s)");
        var rows = Rows(result);
        var single = Assert.Single(rows);
        Assert.Equal(Math.Exp(6), Convert.ToDouble(single[1]), 9);
    }
}

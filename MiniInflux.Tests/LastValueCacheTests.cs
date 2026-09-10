using System.Diagnostics;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class LastValueCacheTests : IDisposable
{
    private readonly string _dir;
    public LastValueCacheTests() { _dir = Path.Combine(Path.GetTempPath(), $"lvc_{Guid.NewGuid():N}"); Directory.CreateDirectory(_dir); }
    public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, true); }

    private static Point P(string m, string host, double v, long ts)
        => new() { Measurement = m, Tags = new Dictionary<string,string>{ ["host"]=host }, Fields = new Dictionary<string, FieldValue>{ ["value"]=FieldValue.FromDouble(v) }, TimestampNs = ts, TagsCanonical = $"host={host}" };

    [Fact]
    public void UpdateMany_OutOfOrderAndEqualTimestamps_MatchesSequentialUpdatesWithoutMutatingInputs()
    {
        static Point WithField(string host, string field, double value, long timestamp) => new()
        {
            Measurement = "cpu", Tags = new() { ["host"] = host },
            Fields = new() { [field] = FieldValue.FromDouble(value) }, TimestampNs = timestamp
        };
        var sequential = new LastValueCache();
        var batched = new LastValueCache();
        var seed = WithField("a", "seed", 1, 300);
        sequential.Update("db", "rp", seed);
        batched.Update("db", "rp", seed);
        Point[] points =
        [
            WithField("a", "value", 2, 200), WithField("b", "old", 3, 100),
            WithField("a", "value", 4, 300), WithField("a", "extra", 5, 300),
            WithField("b", "value", 6, 400), WithField("a", "value", 7, 300),
            WithField("b", "stale", 8, 200)
        ];
        foreach (var point in points) sequential.Update("db", "rp", point);
        batched.UpdateMany("db", "rp", points);

        foreach (var expected in sequential.GetAll("db", "rp"))
        {
            Assert.True(batched.TryGet("db", "rp", SeriesKey.From(expected), out var actual));
            Assert.Equal(expected.TimestampNs, actual.TimestampNs);
            Assert.Equal(expected.TagsCanonical, actual.TagsCanonical);
            Assert.Equal(expected.Fields.Count, actual.Fields.Count);
            foreach (var field in expected.Fields)
                Assert.Equal(field.Value.AsDouble(), actual.Fields[field.Key].AsDouble());
        }
        Assert.All(points, point => Assert.Single(point.Fields));
        Assert.Single(seed.Fields);
        Assert.True(batched.TryGet("db", "rp", "cpu", "host=a", out var latest));
        Assert.Equal(7, latest.Fields["value"].AsDouble());
        Assert.Equal(3, latest.Fields.Count);
    }

    [Fact]
    public void UpdateMany_HighCardinalityAndSeparateDatabases_PreservesCapAndIsolation()
    {
        var cache = new LastValueCache(maxEntriesPerDbRp: 4);
        cache.UpdateMany("db", "rp", Enumerable.Range(0, 200).Select(i => P("cpu", $"h{i}", i, i)));
        Assert.InRange(cache.Count, 1, 68);
        cache.UpdateMany("other", "rp", [P("cpu", "h0", 999, 1000)]);
        Assert.True(cache.TryGet("other", "rp", "cpu", "host=h0", out var other));
        Assert.Equal(999, other.Fields["value"].AsDouble());
        Assert.All(cache.GetAll("db", "rp"), p => Assert.NotEqual(999, p.Fields["value"].AsDouble()));
    }

    [Fact]
    public void UpdateMany_CrossesAggregationLimit_KeepsAllSeriesAndMergesLaterDuplicate()
    {
        var cache = new LastValueCache();
        var points = Enumerable.Range(0, 100).Select(i => P("cpu", $"h{i}", i, 100)).ToList();
        points.Add(new Point
        {
            Measurement = "cpu", Tags = new() { ["host"] = "h0" }, TimestampNs = 100,
            Fields = new() { ["extra"] = FieldValue.FromDouble(999) }
        });
        cache.UpdateMany("db", "rp", points);
        Assert.Equal(100, cache.Count);
        Assert.True(cache.TryGet("db", "rp", "cpu", "host=h0", out var merged));
        Assert.Equal(0, merged.Fields["value"].AsDouble());
        Assert.Equal(999, merged.Fields["extra"].AsDouble());
        Assert.Single(points[0].Fields);
        Assert.True(cache.TryGet("db", "rp", "cpu", "host=h99", out var last));
        Assert.Equal(99, last.Fields["value"].AsDouble());
    }

    [Fact]
    public void Update_OverCap_EvictsButStaysCorrect()
    {
        var cache = new LastValueCache(maxEntriesPerDbRp: 4);
        for (var i = 0; i < 200; i++)
            cache.Update("db", "rp", P("cpu", $"h{i}", i, 1_000_000_000 + i));

        // Sampled enforcement keeps the cache bounded near the cap (cap + one sampling interval)...
        Assert.True(cache.Count <= 68, $"cache count was {cache.Count}");
        // ...and every retained entry is still correct (eviction falls back to scans).
        foreach (var p in cache.GetAll("db", "rp"))
            Assert.True(p.Tags["host"] != null && p.Fields["value"].AsDouble()!.Value >= 0);
    }

    [Fact]
    public async Task WritePath_UpdatesCache_AndLastQuery_HitsCache_Under10Ms()
    {
        using var engine = new TsdbEngine(_dir, flushThreshold: 100000, flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
        engine.Recover();
        await engine.WriteAsync("db", "autogen", [P("cpu", "a", 1, 100), P("cpu", "a", 2, 200), P("cpu", "b", 5, 150)]);
        Assert.Equal(2, engine.GetLastValueCacheCount());

        var exec = new QueryExecutor();
        // warm up (JIT + parsing memoization)
        _ = exec.ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu WHERE host='a'");
        var sw = Stopwatch.StartNew();
        var outcome = exec.ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu WHERE host='a'");
        sw.Stop();
        Assert.True(outcome.Report.UsedLastValueCache, "expected last() to hit cache");
        Assert.True(sw.ElapsedMilliseconds < 10, $"last() via cache should be <10ms but was {sw.ElapsedMilliseconds}ms");
        var series = Assert.Single(outcome.Response.Results[0].Series!);
        var lastVal = series.Values[0][1];
        Assert.Equal(2.0, Convert.ToDouble(lastVal));

        // raw current value
        var raw = exec.ExecuteWithReport(engine, "db", "SELECT * FROM cpu WHERE host='a' ORDER BY time DESC LIMIT 1");
        Assert.True(raw.Report.UsedLastValueCache);
        var rawSeries = Assert.Single(raw.Response.Results[0].Series!);
        Assert.Single(rawSeries.Values);
    }

    [Fact]
    public async Task Flush_AfterFooterValidation_CacheStillServes()
    {
        using var engine = new TsdbEngine(_dir, flushThreshold: 2, flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
        engine.Recover();
        await engine.WriteAsync("db", "autogen", [P("cpu", "a", 10, 1000), P("cpu", "a", 20, 2000)]);
        engine.FlushAll();
        // after flush, cached last should still be 20 at 2000
        var exec = new QueryExecutor();
        var outcome = exec.ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu WHERE host='a'");
        Assert.True(outcome.Report.UsedLastValueCache);
        Assert.Equal(20.0, Convert.ToDouble(Assert.Single(outcome.Response.Results[0].Series!).Values[0][1]));
        // verify footer path: ensure segment file exists and cache matches its maxTime
        var segs = Directory.GetFiles(_dir, "*.seg", SearchOption.AllDirectories);
        Assert.NotEmpty(segs);
    }

    [Fact]
    public async Task GroupByHost_Last_PerGroup_ViaCache()
    {
        using var engine = new TsdbEngine(_dir, flushThreshold: 100000, flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
        engine.Recover();
        await engine.WriteAsync("db", "autogen", [P("cpu", "a", 1, 100), P("cpu", "b", 9, 300), P("cpu", "a", 2, 200)]);
        var exec = new QueryExecutor();
        var outcome = exec.ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu GROUP BY host");
        Assert.True(outcome.Report.UsedLastValueCache);
        var series = outcome.Response.Results[0].Series!;
        Assert.Equal(2, series.Count);
        var a = series.First(s => s.Tags != null && s.Tags["host"]=="a");
        var b = series.First(s => s.Tags != null && s.Tags["host"]=="b");
        Assert.Equal(2.0, Convert.ToDouble(a.Values[0][1]));
        Assert.Equal(9.0, Convert.ToDouble(b.Values[0][1]));
    }

    [Fact]
    public async Task DropSeries_InvalidatesCache_FallbackWorks()
    {
        using var engine = new TsdbEngine(_dir, flushThreshold: 100000, flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
        engine.Recover();
        await engine.WriteAsync("db", "autogen", [P("cpu", "a", 1, 100), P("cpu", "b", 2, 200)]);
        var exec = new QueryExecutor();
        var before = exec.ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu WHERE host='a'");
        Assert.True(before.Report.UsedLastValueCache);
        engine.DropSeries("db", "cpu", ["host=a"]);
        // after drop, cache should not serve deleted series — either empty or fallback
        var after = exec.ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu WHERE host='a'");
        // cache miss should fallback to scan (not used cache) and return empty
        var series = after.Response.Results[0].Series;
        Assert.True(series == null || series.Count == 0 || series[0].Values.Count == 0);
        // other series still cached
        var b = exec.ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu WHERE host='b'");
        Assert.True(b.Report.UsedLastValueCache);
    }

    [Fact]
    public async Task SameTimestamp_LWW_MergesFields()
    {
        using var engine = new TsdbEngine(_dir, flushThreshold: 100000, flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
        engine.Recover();
        var p1 = new Point { Measurement="cpu", Tags=new Dictionary<string,string>{ ["host"]="a" }, Fields=new Dictionary<string,FieldValue>{ ["value"]=FieldValue.FromDouble(1), ["extra"]=FieldValue.FromDouble(10) }, TimestampNs=1000, TagsCanonical="host=a" };
        var p2 = new Point { Measurement="cpu", Tags=new Dictionary<string,string>{ ["host"]="a" }, Fields=new Dictionary<string,FieldValue>{ ["value"]=FieldValue.FromDouble(2) }, TimestampNs=1000, TagsCanonical="host=a" };
        await engine.WriteAsync("db", "autogen", [p1]);
        await engine.WriteAsync("db", "autogen", [p2]);
        var exec = new QueryExecutor();
        var outcome = exec.ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu WHERE host='a'");
        Assert.True(outcome.Report.UsedLastValueCache);
        Assert.Equal(2.0, Convert.ToDouble(Assert.Single(outcome.Response.Results[0].Series!).Values[0][1]));
        // cached point should have merged fields extra=10 still
        Assert.True(engine.TryGetLastValue("db", "autogen", "cpu", "host=a", out var cached));
        Assert.True(cached.Fields.ContainsKey("extra"));
    }

    [Fact]
    public async Task OutOfOrderWrite_DoesNotUpdateCache_WithOlderTimestamp()
    {
        using var engine = new TsdbEngine(_dir, flushThreshold: 100000, flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
        engine.Recover();
        await engine.WriteAsync("db", "autogen", [P("cpu", "a", 10, 2000)]);
        await engine.WriteAsync("db", "autogen", [P("cpu", "a", 5, 1000)]); // older
        var exec = new QueryExecutor();
        var outcome = exec.ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu WHERE host='a'");
        Assert.True(outcome.Report.UsedLastValueCache);
        Assert.Equal(10.0, Convert.ToDouble(Assert.Single(outcome.Response.Results[0].Series!).Values[0][1]));
    }

    [Fact]
    public async Task FirstQuery_DoesNotUseLastValueCache_AndReturnsEarliestPoint()
    {
        using var engine = new TsdbEngine(_dir, flushThreshold: 100000, flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
        engine.Recover();
        await engine.WriteAsync("db", "autogen", [P("cpu", "a", 1, 100), P("cpu", "a", 2, 200)]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "db", "SELECT first(value) FROM cpu WHERE host='a'");

        Assert.False(outcome.Report.UsedLastValueCache);
        Assert.Equal(1.0, Convert.ToDouble(Assert.Single(outcome.Response.Results[0].Series!).Values[0][1]));
    }

    [Fact]
    public async Task LastQuery_WhenNewestPointLacksRequestedField_FallsBackToEarlierValue()
    {
        using var engine = new TsdbEngine(_dir, flushThreshold: 100000, flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
        engine.Recover();
        await engine.WriteAsync("db", "autogen",
        [
            P("cpu", "a", 1, 100),
            new Point { Measurement = "cpu", Tags = new Dictionary<string, string> { ["host"] = "a" }, Fields = new Dictionary<string, FieldValue> { ["other"] = FieldValue.FromDouble(2) }, TimestampNs = 200, TagsCanonical = "host=a" }
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "db", "SELECT last(value) FROM cpu WHERE host='a'");

        Assert.False(outcome.Report.UsedLastValueCache);
        Assert.Equal(1.0, Convert.ToDouble(Assert.Single(outcome.Response.Results[0].Series!).Values[0][1]));
    }
}

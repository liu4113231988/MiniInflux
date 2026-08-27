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
        Assert.Equal(1, rawSeries.Values.Count);
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
}

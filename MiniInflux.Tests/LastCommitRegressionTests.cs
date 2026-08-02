using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class LastCommitRegressionTests : IDisposable
{
    private readonly string _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_last_{Guid.NewGuid():N}");

    public LastCommitRegressionTests() => Directory.CreateDirectory(_testDir);

    public void Dispose() => Directory.Delete(_testDir, recursive: true);

    [Fact]
    public async Task CreateDatabaseWithOptions_PreservesRetentionAndShardDurations()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0,
            flushIntervalMs: 0, compactionIntervalMs: 0);
        var response = await new QueryExecutor().ExecuteAsync(engine, null,
            "CREATE DATABASE \"metrics db\" WITH DURATION 3d REPLICATION 1 SHARD DURATION 2h NAME \"liquid\"");

        Assert.Null(response.Results[0].Error);
        var rp = engine.Meta.GetDefaultRp("metrics db");
        Assert.Equal("liquid", rp.Name);
        Assert.Equal(3 * 86_400_000_000_000L, rp.DurationNs);
        Assert.Equal(2 * 3_600_000_000_000L, rp.ShardDurationNs);
        Assert.Equal(1, rp.Replication);

        await engine.WriteAsync("metrics db", "liquid", [Point("cpu", "value", 1, "a", 3 * 3_600)]);
        engine.FlushAll();
        var shard = Assert.Single(engine.Meta.GetShards("metrics db", "liquid"));
        Assert.Equal(rp.ShardDurationNs, shard.EndTimeNs - shard.StartTimeNs);
    }

    [Fact]
    public async Task CardinalityAndStats_CountSetsAcrossMeasurements()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("other", "autogen",
        [
            Point("cpu", "value", 1, "a", 1),
            Point("memory", "value", 2, "a", 2),
            Point("memory", "used", 3, "a", 3)
        ]);
        var executor = new QueryExecutor();

        var fields = await executor.ExecuteAsync(engine, "ignored", "SHOW FIELD KEY CARDINALITY ON other");
        Assert.Equal(2L, fields.Results[0].Series![0].Values[0][0]);

        var tags = await executor.ExecuteAsync(engine, "ignored", "SHOW TAG KEY CARDINALITY ON other");
        Assert.Equal(1L, tags.Results[0].Series![0].Values[0][0]);

        var stats = await executor.ExecuteAsync(engine, "other", "SHOW STATS");
        Assert.Contains(stats.Results[0].Series![0].Values,
            row => Equals(row[0], "series") && Equals(row[1], 2L));
    }

    [Fact]
    public async Task AggregateWithTagFilter_ReadsOnlyIndexedSeriesMetadata()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0,
            flushIntervalMs: 0, compactionIntervalMs: 0);
        await engine.WriteAsync("db", "autogen", [Point("cpu", "value", 1, "a", 1)]);
        await engine.WriteAsync("db", "autogen", [Point("cpu", "value", 2, "b", 2)]);
        engine.FlushAll();

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "db", "SELECT count(value) FROM cpu WHERE host='a'");

        Assert.True(outcome.Report.UsedAggregatePushdown);
        Assert.Equal(1, outcome.Report.SegmentMetadataFooterHits);
        Assert.Equal(1L, outcome.Response.Results[0].Series![0].Values[0][1]);
    }

    [Fact]
    public async Task PeriodicFlush_KeepsSmallDurableBatchBuffered()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 100, rpCheckIntervalMs: 0,
            flushIntervalMs: 10, compactionIntervalMs: 0);
        await engine.WriteAsync("db", "autogen", [Point("cpu", "value", 1, "a", 1)]);

        await Task.Delay(100);

        Assert.Equal(1, engine.GetBufferedPointCount());
        Assert.Empty(engine.Meta.GetShards("db", "autogen"));
    }

    [Fact]
    public async Task PeriodicFlush_FlushesWriteColdBatch()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 100, rpCheckIntervalMs: 0,
            flushIntervalMs: 5, compactionIntervalMs: 0, flushColdDurationMs: 20);
        await engine.WriteAsync("db", "autogen", [Point("cpu", "value", 1, "a", 1)]);

        for (var i = 0; i < 100 && engine.GetBufferedPointCount() > 0; i++)
            await Task.Delay(5);

        Assert.Equal(0, engine.GetBufferedPointCount());
        Assert.Single(Assert.Single(engine.Meta.GetShards("db", "autogen")).SegmentFiles);
    }

    [Fact]
    public async Task Compactor_CountTriggeredBatch_DrainsAllSmallSegments()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0,
            flushIntervalMs: 0, compactionIntervalMs: 0);
        for (var i = 0; i < 12; i++)
            await engine.WriteAsync("db", "autogen", [Point("cpu", "value", i, "a", i + 1)]);

        var compactor = new Compactor(engine.Meta, new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones, engine.Schema, maxL0Segments: 10, maxL1Segments: 99,
            maxL0Bytes: long.MaxValue);

        Assert.Equal(1, compactor.CompactAll());
        var shard = Assert.Single(engine.Meta.GetShards("db", "autogen"));
        Assert.Single(shard.SegmentFiles);
        Assert.StartsWith("l1-", shard.SegmentFiles[0], StringComparison.OrdinalIgnoreCase);
        Assert.Equal(12, compactor.GetStats().TotalSegmentsMerged);
    }

    private static Point Point(string measurement, string field, double value, string host, long seconds) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { [field] = FieldValue.FromDouble(value) },
        TimestampNs = seconds * 1_000_000_000
    };
}

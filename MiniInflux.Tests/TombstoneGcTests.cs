using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class TombstoneGcTests : IDisposable
{
    private readonly string _testDir;

    public TombstoneGcTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_tgc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    private static Point PointAt(string measurement, double value, long timestampNs) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = "server01" },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };

    private static long Ns(long seconds) => seconds * 1_000_000_000L;

    private async Task WriteTwoFlushedPointsAsync(TsdbEngine engine)
    {
        // Two separately flushed batches produce two L0 segments so compaction can merge them.
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 1, Ns(1))]);
        engine.FlushAll();
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 2, Ns(2))]);
        engine.FlushAll();
    }

    private static Compactor NewCompactor(TsdbEngine engine) => new(
        engine.Meta,
        new ShardManager(engine.RootPath, engine.Meta),
        engine.Tombstones,
        engine.Schema,
        maxL0Segments: 2, maxL1Segments: 1);

    [Fact]
    public async Task Compactor_FullShardRewrite_RetiresAppliedTombstones()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, compactionIntervalMs: 0);
        await WriteTwoFlushedPointsAsync(engine);

        engine.DeleteFromMeasurement("testdb", "cpu", Ns(1), Ns(2));
        Assert.True(engine.Tombstones.HasTombstones("testdb"));

        var query = new QueryExecutor();
        var afterDelete = await query.ExecuteAsync(engine, "testdb", "SELECT value FROM cpu");
        Assert.Empty(afterDelete.Results[0].Series![0].Values);

        Assert.Equal(1, NewCompactor(engine).CompactAll());

        // The rewritten range exactly matched the tombstone, so the coverage is gone...
        Assert.False(engine.Tombstones.HasTombstones("testdb"));
        // ...and the deleted data stays physically deleted.
        var afterCompaction = await query.ExecuteAsync(engine, "testdb", "SELECT value FROM cpu");
        Assert.Empty(afterCompaction.Results[0].Series![0].Values);

        // The retirement persisted: a fresh store over the same data dir loads no tombstones.
        Assert.False(new TombstoneStore(engine.RootPath).HasTombstones("testdb"));
    }

    [Fact]
    public async Task Compactor_PartialRangeRewrite_SplitsTombstoneButKeepsCoverage()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, compactionIntervalMs: 0);
        await WriteTwoFlushedPointsAsync(engine);

        // Delete a range wider than the data; only [1s, 2s] gets physically rewritten.
        engine.DeleteFromMeasurement("testdb", "cpu", Ns(0), Ns(10));

        Assert.Equal(1, NewCompactor(engine).CompactAll());

        // The applied portion [1s, 2s] is retired, but the unapplied remainders remain.
        Assert.True(engine.Tombstones.HasTombstones("testdb"));

        var query = new QueryExecutor();
        var afterCompaction = await query.ExecuteAsync(engine, "testdb", "SELECT value FROM cpu");
        Assert.Empty(afterCompaction.Results[0].Series![0].Values);

        // A point inside the still-covered remainder stays filtered even though it was written
        // after the delete, matching the pre-GC behavior for post-delete covered writes.
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 9, Ns(5))]);
        engine.FlushAll();
        var afterNewWrite = await query.ExecuteAsync(engine, "testdb", "SELECT value FROM cpu WHERE time > 3000000000");
        Assert.Empty(afterNewWrite.Results[0].Series![0].Values);
    }

    [Fact]
    public void RemoveCoveredRange_FullyCoveredTombstone_IsDropped()
    {
        var store = new TombstoneStore(_testDir);
        store.AddMeasurementDelete("db1", "cpu", Ns(0), Ns(10));

        store.RemoveCoveredRange("db1", Ns(0), Ns(10));

        Assert.False(store.HasTombstones("db1"));
    }

    [Fact]
    public void RemoveCoveredRange_PartialOverlap_SplitsIntoRemainders()
    {
        var store = new TombstoneStore(_testDir);
        store.AddMeasurementDelete("db1", "cpu", Ns(0), Ns(10));

        store.RemoveCoveredRange("db1", Ns(4), Ns(6));

        Assert.True(store.HasTombstones("db1"));
        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(1), Ns(3)));
        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(7), Ns(9)));
        Assert.False(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(4), Ns(6)));
        Assert.False(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(0), Ns(10)));
    }

    [Fact]
    public void RemoveCoveredRange_UnboundedTombstone_IsTrimmedToRemainders()
    {
        var store = new TombstoneStore(_testDir);
        store.AddMeasurementDelete("db1", "cpu"); // null bounds = delete everything

        store.RemoveCoveredRange("db1", Ns(4), Ns(6));

        Assert.True(store.HasTombstones("db1"));
        Assert.False(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(4), Ns(6)));
        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(0), Ns(3)));
        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(7), long.MaxValue - 1));
    }

    [Fact]
    public void IsColumnDeleted_MergedRanges_MatchLinearSemantics()
    {
        var store = new TombstoneStore(_testDir);
        store.AddMeasurementDelete("db1", "cpu", Ns(10), Ns(20));
        store.AddMeasurementDelete("db1", "cpu", Ns(15), Ns(30));
        store.AddSeriesDelete("db1", "cpu", "host=server02", Ns(5), Ns(8));

        // Overlapping measurement tombstones coalesce into one range [10s, 30s].
        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(10), Ns(30)));
        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(12), Ns(18)));
        Assert.False(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(5), Ns(9)));
        Assert.False(store.IsColumnDeleted("db1", "cpu", "host=server01", Ns(31), Ns(40)));
        // Series tombstone covers only its own series plus the measurement-wide range.
        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server02", Ns(5), Ns(8)));
        Assert.False(store.IsColumnDeleted("db1", "cpu", "host=server03", Ns(5), Ns(8)));
    }
}

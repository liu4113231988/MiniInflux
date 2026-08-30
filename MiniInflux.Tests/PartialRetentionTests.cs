using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Partial retention expiry: compaction drops points older than now - duration inside a shard
/// that has not aged out as a whole, so expired data does not linger until shard expiry.
/// </summary>
public class PartialRetentionTests : IDisposable
{
    private readonly string _testDir;

    public PartialRetentionTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_ret_{Guid.NewGuid():N}");
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

    [Fact]
    public async Task CompactAll_DropsPointsOlderThanRetentionCutoff()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        // 60s retention, non-default RP (autogen stays infinite).
        engine.Meta.CreateRetentionPolicy("testdb", "short", 60_000_000_000L, isDefault: false);

        var nowNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000;
        var oldTs = nowNs - 120_000_000_000L; // 120s old: past retention
        var freshTs = nowNs - 1_000_000_000L; // 1s old: within retention

        await engine.WriteAsync("testdb", "short", [PointAt(1, oldTs)]);
        engine.FlushAll();
        await engine.WriteAsync("testdb", "short", [PointAt(2, freshTs)]);
        engine.FlushAll();

        // Both points readable before compaction (shard-granular retention).
        Assert.Equal(2, engine.ReadAllPoints("testdb", "short", "cpu", null, null).Count);

        var compactor = new Compactor(engine.Meta, new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 99);
        Assert.Equal(1, compactor.CompactAll());

        // The expired point is gone; the fresh point survives.
        var remaining = engine.ReadAllPoints("testdb", "short", "cpu", null, null);
        var single = Assert.Single(remaining);
        Assert.Equal(freshTs, single.TimestampNs);
        Assert.Equal(2.0, single.Fields["value"].AsDouble());
    }

    [Fact]
    public async Task CompactAll_InfiniteRetention_KeepsAllPoints()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        var oldTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000 - 120_000_000_000L;

        await engine.WriteAsync("testdb", "autogen", [PointAt(1, oldTs)]);
        engine.FlushAll();
        await engine.WriteAsync("testdb", "autogen", [PointAt(2, oldTs + 1_000_000_000)]);
        engine.FlushAll();

        var compactor = new Compactor(engine.Meta, new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 99);
        Assert.Equal(1, compactor.CompactAll());

        Assert.Equal(2, engine.ReadAllPoints("testdb", "autogen", "cpu", null, null).Count);
    }
}

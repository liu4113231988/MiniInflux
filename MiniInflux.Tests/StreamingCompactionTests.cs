using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Compaction merges inputs with a streaming k-way merge; these tests pin the semantics the
/// previous whole-materialization merge provided: last-write-wins on duplicates and correct
/// output across MaxSegmentFileBytes chunk splits.
/// </summary>
public class StreamingCompactionTests : IDisposable
{
    private readonly string _testDir;

    public StreamingCompactionTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_smc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    private static Point PointAt(double value, long timestampNs, string host = "server01") => new()
    {
        Measurement = "cpu",
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };

    private static long Ns(long seconds) => seconds * 1_000_000_000L;

    [Fact]
    public async Task CompactAll_InterleavedDuplicateTimestamps_NewestInputWins()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        // L0 batch A (older values), then L0 batch B overwriting part of the range.
        await engine.WriteAsync("testdb", "autogen",
            [PointAt(1, Ns(1)), PointAt(2, Ns(2)), PointAt(3, Ns(3))]);
        engine.FlushAll();
        await engine.WriteAsync("testdb", "autogen",
            [PointAt(20, Ns(2)), PointAt(30, Ns(3)), PointAt(4, Ns(4))]);
        engine.FlushAll();

        var compactor = new Compactor(engine.Meta, new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 99);
        Assert.Equal(1, compactor.CompactAll());

        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null)
            .OrderBy(p => p.TimestampNs).ToList();
        Assert.Equal(4, points.Count);
        Assert.Equal([1.0, 20.0, 30.0, 4.0], points.Select(p => p.Fields["value"].AsDouble()).ToList());
    }

    [Fact]
    public async Task CompactAll_MultiSeriesAndFields_AllGroupsMerged()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        var batch1 = new List<Point>();
        var batch2 = new List<Point>();
        for (var i = 0; i < 40; i++)
        {
            batch1.Add(PointAt(i, Ns(i + 1), $"host{i % 4}"));
            batch2.Add(PointAt(i + 100, Ns(i + 1), $"host{i % 4}"));
        }
        await engine.WriteAsync("testdb", "autogen", batch1);
        engine.FlushAll();
        await engine.WriteAsync("testdb", "autogen", batch2);
        engine.FlushAll();

        var compactor = new Compactor(engine.Meta, new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 99);
        Assert.Equal(1, compactor.CompactAll());

        // Every series survives; LWW keeps the newer batch's values.
        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null).ToList();
        Assert.Equal(40, points.Count);
        Assert.All(points, p => Assert.Equal(100.0 + (p.TimestampNs / 1_000_000_000 - 1), p.Fields["value"].AsDouble()));
        Assert.Equal(4, points.Select(p => p.Tags["host"]).Distinct().Count());
    }

    [Fact]
    public async Task CompactAll_MaxSegmentFileBytes_SplitsOutputIntoChunks()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        var batch = new List<Point>();
        // 200 series x 50 points spread over distinct fields would be one group per column; use
        // distinct series so many columns exist and the chunker must split.
        for (var s = 0; s < 200; s++)
        for (var i = 0; i < 50; i++)
            batch.Add(PointAt(i, Ns(i + 1), $"host{s:000}"));
        await engine.WriteAsync("testdb", "autogen", batch);
        engine.FlushAll();
        var batch2 = new List<Point>();
        for (var s = 0; s < 200; s++)
        for (var i = 0; i < 50; i++)
            batch2.Add(PointAt(i + 1000, Ns(i + 1), $"host{s:000}"));
        await engine.WriteAsync("testdb", "autogen", batch2);
        engine.FlushAll();

        var compactor = new Compactor(engine.Meta, new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 99,
            maxSegmentFileBytes: 32 * 1024, segmentFillRatio: 0.5);
        Assert.Equal(1, compactor.CompactAll());

        var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
        // The merged output must be split into multiple segment files under the size cap.
        Assert.True(shard.SegmentFiles.Count > 1, $"expected multiple chunks, got {shard.SegmentFiles.Count}");

        // All points readable after the split, newest values win.
        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null).ToList();
        Assert.Equal(200 * 50, points.Count);
        Assert.All(points, p => Assert.True(p.Fields["value"].AsDouble() >= 1000.0));
    }
}

using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Regression tests for the 2026-10 audit P0 fixes:
/// C1 — LIMIT pushdown combined with per-point field filters silently returned fewer rows.
/// C2 — transient IOExceptions quarantined healthy segment files (permanent silent data loss).
/// </summary>
public class P0AuditFixTests : IDisposable
{
    private readonly string _testDir;

    public P0AuditFixTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p0fix_{Guid.NewGuid():N}");
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
    public async Task FieldFilterQuery_WithLimit_ReturnsFullMatchingSet()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);

        // Five ascending points; only values 3 and 4 match the predicate.
        for (var i = 1; i <= 5; i++)
            await engine.WriteAsync("testdb", "autogen", [PointAt(i, i * 1_000_000_000L)]);
        engine.FlushAll();

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT value FROM cpu WHERE value > 2 LIMIT 2");

        Assert.Null(outcome.Response.Results[0].Error);
        var series = Assert.Single(outcome.Response.Results[0].Series!);
        Assert.Equal(2, series.Values.Count);
        Assert.Equal(3.0, Convert.ToDouble(series.Values[0][series.Columns.IndexOf("value")]));
        Assert.Equal(4.0, Convert.ToDouble(series.Values[1][series.Columns.IndexOf("value")]));
    }

    [Fact]
    public async Task FieldFilterQuery_WithLimitAndOffset_ReturnsFullMatchingSet()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);

        for (var i = 1; i <= 5; i++)
            await engine.WriteAsync("testdb", "autogen", [PointAt(i, i * 1_000_000_000L)]);
        engine.FlushAll();

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT value FROM cpu WHERE value > 1 LIMIT 2 OFFSET 2");

        Assert.Null(outcome.Response.Results[0].Error);
        var series = Assert.Single(outcome.Response.Results[0].Series!);
        Assert.Equal(2, series.Values.Count);
        Assert.Equal(4.0, Convert.ToDouble(series.Values[0][series.Columns.IndexOf("value")]));
        Assert.Equal(5.0, Convert.ToDouble(series.Values[1][series.Columns.IndexOf("value")]));
    }

    [Fact]
    public async Task FieldFilterQuery_WithLimit_WithoutRegressionFix_WouldReturnNoRows()
    {
        // Documents the pre-fix behaviour explicitly: the pushed-down raw read budget stopped
        // after (offset + limit) *unfiltered* points, so the predicate removed every row.
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);

        for (var i = 1; i <= 5; i++)
            await engine.WriteAsync("testdb", "autogen", [PointAt(i, i * 1_000_000_000L)]);
        engine.FlushAll();

        // The matching points sit beyond the raw budget; only a post-filter LIMIT finds them.
        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT value FROM cpu WHERE value > 4 LIMIT 1");

        Assert.Null(outcome.Response.Results[0].Error);
        var series = Assert.Single(outcome.Response.Results[0].Series!);
        var row = Assert.Single(series.Values);
        Assert.Equal(5.0, Convert.ToDouble(row[series.Columns.IndexOf("value")]));
    }

    [Fact]
    public async Task TransientIoFailure_HealthySegment_IsNotQuarantinedAndRemainsReadable()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        await engine.WriteAsync("testdb", "autogen", [PointAt(1, 1_000_000_000L)]);
        engine.FlushAll();

        var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
        var shardDir = new ShardManager(engine.RootPath, engine.Meta).ShardDir("testdb", "autogen", shard.Id);
        var segmentPath = Assert.Single(Directory.GetFiles(shardDir, "*.seg"));

        // Hold an exclusive lock on the healthy segment. On Windows this surfaces as an
        // IOException (sharing violation) on read; on other platforms reads still succeed.
        using (var hold = new FileStream(segmentPath, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            var duringLock = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);

            // Whatever the read returned, a transient IO failure must NOT quarantine the file.
            Assert.False(SegmentReader.IsQuarantined(segmentPath));
            if (OperatingSystem.IsWindows())
                Assert.Empty(duringLock); // read skipped the segment instead of throwing
        }

        // After the transient condition clears, the same file must be read normally.
        var afterRelease = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        var point = Assert.Single(afterRelease);
        Assert.Equal(1.0, point.Fields["value"].AsDouble());
        Assert.False(SegmentReader.IsQuarantined(segmentPath));
    }

    [Fact]
    public async Task TruncatedSegment_IsStillQuarantinedAsStructuralCorruption()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        await engine.WriteAsync("testdb", "autogen", [PointAt(1, 1_000_000_000L)]);
        engine.FlushAll();

        var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
        var shardDir = new ShardManager(engine.RootPath, engine.Meta).ShardDir("testdb", "autogen", shard.Id);
        var segmentPath = Assert.Single(Directory.GetFiles(shardDir, "*.seg"));

        // Truncate the file: a torn payload is structural corruption, not a transient error.
        using (var fs = new FileStream(segmentPath, FileMode.Open, FileAccess.Write))
            fs.SetLength(fs.Length / 2);

        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        Assert.Empty(points);
        Assert.True(SegmentReader.IsQuarantined(segmentPath));
    }
}

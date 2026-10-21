using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Regression tests for the P1 admin/storage items in docs/todo-202610.md: SHOW LIMIT/OFFSET,
/// explicit rejection of unsupported SHOW predicates, KILL QUERY errors, and series-key /
/// shard / retention-policy lifecycle on DROP paths.
/// </summary>
public class P1AdminAndStorageTests : IDisposable
{
    private readonly string _testDir;

    public P1AdminAndStorageTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p1s_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, true);
        }
        catch (IOException) { }
    }

    // ---- P1-6 SHOW LIMIT/OFFSET and explicit unsupported-feature errors ----

    [Fact]
    public async Task ShowMeasurements_AppliesLimitAndOffset()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("aaa", 1, "server01", 1_000_000_000L),
            PointAt("bbb", 1, "server01", 1_000_000_000L),
            PointAt("ccc", 1, "server01", 1_000_000_000L)
        ]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SHOW MEASUREMENTS LIMIT 1 OFFSET 1");

        Assert.Null(result.Results[0].Error);
        var row = Assert.Single(result.Results[0].Series![0].Values);
        Assert.Equal("bbb", row[0]);
    }

    [Fact]
    public async Task ShowTagValues_AppliesLimit()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 2, "server02", 1_000_000_000L),
            PointAt("cpu", 3, "server03", 1_000_000_000L)
        ]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SHOW TAG VALUES WITH KEY = \"host\" LIMIT 2");

        Assert.Null(result.Results[0].Error);
        Assert.Equal(2, result.Results[0].Series![0].Values.Count);
    }

    [Fact]
    public async Task ShowMeasurements_WithFieldPredicate_ReturnsExplicitError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SHOW MEASUREMENTS WHERE value > 1");

        Assert.Contains("tag predicates only", result.Results[0].Error);
        Assert.Null(result.Results[0].Series);
    }

    [Fact]
    public async Task ShowSeriesCardinality_WithWhere_ReturnsExplicitError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SHOW SERIES CARDINALITY WHERE host = 'server01'");

        Assert.Contains("WHERE is not supported in SHOW SERIES CARDINALITY", result.Results[0].Error);
    }

    [Fact]
    public async Task KillQuery_UnknownId_ReturnsExplicitError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "KILL QUERY 987654321");

        Assert.Contains("no such query: 987654321", result.Results[0].Error);
    }

    // ---- P1-7 series-key lifecycle ----

    [Fact]
    public async Task DropMeasurement_ReaddedSeries_IsIndexedAgain()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);
        engine.FlushAll();
        Assert.Equal(1, engine.GetSeriesCardinality("testdb"));

        engine.DropMeasurement("testdb", "cpu");
        Assert.Equal(0, engine.GetSeriesCardinality("testdb"));
        Assert.DoesNotContain("cpu", engine.Schema.ListMeasurements("testdb"));

        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 2, "server01", 2_000_000_000L)]);

        var shown = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SHOW SERIES FROM cpu");
        Assert.Null(shown.Results[0].Error);
        Assert.Contains(shown.Results[0].Series![0].Values, row => Convert.ToString(row[0]) == "cpu,host=server01");
        Assert.Equal(1, engine.GetSeriesCardinality("testdb"));
    }

    [Fact]
    public async Task Recover_RebuildsSchemaFromSegmentsWhenSchemaFileIsMissing()
    {
        var schemaPath = Path.Combine(_testDir, "meta", "schema.json");
        using (var engine = new TsdbEngine(_testDir, flushThreshold: 1000))
        {
            await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);
            engine.FlushAll();
        }

        File.Delete(schemaPath);
        using var recovered = new TsdbEngine(_testDir, flushThreshold: 1000);
        recovered.Recover();

        Assert.Equal(FieldKind.Float, recovered.Schema.GetFieldType("testdb", "cpu", "value"));
        Assert.Contains("cpu", recovered.Schema.ListMeasurements("testdb"));
    }

    [Fact]
    public async Task Recover_IndexesSeriesPresentOnlyInWalBeforeFlush()
    {
        var walPath = Path.Combine(_testDir, "wal");
        var manifest = new Manifest(_testDir);
        manifest.EnsureDatabase("testdb");
        manifest.EnsureRp("testdb", "customrp");
        using (var wal = new WalManager(walPath, fsync: false))
            wal.AppendBatch("testdb", "customrp", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);

        using var recovered = new TsdbEngine(_testDir, flushThreshold: 1000);
        recovered.Recover();

        var shown = await new QueryExecutor().ExecuteAsync(recovered, "testdb", "SHOW SERIES FROM cpu");
        Assert.Null(shown.Results[0].Error);
        Assert.Contains(
            shown.Results[0].Series![0].Values,
            row => Convert.ToString(row[0]) == "cpu,host=server01");
        Assert.Contains("cpu", recovered.ListMeasurements("testdb"));
    }

    [Fact]
    public async Task DropSeries_RemovesTrackedSeriesKey()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 2, "server02", 1_000_000_000L)
        ]);
        engine.FlushAll();
        Assert.Equal(2, engine.GetSeriesCardinality("testdb"));

        var dropped = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DROP SERIES FROM cpu WHERE host = 'server01'");
        Assert.Null(dropped.Results[0].Error);
        Assert.Equal(1, engine.GetSeriesCardinality("testdb"));

        // Re-adding the dropped series must be treated as new so manifest indexes include it.
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 3, "server01", 3_000_000_000L)]);
        var shown = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SHOW SERIES FROM cpu");
        Assert.Contains(shown.Results[0].Series![0].Values, row => Convert.ToString(row[0]) == "cpu,host=server01");
        Assert.Equal(2, engine.GetSeriesCardinality("testdb"));
    }

    [Fact]
    public async Task ConcurrentWritesAcrossRetentionPolicies_TrackSeriesKeysWithoutLoss()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("testdb");
        engine.Meta.CreateRetentionPolicy("testdb", "rp1", durationNs: 0, isDefault: false);
        engine.Meta.CreateRetentionPolicy("testdb", "rp2", durationNs: 0, isDefault: false);

        static List<Point> MakePoints(string rp) =>
            Enumerable.Range(0, 100).Select(i => PointAt("cpu", i, $"{rp}-{i}", i + 1)).ToList();

        await Task.WhenAll(
            Task.Run(() => engine.WriteAsync("testdb", "rp1", MakePoints("rp1"))),
            Task.Run(() => engine.WriteAsync("testdb", "rp2", MakePoints("rp2"))));

        Assert.Equal(200, engine.GetSeriesCardinality("testdb"));
        engine.FlushAll();
        Assert.Equal(200, engine.GetSeriesCardinality("testdb"));
    }

    // ---- P1-11 DROP SHARD / DROP RETENTION POLICY ----

    [Fact]
    public async Task DropShard_RemovesFlushedAndBufferedPoints()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 2, "server01", 2_000_000_000L)
        ]);
        engine.FlushAll();
        var (_, shard) = Assert.Single(engine.Meta.GetAllShards("testdb"));

        // Points written after the flush are still buffered; they must not resurrect the shard.
        await engine.WriteAsync("testdb", "autogen", [PointAt("cpu", 3, "server01", 3_000_000_000L)]);

        var dropped = await new QueryExecutor().ExecuteAsync(engine, "testdb", $"DROP SHARD {shard.Id}");
        Assert.Null(dropped.Results[0].Error);

        Assert.Empty(engine.ReadAllPoints("testdb", "autogen", "cpu", null, null));
        Assert.Empty(engine.Meta.GetShards("testdb", "autogen"));
        Assert.Equal(0, engine.GetSeriesCardinality("testdb"));
    }

    [Fact]
    public async Task DropShard_UnknownId_ReturnsExplicitError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);

        var result = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DROP SHARD 424242");

        Assert.Contains("shard not found: 424242", result.Results[0].Error);
    }

    [Fact]
    public async Task DropRetentionPolicy_RemovesDataDirectoryBufferedPointsAndSeriesKeys()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("archivedb");
        engine.Meta.CreateRetentionPolicy("archivedb", "short", durationNs: 3_600_000_000_000L, isDefault: false);
        await engine.WriteAsync("archivedb", "short",
        [
            PointAt("cpu", 1, "server01", 1_000_000_000L),
            PointAt("cpu", 2, "server01", 2_000_000_000L)
        ]);
        engine.FlushAll();
        await engine.WriteAsync("archivedb", "short", [PointAt("cpu", 3, "server01", 3_000_000_000L)]);
        engine.DeleteFromMeasurement("archivedb", "short", "cpu", null, null);

        var rpDir = Path.Combine(_testDir, "db", "archivedb", "short");
        Assert.True(Directory.Exists(rpDir));
        Assert.Equal(1, engine.GetSeriesCardinality("archivedb"));

        var dropped = await new QueryExecutor().ExecuteAsync(engine, "archivedb", "DROP RETENTION POLICY \"short\" ON archivedb");

        Assert.Null(dropped.Results[0].Error);
        Assert.False(Directory.Exists(rpDir));
        Assert.Empty(engine.ReadAllPoints("archivedb", "short", "cpu", null, null));
        Assert.Null(engine.Meta.GetRp("archivedb", "short"));
        Assert.Equal(0, engine.GetSeriesCardinality("archivedb"));
        Assert.False(engine.Tombstones.IsColumnDeleted("archivedb", "cpu", "host=server01", long.MinValue, long.MaxValue, "short"));

        engine.Meta.CreateRetentionPolicy("archivedb", "short", durationNs: 0, isDefault: false);
        await engine.WriteAsync("archivedb", "short", [PointAt("cpu", 4, "server01", 4_000_000_000L)]);
        var recreatedRpPoints = engine.ReadAllPoints("archivedb", "short", "cpu", null, null);
        Assert.Equal(4d, Assert.Single(recreatedRpPoints).Fields["value"].AsDouble());
    }

    [Fact]
    public async Task DropRetentionPolicy_UnknownName_ReturnsExplicitError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("archivedb");

        var result = await new QueryExecutor().ExecuteAsync(engine, "archivedb", "DROP RETENTION POLICY \"missing\" ON archivedb");

        Assert.Contains("retention policy not found: missing", result.Results[0].Error);
    }

    [Fact]
    public async Task DropRetentionPolicy_DoesNotReuseShardIdAfterRestart()
    {
        int droppedShardId;
        using (var engine = new TsdbEngine(_testDir, flushThreshold: 1000))
        {
            engine.CreateDatabase("archivedb");
            engine.Meta.CreateRetentionPolicy("archivedb", "short", durationNs: 0, isDefault: false);
            await engine.WriteAsync("archivedb", "short", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);
            engine.FlushAll();
            droppedShardId = Assert.Single(engine.Meta.GetAllShards("archivedb")).Shard.Id;
            Assert.True(engine.DropRetentionPolicy("archivedb", "short"));
        }

        using var restarted = new TsdbEngine(_testDir, flushThreshold: 1000);
        restarted.Meta.CreateRetentionPolicy("archivedb", "short", durationNs: 0, isDefault: false);
        await restarted.WriteAsync("archivedb", "short", [PointAt("cpu", 2, "server01", 2_000_000_000L)]);
        restarted.FlushAll();
        var recreatedShardId = Assert.Single(restarted.Meta.GetAllShards("archivedb")).Shard.Id;

        Assert.True(recreatedShardId > droppedShardId);
    }

    private static Point PointAt(string measurement, double value, string host, long timestampNs) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };
}

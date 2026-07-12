using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class P0TodoTests : IDisposable
{
    private readonly string _testDir;

    public P0TodoTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p0_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public async Task QueryExecutor_RespectsCancellationToken()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1_000_000);
        await engine.WriteAsync("testdb", "autogen",
            Enumerable.Range(0, 50_000).Select(i => new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(i) },
                TimestampNs = i
            }).ToList());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT value FROM cpu", cts.Token);

        Assert.Contains("query canceled", response.Results[0].Error);
    }

    [Fact]
    public void BackupManager_WritesMetadata_AndValidatesChecksums()
    {
        var source = Path.Combine(_testDir, "data");
        var backup = Path.Combine(_testDir, "backup");
        Directory.CreateDirectory(source);
        File.WriteAllText(Path.Combine(source, "sample.txt"), "hello backup");

        BackupManager.CreateBackup(source, backup);

        Assert.True(File.Exists(Path.Combine(backup, "backup.metadata.json")));
        BackupManager.ValidateBackup(backup);

        File.WriteAllText(Path.Combine(backup, "sample.txt"), "tampered");
        Assert.Throws<InvalidDataException>(() => BackupManager.ValidateBackup(backup));
    }

    [Fact]
    public void BackupManager_PreparesPendingRestore_AndAppliesOnStartup()
    {
        var source = Path.Combine(_testDir, "data");
        var backup = Path.Combine(_testDir, "backup");
        Directory.CreateDirectory(source);
        File.WriteAllText(Path.Combine(source, "live.txt"), "old");

        var backupSource = Path.Combine(_testDir, "backup-source");
        Directory.CreateDirectory(backupSource);
        File.WriteAllText(Path.Combine(backupSource, "live.txt"), "new");
        BackupManager.CreateBackup(backupSource, backup);

        BackupManager.PrepareRestore(backup, source);
        BackupManager.ApplyPendingRestore(source);

        Assert.Equal("new", File.ReadAllText(Path.Combine(source, "live.txt")));
    }

    [Fact]
    public async Task ChunkedRawSelect_StreamsRowsWithoutMaterializingFullResponse()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 50);
        await engine.WriteAsync("testdb", "autogen",
            Enumerable.Range(0, 250).Select(i => new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(i) },
                TimestampNs = i
            }).ToList());

        var outcome = new QueryExecutor(maxQueryMemoryBytes: 20_000)
            .ExecuteChunkedWithReport(engine, "testdb", "SELECT value FROM cpu LIMIT 250", chunkSize: 100);

        var chunks = outcome.Responses.ToList();

        Assert.True(outcome.UsedStreamingRawSelect);
        Assert.Equal(3, chunks.Count);
        Assert.Equal(250, chunks.Sum(c => c.Results[0].Series![0].Values.Count));
        Assert.True(outcome.Report.EstimatedInputBytes > outcome.Report.PeakEstimatedMemoryBytes);
        Assert.Null(outcome.Report.Error);
    }

    [Fact]
    public async Task RawSelect_UsesStreamingScanForNormalQuery()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
            Enumerable.Range(0, 100).Select(i => Point("cpu", i, "server01", i)).ToList());

        var outcome = new QueryExecutor()
            .ExecuteWithReport(engine, "testdb", "SELECT value FROM cpu LIMIT 20");

        var rows = outcome.Response.Results[0].Series![0].Values;
        Assert.True(outcome.Report.UsedStreamingRawSelect);
        Assert.Equal(20, rows.Count);
        Assert.Equal(20, outcome.Report.RowsReturned);
    }

    [Fact]
    public async Task RawSelect_WithSegments_UsesBufferedMaterializedPath()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 2, "server01", 2)]);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01", 1)]);
        engine.FlushAll();

        var outcome = new QueryExecutor()
            .ExecuteWithReport(engine, "testdb", "SELECT value FROM cpu LIMIT 2");

        var rows = outcome.Response.Results[0].Series![0].Values;
        Assert.False(outcome.Report.UsedStreamingRawSelect);
        Assert.Equal("1970-01-01T00:00:00.000000001Z", rows[0][0]);
        Assert.Equal("1970-01-01T00:00:00.000000002Z", rows[1][0]);
    }

    [Fact]
    public async Task GroupByTimeAndTag_UsesStreamingAggregate()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", 1, "server01", 1),
            Point("cpu", 3, "server01", 9),
            Point("cpu", 10, "server01", 11),
            Point("cpu", 20, "server02", 12)
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(
            engine,
            "testdb",
            "SELECT mean(value),count(value),max(value) FROM cpu GROUP BY time(10ns),host");

        Assert.True(outcome.Report.UsedStreamingAggregate);
        var series = outcome.Response.Results[0].Series!;
        var server01 = Assert.Single(series, s => s.Tags?["host"] == "server01");
        Assert.Equal(2, server01.Values.Count);
        Assert.Equal(2.0, server01.Values[0][1]);
        Assert.Equal(2, server01.Values[0][2]);
        Assert.Equal(3.0, server01.Values[0][3]);
    }

    [Fact]
    public void WalReplay_IgnoresInterruptedCheckpointTempFile()
    {
        var walDir = Path.Combine(_testDir, "wal");
        WalPosition secondRecordStart;

        using (var wal = new WalManager(walDir, maxFileBytes: 1024 * 1024))
        {
            wal.Append("testdb", "autogen", [Point("cpu", 1, "server01", 1)]);
            secondRecordStart = Assert.Single(wal.Append("testdb", "autogen", [Point("cpu", 2, "server01", 2)]));
            wal.Checkpoint(secondRecordStart);
        }

        File.WriteAllText(Path.Combine(walDir, "checkpoint.dat.tmp"), "999999:999999");

        using var reopened = new WalManager(walDir, maxFileBytes: 1024 * 1024);
        var replayed = reopened.Replay();

        var only = Assert.Single(replayed);
        Assert.Equal(2.0, only.Points[0].Fields["value"].AsDouble());
    }

    [Fact]
    public async Task Recover_IgnoresInterruptedSegmentTmpFile()
    {
        var dataPath = Path.Combine(_testDir, "data");
        string shardDir;
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01", 1)]);
            engine.FlushAll();
            var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
            shardDir = Path.Combine(dataPath, "db", "testdb", "autogen", "shards", shard.Id.ToString("D6"));
        }

        File.WriteAllBytes(Path.Combine(shardDir, "interrupted.seg.tmp"), [1, 2, 3, 4]);

        using var restarted = new TsdbEngine(dataPath, flushThreshold: 1000, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        var recovery = restarted.Recover();
        var points = restarted.ReadAllPoints("testdb", "autogen", "cpu", null, null);

        Assert.Equal(0, recovery.SegmentsCorrupted);
        Assert.Single(points);
        Assert.Equal(1.0, points[0].Fields["value"].AsDouble());
    }

    [Fact]
    public void ApplyPendingRestore_WithCorruptPendingBackup_DoesNotReplaceLiveData()
    {
        var dataRoot = Path.Combine(_testDir, "data");
        var backupSource = Path.Combine(_testDir, "backup-source");
        var backup = Path.Combine(_testDir, "backup");
        Directory.CreateDirectory(dataRoot);
        Directory.CreateDirectory(backupSource);
        File.WriteAllText(Path.Combine(dataRoot, "live.txt"), "old");
        File.WriteAllText(Path.Combine(backupSource, "live.txt"), "new");
        BackupManager.CreateBackup(backupSource, backup);
        BackupManager.PrepareRestore(backup, dataRoot);

        File.WriteAllText(Path.Combine(dataRoot + ".restore-pending", "live.txt"), "corrupt");

        Assert.Throws<InvalidDataException>(() => BackupManager.ApplyPendingRestore(dataRoot));
        Assert.Equal("old", File.ReadAllText(Path.Combine(dataRoot, "live.txt")));
        Assert.True(Directory.Exists(dataRoot + ".restore-pending"));
    }

    [Fact]
    public async Task Compaction_IgnoresInterruptedOutputTmpFile_AndMergesValidSegments()
    {
        var dataPath = Path.Combine(_testDir, "data");
        using var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01", 1)]);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 2, "server01", 2)]);
        engine.FlushAll();
        var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
        var shardDir = Path.Combine(dataPath, "db", "testdb", "autogen", "shards", shard.Id.ToString("D6"));
        File.WriteAllBytes(Path.Combine(shardDir, "l1-interrupted.seg.tmp"), [9, 9, 9, 9]);

        var compactor = new Compactor(engine.Meta, new ShardManager(engine.RootPath, engine.Meta), engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 1);

        Assert.Equal(2, compactor.CompactAll());
        var repairedShard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
        Assert.Single(repairedShard.SegmentFiles, file => file.StartsWith("l2-", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(repairedShard.SegmentFiles, file => file.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(2, engine.ReadAllPoints("testdb", "autogen", "cpu", null, null).Count);
    }

    [Fact]
    public async Task Repair_ReaddsExistingSegmentFileMissingFromManifest()
    {
        var dataPath = Path.Combine(_testDir, "data");
        string segmentName;
        int shardId;
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01", 1)]);
            engine.FlushAll();
            var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
            shardId = shard.Id;
            segmentName = Assert.Single(shard.SegmentFiles);
            engine.Meta.RemoveSegmentsFromShard("testdb", "autogen", shardId, [segmentName]);
        }

        Assert.DoesNotContain(segmentName, new Manifest(dataPath).GetShards("testdb", "autogen").Single().SegmentFiles);

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(["repair", "--data", dataPath], CreateOptions(dataPath), stdout, stderr);

        var repairedShard = new Manifest(dataPath).GetShards("testdb", "autogen").Single(s => s.Id == shardId);
        Assert.Equal(0, exitCode);
        Assert.Contains(segmentName, repairedShard.SegmentFiles);
        Assert.Contains("segments_scanned=1", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    private static Point Point(string measurement, double value, string host, long timestampNs) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };

    private static MiniInfluxOptions CreateOptions(string dataPath) => new()
    {
        DataPath = dataPath,
        Wal = new WalOptions { Fsync = false, FsyncIntervalMs = 0, MaxWalFileBytes = 1024 * 1024 },
        Storage = new StorageOptions
        {
            RpCheckIntervalMs = 0,
            MaxSeriesPerDatabase = 10_000_000,
            MaxFieldsPerMeasurement = 1024,
            MaxResponseRows = 100_000,
            MaxQueryPoints = 1_000_000,
            MaxBufferPoints = 1_000_000,
            MaxBufferBytes = 0,
            MaxQueryMemoryBytes = 0
        }
    };
}

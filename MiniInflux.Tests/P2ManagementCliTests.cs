using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class P2ManagementCliTests : IDisposable
{
    private readonly string _testDir;

    public P2ManagementCliTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p2_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public async Task InspectSegment_ReportsMetadata()
    {
        var dataPath = Path.Combine(_testDir, "data");
        string segmentPath;
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 12.5, "server01", 1)]);
            engine.FlushAll();
            var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
            segmentPath = Path.Combine(dataPath, "db", "testdb", "autogen", "shards", shard.Id.ToString("D6"), shard.SegmentFiles[0]);
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "segment", "--path", segmentPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("columns=1", stdout.ToString());
        Assert.Contains("measurement=cpu", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public void InspectWal_ReportsReplayableRecords()
    {
        var dataPath = Path.Combine(_testDir, "data");
        Directory.CreateDirectory(dataPath);
        var walDir = Path.Combine(dataPath, "wal");
        Directory.CreateDirectory(walDir);
        using (var wal = new WalManager(walDir, fsync: false, fsyncIntervalMs: 0))
        {
            wal.Append("testdb", "autogen", [Point("cpu", 9, "server01", 1)]);
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "wal", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("replay_records=1", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task Compact_CommandMergesSegments()
    {
        var dataPath = Path.Combine(_testDir, "data");
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01", 1)]);
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 2, "server01", 2)]);
            engine.FlushAll();
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["compact", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        var manifest = new Manifest(dataPath);
        var shard = Assert.Single(manifest.GetShards("testdb", "autogen"));

        Assert.Equal(0, exitCode);
        Assert.Contains("compaction_tasks_merged=", stdout.ToString());
        Assert.Contains(shard.SegmentFiles, file => file.StartsWith("l", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task BackupAndRestore_CommandsRoundTripData()
    {
        var dataPath = Path.Combine(_testDir, "data");
        var backupPath = Path.Combine(_testDir, "backup");
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1000, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 7, "server01", 1)]);
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();

        Assert.Equal(
            0,
            ManagementCli.TryRun(["backup", "--data", dataPath, "--path", backupPath], CreateOptions(dataPath), stdout, stderr));

        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1000, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 99, "server02", 2)]);
        }

        Assert.Equal(
            0,
            ManagementCli.TryRun(["restore", "--data", dataPath, "--path", backupPath], CreateOptions(dataPath), stdout, stderr));

        BackupManager.ApplyPendingRestore(dataPath);

        using var restored = new TsdbEngine(dataPath, flushThreshold: 1000, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        restored.Recover();
        var points = restored.ReadAllPoints("testdb", "autogen", "cpu", null, null);

        Assert.Single(points);
        Assert.Equal(7.0, points[0].Fields["value"].AsDouble());
        Assert.Equal(string.Empty, stderr.ToString());
    }

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

    private static Point Point(string measurement, double value, string host, long timestampNs) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };
}

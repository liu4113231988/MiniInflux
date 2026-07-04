using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;
using System.Text.Json;

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
    public async Task InspectManifest_ReportsShardAndMeasurementSummary()
    {
        var dataPath = Path.Combine(_testDir, "data");
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 12.5, "server01", 1)]);
            engine.FlushAll();
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "manifest", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("databases=1", stdout.ToString());
        Assert.Contains("measurement db=testdb name=cpu series=1", stdout.ToString());
        Assert.Contains("shard db=testdb rp=autogen", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task InspectManifest_CanEmitStableJsonSchema()
    {
        var dataPath = Path.Combine(_testDir, "data");
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 12.5, "server01", 1)]);
            engine.FlushAll();
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "manifest", "--data", dataPath, "--format", "json"],
            CreateOptions(dataPath),
            stdout,
            stderr);

        using var json = JsonDocument.Parse(stdout.ToString());
        Assert.Equal(0, exitCode);
        Assert.Equal(1, json.RootElement.GetProperty("SchemaVersion").GetInt32());
        Assert.Equal("inspect-manifest", json.RootElement.GetProperty("Command").GetString());
        Assert.Equal(1, json.RootElement.GetProperty("Databases").GetInt32());
        Assert.Equal("testdb", json.RootElement.GetProperty("DatabaseEntries")[0].GetProperty("Name").GetString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task InspectSchema_ReportsFieldKinds()
    {
        var dataPath = Path.Combine(_testDir, "data");
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen",
            [
                new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { ["host"] = "server01" },
                    Fields = new Dictionary<string, FieldValue>
                    {
                        ["value"] = FieldValue.FromDouble(12.5),
                        ["ok"] = FieldValue.FromBoolean(true)
                    },
                    TimestampNs = 1
                }
            ]);
            engine.FlushAll();
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "schema", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("measurement db=testdb name=cpu fields=2", stdout.ToString());
        Assert.Contains("field db=testdb measurement=cpu name=ok kind=Boolean", stdout.ToString());
        Assert.Contains("field db=testdb measurement=cpu name=value kind=Float", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public void InspectTombstone_ReportsEntries()
    {
        var dataPath = Path.Combine(_testDir, "data");
        var tombstones = new TombstoneStore(dataPath);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 1, maxTime: 10);
        tombstones.AddSeriesDelete("testdb", "mem", "host=server01", minTime: 20, maxTime: 30);

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "tombstone", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("tombstones=2", stdout.ToString());
        Assert.Contains("database name=testdb tombstones=2", stdout.ToString());
        Assert.Contains("tombstone db=testdb measurement=cpu tags=* min_time_ns=1 max_time_ns=10", stdout.ToString());
        Assert.Contains("tombstone db=testdb measurement=mem tags=host=server01 min_time_ns=20 max_time_ns=30", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public void TombstoneStore_CreatesDirectoryOnlyWhenDeleteIsRecorded()
    {
        var dataPath = Path.Combine(_testDir, "data");
        _ = new TombstoneStore(dataPath);

        var tombstoneDir = Path.Combine(dataPath, "tombstones");
        Assert.False(Directory.Exists(tombstoneDir));

        var tombstones = new TombstoneStore(dataPath);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 1, maxTime: 10);

        Assert.True(Directory.Exists(tombstoneDir));
        Assert.True(File.Exists(Path.Combine(tombstoneDir, "testdb.json")));
    }

    [Fact]
    public void TombstoneStore_MergesOverlappingDeletes()
    {
        var dataPath = Path.Combine(_testDir, "data");
        var tombstones = new TombstoneStore(dataPath);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 10, maxTime: 20);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 15, maxTime: 30);

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "tombstone", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("tombstones=1", stdout.ToString());
        Assert.Contains("tombstone db=testdb measurement=cpu tags=* min_time_ns=10 max_time_ns=30", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public void TombstoneStore_MergesDeleteThatBridgesExistingRanges()
    {
        var dataPath = Path.Combine(_testDir, "data");
        var tombstones = new TombstoneStore(dataPath);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 10, maxTime: 20);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 30, maxTime: 40);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 15, maxTime: 35);

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "tombstone", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("tombstones=1", stdout.ToString());
        Assert.Contains("tombstone db=testdb measurement=cpu tags=* min_time_ns=10 max_time_ns=40", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public void TombstoneStore_FullDeleteCollapsesScopedRanges()
    {
        var dataPath = Path.Combine(_testDir, "data");
        var tombstones = new TombstoneStore(dataPath);
        tombstones.AddSeriesDelete("testdb", "cpu", "host=server01", minTime: 10, maxTime: 20);
        tombstones.AddSeriesDelete("testdb", "cpu", "host=server01");

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "tombstone", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("tombstones=1", stdout.ToString());
        Assert.Contains("tombstone db=testdb measurement=cpu tags=host=server01 min_time_ns=* max_time_ns=*", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public void TombstoneStore_SkipsSeriesDeleteCoveredByMeasurementDelete()
    {
        var dataPath = Path.Combine(_testDir, "data");
        var tombstones = new TombstoneStore(dataPath);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 10, maxTime: 30);
        tombstones.AddSeriesDelete("testdb", "cpu", "host=server01", minTime: 15, maxTime: 20);

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "tombstone", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("tombstones=1", stdout.ToString());
        Assert.Contains("tombstone db=testdb measurement=cpu tags=* min_time_ns=10 max_time_ns=30", stdout.ToString());
        Assert.DoesNotContain("host=server01", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public void TombstoneStore_MeasurementDeleteRemovesCoveredSeriesDeletes()
    {
        var dataPath = Path.Combine(_testDir, "data");
        var tombstones = new TombstoneStore(dataPath);
        tombstones.AddSeriesDelete("testdb", "cpu", "host=server01", minTime: 15, maxTime: 20);
        tombstones.AddSeriesDelete("testdb", "cpu", "host=server02", minTime: 40, maxTime: 50);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 10, maxTime: 30);

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "tombstone", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("tombstones=2", stdout.ToString());
        Assert.Contains("tombstone db=testdb measurement=cpu tags=* min_time_ns=10 max_time_ns=30", stdout.ToString());
        Assert.Contains("tombstone db=testdb measurement=cpu tags=host=server02 min_time_ns=40 max_time_ns=50", stdout.ToString());
        Assert.DoesNotContain("host=server01", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public void TombstoneStore_SkipsDeleteCoveredBySameScope()
    {
        var dataPath = Path.Combine(_testDir, "data");
        var tombstones = new TombstoneStore(dataPath);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 10, maxTime: 30);
        tombstones.AddMeasurementDelete("testdb", "cpu", minTime: 15, maxTime: 20);

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["inspect", "tombstone", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("tombstones=1", stdout.ToString());
        Assert.Contains("tombstone db=testdb measurement=cpu tags=* min_time_ns=10 max_time_ns=30", stdout.ToString());
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
    public async Task Compact_DryRun_DoesNotModifyLiveSegments()
    {
        var dataPath = Path.Combine(_testDir, "data");
        string[] beforeSegments;
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01", 1)]);
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 2, "server01", 2)]);
            engine.FlushAll();
            beforeSegments = new Manifest(dataPath).GetShards("testdb", "autogen").Single().SegmentFiles.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray();
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["compact", "--data", dataPath, "--dry-run"],
            CreateOptions(dataPath),
            stdout,
            stderr);

        var afterSegments = new Manifest(dataPath).GetShards("testdb", "autogen").Single().SegmentFiles.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray();
        Assert.Equal(0, exitCode);
        Assert.Equal(beforeSegments, afterSegments);
        Assert.Contains("dry_run=true", stdout.ToString());
        Assert.Contains("changes_applied=false", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task Compact_DryRun_CanEmitJsonSchema()
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
            ["compact", "--data", dataPath, "--dry-run", "--format", "json"],
            CreateOptions(dataPath),
            stdout,
            stderr);

        using var json = JsonDocument.Parse(stdout.ToString());
        Assert.Equal(0, exitCode);
        Assert.Equal(1, json.RootElement.GetProperty("SchemaVersion").GetInt32());
        Assert.Equal("compact", json.RootElement.GetProperty("Command").GetString());
        Assert.True(json.RootElement.GetProperty("DryRun").GetBoolean());
        Assert.False(json.RootElement.GetProperty("ChangesApplied").GetBoolean());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task ValidateDataDir_ReportsHealthyDirectory()
    {
        var dataPath = Path.Combine(_testDir, "data");
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 3, "server01", 1)]);
            engine.FlushAll();
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["validate", "data-dir", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("issues=0", stdout.ToString());
        Assert.Contains("manifest_segment_files=1", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task ValidateDataDir_FailsWhenManifestReferencesMissingSegment()
    {
        var dataPath = Path.Combine(_testDir, "data");
        string segmentPath;
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 5, "server01", 1)]);
            engine.FlushAll();
            var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
            segmentPath = Path.Combine(dataPath, "db", "testdb", "autogen", "shards", shard.Id.ToString("D6"), shard.SegmentFiles[0]);
        }

        File.Delete(segmentPath);

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["validate", "data-dir", "--data", dataPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(1, exitCode);
        Assert.Contains("issue code=segment_missing_on_disk", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task ValidateDataDir_CanEmitJsonIssues()
    {
        var dataPath = Path.Combine(_testDir, "data");
        string segmentPath;
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 5, "server01", 1)]);
            engine.FlushAll();
            var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
            segmentPath = Path.Combine(dataPath, "db", "testdb", "autogen", "shards", shard.Id.ToString("D6"), shard.SegmentFiles[0]);
        }

        File.Delete(segmentPath);

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["validate", "data-dir", "--data", dataPath, "--format", "json"],
            CreateOptions(dataPath),
            stdout,
            stderr);

        using var json = JsonDocument.Parse(stdout.ToString());
        Assert.Equal(1, exitCode);
        Assert.Equal(1, json.RootElement.GetProperty("SchemaVersion").GetInt32());
        Assert.Equal("validate-data-dir", json.RootElement.GetProperty("Command").GetString());
        Assert.True(json.RootElement.GetProperty("Issues").GetInt32() > 0);
        Assert.Equal("segment_missing_on_disk", json.RootElement.GetProperty("IssueEntries")[0].GetProperty("Code").GetString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task BackupVerify_CommandReportsVerifiedBackup()
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

        stdout.GetStringBuilder().Clear();
        stderr.GetStringBuilder().Clear();

        var exitCode = ManagementCli.TryRun(
            ["backup", "verify", "--path", backupPath],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Contains("verified=true", stdout.ToString());
        Assert.Contains("files=", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task BackupVerify_CommandFailsForTamperedBackup()
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

        var tamperedFile = Directory.GetFiles(backupPath, "*.json", SearchOption.AllDirectories)
            .First(path => !string.Equals(Path.GetFileName(path), "backup.metadata.json", StringComparison.OrdinalIgnoreCase));
        File.AppendAllText(tamperedFile, "tampered");

        stdout.GetStringBuilder().Clear();
        stderr.GetStringBuilder().Clear();

        var exitCode = ManagementCli.TryRun(
            ["backup", "verify", "--path", backupPath],
            CreateOptions(dataPath),
            stdout,
            stderr);
        Assert.Equal(1, exitCode);
        Assert.Contains("backup verification failed:", stderr.ToString());
        Assert.Contains("checksum mismatch", stderr.ToString());
    }

    [Fact]
    public async Task Restore_DryRun_ValidatesBackup_WithoutPreparingPendingDirectory()
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

        stdout.GetStringBuilder().Clear();
        stderr.GetStringBuilder().Clear();

        var exitCode = ManagementCli.TryRun(
            ["restore", "--data", dataPath, "--path", backupPath, "--dry-run"],
            CreateOptions(dataPath),
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.False(Directory.Exists(dataPath + ".restore-pending"));
        Assert.Contains("dry_run=true", stdout.ToString());
        Assert.Contains("validated_backup=true", stdout.ToString());
        Assert.Contains("changes_applied=false", stdout.ToString());
        Assert.Equal(string.Empty, stderr.ToString());
    }

    [Fact]
    public async Task Restore_DryRun_CanEmitJsonSchema()
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

        stdout.GetStringBuilder().Clear();
        stderr.GetStringBuilder().Clear();

        var exitCode = ManagementCli.TryRun(
            ["restore", "--data", dataPath, "--path", backupPath, "--dry-run", "--format", "json"],
            CreateOptions(dataPath),
            stdout,
            stderr);

        using var json = JsonDocument.Parse(stdout.ToString());
        Assert.Equal(0, exitCode);
        Assert.Equal(1, json.RootElement.GetProperty("SchemaVersion").GetInt32());
        Assert.Equal("restore", json.RootElement.GetProperty("Command").GetString());
        Assert.True(json.RootElement.GetProperty("DryRun").GetBoolean());
        Assert.True(json.RootElement.GetProperty("ValidatedBackup").GetBoolean());
        Assert.False(json.RootElement.GetProperty("ChangesApplied").GetBoolean());
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

    [Fact]
    public async Task Repair_DryRun_DoesNotRewriteManifest()
    {
        var dataPath = Path.Combine(_testDir, "data");
        string segmentName;
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01", 1)]);
            engine.FlushAll();
            var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
            segmentName = Assert.Single(shard.SegmentFiles);
            engine.Meta.RemoveSegmentsFromShard("testdb", "autogen", shard.Id, [segmentName]);
        }

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var exitCode = ManagementCli.TryRun(
            ["repair", "--data", dataPath, "--dry-run"],
            CreateOptions(dataPath),
            stdout,
            stderr);

        var shardAfter = new Manifest(dataPath).GetShards("testdb", "autogen").Single();
        Assert.Equal(0, exitCode);
        Assert.DoesNotContain(segmentName, shardAfter.SegmentFiles);
        Assert.Contains("dry_run=true", stdout.ToString());
        Assert.Contains("changes_applied=false", stdout.ToString());
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

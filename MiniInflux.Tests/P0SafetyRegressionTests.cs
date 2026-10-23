using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;
using System.Text.Json;
using MiniInflux.Net10;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Regression coverage for the P0 data-safety issues documented in docs/todo-202610.md.
/// </summary>
public sealed class P0SafetyRegressionTests : IDisposable
{
    private readonly string _testDir;

    public P0SafetyRegressionTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p0_safety_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    private static Point Point(string measurement, double value, long timestampNs, params (string Key, string Value)[] tags) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string>(tags.ToDictionary(t => t.Key, t => t.Value), StringComparer.Ordinal),
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };

    private static TsdbEngine NewEngine(string path, int flushThreshold = 1000) =>
        new(path, flushThreshold: flushThreshold, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);

    /// <summary>
    /// Copies a data directory into a crash image. Sources may still be open for append, hence the
    /// permissive share mode on the read side.
    /// </summary>
    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (var file in Directory.GetFiles(source, "*", SearchOption.AllDirectories))
        {
            var target = Path.Combine(destination, Path.GetRelativePath(source, file));
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            using var input = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var output = new FileStream(target, FileMode.Create, FileAccess.Write, FileShare.None);
            input.CopyTo(output);
        }
    }

    private static void AssertSingleSeriesValue(QueryResponse response, string columnPrefix, double expected)
    {
        Assert.Null(response.Results[0].Error);
        var series = Assert.Single(response.Results[0].Series!);
        Assert.Single(series.Values);
        var column = series.Columns.FindIndex(c => c.StartsWith(columnPrefix, StringComparison.OrdinalIgnoreCase));
        Assert.True(column >= 0, $"columns: {string.Join(",", series.Columns)}");
        Assert.Equal(expected, Convert.ToDouble(series.Values[0][column]));
    }

    [Fact]
    public async Task Query_WithTagKeyThatIsAlsoStringField_DoesNotDropPredicate()
    {
        using var engine = NewEngine(Path.Combine(_testDir, "query"));
        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["foo"] = "bar", ["host"] = "a" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(1) },
                TimestampNs = 1
            },
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "b" },
                Fields = new Dictionary<string, FieldValue>
                {
                    ["foo"] = FieldValue.FromString("bar"),
                    ["value"] = FieldValue.FromDouble(2)
                },
                TimestampNs = 2
            }
        ]);

        var executor = new QueryExecutor();

        var last = await executor.ExecuteAsync(engine, "testdb", "SELECT last(value) FROM cpu WHERE foo = 'bar'");
        AssertSingleSeriesValue(last, "last", 1);

        var mean = await executor.ExecuteAsync(engine, "testdb", "SELECT mean(value) FROM cpu WHERE foo = 'bar'");
        AssertSingleSeriesValue(mean, "mean", 1);

        var raw = await executor.ExecuteAsync(engine, "testdb", "SELECT value FROM cpu WHERE foo = 'bar'");
        AssertSingleSeriesValue(raw, "value", 1);

        // The predicate must survive the buffer -> segment transition too.
        engine.FlushAll();
        var flushedLast = await executor.ExecuteAsync(engine, "testdb", "SELECT last(value) FROM cpu WHERE foo = 'bar'");
        AssertSingleSeriesValue(flushedLast, "last", 1);

        var flushedMean = await executor.ExecuteAsync(engine, "testdb", "SELECT mean(value) FROM cpu WHERE foo = 'bar'");
        AssertSingleSeriesValue(flushedMean, "mean", 1);

        var flushedRaw = await executor.ExecuteAsync(engine, "testdb", "SELECT value FROM cpu WHERE foo = 'bar'");
        AssertSingleSeriesValue(flushedRaw, "value", 1);

        var orRaw = await executor.ExecuteAsync(engine, "testdb", "SELECT value FROM cpu WHERE foo = 'bar' OR host = 'b'");
        Assert.Null(orRaw.Results[0].Error);
        Assert.Equal(2, orRaw.Results[0].Series!.Sum(s => s.Values.Count));
    }

    [Fact]
    public void Wal_WithTornTail_IsTruncatedBeforeAppendAndReplayStillReturnsValidRecords()
    {
        var walDir = Path.Combine(_testDir, "wal");
        using (var wal = new WalManager(walDir, fsync: false))
            wal.AppendBatch("testdb", "autogen", [Point("cpu", 1, 10, ("host", "a"))]);

        var walPath = Assert.Single(Directory.GetFiles(walDir, "*.wal"));
        var validLength = new FileInfo(walPath).Length;
        using (var stream = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.Read))
            stream.Write([0xde, 0xad, 0xbe, 0xef, 0x01]);

        using (var repaired = new WalManager(walDir, fsync: false))
        {
            Assert.Equal(validLength, new FileInfo(walPath).Length);
            var replayed = repaired.Replay();
            var batch = Assert.Single(replayed);
            Assert.Equal("testdb", batch.Db);
            Assert.Equal("autogen", batch.Rp);
            Assert.Single(batch.Points);

            // New records must land after the repaired tail, not after the torn bytes.
            repaired.AppendBatch("testdb", "autogen", [Point("cpu", 2, 20, ("host", "b"))]);
        }

        using (var afterSecondRestart = new WalManager(walDir, fsync: false))
        {
            var replayed = afterSecondRestart.Replay();
            Assert.Equal(2, replayed.Count);
            Assert.Equal(new[] { 1d, 2d }, replayed.Select(batch => batch.Points[0].Fields["value"].Float).ToArray());
        }
    }

    [Fact]
    public void Wal_WithCrcCorruptionInLastRecord_TruncatesItBeforeAppending()
    {
        var walDir = Path.Combine(_testDir, "wal-crc");
        using (var wal = new WalManager(walDir, fsync: false))
            wal.AppendBatch("testdb", "autogen", [Point("cpu", 1, 10, ("host", "a"))]);

        var walPath = Assert.Single(Directory.GetFiles(walDir, "*.wal"));
        var bytes = File.ReadAllBytes(walPath);
        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        Assert.True(payloadLength > 0 && bytes.Length >= 8 + payloadLength);
        bytes[8 + payloadLength - 1] ^= 0xff;
        File.WriteAllBytes(walPath, bytes);

        using (var repaired = new WalManager(walDir, fsync: false))
        {
            Assert.Equal(0, new FileInfo(walPath).Length);
            Assert.Empty(repaired.Replay());
            repaired.AppendBatch("testdb", "autogen", [Point("cpu", 2, 20, ("host", "b"))]);
        }

        using var afterRestart = new WalManager(walDir, fsync: false);
        var replayed = afterRestart.Replay();
        var batch = Assert.Single(replayed);
        Assert.Equal(2, batch.Points[0].Fields["value"].Float);
    }

    [Fact]
    public async Task WalCheckpoint_WaitsForAppendPublication_AndStopsAtEarliestUnflushedReplayFloor()
    {
        using var engine = NewEngine(Path.Combine(_testDir, "wal-checkpoint"));
        var engineType = typeof(TsdbEngine);
        var appendGate = Assert.IsType<ReaderWriterLockSlim>(
            engineType.GetField("_walAppendGate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine));
        var replayFloors = Assert.IsType<ConcurrentDictionary<string, WalPosition>>(
            engineType.GetField("_bufferReplayFloors", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine));
        var wal = Assert.IsType<WalManager>(
            engineType.GetField("_wal", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine));
        var checkpoint = engineType.GetMethod("UpdateWalCheckpoint", BindingFlags.Instance | BindingFlags.NonPublic)!;

        using var gateEntered = new ManualResetEventSlim();
        using var releaseGate = new ManualResetEventSlim();
        var gateOwnerTask = Task.Factory.StartNew(() =>
        {
            appendGate.EnterReadLock();
            gateEntered.Set();
            releaseGate.Wait();
            appendGate.ExitReadLock();
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        Assert.True(gateEntered.Wait(TimeSpan.FromSeconds(5)), "failed to acquire the append-publication gate");

        Task? checkpointTask = null;
        WalPosition? earliestUnflushedPosition = null;
        try
        {
            await engine.WriteAsync(
                "testdb",
                "rp1",
                [Point("cpu", 1, 10, ("host", "a"))]);
            await engine.WriteAsync(
                "testdb",
                "rp2",
                [Point("cpu", 2, 20, ("host", "b"))]);

            Assert.Equal(2, replayFloors.Count);
            earliestUnflushedPosition = replayFloors.Values
                .OrderBy(position => position.FileId)
                .ThenBy(position => position.Offset)
                .First();

            checkpointTask = Task.Run(() => checkpoint.Invoke(engine, null));
            Assert.True(
                SpinWait.SpinUntil(() => appendGate.WaitingWriteCount > 0, TimeSpan.FromSeconds(5)),
                "checkpoint did not wait for the append-publication gate");
            Assert.False(checkpointTask.IsCompleted);
        }
        finally
        {
            releaseGate.Set();
            await gateOwnerTask;
        }

        await checkpointTask!;
        Assert.True(earliestUnflushedPosition.HasValue);
        Assert.Equal(earliestUnflushedPosition.Value, wal.CheckpointPosition);
    }

    [Fact]
    public async Task CrashAfterAckBeforeBufferFloorPublication_RestartReplaysAcknowledgedPoint()
    {
        var dataPath = Path.Combine(_testDir, "wal-crash");
        var crashCopy = Path.Combine(_testDir, "wal-crash-image");
        using var engine = NewEngine(dataPath);
        engine.CreateDatabase("testdb");

        var engineType = typeof(TsdbEngine);
        var appendGate = Assert.IsType<ReaderWriterLockSlim>(
            engineType.GetField("_walAppendGate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine));
        var wal = Assert.IsType<WalManager>(
            engineType.GetField("_wal", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine));
        var checkpoint = engineType.GetMethod("UpdateWalCheckpoint", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var flushWalToDisk = typeof(WalManager).GetMethod("FlushToDisk", BindingFlags.Instance | BindingFlags.NonPublic)!;

        using var appended = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var pausedAppend = Task.Factory.StartNew(() =>
        {
            appendGate.EnterReadLock();
            try
            {
                // Model the write path exactly at its crash window: the WAL record is durable and
                // already acknowledged, but the buffer replay floor has not been published yet.
                wal.AppendBatch("testdb", "autogen", [Point("cpu", 42, 100, ("host", "a"))]);
                flushWalToDisk.Invoke(wal, null);
                appended.Set();
                release.Wait();
            }
            finally { appendGate.ExitReadLock(); }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        Assert.True(appended.Wait(TimeSpan.FromSeconds(5)), "paused append did not complete");
        var checkpointTask = Task.Run(() => checkpoint.Invoke(engine, null));
        Assert.True(
            SpinWait.SpinUntil(() => appendGate.WaitingWriteCount > 0, TimeSpan.FromSeconds(5)),
            "checkpoint did not wait for the paused append");
        Assert.False(checkpointTask.IsCompleted);

        // The crash image is taken while the checkpoint is still blocked: WAL has the record, the
        // checkpoint file does not exist yet. A restart from this image must replay the point.
        CopyDirectory(dataPath, crashCopy);
        Assert.False(File.Exists(Path.Combine(crashCopy, "wal", "checkpoint.dat")));

        release.Set();
        await pausedAppend;
        await checkpointTask;

        using var recovered = NewEngine(crashCopy);
        recovered.Recover();
        var point = Assert.Single(recovered.ReadAllPoints("testdb", "autogen", "cpu", null, null));
        Assert.Equal(42, point.Fields["value"].AsDouble());
    }

    [Fact]
    public void Manifest_WithCorruptPrimary_RecoversFromBackup_AndFailsWhenBothAreCorrupt()
    {
        var root = Path.Combine(_testDir, "manifest");
        var original = new Manifest(root);
        original.EnsureDatabase("testdb");
        original.Save();

        var primary = Path.Combine(root, "meta", "manifest.json");
        var backup = primary + ".bak";
        Assert.True(File.Exists(backup));

        File.WriteAllText(primary, "{ not-json");
        var recovered = new Manifest(root);
        Assert.True(recovered.HasDatabase("testdb"));
        Assert.Single(Directory.GetFiles(Path.Combine(root, "meta"), "manifest.json.corrupt-*"));

        File.Delete(primary);
        var recoveredFromMissingPrimary = new Manifest(root);
        Assert.True(recoveredFromMissingPrimary.HasDatabase("testdb"));

        // With no primary, no backup, and real files on disk, starting fresh would hide data.
        File.Delete(backup);
        Directory.CreateDirectory(Path.Combine(root, "db", "ghost"));
        File.WriteAllText(Path.Combine(root, "db", "ghost", "000001.seg"), "segment");
        Assert.Throws<InvalidDataException>(() => new Manifest(root));

        File.WriteAllText(primary, "{ not-json");
        File.WriteAllText(backup, "{ also-not-json");
        Assert.Throws<InvalidDataException>(() => new Manifest(root));
    }

    [Fact]
    public void Manifest_RejectsNamesThatAliasOnCaseInsensitiveFileSystems()
    {
        var root = Path.Combine(_testDir, "case-aliases");
        var manifest = new Manifest(root);
        manifest.EnsureDatabase("metrics");
        Assert.Throws<InvalidOperationException>(() => manifest.EnsureDatabase("Metrics"));

        manifest.EnsureRp("metrics", "autogen");
        Assert.Throws<InvalidOperationException>(() => manifest.EnsureRp("metrics", "Autogen"));

        var legacy = new ManifestData();
        legacy.Databases["metrics"] = new DatabaseInfo();
        legacy.Databases["Metrics"] = new DatabaseInfo();
        var primary = Path.Combine(root, "meta", "manifest.json");
        var manifestJson = JsonSerializer.Serialize(legacy, ManifestJsonContext.Default.ManifestData);
        File.WriteAllText(primary, manifestJson);
        File.WriteAllText(primary + ".bak", manifestJson);
        Assert.Throws<InvalidDataException>(() => new Manifest(root));
    }

    [Fact]
    public async Task Write_WithTraversalDatabaseName_IsRejected()
    {
        using var engine = NewEngine(Path.Combine(_testDir, "identifiers"));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            engine.WriteAsync("../escape", "autogen", [Point("cpu", 1, 1, ("host", "a"))]));
        Assert.Throws<ArgumentException>(() => engine.CreateDatabase("..\\escape"));
        Assert.Throws<ArgumentException>(() => engine.CreateDatabaseWithRp("rpdb", 0, 0, 1, "../escape"));
        Assert.Throws<ArgumentException>(() =>
            engine.ReadAllPoints("../escape", "autogen", "cpu", null, null));

        Assert.False(IdentifierValidator.IsValidDatabaseName("../escape"));
        Assert.False(IdentifierValidator.IsValidRetentionPolicyName("daily/../x"));
        Assert.False(V2WriteSupport.TryResolveBucket("../escape", out _, out _, out _));
        Assert.True(V2WriteSupport.TryResolveBucket("db/rp", out var db, out var rp, out _));
        Assert.Equal("db", db);
        Assert.Equal("rp", rp);
    }

    [Fact]
    public void QueryPermission_ClassifiesSelectIntoAndDdlAsWrite()
    {
        Assert.Equal("read", QueryPermission.RequiredPermission(InfluxQlParser.Parse("SELECT value FROM cpu")));
        Assert.Equal("read", QueryPermission.RequiredPermission(InfluxQlParser.Parse("SHOW DATABASES")));
        Assert.Equal("write", QueryPermission.RequiredPermission(InfluxQlParser.Parse("SELECT value INTO archive.cpu FROM cpu")));
        Assert.Equal("write", QueryPermission.RequiredPermission(InfluxQlParser.Parse("CREATE DATABASE archive")));
        Assert.Equal("write", QueryPermission.RequiredPermission(InfluxQlParser.Parse("DELETE FROM cpu")));
        Assert.Equal("write", QueryPermission.RequiredPermission(InfluxQlParser.Parse("DROP MEASUREMENT cpu")));
        Assert.Equal("write", QueryPermission.RequiredPermission(InfluxQlParser.Parse("DROP DATABASE testdb")));
    }

    [Fact]
    public void StartupSecurity_WhenAuthOrTlsIsEnabledWithoutConfiguration_FailsClosed()
    {
        Assert.Throws<InvalidOperationException>(() => StartupSecurity.Validate(new MiniInfluxOptions
        {
            Auth = new AuthOptions { Enabled = true, Password = "" }
        }));

        Assert.Throws<InvalidOperationException>(() => StartupSecurity.Validate(new MiniInfluxOptions
        {
            Auth = new AuthOptions { Enabled = true, Username = "admin", Password = "   " }
        }));

        Assert.Throws<InvalidOperationException>(() => StartupSecurity.Validate(new MiniInfluxOptions
        {
            Tls = new TlsOptions { Enabled = true, CertPath = Path.Combine(_testDir, "missing.pfx") }
        }));

        var certPath = Path.Combine(_testDir, "cert.pfx");
        File.WriteAllText(certPath, "test-certificate");
        StartupSecurity.Validate(new MiniInfluxOptions
        {
            Tls = new TlsOptions { Enabled = true, CertPath = certPath }
        });

        StartupSecurity.Validate(new MiniInfluxOptions());
    }

    [Fact]
    public void Backup_WhileDataDirectoryLockIsHeld_CopiesAndRestoresData()
    {
        var source = Path.Combine(_testDir, "data");
        var backup = Path.Combine(_testDir, "backup");
        Directory.CreateDirectory(source);
        File.WriteAllText(Path.Combine(source, "live.txt"), "before");
        // Legacy/transient files must never be copied into a backup.
        File.WriteAllText(Path.Combine(source, ".miniinflux.lock"), "");
        File.WriteAllText(Path.Combine(source, "manifest.json.tmp"), "partial");
        Directory.CreateDirectory(Path.Combine(source, ".restore-old"));
        File.WriteAllText(Path.Combine(source, ".restore-old", "stale.txt"), "stale");

        using var dataLock = DataDirectoryLock.Acquire(source);
        BackupManager.CreateBackup(source, backup);
        Assert.False(File.Exists(Path.Combine(backup, ".miniinflux.lock")));
        Assert.False(File.Exists(Path.Combine(backup, "manifest.json.tmp")));
        Assert.False(Directory.Exists(Path.Combine(backup, ".restore-old")));

        Directory.CreateDirectory(Path.Combine(_testDir, "backup-source"));
        File.WriteAllText(Path.Combine(_testDir, "backup-source", "live.txt"), "after");
        BackupManager.CreateBackup(Path.Combine(_testDir, "backup-source"), backup);

        BackupManager.PrepareRestore(backup, source);
        BackupManager.ApplyPendingRestore(source);

        Assert.Equal("after", File.ReadAllText(Path.Combine(source, "live.txt")));
    }

    [Fact]
    public void CreateBackup_WithDestinationContainingDataDirectory_IsRejected()
    {
        var source = Path.Combine(_testDir, "data");
        Directory.CreateDirectory(source);
        File.WriteAllText(Path.Combine(source, "live.txt"), "keep");

        // The parent of the data directory must be rejected: replacing the destination would
        // delete the live data directory before a single byte is copied.
        var ancestor = _testDir;
        var error = Assert.Throws<InvalidOperationException>(() => BackupManager.CreateBackup(source, ancestor));

        Assert.Contains("must not contain the data directory", error.Message);
        Assert.Equal("keep", File.ReadAllText(Path.Combine(source, "live.txt")));
        Assert.False(File.Exists(Path.Combine(_testDir, "backup.metadata.json")));

        var child = Path.Combine(source, "nested");
        Assert.Throws<InvalidOperationException>(() => BackupManager.CreateBackup(source, child));
    }

    [Fact]
    public void ValidateBackup_WithoutMetadata_RejectsUnverifiableBackup()
    {
        var backup = Path.Combine(_testDir, "unverified");
        Directory.CreateDirectory(backup);
        File.WriteAllText(Path.Combine(backup, "data.txt"), "x");

        Assert.Throws<InvalidDataException>(() => BackupManager.ValidateBackup(backup));
    }

    [Fact]
    public void ValidateBackup_WithTraversalRelativePath_RejectsMetadata()
    {
        var backup = Path.Combine(_testDir, "traversal");
        Directory.CreateDirectory(backup);
        foreach (var unsafePath in new[] { "../outside.txt", "..\\outside.txt", "C:\\outside.txt" })
        {
            var metadata = new BackupMetadata(
                1,
                DateTimeOffset.UtcNow,
                backup,
                [new BackupFileEntry(unsafePath, 0, "")]);
            File.WriteAllText(
                Path.Combine(backup, "backup.metadata.json"),
                JsonSerializer.Serialize(metadata, EngineJsonContext.Default.BackupMetadata));

            Assert.Throws<InvalidDataException>(() => BackupManager.ValidateBackup(backup));
        }
    }

    [Fact]
    public void CreateBackup_WithDestinationContainingSource_IsRejected()
    {
        var source = Path.Combine(_testDir, "nested", "data");
        Directory.CreateDirectory(source);
        File.WriteAllText(Path.Combine(source, "live.txt"), "x");

        Assert.Throws<InvalidOperationException>(() => BackupManager.CreateBackup(source, Path.Combine(_testDir, "nested")));
    }
}

using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class BackupConsistencyTests : IDisposable
{
    private readonly string _testDir;

    public BackupConsistencyTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_bk_{Guid.NewGuid():N}");
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

    private static TsdbEngine NewEngine(string path) => new(path, flushThreshold: 1000, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);

    [Fact]
    public async Task CreateConsistentBackup_WhileWriting_CapturesBufferedPointsAndExcludesLaterWrites()
    {
        var dataPath = Path.Combine(_testDir, "data");
        var backupPath = Path.Combine(_testDir, "backup");

        using (var engine = NewEngine(dataPath))
        {
            await engine.WriteAsync("testdb", "autogen", [PointAt(1, 1_000_000_000)]);
            engine.FlushAll();
            // Stays in the buffer: CreateConsistentBackup must flush it into segments before copying.
            await engine.WriteAsync("testdb", "autogen", [PointAt(2, 2_000_000_000)]);

            engine.CreateConsistentBackup(backupPath);

            // Post-backup writes must not appear in the snapshot.
            await engine.WriteAsync("testdb", "autogen", [PointAt(3, 3_000_000_000)]);
        }

        var restoreRoot = Path.Combine(_testDir, "restored");
        BackupManager.PrepareRestore(backupPath, restoreRoot);
        BackupManager.ApplyPendingRestore(restoreRoot);

        using var restored = NewEngine(restoreRoot);
        restored.Recover();
        var points = restored.ReadAllPoints("testdb", "autogen", "cpu", null, null);

        Assert.Equal([1_000_000_000L, 2_000_000_000L], points.Select(p => p.TimestampNs).OrderBy(t => t).ToList());
    }

    [Fact]
    public async Task CreateConsistentBackup_SequentialCalls_AllSucceed()
    {
        var dataPath = Path.Combine(_testDir, "data2");
        using (var engine = NewEngine(dataPath))
        {
            await engine.WriteAsync("testdb", "autogen", [PointAt(1, 1_000_000_000)]);
            engine.CreateConsistentBackup(Path.Combine(_testDir, "backup-1"));
            engine.CreateConsistentBackup(Path.Combine(_testDir, "backup-2"));
        }

        Assert.True(File.Exists(Path.Combine(_testDir, "backup-1", "meta", "manifest.json")));
        Assert.True(File.Exists(Path.Combine(_testDir, "backup-2", "meta", "manifest.json")));
    }
}

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
}

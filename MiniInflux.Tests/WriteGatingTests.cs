using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class WriteGatingTests : IDisposable
{
    private readonly string _testDir;

    public WriteGatingTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_gate_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    private static Point PointAt(double value) => new()
    {
        Measurement = "cpu",
        Tags = new Dictionary<string, string> { ["host"] = "server01" },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = 1_000_000_000
    };

    [Fact]
    public async Task Write_MinFreeDiskBytesExceeded_ThrowsDiskSpaceExceeded()
    {
        // A floor no drive can satisfy gates every write.
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, minFreeDiskBytes: long.MaxValue / 2);

        await Assert.ThrowsAsync<DiskSpaceExceededException>(
            () => engine.WriteAsync("testdb", "autogen", [PointAt(1)]));
    }

    [Fact]
    public async Task Write_HealthyDisk_AcceptsWrites()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, minFreeDiskBytes: 1);
        await engine.WriteAsync("testdb", "autogen", [PointAt(1)]);
        Assert.Single(engine.ReadAllPoints("testdb", "autogen", "cpu", null, null));
    }

    [Fact]
    public async Task Write_LatchedFailure_RecoveredBySuccessfulAppend()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1);
        engine.Health.RecordFailure("wal_append", new IOException("disk full"), blocksWrites: true);
        Assert.False(engine.Health.WriteAvailable);

        await Assert.ThrowsAsync<IOException>(() => engine.WriteAsync("testdb", "autogen", [PointAt(1)]));
        Assert.False(engine.Health.WriteAvailable);

        // Simulate the disk recovering: the periodic probe succeeds and unlatches writes.
        Assert.True(engine.Health.TryRecover(() => true));
        Assert.True(engine.Health.WriteAvailable);
        await engine.WriteAsync("testdb", "autogen", [PointAt(1)]);
        Assert.Single(engine.ReadAllPoints("testdb", "autogen", "cpu", null, null));
    }

    [Fact]
    public async Task Write_LatchedFailure_UnlatchedBySuccessfulAppend()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1);
        engine.Health.RecordFailure("wal_append", new IOException("disk full"), blocksWrites: true);
        Assert.False(engine.Health.WriteAvailable);

        // A successful append is proof the path works again (even with fsync enabled).
        await Assert.ThrowsAsync<IOException>(() => engine.WriteAsync("testdb", "autogen", [PointAt(1)]));
        engine.Health.TryRecover(() => true);
        await engine.WriteAsync("testdb", "autogen", [PointAt(2)]);
        Assert.True(engine.Health.WriteAvailable);
    }
}

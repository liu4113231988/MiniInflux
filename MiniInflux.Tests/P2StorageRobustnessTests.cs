using System.Text;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// P2-4 存储健壮性：损坏段隔离、tombstone 持久化、临时文件清理、发布闸门超时、
/// 过期 shard 索引清理与未使用缓存删除。
/// </summary>
public class P2StorageRobustnessTests : IDisposable
{
    private readonly string _testDir;

    public P2StorageRobustnessTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p2storage_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir)) Directory.Delete(_testDir, true);
        }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    private static Point PointAt(string measurement, string host, double value, long timestampNs) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };

    [Fact]
    public void ReadMetadata_WithValidCrcButTruncatedPayload_QuarantinesPath()
    {
        var path = Path.Combine(_testDir, "truncated.seg");
        using (var ms = new MemoryStream())
        using (var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(0x4D545344u); // segment magic
            bw.Write((byte)4);     // format version
            bw.Write(100);         // huge column count without payload
            bw.Flush();
            var data = ms.ToArray();
            var fileBytes = new byte[data.Length + 4];
            data.CopyTo(fileBytes, 0);
            BitConverter.TryWriteBytes(fileBytes.AsSpan(data.Length), Crc32.Compute(data));
            File.WriteAllBytes(path, fileBytes);
        }

        // Truncation after the header surfaces as EndOfStreamException, not InvalidDataException.
        Assert.Throws<EndOfStreamException>(() => SegmentReader.ReadMetadata(path));
        Assert.True(SegmentReader.IsQuarantined(path));
        SegmentReader.ClearQuarantined(path);
    }

    [Fact]
    public void TombstoneStore_WithCorruptFile_ReportsFailureAndPersistsWithoutTempFile()
    {
        var health = new StorageHealth();
        var tombstoneDir = Path.Combine(_testDir, "tombstones");
        Directory.CreateDirectory(tombstoneDir);
        File.WriteAllText(Path.Combine(tombstoneDir, "db1.json"), "{ this is not json");

        var store = new TombstoneStore(_testDir, health);
        Assert.Equal(1, health.FailureCount);
        Assert.Equal("tombstone_load", health.LastFailureComponent);
        Assert.False(store.HasTombstones("db1"));

        store.AddSeriesDelete("db1", "cpu", "host=a", 1, 2);
        Assert.True(File.Exists(Path.Combine(tombstoneDir, "db1.json")));
        Assert.False(File.Exists(Path.Combine(tombstoneDir, "db1.json.tmp")));

        var reloaded = new TombstoneStore(_testDir);
        Assert.True(reloaded.IsColumnDeleted("db1", "cpu", "host=a", 1, 2));
    }

    [Fact]
    public void WriteColumns_WhenEncodingFails_LeavesNoSegmentOrTempFile()
    {
        var path = Path.Combine(_testDir, "bad.seg");
        var emptyColumn = new SegmentColumn("cpu", "host=a", "value", FieldKind.Float,
            0, 0, [], []);

        Assert.NotNull(Record.Exception(() => SegmentWriter.WriteColumns(path, [emptyColumn])));
        Assert.False(File.Exists(path));
        Assert.False(File.Exists(path + ".tmp"));
    }

    [Fact]
    public void Publish_WhileReadLeaseHeld_TimesOutAndHonorsCancellation()
    {
        var gate = new SegmentPublicationGate();
        using var read = gate.Read();

        Assert.Throws<TimeoutException>(() => gate.Publish(TimeSpan.FromMilliseconds(100)));

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.Throws<OperationCanceledException>(
            () => gate.Publish(TimeSpan.FromSeconds(5), cts.Token));
    }

    [Fact]
    public async Task CleanupExpiredShards_PrunesSeriesAndTagIndexesForExpiredOnlyData()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0,
            flushIntervalMs: 0, compactionIntervalMs: 0);
        engine.CreateDatabaseWithRp("expiredb", 60_000_000_000L, 3_600_000_000_000L, 1, "short");

        var nowNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000;
        var oldTs = nowNs - 120_000_000_000L;
        engine.Meta.AddShardGroup("expiredb", "short", new ShardGroupInfo
        {
            Id = 1,
            StartTimeNs = oldTs - 1_000_000_000L,
            EndTimeNs = oldTs + 1_000_000_000L
        });

        await engine.WriteAsync("expiredb", "short", [PointAt("oldcpu", "old", 1, oldTs)]);
        engine.FlushAll();
        await engine.WriteAsync("expiredb", "short", [PointAt("livecpu", "live", 2, nowNs - 1_000_000_000L)]);
        engine.FlushAll();

        Assert.NotEmpty(engine.ListSeries("expiredb", "oldcpu"));
        Assert.NotEmpty(engine.ListTagValues("expiredb", "oldcpu", "host"));

        engine.CleanupExpiredShards();

        Assert.Empty(engine.ListSeries("expiredb", "oldcpu"));
        Assert.Empty(engine.ListTagValues("expiredb", "oldcpu", "host"));
        Assert.NotEmpty(engine.ListSeries("expiredb", "livecpu"));
        Assert.Empty(engine.ReadAllPoints("expiredb", "short", "oldcpu", null, null));
    }
}

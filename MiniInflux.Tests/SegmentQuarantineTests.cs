using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class SegmentQuarantineTests : IDisposable
{
    private readonly string _testDir;

    public SegmentQuarantineTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_qtn_{Guid.NewGuid():N}");
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
    public async Task Read_CorruptSegment_IsQuarantinedAndExcludedFromReads()
    {
        var dataPath = Path.Combine(_testDir, "data");
        string corruptPath;
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [PointAt(1, 1_000_000_000)]);
            engine.FlushAll();
            await engine.WriteAsync("testdb", "autogen", [PointAt(2, 2_000_000_000)]);
            engine.FlushAll();

            var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
            var shardDir = new ShardManager(engine.RootPath, engine.Meta).ShardDir("testdb", "autogen", shard.Id);
            var segments = Directory.GetFiles(shardDir, "*.seg");
            Assert.Equal(2, segments.Length);

            // Corrupt one segment beyond repair (bad magic), keeping the other intact.
            corruptPath = segments[0];
            File.WriteAllBytes(corruptPath, new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });

            // First read detects the corruption; the surviving segment stays queryable.
            var points1 = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
            Assert.Single(points1);
            Assert.Contains(engine.QuarantinedSegments, p => string.Equals(p, corruptPath, StringComparison.OrdinalIgnoreCase));
        }

        // Quarantine is process-wide (static): the corrupted path is reported without re-reading.
        Assert.Contains(SegmentReader.Quarantined, p => string.Equals(p, corruptPath, StringComparison.OrdinalIgnoreCase));
        Assert.True(SegmentReader.IsQuarantined(corruptPath));
    }
}

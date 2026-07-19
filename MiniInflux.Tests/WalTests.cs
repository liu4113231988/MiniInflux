using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class WalTests : IDisposable
{
    private readonly string _testDir;

    public WalTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public void Append_SinglePoint_WritesToWal()
    {
        var walDir = Path.Combine(_testDir, "wal");
        using var wal = new WalManager(walDir);

        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.5) } },
                TimestampNs = 1000_000_000
            }
        };

        wal.Append("testdb", "autogen", points);

        var walFiles = Directory.GetFiles(walDir, "*.wal");
        Assert.Single(walFiles);
    }

    [Fact]
    public void Replay_AfterAppend_ReturnsRecords()
    {
        var walDir = Path.Combine(_testDir, "wal");
        using (var wal = new WalManager(walDir))
        {
            var points = new List<Point>
            {
                new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { { "host", "server01" } },
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.5) } },
                    TimestampNs = 1000_000_000
                }
            };
            wal.Append("testdb", "autogen", points);
        }

        using var wal2 = new WalManager(walDir);
        var records = wal2.Replay();

        Assert.Single(records);
        Assert.Equal("testdb", records[0].Db);
        Assert.Equal("autogen", records[0].Rp);
        Assert.Single(records[0].Points);
    }

    [Fact]
    public void Checkpoint_DeletesOldWalFiles()
    {
        var walDir = Path.Combine(_testDir, "wal");
        using var wal = new WalManager(walDir, maxFileBytes: 100); // Small size to force rotation

        for (int i = 0; i < 10; i++)
        {
            var points = new List<Point>
            {
                new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string>(),
                    Fields = new Dictionary<string, FieldValue> { { $"value{i}", FieldValue.FromDouble(i) } },
                    TimestampNs = i * 1000_000_000
                }
            };
            wal.Append("testdb", "autogen", points);
        }

        wal.Checkpoint(wal.CurrentPosition);

        // Old files should be deleted, only current file remains
        var walFiles = Directory.GetFiles(walDir, "*.wal");
        Assert.True(walFiles.Length >= 1);
    }

    [Fact]
    public void Append_MultiplePoints_AllReplayed()
    {
        var walDir = Path.Combine(_testDir, "wal");
        using (var wal = new WalManager(walDir))
        {
            var positions = wal.Append("testdb", "autogen",
            [
                new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string>(),
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1) } },
                    TimestampNs = 1 * 1000_000_000
                },
                new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string>(),
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(2) } },
                    TimestampNs = 2 * 1000_000_000
                }
            ]);
            Assert.Equal(2, positions.Count);
            Assert.Equal(positions[0], positions[1]);
        }

        using var wal2 = new WalManager(walDir);
        var records = wal2.Replay();

        Assert.Equal(2, records.Count);
    }

    [Fact]
    public void Rotation_WhenSizeExceeded_CreatesNewFile()
    {
        var walDir = Path.Combine(_testDir, "wal");
        using var wal = new WalManager(walDir, maxFileBytes: 100); // Very small to force rotation

        for (int i = 0; i < 20; i++)
        {
            var points = new List<Point>
            {
                new Point
                {
                    Measurement = "measurement_with_long_name",
                    Tags = new Dictionary<string, string> { { "tag_key", "tag_value_with_some_data" } },
                    Fields = new Dictionary<string, FieldValue> { { "field_key", FieldValue.FromDouble(12345.6789) } },
                    TimestampNs = i * 1000_000_000
                }
            };
            wal.Append("testdb", "autogen", points);
        }

        var walFiles = Directory.GetFiles(walDir, "*.wal");
        Assert.True(walFiles.Length > 1, "Should have rotated to multiple WAL files");
    }

    [Fact]
    public void Checkpoint_WithRecordOffset_ReplaysOnlyUncheckpointedRecords()
    {
        var walDir = Path.Combine(_testDir, "wal");
        WalPosition secondRecordStart;

        using (var wal = new WalManager(walDir, maxFileBytes: 1024 * 1024))
        {
            wal.Append("testdb", "autogen",
            [
                new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string>(),
                    Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(1) },
                    TimestampNs = 1
                }
            ]);

            secondRecordStart = Assert.Single(wal.Append("testdb", "autogen",
            [
                new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string>(),
                    Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(2) },
                    TimestampNs = 2
                }
            ]));

            wal.Checkpoint(secondRecordStart);
        }

        using var reopened = new WalManager(walDir, maxFileBytes: 1024 * 1024);
        var replayed = reopened.Replay();

        var only = Assert.Single(replayed);
        Assert.Equal(2, only.Points[0].Fields["value"].Float);
    }

    [Fact]
    public void Checkpoint_CurrentPosition_ReclaimsCheckpointedCurrentFile()
    {
        var walDir = Path.Combine(_testDir, "wal");
        using var wal = new WalManager(walDir, maxFileBytes: 1024 * 1024);

        wal.Append("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string>(),
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(1) },
                TimestampNs = 1
            }
        ]);
        wal.Checkpoint(wal.CurrentPosition);

        var file = Assert.Single(Directory.GetFiles(walDir, "*.wal"));
        Assert.Equal("000002.wal", Path.GetFileName(file));
        Assert.Equal(0, new FileInfo(file).Length);
        Assert.Empty(wal.Replay());
    }

    [Fact]
    public void Checkpoint_LoadedCurrentPosition_ReclaimsCheckpointedCurrentFile()
    {
        var walDir = Path.Combine(_testDir, "wal");
        WalPosition checkpoint;
        using (var wal = new WalManager(walDir, maxFileBytes: 1024 * 1024))
        {
            wal.Append("testdb", "autogen",
            [
                new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string>(),
                    Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(1) },
                    TimestampNs = 1
                }
            ]);
            checkpoint = wal.CurrentPosition;
            File.WriteAllText(Path.Combine(walDir, "checkpoint.dat"), $"{checkpoint.FileId}:{checkpoint.Offset}");
        }

        using var reopened = new WalManager(walDir, maxFileBytes: 1024 * 1024);
        reopened.Checkpoint(reopened.CurrentPosition);

        var file = Assert.Single(Directory.GetFiles(walDir, "*.wal"));
        Assert.Equal("000002.wal", Path.GetFileName(file));
        Assert.Equal(0, new FileInfo(file).Length);
        Assert.Empty(reopened.Replay());
    }
}

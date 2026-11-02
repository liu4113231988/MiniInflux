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
    public void AppendBatch_Empty_DoesNotAdvancePosition()
    {
        using var wal = new WalManager(Path.Combine(_testDir, "empty"));
        var before = wal.CurrentPosition;
        Assert.Equal(WalPosition.Start, wal.AppendBatch("db", "rp", []));
        Assert.Equal(before, wal.CurrentPosition);
    }

    [Fact]
    public void AppendBatch_UnicodeAndBufferGrowth_PreservesPayloadAndReplayPosition()
    {
        var dir = Path.Combine(_testDir, "unicode");
        var value = new string('中', 2000) + "😀";
        var point = new Point
        {
            Measurement = "温度",
            Tags = new() { ["位置"] = "北京" },
            Fields = new()
            {
                ["文本"] = FieldValue.FromString(value),
                ["整数"] = FieldValue.FromInteger(long.MinValue),
                ["浮点"] = FieldValue.FromDouble(1.25),
                ["开"] = FieldValue.FromBoolean(true),
                ["关"] = FieldValue.FromBoolean(false)
            },
            TimestampNs = 123
        };
        WalPosition position;
        using (var wal = new WalManager(dir, maxFileBytes: 100))
            position = wal.AppendBatch("数据库", "保留", [point, point]);
        var bytes = File.ReadAllBytes(Path.Combine(dir, "000001.wal"));
        // Binary v1 payload starts with magic 0x01 (frame header is 8 bytes).
        Assert.Equal(0x01, bytes[8]);
        using var reopened = new WalManager(dir);
        var replay = reopened.ReplayWithPositions();
        Assert.Equal(2, replay.Count);
        Assert.All(replay, record =>
        {
            Assert.Equal(position, record.Position);
            Assert.Equal("温度", record.Point.Measurement);
            Assert.Equal("北京", record.Point.Tags["位置"]);
            Assert.Equal(value, record.Point.Fields["文本"].String);
            Assert.Equal(long.MinValue, record.Point.Fields["整数"].Integer);
            Assert.Equal(1.25, record.Point.Fields["浮点"].Float);
            Assert.True(record.Point.Fields["开"].Boolean);
            Assert.False(record.Point.Fields["关"].Boolean);
            Assert.Equal(123, record.Point.TimestampNs);
        });
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

        // Checkpointing to the current position must reclaim every rotated file, leaving one live WAL.
        var walFiles = Directory.GetFiles(walDir, "*.wal");
        Assert.Single(walFiles);
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

    private static void WriteRawRecord(string path, byte[] payload)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        using var fs = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
        var header = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(header, payload.Length);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4), MiniInflux.Net10.Storage.Crc32.Compute(payload));
        fs.Write(header);
        fs.Write(payload);
    }

    [Fact]
    public void Replay_MixedTextAndBinaryRecords_ReturnsBoth()
    {
        var walDir = Path.Combine(_testDir, "mixed");
        // Write a legacy text record directly into the WAL file.
        var textPayload = System.Text.Encoding.UTF8.GetBytes("testdb\tautogen\tcpu value=1.5 1000000000\n");
        WriteRawRecord(Path.Combine(walDir, "000001.wal"), textPayload);

        // Append a new binary record through the manager.
        using (var wal = new WalManager(walDir))
        {
            wal.Append("testdb", "autogen",
            [
                new Point
                {
                    Measurement = "mem",
                    Tags = new Dictionary<string, string> { ["host"] = "h1" },
                    Fields = new Dictionary<string, FieldValue> { ["used"] = FieldValue.FromInteger(42) },
                    TimestampNs = 2000000000
                }
            ]);
        }

        using var reopened = new WalManager(walDir);
        var records = reopened.Replay();
        Assert.Equal(2, records.Count);
        Assert.Equal("cpu", records[0].Points[0].Measurement);
        Assert.Equal(1.5, records[0].Points[0].Fields["value"].Float);
        Assert.Equal("mem", records[1].Points[0].Measurement);
        Assert.Equal(42, records[1].Points[0].Fields["used"].Integer);
    }

    [Fact]
    public void Replay_UnknownVersionMagic_StopsAtSafePoint()
    {
        var walDir = Path.Combine(_testDir, "unknownver");
        // A valid binary record first.
        using (var wal = new WalManager(walDir))
            wal.Append("db", "rp", [new Point { Measurement = "m", Tags = new(), Fields = new() { ["v"] = FieldValue.FromDouble(1) }, TimestampNs = 1 }]);

        // A record with an unknown magic byte (0x02) but valid CRC.
        var unknownPayload = new byte[] { 0x02, 0x00 };
        WriteRawRecord(Path.Combine(walDir, "000001.wal"), unknownPayload);

        using var reopened = new WalManager(walDir);
        var records = reopened.Replay();
        // Only the first (valid v1) record is replayed; the unknown-version record stops replay.
        Assert.Single(records);
        Assert.Equal("m", records[0].Points[0].Measurement);
    }

    [Fact]
    public void Replay_CrcCorruption_StopsAtLastValidRecord()
    {
        var walDir = Path.Combine(_testDir, "corrupt");
        using (var wal = new WalManager(walDir))
        {
            wal.Append("db", "rp", [new Point { Measurement = "a", Tags = new(), Fields = new() { ["v"] = FieldValue.FromDouble(1) }, TimestampNs = 1 }]);
            wal.Append("db", "rp", [new Point { Measurement = "b", Tags = new(), Fields = new() { ["v"] = FieldValue.FromDouble(2) }, TimestampNs = 2 }]);
        }

        // Corrupt one byte inside the second record's payload (after the 8-byte frame header
        // of the second record). Find the second record start by reading the first record's length.
        var path = Path.Combine(walDir, "000001.wal");
        var bytes = File.ReadAllBytes(path);
        var firstLen = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes);
        var secondHeaderStart = 8 + firstLen;
        var secondPayloadStart = secondHeaderStart + 8;
        bytes[secondPayloadStart] ^= 0xFF; // flip magic/byte -> CRC mismatch
        File.WriteAllBytes(path, bytes);

        using var reopened = new WalManager(walDir);
        var records = reopened.Replay();
        // First record intact; second record's CRC fails and stops replay.
        Assert.Single(records);
        Assert.Equal("a", records[0].Points[0].Measurement);
    }

    [Fact]
    public void Replay_TruncatedTail_ReplaysOnlyCompleteRecords()
    {
        var walDir = Path.Combine(_testDir, "trunc");
        using (var wal = new WalManager(walDir))
        {
            wal.Append("db", "rp", [new Point { Measurement = "a", Tags = new(), Fields = new() { ["v"] = FieldValue.FromDouble(1) }, TimestampNs = 1 }]);
            wal.Append("db", "rp", [new Point { Measurement = "b", Tags = new(), Fields = new() { ["v"] = FieldValue.FromDouble(2) }, TimestampNs = 2 }]);
        }

        var path = Path.Combine(walDir, "000001.wal");
        var full = File.ReadAllBytes(path);
        // Truncate in the middle of the second record's payload.
        var firstLen = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(full);
        var truncateAt = 8 + firstLen + 8 + 3; // second header + 3 payload bytes
        using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            fs.SetLength(truncateAt);

        using var reopened = new WalManager(walDir);
        var records = reopened.Replay();
        Assert.Single(records);
        Assert.Equal("a", records[0].Points[0].Measurement);
    }

    [Fact]
    public void RestartRecovery_MultiSeriesMultiField_DataConsistent()
    {
        var walDir = Path.Combine(_testDir, "restart");
        var points = new List<Point>();
        for (var i = 0; i < 100; i++)
        {
            points.Add(new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = $"srv{i % 4}", ["region"] = i % 2 == 0 ? "cn" : "us" },
                Fields = new Dictionary<string, FieldValue>
                {
                    ["value"] = FieldValue.FromDouble(i * 0.5),
                    ["count"] = FieldValue.FromInteger(i),
                    ["ok"] = FieldValue.FromBoolean(i % 2 == 0)
                },
                TimestampNs = i
            });
        }

        using (var wal = new WalManager(walDir))
            wal.Append("db", "rp", points);

        using var reopened = new WalManager(walDir);
        var replay = reopened.ReplayWithPositions();
        Assert.Equal(100, replay.Count);
        for (var i = 0; i < 100; i++)
        {
            var p = replay[i].Point;
            Assert.Equal("cpu", p.Measurement);
            Assert.Equal($"srv{i % 4}", p.Tags["host"]);
            Assert.Equal(i * 0.5, p.Fields["value"].Float);
            Assert.Equal(i, p.Fields["count"].Integer);
            Assert.Equal(i % 2 == 0, p.Fields["ok"].Boolean);
            Assert.Equal(i, p.TimestampNs);
        }
    }
}

using System.Buffers.Binary;
using System.Text;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Write-Ahead Log with rotation, checkpoint, group commit, and replay.
/// Record format: [length:4][crc32:4][payload:N]
/// Payload: "db\trp\tmeasurement,tag=val field=val timestamp\n"
/// </summary>
public sealed class WalManager : IDisposable
{
    private readonly string _walDir;
    private readonly long _maxFileBytes;
    private readonly bool _fsync;
    private readonly int _fsyncIntervalMs;
    private readonly object _lock = new();
    private FileStream? _currentStream;
    private int _currentFileId;
    private long _currentFileSize;
    private int _checkpointFileId;
    private Timer? _fsyncTimer;
    private bool _disposed;

    public WalManager(string walDir, long maxFileBytes = 16 * 1024 * 1024, bool fsync = true, int fsyncIntervalMs = 1000)
    {
        _walDir = walDir;
        _maxFileBytes = maxFileBytes;
        _fsync = fsync;
        _fsyncIntervalMs = fsyncIntervalMs;
        Directory.CreateDirectory(walDir);
        LoadCheckpoint();
        OpenOrCreateCurrentFile();
        if (_fsync && _fsyncIntervalMs > 0)
            _fsyncTimer = new Timer(_ => FlushToDisk(), null, _fsyncIntervalMs, _fsyncIntervalMs);
    }

    private string GetWalFilePath(int id) => Path.Combine(_walDir, $"{id:D6}.wal");
    private string CheckpointPath => Path.Combine(_walDir, "checkpoint.dat");

    private void LoadCheckpoint()
    {
        if (File.Exists(CheckpointPath))
        {
            var text = File.ReadAllText(CheckpointPath).Trim();
            if (int.TryParse(text, out var id))
                _checkpointFileId = id;
        }
    }

    private void OpenOrCreateCurrentFile()
    {
        var files = Directory.Exists(_walDir)
            ? Directory.GetFiles(_walDir, "*.wal")
                .Select(f => int.TryParse(Path.GetFileNameWithoutExtension(f), out var id) ? id : 0)
                .Where(id => id > 0)
                .OrderBy(id => id)
                .ToArray()
            : [];

        _currentFileId = files.Length > 0 ? files.Max() : 0;

        if (_currentFileId > 0)
        {
            var path = GetWalFilePath(_currentFileId);
            if (File.Exists(path))
            {
                _currentStream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
                _currentFileSize = _currentStream.Length;
                return;
            }
        }

        _currentFileId++;
        _currentStream = new FileStream(GetWalFilePath(_currentFileId), FileMode.Create, FileAccess.Write, FileShare.Read);
        _currentFileSize = 0;
    }

    /// <summary>
    /// Append points to the WAL. Each point is written as a single record.
    /// </summary>
    public void Append(string db, string rp, IEnumerable<Point> points)
    {
        lock (_lock)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(WalManager));
            foreach (var p in points)
            {
                var line = FormatRecord(db, rp, p);
                var payload = Encoding.UTF8.GetBytes(line);
                WriteRecord(payload);

                if (_currentFileSize >= _maxFileBytes)
                    RotateLocked();
            }

            if (!_fsync || _fsyncIntervalMs <= 0)
                _currentStream?.Flush(false);
        }
    }

    private void WriteRecord(byte[] payload)
    {
        if (_currentStream == null) return;
        var header = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0, 4), payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4, 4), Crc32.Compute(payload));
        _currentStream.Write(header);
        _currentStream.Write(payload);
        _currentFileSize += 8 + payload.Length;
    }

    private static string FormatRecord(string db, string rp, Point p)
    {
        var fields = string.Join(",", p.Fields.Select(f => $"{f.Key}={FormatFieldValue(f.Value)}"));
        var tags = p.Tags.Count > 0 ? "," + string.Join(",", p.Tags.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value}")) : "";
        return $"{db}\t{rp}\t{p.Measurement}{tags} {fields} {p.TimestampNs}\n";
    }

    private static string FormatFieldValue(FieldValue v) => v.Kind switch
    {
        FieldKind.Integer => $"{v.Integer}i",
        FieldKind.Float => v.Float.ToString(System.Globalization.CultureInfo.InvariantCulture),
        FieldKind.Boolean => v.Boolean ? "true" : "false",
        FieldKind.String => $"\"{v.String}\"",
        _ => ""
    };

    private void RotateLocked()
    {
        _currentStream?.Flush(true);
        _currentStream?.Dispose();
        _currentFileId++;
        _currentStream = new FileStream(GetWalFilePath(_currentFileId), FileMode.Create, FileAccess.Write, FileShare.Read);
        _currentFileSize = 0;
    }

    private void FlushToDisk()
    {
        lock (_lock)
        {
            if (_disposed) return;
            try { _currentStream?.Flush(true); } catch { /* ignore */ }
        }
    }

    /// <summary>
    /// Mark all WAL files up to and including the given file ID as checkpointed.
    /// Deletes old WAL files before the checkpoint.
    /// </summary>
    public void Checkpoint(int upToFileId)
    {
        lock (_lock)
        {
            if (upToFileId <= _checkpointFileId) return;
            _checkpointFileId = upToFileId;

            // Atomic checkpoint write: write to .tmp then rename (same pattern as segment writes)
            var tmpPath = CheckpointPath + ".tmp";
            File.WriteAllText(tmpPath, _checkpointFileId.ToString());
            File.Move(tmpPath, CheckpointPath, overwrite: true);

            foreach (var file in Directory.GetFiles(_walDir, "*.wal"))
            {
                if (int.TryParse(Path.GetFileNameWithoutExtension(file), out var id) && id < _checkpointFileId)
                {
                    try { File.Delete(file); } catch { /* best effort */ }
                }
            }
        }
    }

    /// <summary>
    /// Get the current WAL file ID (used for checkpoint tracking).
    /// </summary>
    public int CurrentFileId
    {
        get { lock (_lock) return _currentFileId; }
    }

    /// <summary>
    /// Replay all WAL records after the checkpoint.
    /// Returns list of (db, rp, points) tuples.
    /// </summary>
    public List<(string Db, string Rp, List<Point> Points)> Replay()
    {
        var result = new List<(string, string, List<Point>)>();
        var files = Directory.Exists(_walDir)
            ? Directory.GetFiles(_walDir, "*.wal")
                .Select(f => int.TryParse(Path.GetFileNameWithoutExtension(f), out var id) ? (path: f, id) : (path: (string?)null, id: 0))
                .Where(x => x.path != null && x.id >= _checkpointFileId)
                .OrderBy(x => x.id)
                .ToArray()
            : [];

        foreach (var (path, _) in files)
        {
            if (path == null) continue;
            var records = ReadWalFile(path);
            foreach (var (db, rp, pts) in records)
                result.Add((db, rp, pts));
        }
        return result;
    }

    private static List<(string Db, string Rp, List<Point> Points)> ReadWalFile(string path)
    {
        var result = new List<(string, string, List<Point>)>();
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var br = new BinaryReader(fs, Encoding.UTF8);

        while (fs.Position < fs.Length - 8)
        {
            if (fs.Length - fs.Position < 8) break;
            var length = br.ReadInt32();
            var expectedCrc = br.ReadUInt32();
            if (fs.Length - fs.Position < length) break;

            var payload = br.ReadBytes(length);
            var actualCrc = Crc32.Compute(payload);
            if (actualCrc != expectedCrc)
                break; // Stop reading this file on CRC mismatch (common after crash: partial last write). Keep already-parsed records.

            var line = Encoding.UTF8.GetString(payload).TrimEnd('\n');
            if (string.IsNullOrEmpty(line)) continue;

            var parts = line.Split('\t', 3);
            if (parts.Length < 3) continue;

            var db = parts[0];
            var rp = parts[1];
            var lineProtocol = parts[2];

            try
            {
                var points = LineProtocolParser.ParseMany(lineProtocol, TimestampPrecision.Parse("ns"));
                result.Add((db, rp, points));
            }
            catch
            {
                // Skip malformed records during replay
            }
        }
        return result;
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _disposed = true;
            _fsyncTimer?.Dispose();
            _currentStream?.Flush(true);
            _currentStream?.Dispose();
        }
    }
}

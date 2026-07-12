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
    private readonly StorageHealth _health;
    private readonly object _lock = new();
    private FileStream? _currentStream;
    private int _currentFileId;
    private long _currentFileSize;
    private WalPosition _checkpoint = WalPosition.Start;
    private Timer? _fsyncTimer;
    private bool _disposed;

    public WalManager(string walDir, long maxFileBytes = 16 * 1024 * 1024, bool fsync = true, int fsyncIntervalMs = 1000,
        StorageHealth? health = null)
    {
        _walDir = walDir;
        _maxFileBytes = maxFileBytes;
        _fsync = fsync;
        _fsyncIntervalMs = fsyncIntervalMs;
        _health = health ?? new StorageHealth();
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
            var parts = text.Split(':', 2, StringSplitOptions.TrimEntries);
            if (parts.Length == 2
                && int.TryParse(parts[0], out var fileId)
                && long.TryParse(parts[1], out var offset))
            {
                _checkpoint = new WalPosition(fileId, Math.Max(0, offset));
                return;
            }

            if (int.TryParse(text, out var legacyId))
                _checkpoint = new WalPosition(legacyId, 0);
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
    /// Append points to the WAL. A write batch is stored as one record.
    /// </summary>
    public IReadOnlyList<WalPosition> Append(string db, string rp, IEnumerable<Point> points)
    {
        var pointList = points as IReadOnlyList<Point> ?? points.ToList();
        if (pointList.Count == 0) return [];

        var positions = new List<WalPosition>(pointList.Count);
        lock (_lock)
        {
            try
            {
                if (_disposed) throw new ObjectDisposedException(nameof(WalManager));
                var payload = Encoding.UTF8.GetBytes(FormatRecord(db, rp, pointList));
                var position = WriteRecord(payload);
                for (var i = 0; i < pointList.Count; i++)
                    positions.Add(position);

                if (_currentFileSize >= _maxFileBytes)
                    RotateLocked();

                if (!_fsync || _fsyncIntervalMs <= 0)
                {
                    _currentStream?.Flush(_fsync);
                    _health.RecordWriteSuccess();
                }
            }
            catch (Exception ex)
            {
                _health.RecordFailure("wal_append", ex, blocksWrites: true);
                throw;
            }
        }
        return positions;
    }

    private WalPosition WriteRecord(ReadOnlySpan<byte> payload)
    {
        if (_currentStream == null) return CurrentPosition;
        var recordStart = _currentFileSize;
        Span<byte> header = stackalloc byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(header[..4], payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header[4..], Crc32.Compute(payload));
        _currentStream.Write(header);
        _currentStream.Write(payload);
        _currentFileSize += 8 + payload.Length;
        return new WalPosition(_currentFileId, recordStart);
    }

    private static string FormatRecord(string db, string rp, IReadOnlyList<Point> points)
    {
        var estimatedBytes = db.Length + rp.Length + points.Count * 80;
        var sb = new StringBuilder(estimatedBytes);
        sb.Append(db).Append('\t').Append(rp).Append('\t');
        for (var i = 0; i < points.Count; i++)
            AppendLineProtocol(sb, points[i]);
        return sb.ToString();
    }

    private static void AppendLineProtocol(StringBuilder sb, Point p)
    {
        sb.Append(p.Measurement);
        foreach (var tag in p.Tags)
            sb.Append(',').Append(tag.Key).Append('=').Append(tag.Value);

        sb.Append(' ');
        var first = true;
        foreach (var field in p.Fields)
        {
            if (!first) sb.Append(',');
            first = false;
            sb.Append(field.Key).Append('=');
            AppendFieldValue(sb, field.Value);
        }
        sb.Append(' ').Append(p.TimestampNs).Append('\n');
    }

    private static void AppendFieldValue(StringBuilder sb, FieldValue v)
    {
        switch (v.Kind)
        {
            case FieldKind.Integer:
                sb.Append(v.Integer).Append('i');
                break;
            case FieldKind.Float:
                sb.Append(v.Float.ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;
            case FieldKind.Boolean:
                sb.Append(v.Boolean ? "true" : "false");
                break;
            case FieldKind.String:
                sb.Append('"').Append(v.String).Append('"');
                break;
        }
    }

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
            try
            {
                _currentStream?.Flush(true);
                _health.RecordWriteSuccess();
            }
            catch (Exception ex)
            {
                _health.RecordFailure("wal_fsync", ex, blocksWrites: true);
            }
        }
    }

    /// <summary>
    /// Mark all WAL files up to and including the given file ID as checkpointed.
    /// Deletes old WAL files before the checkpoint.
    /// </summary>
    public void Checkpoint(WalPosition position)
    {
        lock (_lock)
        {
            if (Compare(position, _checkpoint) <= 0) return;
            _checkpoint = position;

            // Atomic checkpoint write: write to .tmp then rename (same pattern as segment writes)
            var tmpPath = CheckpointPath + ".tmp";
            File.WriteAllText(tmpPath, $"{_checkpoint.FileId}:{_checkpoint.Offset}");
            File.Move(tmpPath, CheckpointPath, overwrite: true);

            foreach (var file in Directory.GetFiles(_walDir, "*.wal"))
            {
                if (int.TryParse(Path.GetFileNameWithoutExtension(file), out var id) && id < _checkpoint.FileId)
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

    public WalPosition CurrentPosition
    {
        get { lock (_lock) return new WalPosition(_currentFileId, _currentFileSize); }
    }

    public WalPosition CheckpointPosition
    {
        get { lock (_lock) return _checkpoint; }
    }

    /// <summary>
    /// Replay all WAL records after the checkpoint.
    /// Returns list of (db, rp, points) tuples.
    /// </summary>
    public List<(string Db, string Rp, List<Point> Points)> Replay()
    {
        var result = new List<(string, string, List<Point>)>();
        foreach (var item in ReplayWithPositions())
            result.Add((item.Db, item.Rp, [item.Point]));
        return result;
    }

    public List<WalReplayPoint> ReplayWithPositions()
    {
        var result = new List<WalReplayPoint>();
        var files = Directory.Exists(_walDir)
            ? Directory.GetFiles(_walDir, "*.wal")
                .Select(f => int.TryParse(Path.GetFileNameWithoutExtension(f), out var id) ? (path: f, id) : (path: (string?)null, id: 0))
                .Where(x => x.path != null && (x.id > _checkpoint.FileId || (x.id == _checkpoint.FileId && File.Exists(x.path!))))
                .OrderBy(x => x.id)
                .ToArray()
            : [];

        foreach (var (path, id) in files)
        {
            if (path == null) continue;
            var startOffset = id == _checkpoint.FileId ? _checkpoint.Offset : 0;
            result.AddRange(ReadWalFile(path, id, startOffset));
        }
        return result;
    }

    private static List<WalReplayPoint> ReadWalFile(string path, int fileId, long startOffset)
    {
        var result = new List<WalReplayPoint>();
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var br = new BinaryReader(fs, Encoding.UTF8);
        if (startOffset > 0 && startOffset < fs.Length)
            fs.Position = startOffset;
        else if (startOffset >= fs.Length)
            return result;

        while (fs.Position < fs.Length - 8)
        {
            var recordStart = fs.Position;
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
                var position = new WalPosition(fileId, recordStart);
                foreach (var point in points)
                    result.Add(new WalReplayPoint(db, rp, point, position));
            }
            catch
            {
                // Skip malformed records during replay
            }
        }
        return result;
    }

    private static int Compare(WalPosition left, WalPosition right)
    {
        var fileCompare = left.FileId.CompareTo(right.FileId);
        return fileCompare != 0 ? fileCompare : left.Offset.CompareTo(right.Offset);
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

public readonly record struct WalPosition(int FileId, long Offset)
{
    public static WalPosition Start => new(0, 0);
}

public sealed record WalReplayPoint(string Db, string Rp, Point Point, WalPosition Position);

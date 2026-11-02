using System.Buffers.Binary;
using System.Buffers;
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
    
    // 优化的批量提交参数

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
        RepairTruncatedTails();
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

    /// <summary>
    /// A crash can leave a partial record at the tail of any WAL file. Appending new records after
    /// those bytes makes them unreachable on the next replay (replay stops at the torn record), so
    /// truncate every file to its last complete, CRC-valid record before opening it for append.
    /// </summary>
    private void RepairTruncatedTails()
    {
        if (!Directory.Exists(_walDir)) return;

        var maxFileId = 0;
        foreach (var path in Directory.GetFiles(_walDir, "*.wal"))
        {
            if (!int.TryParse(Path.GetFileNameWithoutExtension(path), out var fileId) || fileId <= 0)
                continue;
            maxFileId = Math.Max(maxFileId, fileId);

            var length = new FileInfo(path).Length;
            var validLength = FindLastValidRecordEnd(path, length);
            if (validLength < length)
            {
                using (var repair = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
                    repair.SetLength(validLength);
            }

            // A stale checkpoint beyond the repaired tail would otherwise skip all recovered
            // records in this file. Clamp it so replay starts at the last valid offset.
            if (fileId == _checkpoint.FileId && _checkpoint.Offset > validLength)
                _checkpoint = new WalPosition(fileId, validLength);
        }

        // A checkpoint file that points past the newest WAL file would make replay skip every
        // remaining record. Replaying from the newest file is safer than silently losing data.
        if (_checkpoint.FileId > maxFileId)
            _checkpoint = new WalPosition(maxFileId, 0);
    }

    private static long FindLastValidRecordEnd(string path, long length)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var br = new BinaryReader(fs, Encoding.UTF8);
        long position = 0;
        while (position + 8 <= length)
        {
            fs.Position = position;
            var recordLength = br.ReadInt32();
            var expectedCrc = br.ReadUInt32();
            if (recordLength < 0 || position + 8 + recordLength > length)
                break;

            var payload = br.ReadBytes(recordLength);
            if (payload.Length != recordLength || Crc32.Compute(payload) != expectedCrc)
                break;

            position += 8 + recordLength;
        }
        return position;
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
        var position = AppendBatch(db, rp, pointList);
        var positions = new WalPosition[pointList.Count];
        Array.Fill(positions, position);
        return positions;
    }

    /// <summary>
    /// Append one record and return its shared replay position without a per-point array.
    /// An empty batch writes nothing and returns WalPosition.Start.
    /// </summary>
    public WalPosition AppendBatch(string db, string rp, IReadOnlyList<Point> pointList)
    {
        using var timing = WriteDiagnostics.Measure(WriteDiagnostics.Stage.WalAppend);
        if (pointList.Count == 0) return WalPosition.Start;
        // Format the record *and* compute its CRC outside the lock: both are O(payload) work over
        // immutable data with no shared state, so the global WAL lock only covers the file write.
        using var writer = new PooledBufferWriter(EstimatePayloadSize(db, rp, pointList));
        using (WriteDiagnostics.Measure(WriteDiagnostics.Stage.WalEncode))
            WriteRecordPayload(writer, db, rp, pointList);
        var payloadSpan = writer.WrittenSpan;
        uint crc;
        using (WriteDiagnostics.Measure(WriteDiagnostics.Stage.WalCrc))
            crc = Crc32.Compute(payloadSpan);
        using (WriteDiagnostics.Measure(WriteDiagnostics.Stage.WalLockWait))
            Monitor.Enter(_lock);
        WalPosition position;
        try
        {
            try
            {
                if (_disposed) throw new ObjectDisposedException(nameof(WalManager));

                position = WriteRecord(payloadSpan, crc);

                if (_currentFileSize >= _maxFileBytes)
                    RotateLocked();

                // A successful append is itself proof the write path works again, so it unlatches
                // the health gate in every fsync mode (with fsync on, this is the only self-heal
                // signal apart from the engine's periodic probe).
                _health.RecordWriteSuccess();
            }
            catch (Exception ex)
            {
                _health.RecordFailure("wal_append", ex, blocksWrites: true);
                throw;
            }
        }
        finally { Monitor.Exit(_lock); }
        return position;
    }

    private const byte BinaryMagicV1 = 0x01;

    /// <summary>
    /// Binary v1 payload layout (frame header [len:4][crc:4] is unchanged):
    ///   magic: u8 = 0x01
    ///   db: u16 len + utf8
    ///   rp: u16 len + utf8
    ///   series_count: u16
    ///   per series: measurement(u16+utf8), tag_count(u8), per tag(key,val u16+utf8),
    ///               field_count(u8), per field(name u16+utf8, kind u8)  -- union of fields seen
    ///   point_count: u32
    ///   per point: series_index(u16), timestamp(varint-zigzag),
    ///              field_mask(ceil(field_count/8) bytes, bit i = field i present),
    ///              for each set bit in mask: value in series-field order
    ///              (float: f64 LE, integer: varint-zigzag, bool: u8, string: u16+utf8)
    /// The field mask handles points in the same series that carry different field subsets.
    /// </summary>
    private static void WriteRecordPayload(PooledBufferWriter writer, string db, string rp, IReadOnlyList<Point> points)
    {
        writer.WriteByte(BinaryMagicV1);
        writer.WriteString16(db);
        writer.WriteString16(rp);

        var seriesList = new List<(SeriesKey Key, Point Sample, List<(string Name, FieldKind Kind)> Fields)>();
        var seriesIndex = new Dictionary<SeriesKey, int>();
        var pointSeries = new int[points.Count];
        for (var i = 0; i < points.Count; i++)
        {
            var p = points[i];
            var key = SeriesKey.From(p);
            if (!seriesIndex.TryGetValue(key, out var idx))
            {
                idx = seriesList.Count;
                seriesIndex[key] = idx;
                var fields = new List<(string, FieldKind)>(p.Fields.Count);
                foreach (var f in p.Fields) fields.Add((f.Key, f.Value.Kind));
                seriesList.Add((key, p, fields));
            }
            else
            {
                // Merge any new field names into this series' field table (preserve insertion order).
                var entry = seriesList[idx];
                foreach (var f in p.Fields)
                {
                    var exists = false;
                    for (var k = 0; k < entry.Fields.Count; k++)
                        if (entry.Fields[k].Name == f.Key) { exists = true; break; }
                    if (!exists) entry.Fields.Add((f.Key, f.Value.Kind));
                }
            }
            pointSeries[i] = idx;
        }

        writer.WriteUInt16((ushort)seriesList.Count);
        foreach (var entry in seriesList)
        {
            writer.WriteString16(entry.Sample.Measurement);
            writer.WriteByte((byte)entry.Sample.Tags.Count);
            foreach (var t in entry.Sample.Tags)
            {
                writer.WriteString16(t.Key);
                writer.WriteString16(t.Value);
            }
            writer.WriteByte((byte)entry.Fields.Count);
            foreach (var (name, kind) in entry.Fields)
            {
                writer.WriteString16(name);
                writer.WriteByte((byte)kind);
            }
        }

        writer.WriteUInt32((uint)points.Count);
        for (var i = 0; i < points.Count; i++)
        {
            var p = points[i];
            var sidx = pointSeries[i];
            writer.WriteUInt16((ushort)sidx);
            writer.WriteVarintZigzag(p.TimestampNs);
            var fields = seriesList[sidx].Fields;
            var maskBytes = (fields.Count + 7) / 8;
            var mask = new byte[maskBytes];
            for (var f = 0; f < fields.Count; f++)
                if (p.Fields.ContainsKey(fields[f].Name))
                    mask[f >> 3] |= (byte)(1 << (f & 7));
            writer.WriteBytes(mask);
            for (var f = 0; f < fields.Count; f++)
            {
                if ((mask[f >> 3] & (1 << (f & 7))) == 0) continue;
                var v = p.Fields[fields[f].Name];
                switch (v.Kind)
                {
                    case FieldKind.Float: writer.WriteDouble(v.Float); break;
                    case FieldKind.Integer: writer.WriteVarintZigzag(v.Integer); break;
                    case FieldKind.Boolean: writer.WriteByte(v.Boolean ? (byte)1 : (byte)0); break;
                    case FieldKind.String: writer.WriteString16(v.String ?? ""); break;
                }
            }
        }
    }

    private WalPosition WriteRecord(ReadOnlySpan<byte> payload, uint crc)
    {
        using var timing = WriteDiagnostics.Measure(WriteDiagnostics.Stage.WalFileWrite);
        if (_currentStream == null) return CurrentPosition;
        var recordStart = _currentFileSize;
        Span<byte> header = stackalloc byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(header[..4], payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header[4..], crc);
        _currentStream.Write(header);
        _currentStream.Write(payload);
        _currentFileSize += 8 + payload.Length;
        return new WalPosition(_currentFileId, recordStart);
    }

    private static int EstimatePayloadSize(string db, string rp, IReadOnlyList<Point> points)
    {
        // Binary payload is much smaller than text; 40 B/point covers series index + varint
        // timestamp + a couple of numeric fields with headroom for the shared series table.
        var size = (db.Length + rp.Length) * 2 + 16 + points.Count * 40;
        return size;
    }

    /// <summary>
    /// Pool-backed <see cref="IBufferWriter{T}"/>: rents from <see cref="ArrayPool{T}.Shared"/>,
    /// grows by re-renting (copying only on underestimate), and returns the array on dispose.
    /// Lets the WAL format records straight into their final buffer with no intermediate copy.
    /// </summary>
    private sealed class PooledBufferWriter(int initialCapacity) : IBufferWriter<byte>, IDisposable
    {
        private byte[] _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(initialCapacity, 64));
        private int _written;

        public int WrittenCount => _written;
        public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _written);

        public void WriteByte(byte value)
        {
            Ensure(1)[0] = value;
            _written++;
        }

        public void WriteBytes(ReadOnlySpan<byte> value)
        {
            value.CopyTo(Ensure(value.Length));
            _written += value.Length;
        }

        public void WriteUInt16(ushort value)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(Ensure(2), value);
            _written += 2;
        }

        public void WriteUInt32(uint value)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(Ensure(4), value);
            _written += 4;
        }

        public void WriteDouble(double value)
        {
            BinaryPrimitives.WriteDoubleLittleEndian(Ensure(8), value);
            _written += 8;
        }

        public void WriteString16(string value)
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            WriteUInt16((ushort)byteCount);
            _written += Encoding.UTF8.GetBytes(value, Ensure(byteCount));
        }

        public void WriteVarintZigzag(long value)
        {
            var zz = (ulong)((value << 1) ^ (value >> 63));
            var span = Ensure(10);
            var n = 0;
            while (zz >= 0x80) { span[n++] = (byte)(zz | 0x80); zz >>= 7; }
            span[n++] = (byte)zz;
            _written += n;
        }

        public void Advance(int count)
        {
            if (count < 0 || _written + count > _buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count));
            _written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            Ensure(sizeHint);
            return _buffer.AsMemory(_written);
        }

        public Span<byte> GetSpan(int sizeHint = 0) => Ensure(sizeHint);

        private Span<byte> Ensure(int sizeHint)
        {
            var required = _written + Math.Max(sizeHint, 1);
            if (required <= _buffer.Length)
                return _buffer.AsSpan(_written);

            var newSize = Math.Max(required, _buffer.Length * 2);
            var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
            Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _written);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = newBuffer;
            return _buffer.AsSpan(_written);
        }

        public void Dispose() => ArrayPool<byte>.Shared.Return(_buffer);
    }

    private void RotateLocked()
    {
        using var timing = WriteDiagnostics.Measure(WriteDiagnostics.Stage.WalRotate);
        FlushCurrentToDisk();
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
                FlushCurrentToDisk();
                _health.RecordWriteSuccess();
            }
            catch (Exception ex)
            {
                _health.RecordFailure("wal_fsync", ex, blocksWrites: true);
            }
        }
    }

    // Caller holds _lock. Shared timing covers rotation, the timer, and final disposal.
    private void FlushCurrentToDisk()
    {
        using var timing = WriteDiagnostics.Measure(WriteDiagnostics.Stage.WalFsync);
        _currentStream?.Flush(true);
    }

    /// <summary>
    /// Mark all WAL files up to and including the given file ID as checkpointed.
    /// Deletes old WAL files before the checkpoint.
    /// </summary>
    public void Checkpoint(WalPosition position)
    {
        lock (_lock)
        {
            var compare = Compare(position, _checkpoint);
            var canReclaimCurrentFile = _currentStream != null
                && position.FileId == _currentFileId
                && position.Offset >= _currentFileSize
                && _currentFileSize > 0;
            if (compare < 0 || (compare == 0 && !canReclaimCurrentFile)) return;
            if (canReclaimCurrentFile)
            {
                RotateLocked();
                position = new WalPosition(_currentFileId, 0);
            }

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
            if (length < 0 || fs.Length - fs.Position < length) break;

            var payload = br.ReadBytes(length);
            var actualCrc = Crc32.Compute(payload);
            if (actualCrc != expectedCrc)
                break; // CRC mismatch: torn/corrupt record, stop at the last safe record.

            if (payload.Length == 0) continue;

            var position = new WalPosition(fileId, recordStart);
            try
            {
                var magic = payload[0];
                if (magic == BinaryMagicV1)
                {
                    var (db, rp, points) = DecodeBinaryPayload(payload);
                    foreach (var point in points)
                        result.Add(new WalReplayPoint(db, rp, point, position));
                }
                else if (magic >= 0x20 || magic == (byte)'\t')
                {
                    // Legacy text payload: "db\trp\tline-protocol\n"
                    var line = Encoding.UTF8.GetString(payload).TrimEnd('\n');
                    if (string.IsNullOrEmpty(line)) continue;
                    var parts = line.Split('\t', 3);
                    if (parts.Length < 3) continue;
                    var points = LineProtocolParser.ParseMany(parts[2], TimestampPrecision.Parse("ns"));
                    foreach (var point in points)
                        result.Add(new WalReplayPoint(parts[0], parts[1], point, position));
                }
                else
                {
                    // Unknown payload version: stop to avoid misinterpreting future formats.
                    break;
                }
            }
            catch
            {
                // A frame-valid but unparseable payload is treated like corruption: stop this
                // file rather than skip, because skipping could silently drop later records.
                break;
            }
        }
        return result;
    }

    private static (string Db, string Rp, List<Point> Points) DecodeBinaryPayload(byte[] payload)
    {
        var span = payload.AsSpan();
        var pos = 1; // skip magic byte
        var db = ReadString16(span, ref pos);
        var rp = ReadString16(span, ref pos);
        var seriesCount = ReadUInt16(span, ref pos);

        var series = new (string Measurement, string[] TagKeys, string[] TagVals, string[] FieldNames, FieldKind[] Kinds)[seriesCount];
        for (var s = 0; s < seriesCount; s++)
        {
            var meas = ReadString16(span, ref pos);
            var tagCount = span[pos++];
            var tk = new string[tagCount]; var tv = new string[tagCount];
            for (var t = 0; t < tagCount; t++) { tk[t] = ReadString16(span, ref pos); tv[t] = ReadString16(span, ref pos); }
            var fcount = span[pos++];
            var fn = new string[fcount]; var fk = new FieldKind[fcount];
            for (var f = 0; f < fcount; f++) { fn[f] = ReadString16(span, ref pos); fk[f] = (FieldKind)span[pos++]; }
            series[s] = (meas, tk, tv, fn, fk);
        }

        var pointCount = ReadUInt32(span, ref pos);
        var points = new List<Point>((int)pointCount);
        for (var i = 0; i < pointCount; i++)
        {
            var sidx = ReadUInt16(span, ref pos);
            if (sidx >= series.Length) throw new InvalidDataException("series index out of range");
            var ts = ReadVarintZigzag(span, ref pos);
            var s = series[sidx];
            var maskBytes = (s.FieldNames.Length + 7) / 8;
            if (pos + maskBytes > span.Length) throw new InvalidDataException("payload truncated at field mask");
            var mask = span.Slice(pos, maskBytes);
            pos += maskBytes;
            var tags = new Dictionary<string, string>(s.TagKeys.Length);
            for (var t = 0; t < s.TagKeys.Length; t++) tags[s.TagKeys[t]] = s.TagVals[t];
            var fields = new Dictionary<string, FieldValue>(s.FieldNames.Length);
            for (var f = 0; f < s.FieldNames.Length; f++)
            {
                if ((mask[f >> 3] & (1 << (f & 7))) == 0) continue;
                FieldValue fv = s.Kinds[f] switch
                {
                    FieldKind.Float => FieldValue.FromDouble(ReadDouble(span, ref pos)),
                    FieldKind.Integer => FieldValue.FromInteger(ReadVarintZigzag(span, ref pos)),
                    FieldKind.Boolean => FieldValue.FromBoolean(span[pos++] == 1),
                    FieldKind.String => FieldValue.FromString(ReadString16(span, ref pos)),
                    _ => throw new InvalidDataException($"unknown field kind {(byte)s.Kinds[f]}")
                };
                fields[s.FieldNames[f]] = fv;
            }
            points.Add(new Point { Measurement = s.Measurement, Tags = tags, Fields = fields, TimestampNs = ts });
        }
        return (db, rp, points);
    }

    private static ushort ReadUInt16(ReadOnlySpan<byte> span, ref int pos)
    {
        if (pos + 2 > span.Length) throw new InvalidDataException("payload truncated at u16");
        var v = BinaryPrimitives.ReadUInt16LittleEndian(span[pos..]);
        pos += 2;
        return v;
    }

    private static uint ReadUInt32(ReadOnlySpan<byte> span, ref int pos)
    {
        if (pos + 4 > span.Length) throw new InvalidDataException("payload truncated at u32");
        var v = BinaryPrimitives.ReadUInt32LittleEndian(span[pos..]);
        pos += 4;
        return v;
    }

    private static double ReadDouble(ReadOnlySpan<byte> span, ref int pos)
    {
        if (pos + 8 > span.Length) throw new InvalidDataException("payload truncated at f64");
        var v = BinaryPrimitives.ReadDoubleLittleEndian(span[pos..]);
        pos += 8;
        return v;
    }

    private static string ReadString16(ReadOnlySpan<byte> span, ref int pos)
    {
        var len = ReadUInt16(span, ref pos);
        if (pos + len > span.Length) throw new InvalidDataException("payload truncated at string");
        var s = Encoding.UTF8.GetString(span.Slice(pos, len));
        pos += len;
        return s;
    }

    private static long ReadVarintZigzag(ReadOnlySpan<byte> span, ref int pos)
    {
        ulong result = 0; int shift = 0; byte b;
        do
        {
            if (pos >= span.Length) throw new InvalidDataException("payload truncated at varint");
            b = span[pos++];
            result |= (ulong)(b & 0x7f) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return (long)((result >> 1) ^ (~(result & 1) + 1));
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
            FlushCurrentToDisk();
            _currentStream?.Dispose();
        }
    }
}

public readonly record struct WalPosition(int FileId, long Offset)
{
    public static WalPosition Start => new(0, 0);
}

public sealed record WalReplayPoint(string Db, string Rp, Point Point, WalPosition Position);

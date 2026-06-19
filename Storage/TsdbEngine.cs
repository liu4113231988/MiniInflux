using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

public sealed class TsdbEngine : IDisposable
{
    private readonly string _root;
    private readonly WalManager _wal;
    private readonly SchemaRegistry _schema;
    private readonly Manifest _manifest;
    private readonly ShardManager _shards;
    private readonly TombstoneStore _tombstones;
    private readonly Compactor _compactor;
    private readonly Dictionary<string, ReaderWriterLockSlim> _locks = new(StringComparer.Ordinal);
    private readonly ReaderWriterLockSlim _globalLock = new();
    private readonly Dictionary<string, List<Point>> _buf = new(StringComparer.Ordinal);
    private readonly int _threshold;
    private readonly long _maxSeriesPerDb;
    private readonly Dictionary<string, HashSet<string>> _seriesKeys = new(StringComparer.Ordinal);
    private int _lastFlushedWalFileId;
    private Timer? _rpExpiryTimer;
    private Timer? _compactionTimer;
    private Timer? _flushTimer;

    public TsdbEngine(string rootPath, int flushThreshold = 50000,
        long maxWalFileBytes = 16 * 1024 * 1024, bool walFsync = true, int walFsyncIntervalMs = 1000,
        int rpCheckIntervalMs = 60000, long maxSeriesPerDb = 10_000_000, int maxFieldsPerMeasurement = 1024,
        int flushIntervalMs = 5000)
    {
        _root = rootPath; _threshold = flushThreshold; _maxSeriesPerDb = maxSeriesPerDb;
        Directory.CreateDirectory(_root);
        _wal = new WalManager(Path.Combine(_root, "wal"), maxWalFileBytes, walFsync, walFsyncIntervalMs);
        _manifest = new Manifest(_root);
        _schema = new SchemaRegistry(_root, maxFieldsPerMeasurement);
        _shards = new ShardManager(_root, _manifest);
        _tombstones = new TombstoneStore(_root);
        _compactor = new Compactor(_manifest, _shards, _tombstones, _schema);
        if (rpCheckIntervalMs > 0) _rpExpiryTimer = new Timer(_ => CleanupExpiredShards(), null, rpCheckIntervalMs, rpCheckIntervalMs);
        _compactionTimer = new Timer(_ => _compactor.CompactAll(), null, 30000, 30000);
        if (flushIntervalMs > 0) _flushTimer = new Timer(_ => PeriodicFlush(), null, flushIntervalMs, flushIntervalMs);
    }

    public RecoveryResult Recover()
    {
        var result = new RecoveryResult();

        // Phase 1: Replay WAL records into buffer with schema validation
        foreach (var (db, rp, points) in _wal.Replay())
        {
            result.WalRecordsReplayed += points.Count;
            CreateDatabase(db);

            // Validate schema for replayed points (skip conflicting records instead of aborting startup)
            var validPoints = new List<Point>();
            foreach (var group in points.GroupBy(p => p.Measurement))
            {
                try { _schema.ValidateAndRegister(db, group.Key, group); validPoints.AddRange(group); }
                catch (FieldConflictException) { result.SchemaConflictsSkipped += group.Count(); }
            }

            var lk = GetLock(K(db, rp));
            lk.EnterWriteLock();
            try
            {
                var key = K(db, rp);
                if (!_buf.TryGetValue(key, out var list)) { list = []; _buf[key] = list; }
                list.AddRange(validPoints); TrackSeriesKeys(db, validPoints);
            }
            finally { lk.ExitWriteLock(); }
        }
        _lastFlushedWalFileId = _wal.CurrentFileId;

        // Phase 2: Rebuild in-memory state from existing segment files
        foreach (var db in _manifest.ListDatabases())
        {
            var allShards = _manifest.GetAllShards(db);
            foreach (var (shardRp, shard) in allShards)
            {
                var shardDir = _shards.ShardDir(db, shardRp, shard.Id);
                if (!Directory.Exists(shardDir)) continue;

                foreach (var segFile in Directory.GetFiles(shardDir, "*.seg"))
                {
                    result.SegmentsScanned++;
                    try
                    {
                        var metas = SegmentReader.ReadMetadata(segFile);
                        var pointsForIndex = new List<(string Measurement, string TagsCanonical, Dictionary<string, string> Tags)>();
                        foreach (var m in metas)
                        {
                            // Rebuild series keys
                            _globalLock.EnterWriteLock();
                            try
                            {
                                if (!_seriesKeys.TryGetValue(db, out var keys)) { keys = new(StringComparer.Ordinal); _seriesKeys[db] = keys; }
                                keys.Add($"{m.Measurement},{m.TagsCanonical}");
                            }
                            finally { _globalLock.ExitWriteLock(); }

                            // Collect for index update
                            var tags = ParseTags(m.TagsCanonical);
                            pointsForIndex.Add((m.Measurement, m.TagsCanonical, tags));

                            // Register schema for each field kind found in segment metadata
                            // SchemaRegistry is idempotent for matching types
                        }
                        _manifest.UpdateIndexes(db, pointsForIndex);
                    }
                    catch (InvalidDataException) { result.SegmentsCorrupted++; }
                }
            }
        }

        return result;
    }

    public Task WriteAsync(string db, string rp, List<Point> pts) => WriteInternalAsync(db, rp, pts);

    public Task WriteInternalAsync(string db, string rp, List<Point> pts)
    {
        CreateDatabase(db); _manifest.EnsureRp(db, rp);
        pts = DeduplicatePoints(pts);
        CheckCardinality(db, pts);
        foreach (var group in pts.GroupBy(p => p.Measurement)) _schema.ValidateAndRegister(db, group.Key, group);
        _wal.Append(db, rp, pts);
        var lk = GetLock(K(db, rp));
        lk.EnterWriteLock();
        try
        {
            var key = K(db, rp);
            if (!_buf.TryGetValue(key, out var list)) { list = []; _buf[key] = list; }
            list.AddRange(pts); TrackSeriesKeys(db, pts);
            _manifest.UpdateIndexes(db, pts.Select(p => (p.Measurement, SeriesKey.From(p).TagsCanonical, p.Tags)));
            if (list.Count >= _threshold) FlushLocked(db, rp, list);
        }
        finally { lk.ExitWriteLock(); }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Deduplicate points within a batch: same measurement+tags+timestamp = field set merge,
    /// same-named field last-write-wins (InfluxDB semantics).
    /// </summary>
    private static List<Point> DeduplicatePoints(List<Point> pts)
    {
        if (pts.Count <= 1) return pts;
        var map = new Dictionary<(string Meas, string Tags, long Ts), Point>();
        foreach (var p in pts)
        {
            var sk = SeriesKey.From(p);
            var key = (p.Measurement, sk.TagsCanonical, p.TimestampNs);
            if (map.TryGetValue(key, out var existing))
            {
                foreach (var kv in p.Fields) existing.Fields[kv.Key] = kv.Value;
            }
            else
            {
                map[key] = new Point
                {
                    Measurement = p.Measurement,
                    Tags = p.Tags,
                    Fields = new Dictionary<string, FieldValue>(p.Fields, StringComparer.Ordinal),
                    TimestampNs = p.TimestampNs
                };
            }
        }
        return map.Values.ToList();
    }

    public void CreateDatabase(string db) { _manifest.EnsureDatabase(db); _manifest.EnsureRp(db, "autogen"); Directory.CreateDirectory(Path.Combine(_root, "db", db, "autogen")); }

    public IReadOnlyList<string> ListDatabases() => _manifest.ListDatabases();

    public List<Point> ReadAllPoints(string db, string rp, string? meas, long? min, long? max, HashSet<string>? requestedFields = null)
    {
        var res = new List<Point>();
        var lk = GetLock(K(db, rp));
        lk.EnterReadLock();
        try
        {
            if (_buf.TryGetValue(K(db, rp), out var l))
            {
                var bufMatched = l.Where(p => Match(p, meas, min, max));
                if (requestedFields != null)
                    bufMatched = bufMatched.Select(p => new Point
                    {
                        Measurement = p.Measurement, Tags = p.Tags, TimestampNs = p.TimestampNs,
                        Fields = p.Fields.Where(f => requestedFields.Contains(f.Key)).ToDictionary(f => f.Key, f => f.Value)
                    });
                res.AddRange(bufMatched);
            }
        }
        finally { lk.ExitReadLock(); }

        foreach (var (segPath, _) in _shards.ListSegments(db, rp, min, max))
        {
            try
            {
                // Time range pushdown: use metadata to skip segments that don't overlap the query range
                if (meas != null || (min.HasValue && max.HasValue))
                {
                    try
                    {
                        var metas = SegmentReader.ReadMetadata(segPath);
                        if (meas != null && !metas.Any(m => m.Measurement == meas)) continue;
                        if (min.HasValue && !metas.Any(m => m.MaxTime >= min.Value)) continue;
                        if (max.HasValue && !metas.Any(m => m.MinTime <= max.Value)) continue;
                    }
                    catch { /* fall through to full read */ }
                }

                var cols = SegmentReader.ReadSegment(segPath, requestedFields)
                    .Where(c => (meas == null || c.Measurement == meas) && (!min.HasValue || c.MaxTime >= min) && (!max.HasValue || c.MinTime <= max)).ToList();
                var filtered = new List<SegmentColumn>();
                foreach (var col in cols)
                {
                    if (_tombstones.IsColumnDeleted(db, col.Measurement, col.TagsCanonical, col.MinTime, col.MaxTime)) continue;
                    var (ts, vals) = _tombstones.FilterColumnDeleted(db, col.Measurement, col.TagsCanonical, col.Timestamps, col.Values);
                    if (ts.Count > 0) filtered.Add(new SegmentColumn(col.Measurement, col.TagsCanonical, col.Field, col.Kind, ts[0], ts[^1], ts, vals, col.Stats));
                }
                res.AddRange(Rebuild(filtered, min, max));
            }
            catch (InvalidDataException) { }
        }
        return res.OrderBy(x => x.TimestampNs).ToList();
    }

    public IReadOnlyList<string> ListMeasurements(string db)
    {
        var r = _schema.GetFields(db, null).Select(f => f.FieldKey).Distinct().ToList();
        var lk = GetLock(K(db, "autogen"));
        lk.EnterReadLock();
        try { if (_buf.TryGetValue(K(db, "autogen"), out var buf)) foreach (var p in buf) if (!r.Contains(p.Measurement)) r.Add(p.Measurement); }
        finally { lk.ExitReadLock(); }
        return r.Order().ToArray();
    }

    public IReadOnlyList<string> ListTagKeys(string db, string? m) => ReadAllPoints(db, "autogen", m, null, null).SelectMany(p => p.Tags.Keys).Distinct(StringComparer.Ordinal).Order().ToArray();
    public IReadOnlyList<(string Key, string Value)> ListTagValues(string db, string? m, string key)
    {
        var indexed = _manifest.GetTagValues(db, m, key);
        if (indexed.Count > 0) return indexed;
        return ReadAllPoints(db, "autogen", m, null, null).Where(p => p.Tags.ContainsKey(key)).Select(p => (key, p.Tags[key])).Distinct().OrderBy(x => x.Item2).ToArray();
    }
    public IReadOnlyList<(string Field, FieldKind Kind)> ListFieldKeys(string db, string? m) => _schema.GetFields(db, m);
    public SchemaRegistry Schema => _schema;
    public Manifest Meta => _manifest;
    public TombstoneStore Tombstones => _tombstones;
    public string RootPath => _root;

    public long GetBufferedPointCount()
    {
        long count = 0;
        _globalLock.EnterReadLock();
        try { foreach (var kv in _buf) count += kv.Value.Count; }
        finally { _globalLock.ExitReadLock(); }
        return count;
    }

    public void DropDatabase(string db)
    {
        FlushDatabase(db);
        var dbDir = Path.Combine(_root, "db", db);
        if (Directory.Exists(dbDir)) try { Directory.Delete(dbDir, true); } catch { }
        _manifest.DropDatabase(db); _tombstones.DropDatabase(db);
        _globalLock.EnterWriteLock();
        try { foreach (var k in _buf.Keys.Where(k => k.StartsWith(db + "|")).ToList()) { _buf.Remove(k); _locks.Remove(k); } _seriesKeys.Remove(db); }
        finally { _globalLock.ExitWriteLock(); }
    }

    public void DropMeasurement(string db, string measurement)
    {
        _tombstones.AddMeasurementDelete(db, measurement);
        _manifest.RemoveMeasurementIndex(db, measurement);
        var lk = GetLock(K(db, "autogen"));
        lk.EnterWriteLock();
        try { if (_buf.TryGetValue(K(db, "autogen"), out var list)) list.RemoveAll(p => p.Measurement == measurement); }
        finally { lk.ExitWriteLock(); }
    }

    public void DeleteFromMeasurement(string db, string measurement, long? minTime, long? maxTime)
    {
        _tombstones.AddMeasurementDelete(db, measurement, minTime, maxTime);
        var lk = GetLock(K(db, "autogen"));
        lk.EnterWriteLock();
        try { if (_buf.TryGetValue(K(db, "autogen"), out var list)) list.RemoveAll(p => p.Measurement == measurement && (!minTime.HasValue || p.TimestampNs >= minTime.Value) && (!maxTime.HasValue || p.TimestampNs <= maxTime.Value)); }
        finally { lk.ExitWriteLock(); }
    }

    private static IEnumerable<Point> Rebuild(List<SegmentColumn> cols, long? min, long? max)
    {
        var map = new Dictionary<(string, string, long), Dictionary<string, FieldValue>>();
        foreach (var c in cols) for (int i = 0; i < c.Timestamps.Count; i++)
        {
            var ts = c.Timestamps[i]; if ((min.HasValue && ts < min) || (max.HasValue && ts > max)) continue;
            var key = (c.Measurement, c.TagsCanonical, ts);
            if (!map.TryGetValue(key, out var fs)) { fs = new(StringComparer.Ordinal); map[key] = fs; }
            fs[c.Field] = c.Values[i];
        }
        foreach (var it in map) yield return new Point { Measurement = it.Key.Item1, Tags = ParseTags(it.Key.Item2), TimestampNs = it.Key.Item3, Fields = it.Value };
    }

    private static Dictionary<string, string> ParseTags(string s)
    { var d = new Dictionary<string, string>(StringComparer.Ordinal); if (string.IsNullOrEmpty(s)) return d; foreach (var p in s.Split(',')) { var i = p.IndexOf('='); if (i > 0) d[p[..i]] = p[(i + 1)..]; } return d; }

    private static bool Match(Point p, string? m, long? min, long? max) => (m == null || p.Measurement == m) && (!min.HasValue || p.TimestampNs >= min) && (!max.HasValue || p.TimestampNs <= max);

    private void FlushLocked(string db, string rp, List<Point> l)
    {
        if (l.Count == 0) return;
        foreach (var group in l.GroupBy(p => { var (id, _) = _shards.GetOrCreateShard(db, rp, p.TimestampNs); return id; }))
        {
            var shardDir = _shards.ShardDir(db, rp, group.Key);
            var segPath = Path.Combine(shardDir, $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}.seg");
            SegmentWriter.WriteSegment(segPath, group);
            _shards.RegisterSegment(db, rp, group.Key, segPath);
        }
        l.Clear(); _lastFlushedWalFileId = _wal.CurrentFileId; _wal.Checkpoint(_lastFlushedWalFileId);
    }

    private void FlushDatabase(string db)
    {
        _globalLock.EnterWriteLock();
        try { foreach (var kv in _buf.Where(kv => kv.Key.StartsWith(db + "|")).ToList()) { var p = kv.Key.Split('|'); var lk = GetLock(kv.Key, alreadyHoldingGlobalWrite: true); lk.EnterWriteLock(); try { FlushLocked(p[0], p[1], kv.Value); } finally { lk.ExitWriteLock(); } } }
        finally { _globalLock.ExitWriteLock(); }
    }

    public void FlushAll()
    {
        _globalLock.EnterWriteLock();
        try { foreach (var kv in _buf.ToArray()) { var p = kv.Key.Split('|'); var lk = GetLock(kv.Key, alreadyHoldingGlobalWrite: true); lk.EnterWriteLock(); try { FlushLocked(p[0], p[1], kv.Value); } finally { lk.ExitWriteLock(); } } }
        finally { _globalLock.ExitWriteLock(); }
    }

    private void CheckCardinality(string db, List<Point> pts)
    {
        _globalLock.EnterReadLock();
        try
        {
            if (!_seriesKeys.TryGetValue(db, out var existing)) return;
            var total = existing.Count + pts.Count(p => !existing.Contains(SeriesKey.From(p).ToString()));
            if (total > _maxSeriesPerDb) throw new CardinalityLimitExceededException($"series cardinality limit exceeded for database '{db}': {total} > {_maxSeriesPerDb}");
        }
        finally { _globalLock.ExitReadLock(); }
    }

    private void TrackSeriesKeys(string db, List<Point> pts)
    {
        _globalLock.EnterWriteLock();
        try
        {
            if (!_seriesKeys.TryGetValue(db, out var keys)) { keys = new(StringComparer.Ordinal); _seriesKeys[db] = keys; }
            foreach (var p in pts) keys.Add(SeriesKey.From(p).ToString());
        }
        finally { _globalLock.ExitWriteLock(); }
    }

    public int GetSeriesCardinality(string db) { _globalLock.EnterReadLock(); try { return _seriesKeys.TryGetValue(db, out var keys) ? keys.Count : 0; } finally { _globalLock.ExitReadLock(); } }

    private ReaderWriterLockSlim GetLock(string key, bool alreadyHoldingGlobalWrite = false)
    {
        if (alreadyHoldingGlobalWrite)
        {
            if (!_locks.TryGetValue(key, out var lk))
            { lk = new ReaderWriterLockSlim(); _locks[key] = lk; }
            return lk;
        }
        _globalLock.EnterUpgradeableReadLock();
        try { if (_locks.TryGetValue(key, out var lk)) return lk; _globalLock.EnterWriteLock(); try { if (!_locks.TryGetValue(key, out lk)) { lk = new ReaderWriterLockSlim(); _locks[key] = lk; } return lk; } finally { _globalLock.ExitWriteLock(); } }
        finally { _globalLock.ExitUpgradeableReadLock(); }
    }

    private void CleanupExpiredShards() { try { _shards.CleanupExpiredShards(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000); } catch { } }

    private static string K(string db, string rp) => db + "|" + rp;

    public void Dispose()
    {
        _rpExpiryTimer?.Dispose(); _compactionTimer?.Dispose(); _flushTimer?.Dispose(); FlushAll(); _wal.Dispose(); _globalLock.Dispose();
        foreach (var lk in _locks.Values) lk.Dispose();
    }

    private void PeriodicFlush()
    {
        try
        {
            _globalLock.EnterWriteLock();
            try
            {
                foreach (var kv in _buf.ToArray())
                {
                    if (kv.Value.Count == 0) continue;
                    var p = kv.Key.Split('|');
                    var lk = GetLock(kv.Key, alreadyHoldingGlobalWrite: true);
                    lk.EnterWriteLock();
                    try { FlushLocked(p[0], p[1], kv.Value); }
                    finally { lk.ExitWriteLock(); }
                }
            }
            finally { _globalLock.ExitWriteLock(); }
        }
        catch { /* best effort */ }
    }
}

/// <summary>
/// Result of TsdbEngine.Recover() with stats about what was restored.
/// </summary>
public sealed class RecoveryResult
{
    public int WalRecordsReplayed { get; set; }
    public int SegmentsScanned { get; set; }
    public int SegmentsCorrupted { get; set; }
    public int SchemaConflictsSkipped { get; set; }
}

public sealed class CardinalityLimitExceededException : Exception
{
    public CardinalityLimitExceededException(string message) : base(message) { }
}

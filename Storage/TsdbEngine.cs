using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;

namespace MiniInflux.Net10.Storage;

public sealed class TsdbEngine : IDisposable
{
    private sealed record BufferedPoint(Point Point, WalPosition Position);

    private readonly string _root;
    private readonly WalManager _wal;
    private readonly SchemaRegistry _schema;
    private readonly Manifest _manifest;
    private readonly ShardManager _shards;
    private readonly TombstoneStore _tombstones;
    private readonly Compactor _compactor;
    private readonly Dictionary<string, ReaderWriterLockSlim> _locks = new(StringComparer.Ordinal);
    private readonly ReaderWriterLockSlim _globalLock = new();
    private readonly Dictionary<string, List<BufferedPoint>> _buf = new(StringComparer.Ordinal);
    private readonly Dictionary<string, WalPosition> _bufferReplayFloors = new(StringComparer.Ordinal);
    private readonly int _threshold;
    private readonly long _maxSeriesPerDb;
    private readonly long _maxBufferPoints;
    private readonly long _maxBufferBytes;
    private readonly Dictionary<string, HashSet<string>> _seriesKeys = new(StringComparer.Ordinal);
    private Timer? _rpExpiryTimer;
    private Timer? _compactionTimer;
    private Timer? _flushTimer;

    public TsdbEngine(string rootPath, int flushThreshold = 50000,
        long maxWalFileBytes = 16 * 1024 * 1024, bool walFsync = true, int walFsyncIntervalMs = 1000,
        int rpCheckIntervalMs = 60000, long maxSeriesPerDb = 10_000_000, int maxFieldsPerMeasurement = 1024,
        int flushIntervalMs = 5000, long maxBufferPoints = 1_000_000, long maxBufferBytes = 0, int compactionIntervalMs = 30000)
    {
        _root = rootPath; _threshold = flushThreshold; _maxSeriesPerDb = maxSeriesPerDb; _maxBufferPoints = maxBufferPoints; _maxBufferBytes = maxBufferBytes;
        Directory.CreateDirectory(_root);
        _wal = new WalManager(Path.Combine(_root, "wal"), maxWalFileBytes, walFsync, walFsyncIntervalMs);
        _manifest = new Manifest(_root);
        _schema = new SchemaRegistry(_root, maxFieldsPerMeasurement);
        _shards = new ShardManager(_root, _manifest);
        _tombstones = new TombstoneStore(_root);
        _compactor = new Compactor(_manifest, _shards, _tombstones, _schema);
        if (rpCheckIntervalMs > 0) _rpExpiryTimer = new Timer(_ => CleanupExpiredShards(), null, rpCheckIntervalMs, rpCheckIntervalMs);
        if (compactionIntervalMs > 0) _compactionTimer = new Timer(_ => _compactor.CompactAll(), null, compactionIntervalMs, compactionIntervalMs);
        if (flushIntervalMs > 0) _flushTimer = new Timer(_ => PeriodicFlush(), null, flushIntervalMs, flushIntervalMs);
    }

    public RecoveryResult Recover()
    {
        var result = new RecoveryResult();

        // Phase 1: Replay WAL records into buffer with schema validation
        foreach (var replayPoint in _wal.ReplayWithPositions())
        {
            result.WalRecordsReplayed++;
            CreateDatabase(replayPoint.Db);

            // Validate schema for replayed points (skip conflicting records instead of aborting startup)
            var validPoints = new List<BufferedPoint>();
            try
            {
                _schema.ValidateAndRegister(replayPoint.Db, replayPoint.Point.Measurement, [replayPoint.Point]);
                validPoints.Add(new BufferedPoint(replayPoint.Point, replayPoint.Position));
            }
            catch (FieldConflictException) { result.SchemaConflictsSkipped++; }

            var lk = GetLock(K(replayPoint.Db, replayPoint.Rp));
            lk.EnterWriteLock();
            try
            {
                var key = K(replayPoint.Db, replayPoint.Rp);
                if (!_buf.TryGetValue(key, out var list)) { list = []; _buf[key] = list; }
                list.AddRange(validPoints); TrackSeriesKeys(replayPoint.Db, validPoints);
                UpdateBufferReplayFloor(key, list);
            }
            finally { lk.ExitWriteLock(); }
        }

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
        CheckBufferLimit(pts);
        foreach (var group in pts.GroupBy(p => p.Measurement)) _schema.ValidateAndRegister(db, group.Key, group);
        var walPositions = _wal.Append(db, rp, pts);
        var lk = GetLock(K(db, rp));
        lk.EnterWriteLock();
        try
        {
            var key = K(db, rp);
            if (!_buf.TryGetValue(key, out var list)) { list = []; _buf[key] = list; }
            var buffered = pts.Zip(walPositions, (point, position) => new BufferedPoint(point, position)).ToList();
            list.AddRange(buffered); TrackSeriesKeys(db, buffered);
            UpdateBufferReplayFloor(key, list);
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

    public string GetDefaultRpName(string db) => _manifest.GetDefaultRp(db).Name;
    public IReadOnlyList<string> ListSeries(string db, string? measurement) => _manifest.GetSeries(db, measurement);
    public int GetMeasurementCardinality(string db) => _manifest.ListMeasurements(db).Count;
    public int GetTagValueCardinality(string db, string? measurement, string? tagKey) => _manifest.GetTagValueCardinality(db, measurement, tagKey);

    public List<Point> ReadAllPoints(string db, string rp, string? meas, long? min, long? max,
        HashSet<string>? requestedFields = null, HashSet<string>? allowedTagsCanonical = null, List<FieldFilter>? fieldFilters = null,
        CancellationToken cancellationToken = default)
    {
        var res = new List<Point>();
        var lk = GetLock(K(db, rp));
        lk.EnterReadLock();
        try
        {
            if (_buf.TryGetValue(K(db, rp), out var l))
            {
                var bufMatched = l.Select(x => x.Point).Where(p => Match(p, meas, min, max)
                    && (allowedTagsCanonical == null || allowedTagsCanonical.Contains(SeriesKey.From(p).TagsCanonical)));
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
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                // Time range pushdown: use metadata to skip segments that don't overlap the query range
                if (meas != null || (min.HasValue && max.HasValue) || (fieldFilters != null && fieldFilters.Count > 0) || allowedTagsCanonical != null)
                {
                    try
                    {
                        var metas = SegmentReader.ReadMetadata(segPath);
                        if (meas != null && !metas.Any(m => m.Measurement == meas)) continue;
                        if (min.HasValue && !metas.Any(m => m.MaxTime >= min.Value)) continue;
                        if (max.HasValue && !metas.Any(m => m.MinTime <= max.Value)) continue;
                        if (allowedTagsCanonical != null && !metas.Any(m => allowedTagsCanonical.Contains(m.TagsCanonical))) continue;
                        if (fieldFilters != null && fieldFilters.Count > 0 && !CouldSegmentMatchFieldFilters(metas, meas, allowedTagsCanonical, fieldFilters))
                            continue;
                    }
                    catch { /* fall through to full read */ }
                }

                var cols = SegmentReader.ReadSegment(segPath, requestedFields)
                    .Where(c => (meas == null || c.Measurement == meas)
                        && (!min.HasValue || c.MaxTime >= min)
                        && (!max.HasValue || c.MinTime <= max)
                        && (allowedTagsCanonical == null || allowedTagsCanonical.Contains(c.TagsCanonical))).ToList();
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
        return DeduplicatePoints(res.OrderBy(x => x.TimestampNs).ToList());
    }

    public IReadOnlyList<string> ListMeasurements(string db)
    {
        var r = _manifest.ListMeasurements(db).Concat(_schema.ListMeasurements(db)).Distinct(StringComparer.Ordinal).ToList();
        var rp = GetDefaultRpName(db);
        var lk = GetLock(K(db, rp));
        lk.EnterReadLock();
        try { if (_buf.TryGetValue(K(db, rp), out var buf)) foreach (var p in buf.Select(x => x.Point)) if (!r.Contains(p.Measurement)) r.Add(p.Measurement); }
        finally { lk.ExitReadLock(); }
        return r.Order().ToArray();
    }

    public IReadOnlyList<string> ListTagKeys(string db, string? m) => ReadAllPoints(db, GetDefaultRpName(db), m, null, null).SelectMany(p => p.Tags.Keys).Distinct(StringComparer.Ordinal).Order().ToArray();
    public IReadOnlyList<(string Key, string Value)> ListTagValues(string db, string? m, string key)
    {
        var indexed = _manifest.GetTagValues(db, m, key);
        if (indexed.Count > 0) return indexed;
        return ReadAllPoints(db, GetDefaultRpName(db), m, null, null).Where(p => p.Tags.ContainsKey(key)).Select(p => (key, p.Tags[key])).Distinct().OrderBy(x => x.Item2).ToArray();
    }
    public IReadOnlyList<(string Field, FieldKind Kind)> ListFieldKeys(string db, string? m) => _schema.GetFields(db, m);
    public SchemaRegistry Schema => _schema;
    public Manifest Meta => _manifest;
    public TombstoneStore Tombstones => _tombstones;
    public string RootPath => _root;
    public IReadOnlyList<string> GetSeriesForTagValue(string db, string measurement, string tagKey, string tagValue) =>
        _manifest.GetSeriesForTagValue(db, measurement, tagKey, tagValue);
    public IReadOnlyList<string> GetSeriesForTagKey(string db, string measurement, string tagKey) =>
        _manifest.GetSeriesForTagKey(db, measurement, tagKey);
    public IReadOnlyList<string> GetSeriesForTagRegex(string db, string measurement, string tagKey, string pattern, bool negate = false) =>
        _manifest.GetSeriesForTagRegex(db, measurement, tagKey, pattern, negate);

    public long GetBufferedPointCount()
    {
        long count = 0;
        _globalLock.EnterReadLock();
        try { foreach (var kv in _buf) count += kv.Value.Count; }
        finally { _globalLock.ExitReadLock(); }
        return count;
    }

    public long GetBufferedByteCount()
    {
        long bytes = 0;
        _globalLock.EnterReadLock();
        try
        {
            foreach (var kv in _buf)
                bytes += kv.Value.Sum(p => EstimateBufferedPointBytes(p.Point));
        }
        finally { _globalLock.ExitReadLock(); }
        return bytes;
    }

    public List<Point> ReadBufferedPoints(string db, string rp, string? meas, long? min, long? max,
        HashSet<string>? requestedFields = null, HashSet<string>? allowedTagsCanonical = null)
    {
        var res = new List<Point>();
        var lk = GetLock(K(db, rp));
        lk.EnterReadLock();
        try
        {
            if (_buf.TryGetValue(K(db, rp), out var list))
            {
                var matched = list.Select(x => x.Point).Where(p => Match(p, meas, min, max)
                    && (allowedTagsCanonical == null || allowedTagsCanonical.Contains(SeriesKey.From(p).TagsCanonical)));
                if (requestedFields != null)
                {
                    matched = matched.Select(p => new Point
                    {
                        Measurement = p.Measurement,
                        Tags = p.Tags,
                        TimestampNs = p.TimestampNs,
                        Fields = p.Fields.Where(f => requestedFields.Contains(f.Key)).ToDictionary(f => f.Key, f => f.Value)
                    });
                }
                res.AddRange(matched);
            }
        }
        finally { lk.ExitReadLock(); }
        return res;
    }

    public List<SegmentColumnMeta> ReadSegmentMetadata(string db, string rp, string? meas, long? min, long? max,
        HashSet<string>? requestedFields = null, HashSet<string>? allowedTagsCanonical = null, CancellationToken cancellationToken = default)
    {
        var result = new List<SegmentColumnMeta>();
        foreach (var (segPath, _) in _shards.ListSegments(db, rp, min, max))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                result.AddRange(SegmentReader.ReadMetadata(segPath)
                    .Where(m => (meas == null || m.Measurement == meas)
                        && (!min.HasValue || m.MaxTime >= min.Value)
                        && (!max.HasValue || m.MinTime <= max.Value)
                        && (requestedFields == null || requestedFields.Contains(m.Field))
                        && (allowedTagsCanonical == null || allowedTagsCanonical.Contains(m.TagsCanonical))));
            }
            catch (InvalidDataException) { }
        }
        return result;
    }

    public CompactionStatsSnapshot GetCompactionStats() => _compactor.GetStats();
    public int CompactNow() => _compactor.CompactAll();

    public void DropDatabase(string db)
    {
        FlushDatabase(db);
        var dbDir = Path.Combine(_root, "db", db);
        if (Directory.Exists(dbDir)) try { Directory.Delete(dbDir, true); } catch { }
        _manifest.DropDatabase(db); _tombstones.DropDatabase(db);
        _globalLock.EnterWriteLock();
        try { foreach (var k in _buf.Keys.Where(k => k.StartsWith(db + "|")).ToList()) { _buf.Remove(k); _locks.Remove(k); _bufferReplayFloors.Remove(k); } _seriesKeys.Remove(db); }
        finally { _globalLock.ExitWriteLock(); }
    }

    public void DropMeasurement(string db, string measurement)
    {
        _tombstones.AddMeasurementDelete(db, measurement);
        _manifest.RemoveMeasurementIndex(db, measurement);
        foreach (var rp in _manifest.ListRetentionPolicies(db).Select(r => r.Name).DefaultIfEmpty("autogen"))
        {
            var key = K(db, rp);
            var lk = GetLock(key);
            lk.EnterWriteLock();
            try
            {
                if (_buf.TryGetValue(key, out var list))
                {
                    list.RemoveAll(p => p.Point.Measurement == measurement);
                    UpdateBufferReplayFloor(key, list);
                }
            }
            finally { lk.ExitWriteLock(); }
        }
    }

    public void DeleteFromMeasurement(string db, string measurement, long? minTime, long? maxTime)
    {
        _tombstones.AddMeasurementDelete(db, measurement, minTime, maxTime);
        DeleteBuffered(db, GetDefaultRpName(db), measurement, minTime, maxTime, _ => true);
    }

    public void DeleteFromMeasurement(string db, string measurement, long? minTime, long? maxTime, Predicate<Point> predicate)
    {
        var rp = GetDefaultRpName(db);
        var matches = ReadAllPoints(db, rp, measurement, minTime, maxTime)
            .Where(p => predicate(p))
            .GroupBy(p => SeriesKey.From(p).TagsCanonical);

        foreach (var group in matches)
            _tombstones.AddSeriesDelete(db, measurement, group.Key, minTime, maxTime);

        DeleteBuffered(db, rp, measurement, minTime, maxTime, predicate);
    }

    public void DropSeries(string db, string? measurement, List<string> tagsCanonical)
    {
        if (measurement == null)
        {
            foreach (var m in ListMeasurements(db))
                DropSeries(db, m, tagsCanonical);
            return;
        }

        var tagSet = new HashSet<string>(tagsCanonical, StringComparer.Ordinal);
        foreach (var tags in tagSet)
            _tombstones.AddSeriesDelete(db, measurement, tags);
        _manifest.RemoveSeriesIndex(db, measurement, tagSet);

        foreach (var rp in _manifest.ListRetentionPolicies(db).Select(r => r.Name).DefaultIfEmpty("autogen"))
        {
            var key = K(db, rp);
            var lk = GetLock(key);
            lk.EnterWriteLock();
            try
            {
                if (_buf.TryGetValue(key, out var list))
                {
                    list.RemoveAll(p => p.Point.Measurement == measurement && tagSet.Contains(SeriesKey.From(p.Point).TagsCanonical));
                    UpdateBufferReplayFloor(key, list);
                }
            }
            finally { lk.ExitWriteLock(); }
        }
    }

    public bool DropShard(int shardId)
    {
        foreach (var db in _manifest.ListDatabases())
        {
            foreach (var rp in _manifest.ListRetentionPolicies(db).Select(r => r.Name))
            {
                var shard = _manifest.GetShards(db, rp).FirstOrDefault(s => s.Id == shardId);
                if (shard == null) continue;
                var dir = _shards.ShardDir(db, rp, shardId);
                if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
                _manifest.RemoveShardGroup(db, rp, shardId);
                return true;
            }
        }
        return false;
    }

    private void DeleteBuffered(string db, string rp, string measurement, long? minTime, long? maxTime, Predicate<Point> predicate)
    {
        var lk = GetLock(K(db, rp));
        lk.EnterWriteLock();
        try
        {
            if (_buf.TryGetValue(K(db, rp), out var list))
            {
                list.RemoveAll(p => p.Point.Measurement == measurement
                    && (!minTime.HasValue || p.Point.TimestampNs >= minTime.Value)
                    && (!maxTime.HasValue || p.Point.TimestampNs <= maxTime.Value)
                    && predicate(p.Point));
                UpdateBufferReplayFloor(K(db, rp), list);
            }
        }
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

    private static bool CouldSegmentMatchFieldFilters(
        List<SegmentColumnMeta> metas,
        string? measurement,
        HashSet<string>? allowedTagsCanonical,
        List<FieldFilter> fieldFilters)
    {
        var relevantMetas = metas.Where(m =>
            (measurement == null || m.Measurement == measurement)
            && (allowedTagsCanonical == null || allowedTagsCanonical.Contains(m.TagsCanonical)))
            .ToList();

        if (relevantMetas.Count == 0)
            return false;

        foreach (var filter in fieldFilters)
        {
            var candidates = relevantMetas.Where(m => string.Equals(m.Field, filter.Field, StringComparison.Ordinal)).ToList();
            if (candidates.Count == 0)
                return false;
            if (!candidates.Any(meta => CouldColumnMatch(meta, filter)))
                return false;
        }

        return true;
    }

    private static bool CouldColumnMatch(SegmentColumnMeta meta, FieldFilter filter)
    {
        var stats = meta.Stats;
        if (stats == null)
            return true;

        return filter.Op switch
        {
            FieldOp.Eq => stats.Min <= filter.Value && stats.Max >= filter.Value,
            FieldOp.Neq => !(stats.Count > 0 && Math.Abs(stats.Min - filter.Value) < 1e-9 && Math.Abs(stats.Max - filter.Value) < 1e-9),
            FieldOp.Gt => stats.Max > filter.Value,
            FieldOp.Gte => stats.Max >= filter.Value,
            FieldOp.Lt => stats.Min < filter.Value,
            FieldOp.Lte => stats.Min <= filter.Value,
            _ => true
        };
    }

    private void FlushLocked(string db, string rp, List<BufferedPoint> l)
    {
        if (l.Count == 0) return;
        foreach (var group in l.GroupBy(p => { var (id, _) = _shards.GetOrCreateShard(db, rp, p.Point.TimestampNs); return id; }))
        {
            var shardDir = _shards.ShardDir(db, rp, group.Key);
            var segPath = Path.Combine(shardDir, $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}.seg");
            SegmentWriter.WriteSegment(segPath, group.Select(x => x.Point));
            _shards.RegisterSegment(db, rp, group.Key, segPath);
        }
        l.Clear();
        var key = K(db, rp);
        UpdateBufferReplayFloor(key, l);
        UpdateWalCheckpoint();
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
            if (!_seriesKeys.TryGetValue(db, out var existing))
                existing = [];
            var newSeries = pts.Select(p => SeriesKey.From(p).ToString())
                .Distinct(StringComparer.Ordinal)
                .Count(k => !existing.Contains(k));
            var total = existing.Count + newSeries;
            if (total > _maxSeriesPerDb) throw new CardinalityLimitExceededException($"series cardinality limit exceeded for database '{db}': {total} > {_maxSeriesPerDb}");
        }
        finally { _globalLock.ExitReadLock(); }
    }

    private void CheckBufferLimit(List<Point> incomingPoints)
    {
        var bufferedPoints = GetBufferedPointCount();
        if (_maxBufferPoints > 0 && bufferedPoints + incomingPoints.Count > _maxBufferPoints)
            throw new MemoryLimitExceededException($"memory buffer point limit exceeded: {bufferedPoints + incomingPoints.Count} > {_maxBufferPoints}");

        if (_maxBufferBytes > 0)
        {
            var bufferedBytes = GetBufferedByteCount();
            var incomingBytes = incomingPoints.Sum(EstimateBufferedPointBytes);
            if (bufferedBytes + incomingBytes > _maxBufferBytes)
                throw new MemoryLimitExceededException($"memory buffer byte limit exceeded: {bufferedBytes + incomingBytes} > {_maxBufferBytes}");
        }
    }

    private static long EstimateBufferedPointBytes(Point point)
    {
        long size = 96 + EstimateStringBytes(point.Measurement) + 8;
        foreach (var tag in point.Tags)
            size += 32 + EstimateStringBytes(tag.Key) + EstimateStringBytes(tag.Value);
        foreach (var field in point.Fields)
            size += 48 + EstimateStringBytes(field.Key) + EstimateFieldValueBytes(field.Value);
        return size;
    }

    private static long EstimateFieldValueBytes(FieldValue value) => value.Kind switch
    {
        FieldKind.String => 24 + EstimateStringBytes(value.String),
        _ => 16
    };

    private static long EstimateStringBytes(string? value) => string.IsNullOrEmpty(value) ? 0 : 24 + value.Length * 2L;

    private void TrackSeriesKeys(string db, List<BufferedPoint> pts)
    {
        _globalLock.EnterWriteLock();
        try
        {
            if (!_seriesKeys.TryGetValue(db, out var keys)) { keys = new(StringComparer.Ordinal); _seriesKeys[db] = keys; }
            foreach (var p in pts) keys.Add(SeriesKey.From(p.Point).ToString());
        }
        finally { _globalLock.ExitWriteLock(); }
    }

    private void UpdateBufferReplayFloor(string key, List<BufferedPoint> list)
    {
        var lockHeld = _globalLock.IsWriteLockHeld;
        if (!lockHeld) _globalLock.EnterWriteLock();
        try
        {
            if (list.Count == 0) _bufferReplayFloors.Remove(key);
            else _bufferReplayFloors[key] = list
                .OrderBy(x => x.Position.FileId)
                .ThenBy(x => x.Position.Offset)
                .First().Position;
        }
        finally { if (!lockHeld) _globalLock.ExitWriteLock(); }
    }

    private void UpdateWalCheckpoint()
    {
        var writeHeld = _globalLock.IsWriteLockHeld;
        var readHeld = _globalLock.IsReadLockHeld;
        if (!writeHeld && !readHeld) _globalLock.EnterReadLock();
        try
        {
            var checkpoint = _bufferReplayFloors.Count == 0
                ? _wal.CurrentPosition
                : _bufferReplayFloors.Values.OrderBy(p => p.FileId).ThenBy(p => p.Offset).First();
            _wal.Checkpoint(checkpoint);
        }
        finally { if (!writeHeld && !readHeld) _globalLock.ExitReadLock(); }
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

public sealed class MemoryLimitExceededException : Exception
{
    public MemoryLimitExceededException(string message) : base(message) { }
}

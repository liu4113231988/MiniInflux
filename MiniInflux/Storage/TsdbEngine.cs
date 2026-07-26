using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;

namespace MiniInflux.Net10.Storage;

public sealed class TsdbEngine : IDisposable
{
    public sealed record SegmentMetadataQueryResult(List<SegmentColumnMeta> Metas, int FooterHits, int FullReads);
    public sealed record DescendingSeriesReadResult(List<Point> Points, int SegmentColumnsRead, int PointsMaterialized, string? LimitPushdownStopReason);
    public sealed record DescendingFieldReadResult(List<long> Timestamps, List<FieldValue> Values, int SegmentColumnsRead, string? LimitPushdownStopReason);
    public sealed record DescendingFieldsReadResult(List<long> Timestamps, List<FieldValue?[]> Rows, int SegmentColumnsRead, string? LimitPushdownStopReason);

    private sealed record BufferedPoint(Point Point, WalPosition Position, SeriesKey SeriesKey);
    private sealed class PendingPoint(Point point, SeriesKey seriesKey)
    {
        public Point Point = point;
        public readonly SeriesKey SeriesKey = seriesKey;
        public bool Cloned;
    }

    private readonly string _root;
    private readonly WalManager _wal;
    private readonly StorageHealth _health = new();
    private readonly SchemaRegistry _schema;
    private readonly Manifest _manifest;
    private readonly ShardManager _shards;
    private readonly TombstoneStore _tombstones;
    private readonly Compactor _compactor;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ReaderWriterLockSlim> _locks = new(StringComparer.Ordinal);
    private readonly ReaderWriterLockSlim _globalLock = new();
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, (long Length, DateTime LastWriteUtc, List<SegmentColumnMeta> Metas, bool UsedFooter)> _segmentMetadataCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, List<BufferedPoint>> _buf = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, Dictionary<SeriesKey, List<BufferedPoint>>> _bufBySeries = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, WalPosition> _bufferReplayFloors = new(StringComparer.Ordinal);
    private readonly int _threshold;
    private readonly long _maxSeriesPerDb;
    private readonly long _maxBufferPoints;
    private readonly long _maxBufferBytes;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, HashSet<SeriesKey>> _seriesKeys = new(StringComparer.Ordinal);
    private long _bufferedPointCount;
    private long _bufferedByteCount;
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
        _wal = new WalManager(Path.Combine(_root, "wal"), maxWalFileBytes, walFsync, walFsyncIntervalMs, _health);
        _manifest = new Manifest(_root);
        _schema = new SchemaRegistry(_root, maxFieldsPerMeasurement);
        _shards = new ShardManager(_root, _manifest);
        _tombstones = new TombstoneStore(_root);
        _compactor = new Compactor(_manifest, _shards, _tombstones, _schema, health: _health);
        if (rpCheckIntervalMs > 0) _rpExpiryTimer = new Timer(_ => CleanupExpiredShards(), null, rpCheckIntervalMs, rpCheckIntervalMs);
        if (compactionIntervalMs > 0) _compactionTimer = new Timer(_ => RunCompaction(), null, compactionIntervalMs, compactionIntervalMs);
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
                var seriesKey = SeriesKey.From(replayPoint.Point);
                validPoints.Add(new BufferedPoint(replayPoint.Point, replayPoint.Position, seriesKey));
            }
            catch (FieldConflictException) { result.SchemaConflictsSkipped++; }

            _globalLock.EnterWriteLock();
            try
            {
                var key = K(replayPoint.Db, replayPoint.Rp);
                var lk = GetLock(key, alreadyHoldingGlobalWrite: true);
                lk.EnterWriteLock();
                try
                {
                    if (!_buf.TryGetValue(key, out var list)) { list = []; _buf[key] = list; }
                    AddBufferedPoints(key, list, validPoints); TrackSeriesKeys(replayPoint.Db, validPoints);
                    UpdateBufferReplayFloor(key, list);
                }
                finally { lk.ExitWriteLock(); }
            }
            finally { _globalLock.ExitWriteLock(); }
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
                        var metas = ReadSegmentMetadataCached(segFile).Metas;
                        var pointsForIndex = new List<(string Measurement, string TagsCanonical, Dictionary<string, string> Tags)>();
                        foreach (var m in metas)
                        {
                            // Rebuild series keys
                            _globalLock.EnterWriteLock();
                            try
                            {
                                if (!_seriesKeys.TryGetValue(db, out var keys)) { keys = []; _seriesKeys[db] = keys; }
                                keys.Add(new SeriesKey(m.Measurement, m.TagsCanonical));
                            }
                            finally { _globalLock.ExitWriteLock(); }

                            // Collect for index update
                            var tags = ParseTags(m.TagsCanonical);
                            pointsForIndex.Add((m.Measurement, m.TagsCanonical, tags));

                            // Register schema for each field kind found in segment metadata
                            // SchemaRegistry is idempotent for matching types
                        }
                        _manifest.AddSegmentToShard(db, shardRp, shard.Id, segFile);
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
        if (!_health.WriteAvailable)
            throw new IOException("write path is unavailable after a WAL persistence failure");
        CreateDatabase(db); _manifest.EnsureRp(db, rp);
        var pending = DeduplicateWritePoints(pts);
        var writePoints = pending.Count == pts.Count ? pts : MaterializePendingPoints(pending);
        ValidateSchema(db, writePoints);
        // Use per-db|rp lock only; the global lock is no longer needed for writes because
        // _locks is a ConcurrentDictionary and _seriesKeys/_bufferedPointCount are updated under the per-key lock.
        var key = K(db, rp);
        var lk = GetLock(key);
        lk.EnterWriteLock();
        try
        {
            // Cardinality check inside the write lock to avoid a TOCTOU race.
            CheckCardinalityLocked(db, pending);
            // CheckBufferLimit inside the lock to prevent concurrent writes from exceeding limits.
            CheckBufferLimit(writePoints);
            var walPositions = _wal.Append(db, rp, writePoints);
            if (!_buf.TryGetValue(key, out var list)) { list = []; _buf[key] = list; }
            AddWrittenPoints(db, key, list, pending, walPositions);
            UpdateBufferReplayFloor(key, list);
            if (list.Count >= _threshold) FlushLocked(db, rp, list);
        }
        finally { lk.ExitWriteLock(); }
        return Task.CompletedTask;
    }

    private static List<Point> MaterializePendingPoints(List<PendingPoint> pending)
    {
        var points = new List<Point>(pending.Count);
        foreach (var p in pending)
            points.Add(p.Point);
        return points;
    }

    private void ValidateSchema(string db, List<Point> pts)
    {
        if (pts.Count == 0) return;
        var measurement = pts[0].Measurement;
        for (var i = 1; i < pts.Count; i++)
        {
            if (pts[i].Measurement == measurement) continue;
            foreach (var group in pts.GroupBy(p => p.Measurement))
                _schema.ValidateAndRegister(db, group.Key, group);
            return;
        }

        _schema.ValidateAndRegister(db, measurement, pts);
    }

    private static List<PendingPoint> DeduplicateWritePoints(List<Point> pts)
    {
        if (pts.Count == 0) return [];

        var pending = new List<PendingPoint>(pts.Count);
        var first = pts[0];
        pending.Add(new PendingPoint(first, FastSeriesKey(first)));
        if (pts.Count == 1) return pending;

        var strictlyIncreasingTimestamps = true;
        var lastTimestamp = first.TimestampNs;
        for (var i = 1; i < pts.Count; i++)
        {
            var point = pts[i];
            if (point.TimestampNs <= lastTimestamp) strictlyIncreasingTimestamps = false;
            lastTimestamp = point.TimestampNs;
            pending.Add(new PendingPoint(point, FastSeriesKey(point)));
        }

        if (strictlyIncreasingTimestamps) return pending;

        var map = new Dictionary<(string Meas, string Tags, long Ts), PendingPoint>();
        foreach (var candidate in pending)
        {
            var p = candidate.Point;
            var key = (p.Measurement, candidate.SeriesKey.TagsCanonical, p.TimestampNs);
            if (map.TryGetValue(key, out var existing))
            {
                if (!existing.Cloned)
                {
                    existing.Point = new Point
                    {
                        Measurement = existing.Point.Measurement,
                        Tags = existing.Point.Tags,
                        Fields = new Dictionary<string, FieldValue>(existing.Point.Fields, StringComparer.Ordinal),
                        TimestampNs = existing.Point.TimestampNs,
                        TagsCanonical = existing.Point.TagsCanonical
                    };
                    existing.Cloned = true;
                }
                foreach (var kv in p.Fields) existing.Point.Fields[kv.Key] = kv.Value;
            }
            else
            {
                map[key] = candidate;
            }
        }
        return map.Values.ToList();
    }

    /// <summary>
    /// Fast SeriesKey creation: skip tag sorting when TagsCanonical is already set by the parser.
    /// </summary>
    private static SeriesKey FastSeriesKey(Point p)
    {
        // LineProtocolParser pre-computes TagsCanonical when tags are already sorted.
        // Only fall back to SeriesKey.From (which sorts) when canonical form is not available.
        if (p.TagsCanonical != null)
            return new SeriesKey(p.Measurement, p.TagsCanonical);
        return SeriesKey.From(p);
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
            var key = (p.Measurement, QuerySeriesIdentity(p), p.TimestampNs);
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

    private static string QuerySeriesIdentity(Point point)
    {
        var tags = SeriesKey.From(point).TagsCanonical;
        if (tags.Length == 0
            && point.Fields.TryGetValue("tag", out var legacyTag)
            && legacyTag.Kind == FieldKind.String)
            return $"tag={legacyTag.String}";
        return tags;
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
        var buffered = new List<Point>();
        var lk = GetLock(K(db, rp));
        lk.EnterReadLock();
        try
        {
            var key = K(db, rp);
            if (_buf.TryGetValue(key, out var l))
            {
                var bufMatched = BufferedCandidates(key, l, meas, allowedTagsCanonical)
                    .Where(p => Match(p.Point, meas, min, max))
                    .Select(p => p.Point);
                if (requestedFields != null)
                    bufMatched = bufMatched.Select(p => new Point
                    {
                        Measurement = p.Measurement, Tags = p.Tags, TimestampNs = p.TimestampNs,
                        Fields = SelectFields(p.Fields, requestedFields),
                        TagsCanonical = p.TagsCanonical
                    });
                buffered.AddRange(bufMatched);
            }
        }
        finally { lk.ExitReadLock(); }

        var segments = _shards.ListSegments(db, rp, min, max);

        // Fast path: single segment or small result — read sequentially.
        if (segments.Count <= 1)
        {
            var result = new List<Point>(buffered);
            foreach (var (segPath, _) in segments)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ReadSegmentInto(result, db, segPath, requestedFields, meas, min, max, allowedTagsCanonical, fieldFilters);
            }
            result.AddRange(buffered);
            return [.. DeduplicatePoints(result).OrderBy(x => x.TimestampNs)];
        }

        // Parallel path: multiple segments — read concurrently.
        var segmentResults = new List<Point>[segments.Count];
        var options = new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, 8)
        };
        Parallel.For(0, segments.Count, options, i =>
        {
            var segPath = segments[i].SegPath;
            cancellationToken.ThrowIfCancellationRequested();
            var list = new List<Point>();
            ReadSegmentInto(list, db, segPath, requestedFields, meas, min, max, allowedTagsCanonical, fieldFilters);
            segmentResults[i] = list;
        });

        // Merge results in order
        long totalCount = 0;
        for (var i = 0; i < segmentResults.Length; i++)
            totalCount += segmentResults[i].Count;
        totalCount += buffered.Count;

        var res = new List<Point>((int)Math.Min(totalCount, int.MaxValue));
        for (var i = 0; i < segmentResults.Length; i++)
            res.AddRange(segmentResults[i]);
        res.AddRange(buffered); // Buffer contains the newest writes and must win over flushed segments.
        return [.. DeduplicatePoints(res).OrderBy(x => x.TimestampNs)];
    }

    /// <summary>
    /// Read a single segment into the target list, applying metadata pushdown to skip irrelevant segments.
    /// </summary>
    private void ReadSegmentInto(List<Point> target, string db, string segPath,
        HashSet<string>? requestedFields, string? meas, long? min, long? max,
        HashSet<string>? allowedTagsCanonical, List<FieldFilter>? fieldFilters)
    {
        try
        {
            if (meas != null || (min.HasValue && max.HasValue) || (fieldFilters != null && fieldFilters.Count > 0) || allowedTagsCanonical != null)
            {
                try
                {
                    var metas = ReadSegmentMetadataCached(segPath).Metas;
                    if (meas != null && !metas.Any(m => m.Measurement == meas)) return;
                    if (min.HasValue && !metas.Any(m => m.MaxTime >= min.Value)) return;
                    if (max.HasValue && !metas.Any(m => m.MinTime <= max.Value)) return;
                    if (allowedTagsCanonical != null && !metas.Any(m => allowedTagsCanonical.Contains(m.TagsCanonical))) return;
                    if (fieldFilters != null && fieldFilters.Count > 0 && !CouldSegmentMatchFieldFilters(metas, meas, allowedTagsCanonical, fieldFilters))
                        return;
                }
                catch { /* fall through to full read */ }
            }

            target.AddRange(Rebuild(ReadSegmentColumns(db, segPath, requestedFields, meas, min, max, allowedTagsCanonical), min, max));
        }
        catch (InvalidDataException) { }
        catch (FileNotFoundException) { }
    }

    public bool HasSegments(string db, string rp, long? min, long? max) =>
        _shards.ListSegments(db, rp, min, max).Count > 0;

    public DescendingSeriesReadResult? TryReadBufferedSeriesDescending(string db, string rp, string measurement, string tagsCanonical,
        long? min, long? max, HashSet<string>? requestedFields = null, int? limit = null, CancellationToken cancellationToken = default)
    {
        var key = K(db, rp);
        var seriesKey = new SeriesKey(measurement, tagsCanonical);
        var lk = GetLock(key);
        lk.EnterReadLock();
        try
        {
            if (!_bufBySeries.TryGetValue(key, out var bySeries) || !bySeries.TryGetValue(seriesKey, out var buffered))
                return new DescendingSeriesReadResult([], 0, 0, "buffer-empty");

            for (var i = 1; i < buffered.Count; i++)
                if (buffered[i].Point.TimestampNs < buffered[i - 1].Point.TimestampNs)
                    return null;

            var result = new Dictionary<long, Point>();
            HashSet<long>? cloned = null;
            for (var i = buffered.Count - 1; i >= 0; i--)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var point = buffered[i].Point;
                if (!Match(point, measurement, min, max)) continue;

                if (requestedFields != null)
                {
                    point = new Point
                    {
                        Measurement = point.Measurement,
                        Tags = point.Tags,
                        Fields = SelectFields(point.Fields, requestedFields),
                        TimestampNs = point.TimestampNs,
                        TagsCanonical = point.TagsCanonical
                    };
                }

                if (result.TryGetValue(point.TimestampNs, out var existing))
                {
                    cloned ??= [];
                    if (cloned.Add(point.TimestampNs))
                    {
                        existing = new Point
                        {
                            Measurement = existing.Measurement,
                            Tags = existing.Tags,
                            Fields = new Dictionary<string, FieldValue>(existing.Fields, StringComparer.Ordinal),
                            TimestampNs = existing.TimestampNs,
                            TagsCanonical = existing.TagsCanonical
                        };
                        result[point.TimestampNs] = existing;
                    }

                    foreach (var field in point.Fields)
                        if (!existing.Fields.ContainsKey(field.Key))
                            existing.Fields[field.Key] = field.Value;
                }
                else
                {
                    result[point.TimestampNs] = point;
                }

                if (limit.HasValue && result.Count >= limit.Value)
                    break;
            }

            return new DescendingSeriesReadResult(
                result.Values.ToList(),
                0,
                result.Count,
                limit.HasValue && result.Count >= limit.Value ? "buffer-limit" : "buffer-exhausted");
        }
        finally { lk.ExitReadLock(); }
    }

    public DescendingSeriesReadResult? TryReadSeriesDescending(string db, string rp, string measurement, string tagsCanonical,
        long? min, long? max, HashSet<string>? requestedFields = null, int? limit = null, CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<long, Point>();
        var buffered = TryReadBufferedSeriesDescending(db, rp, measurement, tagsCanonical, min, max, requestedFields, limit, cancellationToken);
        if (buffered == null) return null;
        AddDescendingPoints(result, buffered.Points, limit);
        if (limit.HasValue && result.Count >= limit.Value)
            return new DescendingSeriesReadResult(result.Values.ToList(), 0, result.Count, "buffer-limit");

        var segments = new List<(string Path, long MaxTime)>();
        foreach (var (segPath, _) in _shards.ListSegments(db, rp, min, max))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var maxTime = ReadSegmentMetadataCached(segPath).Metas
                    .Where(m => m.Measurement == measurement && m.TagsCanonical == tagsCanonical
                        && (!min.HasValue || m.MaxTime >= min.Value)
                        && (!max.HasValue || m.MinTime <= max.Value)
                        && (requestedFields == null || requestedFields.Contains(m.Field)))
                    .Select(m => (long?)m.MaxTime)
                    .Max();
                if (maxTime.HasValue) segments.Add((segPath, maxTime.Value));
            }
            catch (InvalidDataException) { }
            catch (FileNotFoundException) { }
        }

        var allowed = new HashSet<string>(StringComparer.Ordinal) { tagsCanonical };
        var segmentColumnsRead = 0;
        foreach (var seg in segments.OrderByDescending(s => s.MaxTime))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var columns = ReadSegmentColumns(db, seg.Path, requestedFields, measurement, min, max, allowed);
                segmentColumnsRead += columns.Count;
                AddSegmentColumnsDescending(
                    result,
                    columns,
                    min,
                    max,
                    limit);
                if (limit.HasValue && result.Count >= limit.Value) break;
            }
            catch (InvalidDataException) { }
            catch (FileNotFoundException) { }
        }

        return new DescendingSeriesReadResult(
            result.Values.ToList(),
            segmentColumnsRead,
            result.Count,
            limit.HasValue && result.Count >= limit.Value ? "segment-limit" : "segments-exhausted");
    }

    public DescendingFieldReadResult? TryReadFlushedFieldDescending(string db, string rp, string measurement, string tagsCanonical,
        string field, long? min, long? max, int? limit = null, CancellationToken cancellationToken = default)
    {
        var key = K(db, rp);
        var seriesKey = new SeriesKey(measurement, tagsCanonical);
        var lk = GetLock(key);
        lk.EnterReadLock();
        try
        {
            if (_bufBySeries.TryGetValue(key, out var bySeries)
                && bySeries.TryGetValue(seriesKey, out var buffered)
                && buffered.Count > 0)
                return null;
        }
        finally { lk.ExitReadLock(); }

        var segments = new List<(string Path, long MaxTime)>();
        foreach (var (segPath, _) in _shards.ListSegments(db, rp, min, max))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var maxTime = ReadSegmentMetadataCached(segPath).Metas
                    .Where(m => m.Measurement == measurement
                        && m.TagsCanonical == tagsCanonical
                        && m.Field == field
                        && (!min.HasValue || m.MaxTime >= min.Value)
                        && (!max.HasValue || m.MinTime <= max.Value))
                    .Select(m => (long?)m.MaxTime)
                    .Max();
                if (maxTime.HasValue) segments.Add((segPath, maxTime.Value));
            }
            catch (InvalidDataException) { }
            catch (FileNotFoundException) { }
        }

        var timestamps = new List<long>(limit ?? 0);
        var values = new List<FieldValue>(limit ?? 0);
        var allowed = new HashSet<string>(StringComparer.Ordinal) { tagsCanonical };
        var fields = new HashSet<string>(StringComparer.Ordinal) { field };
        var segmentColumnsRead = 0;
        foreach (var seg in segments.OrderByDescending(s => s.MaxTime))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var columns = ReadSegmentColumns(db, seg.Path, fields, measurement, min, max, allowed);
                segmentColumnsRead += columns.Count;
                foreach (var column in columns)
                {
                    for (var i = column.Timestamps.Count - 1; i >= 0; i--)
                    {
                        var ts = column.Timestamps[i];
                        if (min.HasValue && ts < min.Value) break;
                        if (max.HasValue && ts > max.Value) continue;
                        timestamps.Add(ts);
                        values.Add(column.Values[i]);
                        if (limit.HasValue && timestamps.Count >= limit.Value)
                            return new DescendingFieldReadResult(timestamps, values, segmentColumnsRead, "segment-limit");
                    }
                }
            }
            catch (InvalidDataException) { }
            catch (FileNotFoundException) { }
        }

        return new DescendingFieldReadResult(timestamps, values, segmentColumnsRead, "segments-exhausted");
    }

    public DescendingFieldsReadResult? TryReadFlushedFieldsDescending(string db, string rp, string measurement, string tagsCanonical,
        IReadOnlyList<string> fields, long? min, long? max, int? limit = null, CancellationToken cancellationToken = default)
    {
        if (fields.Count == 0) return null;
        var key = K(db, rp);
        var seriesKey = new SeriesKey(measurement, tagsCanonical);
        var lk = GetLock(key);
        lk.EnterReadLock();
        try
        {
            if (_bufBySeries.TryGetValue(key, out var bySeries)
                && bySeries.TryGetValue(seriesKey, out var buffered)
                && buffered.Count > 0)
                return null;
        }
        finally { lk.ExitReadLock(); }

        var fieldSet = new HashSet<string>(fields, StringComparer.Ordinal);
        var segments = new List<(string Path, long MaxTime)>();
        foreach (var (segPath, _) in _shards.ListSegments(db, rp, min, max))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var metas = ReadSegmentMetadataCached(segPath).Metas
                    .Where(m => m.Measurement == measurement
                        && m.TagsCanonical == tagsCanonical
                        && fieldSet.Contains(m.Field)
                        && (!min.HasValue || m.MaxTime >= min.Value)
                        && (!max.HasValue || m.MinTime <= max.Value))
                    .ToList();
                if (metas.Count == 0) continue;
                if (metas.Select(m => (m.MinTime, m.MaxTime, m.PointCount)).Distinct().Count() != 1)
                    return null;
                segments.Add((segPath, metas.Max(m => m.MaxTime)));
            }
            catch (InvalidDataException) { }
            catch (FileNotFoundException) { }
        }

        var timestamps = new List<long>(limit ?? 0);
        var rows = new List<FieldValue?[]>(limit ?? 0);
        var rowIndex = new Dictionary<long, int>();
        var allowed = new HashSet<string>(StringComparer.Ordinal) { tagsCanonical };
        var fieldIndexes = fields.Select((field, index) => (field, index)).ToDictionary(x => x.field, x => x.index, StringComparer.Ordinal);
        var segmentColumnsRead = 0;
        foreach (var seg in segments.OrderByDescending(s => s.MaxTime))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var columns = ReadSegmentColumns(db, seg.Path, fieldSet, measurement, min, max, allowed);
                segmentColumnsRead += columns.Count;
                foreach (var column in columns)
                {
                    var fieldIndex = fieldIndexes[column.Field];
                    for (var i = column.Timestamps.Count - 1; i >= 0; i--)
                    {
                        var ts = column.Timestamps[i];
                        if (min.HasValue && ts < min.Value) break;
                        if (max.HasValue && ts > max.Value) continue;
                        if (!rowIndex.TryGetValue(ts, out var index))
                        {
                            if (limit.HasValue && timestamps.Count >= limit.Value) continue;
                            index = timestamps.Count;
                            rowIndex[ts] = index;
                            timestamps.Add(ts);
                            rows.Add(new FieldValue?[fields.Count]);
                        }
                        rows[index][fieldIndex] = column.Values[i];
                    }
                }
                if (limit.HasValue && timestamps.Count >= limit.Value)
                    return new DescendingFieldsReadResult(timestamps, rows, segmentColumnsRead, "segment-limit");
            }
            catch (InvalidDataException) { }
            catch (FileNotFoundException) { }
        }

        return new DescendingFieldsReadResult(timestamps, rows, segmentColumnsRead, "segments-exhausted");
    }

    public IEnumerable<Point> EnumeratePoints(string db, string rp, string? meas, long? min, long? max,
        HashSet<string>? requestedFields = null, HashSet<string>? allowedTagsCanonical = null, List<FieldFilter>? fieldFilters = null,
        CancellationToken cancellationToken = default)
    {
        // ponytail: materialize for correct last-write-wins; use a recency-aware k-way merge if query memory becomes limiting.
        foreach (var point in ReadAllPoints(db, rp, meas, min, max, requestedFields, allowedTagsCanonical, fieldFilters, cancellationToken))
            yield return point;
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

    public IReadOnlyList<string> ListTagKeys(string db, string? m) => _manifest.GetTagKeys(db, m);
    public IReadOnlyList<(string Key, string Value)> ListTagValues(string db, string? m, string key) => _manifest.GetTagValues(db, m, key);
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
return Interlocked.Read(ref _bufferedPointCount);
}

public long GetBufferedByteCount()
{
return Interlocked.Read(ref _bufferedByteCount);
}

    public List<Point> ReadBufferedPoints(string db, string rp, string? meas, long? min, long? max,
        HashSet<string>? requestedFields = null, HashSet<string>? allowedTagsCanonical = null)
    {
        var res = new List<Point>();
        var lk = GetLock(K(db, rp));
        lk.EnterReadLock();
        try
        {
            var key = K(db, rp);
            if (_buf.TryGetValue(key, out var list))
            {
                var matched = BufferedCandidates(key, list, meas, allowedTagsCanonical)
                    .Where(p => Match(p.Point, meas, min, max))
                    .Select(p => p.Point);
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

    public BufferedStatsSnapshot ReadBufferedStats(string db, string rp, string? meas, long? min, long? max,
        HashSet<string>? requestedFields = null, HashSet<string>? allowedTagsCanonical = null)
    {
        var fields = new Dictionary<string, BufferedFieldStats>(StringComparer.Ordinal);
        var matchedPoints = 0;
        long maxTime = 0;
        var lk = GetLock(K(db, rp));
        lk.EnterReadLock();
        try
        {
            var key = K(db, rp);
            if (!_buf.TryGetValue(key, out var list))
                return new BufferedStatsSnapshot(0, 0, fields);

            foreach (var buffered in BufferedCandidates(key, list, meas, allowedTagsCanonical))
            {
                var point = buffered.Point;
                if (!Match(point, meas, min, max))
                    continue;

                matchedPoints++;
                maxTime = Math.Max(maxTime, point.TimestampNs);
                foreach (var (field, value) in point.Fields)
                {
                    if (requestedFields != null && !requestedFields.Contains(field))
                        continue;
                    var number = value.AsDouble();
                    if (!number.HasValue)
                        continue;
                    fields[field] = fields.TryGetValue(field, out var stats)
                        ? stats.Add(number.Value)
                        : BufferedFieldStats.Single(number.Value);
                }
            }
        }
        finally { lk.ExitReadLock(); }
        return new BufferedStatsSnapshot(matchedPoints, maxTime, fields);
    }

    public List<SegmentColumnMeta> ReadSegmentMetadata(string db, string rp, string? meas, long? min, long? max,
        HashSet<string>? requestedFields = null, HashSet<string>? allowedTagsCanonical = null, CancellationToken cancellationToken = default)
    {
        return ReadSegmentMetadataWithStats(db, rp, meas, min, max, requestedFields, allowedTagsCanonical, cancellationToken).Metas;
    }

    public SegmentMetadataQueryResult ReadSegmentMetadataWithStats(string db, string rp, string? meas, long? min, long? max,
        HashSet<string>? requestedFields = null, HashSet<string>? allowedTagsCanonical = null, CancellationToken cancellationToken = default)
    {
        var reads = new System.Collections.Concurrent.ConcurrentBag<(List<SegmentColumnMeta> Metas, bool UsedFooter)>();
        Parallel.ForEach(
            _shards.ListSegments(db, rp, min, max),
            new ParallelOptions { CancellationToken = cancellationToken, MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, 8) },
            segment =>
            {
                try
                {
                    reads.Add(ReadSegmentMetadataCached(segment.SegPath));
                }
                catch (InvalidDataException) { }
                catch (FileNotFoundException) { }
            });

        var result = new List<SegmentColumnMeta>();
        var footerHits = 0;
        var fullReads = 0;
        foreach (var metadata in reads)
        {
            if (metadata.UsedFooter) footerHits++;
            else fullReads++;
            result.AddRange(metadata.Metas
                .Where(m => (meas == null || m.Measurement == meas)
                    && (!min.HasValue || m.MaxTime >= min.Value)
                    && (!max.HasValue || m.MinTime <= max.Value)
                    && (requestedFields == null || requestedFields.Contains(m.Field))
                    && (allowedTagsCanonical == null || allowedTagsCanonical.Contains(m.TagsCanonical))));
        }
        return new SegmentMetadataQueryResult(result, footerHits, fullReads);
    }

    public sealed record PointCountResult(Dictionary<string, long> FieldCounts, long MaxTimestampNs, int ScannedPoints);

    /// <summary>
    /// Count non-null field values per field using timestamp-only reads (skips value block decoding).
    /// This is a fast fallback when the metadata-based aggregate pushdown fails due to overlapping
    /// segments or missing stats. It avoids the expensive field-value decoding that ReadAllPoints performs.
    /// </summary>
    public PointCountResult? CountPointsByField(
        string db, string rp, string? measurement, long? min, long? max,
        HashSet<string>? requestedFields, HashSet<string>? allowedTagsCanonical,
        CancellationToken cancellationToken)
    {
        var hasTombstones = _tombstones.HasTombstones(db);
        var segments = _shards.ListSegments(db, rp, min, max);

        // Read timestamps from segments in parallel; value blocks are skipped.
        var bag = new System.Collections.Concurrent.ConcurrentBag<List<SegmentTimestampColumn>>();

        if (segments.Count > 0)
        {
            Parallel.ForEach(segments, new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, 8)
            }, segment =>
            {
                try
                {
                    // Use cached metadata to skip segments that have no matching columns.
                    var metas = ReadSegmentMetadataCached(segment.SegPath).Metas;
                    var hasMatch = metas.Any(m =>
                        (measurement == null || m.Measurement == measurement)
                        && (!min.HasValue || m.MaxTime >= min.Value)
                        && (!max.HasValue || m.MinTime <= max.Value)
                        && (requestedFields == null || requestedFields.Contains(m.Field))
                        && (allowedTagsCanonical == null || allowedTagsCanonical.Contains(m.TagsCanonical)));

                    if (!hasMatch) return;

                    var tsColumns = SegmentReader.ReadSegmentTimestampsOnly(
                        segment.SegPath, requestedFields, measurement, min, max, allowedTagsCanonical);
                    bag.Add(tsColumns);
                }
                catch (InvalidDataException) { }
                catch (FileNotFoundException) { }
            });
        }

        // Merge timestamps per (measurement, tags, field) using HashSet for last-write-wins deduplication.
        var fieldTsMap = new Dictionary<(string Measurement, string Tags, string Field), HashSet<long>>();
        var maxTime = 0L;
        var scannedPoints = 0;

        foreach (var columns in bag)
        {
            foreach (var col in columns)
            {
                // Apply tombstone filtering.
                var timestamps = col.Timestamps;
                if (hasTombstones)
                {
                    if (_tombstones.IsColumnDeleted(db, col.Measurement, col.TagsCanonical, col.MinTime, col.MaxTime))
                        continue;
                    timestamps = _tombstones.FilterTimestampsDeleted(db, col.Measurement, col.TagsCanonical, col.Timestamps);
                }

                if (timestamps.Count == 0) continue;

                var key = (col.Measurement, col.TagsCanonical, col.Field);
                if (!fieldTsMap.TryGetValue(key, out var tsSet))
                {
                    tsSet = new HashSet<long>();
                    fieldTsMap[key] = tsSet;
                }

                foreach (var ts in timestamps)
                {
                    tsSet.Add(ts);
                    if (ts > maxTime) maxTime = ts;
                }

                scannedPoints += timestamps.Count;
            }
        }

        // Add buffered points (newest writes that haven't been flushed to segments yet).
        var bufferPoints = ReadBufferedPoints(db, rp, measurement, min, max, requestedFields, allowedTagsCanonical);
        foreach (var p in bufferPoints)
        {
            var tags = SeriesKey.From(p).TagsCanonical;
            if (p.TimestampNs > maxTime) maxTime = p.TimestampNs;

            foreach (var field in p.Fields.Keys)
            {
                if (requestedFields != null && !requestedFields.Contains(field))
                    continue;
                var key = (p.Measurement, tags, field);
                if (!fieldTsMap.TryGetValue(key, out var tsSet))
                {
                    tsSet = new HashSet<long>();
                    fieldTsMap[key] = tsSet;
                }
                tsSet.Add(p.TimestampNs);
            }
            scannedPoints++;
        }

        // Aggregate unique timestamp counts per field.
        var result = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var group in fieldTsMap.GroupBy(kv => kv.Key.Field, StringComparer.Ordinal))
            result[group.Key] = group.Sum(kv => kv.Value.Count);

        if (result.Count == 0) return null;

        return new PointCountResult(result, maxTime, scannedPoints);
    }

    public CompactionStatsSnapshot GetCompactionStats() => _compactor.GetStats();
    public StorageHealth Health => _health;
    public int CompactNow() => _compactor.CompactAll();

    private void RunCompaction()
    {
        try { _compactor.CompactAll(); }
        catch (Exception ex) { _health.RecordFailure("compaction", ex); }
    }

    public void DropDatabase(string db)
    {
        FlushDatabase(db);
        var dbDir = Path.Combine(_root, "db", db);
        if (Directory.Exists(dbDir)) try { Directory.Delete(dbDir, true); } catch { }
        _manifest.DropDatabase(db); _manifest.SaveIfDirty(); _tombstones.DropDatabase(db);
        _globalLock.EnterWriteLock();
        try { foreach (var k in _buf.Keys.Where(k => k.StartsWith(db + "|")).ToList()) { if (_buf[k].Count > 0) { _bufferedPointCount -= _buf[k].Count; } _buf.TryRemove(k, out _); _bufBySeries.TryRemove(k, out _); _locks.TryRemove(k, out var lk); lk?.Dispose(); _bufferReplayFloors.TryRemove(k, out _); } _seriesKeys.TryRemove(db, out _); if (_maxBufferBytes > 0) RecalculateBufferedBytes(); }
        finally { _globalLock.ExitWriteLock(); }
    }

    public void DropMeasurement(string db, string measurement)
    {
        _tombstones.AddMeasurementDelete(db, measurement);
        _manifest.RemoveMeasurementIndex(db, measurement);
        foreach (var rp in _manifest.ListRetentionPolicies(db).Select(r => r.Name).DefaultIfEmpty("autogen"))
        {
            var key = K(db, rp);
            _globalLock.EnterWriteLock();
            try
            {
                var lk = GetLock(key, alreadyHoldingGlobalWrite: true);
                lk.EnterWriteLock();
                try
                {
                    if (_buf.TryGetValue(key, out var list))
                    {
                        var removed = list.RemoveAll(p => p.Point.Measurement == measurement);
                        if (removed > 0) { _bufferedPointCount -= removed; if (_maxBufferBytes > 0) RecalculateBufferedBytes(); }
                        RebuildBufferSeriesIndex(key, list);
                        UpdateBufferReplayFloor(key, list, forceRecalculate: true);
                    }
                }
                finally { lk.ExitWriteLock(); }
            }
            finally { _globalLock.ExitWriteLock(); }
        }
    }

    public void DeleteFromMeasurement(string db, string measurement, long? minTime, long? maxTime)
    {
        _tombstones.AddMeasurementDelete(db, measurement, minTime, maxTime);
        foreach (var rp in _manifest.ListRetentionPolicies(db).Select(r => r.Name).DefaultIfEmpty("autogen"))
            DeleteBuffered(db, rp, measurement, minTime, maxTime, _ => true);
    }

    public void DeleteFromMeasurement(string db, string rp, string measurement, long? minTime, long? maxTime)
    {
        _tombstones.AddMeasurementDelete(db, measurement, minTime, maxTime);
        DeleteBuffered(db, rp, measurement, minTime, maxTime, _ => true);
    }

    public void DeleteFromMeasurement(string db, string measurement, long? minTime, long? maxTime, Predicate<Point> predicate)
    {
        foreach (var rp in _manifest.ListRetentionPolicies(db).Select(r => r.Name).DefaultIfEmpty("autogen"))
        {
            var matches = ReadAllPoints(db, rp, measurement, minTime, maxTime)
                .Where(p => predicate(p))
                .GroupBy(p => SeriesKey.From(p).TagsCanonical);

            foreach (var group in matches)
            {
                _tombstones.AddSeriesDeletes(db, measurement, group.Select(point => (group.Key, (long?)point.TimestampNs, (long?)point.TimestampNs)));
            }

            DeleteBuffered(db, rp, measurement, minTime, maxTime, predicate);
        }
    }

    public void DeleteFromMeasurement(string db, string rp, string measurement, long? minTime, long? maxTime, Predicate<Point> predicate)
    {
        var matches = ReadAllPoints(db, rp, measurement, minTime, maxTime)
            .Where(p => predicate(p))
            .GroupBy(p => SeriesKey.From(p).TagsCanonical);

        foreach (var group in matches)
        {
            _tombstones.AddSeriesDeletes(db, measurement, group.Select(point => (group.Key, (long?)point.TimestampNs, (long?)point.TimestampNs)));
        }

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
        _tombstones.AddSeriesDeletes(db, measurement, tagSet.Select(tags => (tags, (long?)null, (long?)null)));
        _manifest.RemoveSeriesIndex(db, measurement, tagSet);

        foreach (var rp in _manifest.ListRetentionPolicies(db).Select(r => r.Name).DefaultIfEmpty("autogen"))
        {
            var key = K(db, rp);
            _globalLock.EnterWriteLock();
            try
            {
                var lk = GetLock(key, alreadyHoldingGlobalWrite: true);
                lk.EnterWriteLock();
                try
                {
                    if (_buf.TryGetValue(key, out var list))
                    {
                        var removed = list.RemoveAll(p => p.Point.Measurement == measurement && tagSet.Contains(p.SeriesKey.TagsCanonical));
                        if (removed > 0) { _bufferedPointCount -= removed; if (_maxBufferBytes > 0) RecalculateBufferedBytes(); }
                        RebuildBufferSeriesIndex(key, list);
                        UpdateBufferReplayFloor(key, list, forceRecalculate: true);
                    }
                }
                finally { lk.ExitWriteLock(); }
            }
            finally { _globalLock.ExitWriteLock(); }
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
        var key = K(db, rp);
        _globalLock.EnterWriteLock();
        try
        {
            var lk = GetLock(key, alreadyHoldingGlobalWrite: true);
            lk.EnterWriteLock();
            try
            {
                if (_buf.TryGetValue(key, out var list))
                {
                    var removed = list.RemoveAll(p => p.Point.Measurement == measurement
                        && (!minTime.HasValue || p.Point.TimestampNs >= minTime.Value)
                        && (!maxTime.HasValue || p.Point.TimestampNs <= maxTime.Value)
                        && predicate(p.Point));
                    if (removed > 0) { _bufferedPointCount -= removed; if (_maxBufferBytes > 0) RecalculateBufferedBytes(); }
                    RebuildBufferSeriesIndex(key, list);
                    UpdateBufferReplayFloor(key, list, forceRecalculate: true);
                }
            }
            finally { lk.ExitWriteLock(); }
        }
        finally { _globalLock.ExitWriteLock(); }
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

    private static void AddDescendingPoints(Dictionary<long, Point> result, IEnumerable<Point> points, int? limit)
    {
        foreach (var point in points)
        {
            if (result.TryGetValue(point.TimestampNs, out var existing))
            {
                foreach (var field in point.Fields)
                    if (!existing.Fields.ContainsKey(field.Key))
                        existing.Fields[field.Key] = field.Value;
            }
            else
            {
                result[point.TimestampNs] = point;
                if (limit.HasValue && result.Count >= limit.Value) break;
            }
        }
    }

    private static void AddSegmentColumnsDescending(Dictionary<long, Point> result, List<SegmentColumn> columns, long? min, long? max, int? limit)
    {
        foreach (var column in columns)
        {
            var tags = ParseTags(column.TagsCanonical);
            for (var i = column.Timestamps.Count - 1; i >= 0; i--)
            {
                var ts = column.Timestamps[i];
                if (min.HasValue && ts < min.Value) break;
                if (max.HasValue && ts > max.Value) continue;
                if (!result.TryGetValue(ts, out var point))
                {
                    if (limit.HasValue && result.Count >= limit.Value) continue;
                    point = new Point
                    {
                        Measurement = column.Measurement,
                        Tags = tags,
                        TimestampNs = ts,
                        Fields = new Dictionary<string, FieldValue>(StringComparer.Ordinal)
                    };
                    result[ts] = point;
                }
                // Buffer data is always newer than flushed segment data, so never overwrite existing fields.
                if (!point.Fields.ContainsKey(column.Field))
                    point.Fields[column.Field] = column.Values[i];
            }
        }
    }

    private static Dictionary<string, string> ParseTags(string s)
    { var d = new Dictionary<string, string>(StringComparer.Ordinal); if (string.IsNullOrEmpty(s)) return d; foreach (var p in s.Split(',')) { var i = p.IndexOf('='); if (i > 0) d[p[..i]] = p[(i + 1)..]; } return d; }

    private List<SegmentColumn> ReadSegmentColumns(string db, string segPath, HashSet<string>? requestedFields, string? meas, long? min, long? max, HashSet<string>? allowedTagsCanonical)
    {
        var cols = SegmentReader.ReadSegment(segPath, requestedFields, meas, min, max, allowedTagsCanonical);
        var filtered = new List<SegmentColumn>(cols.Count);
        foreach (var col in cols)
        {
            if (_tombstones.IsColumnDeleted(db, col.Measurement, col.TagsCanonical, col.MinTime, col.MaxTime)) continue;
            var (ts, vals) = _tombstones.FilterColumnDeleted(db, col.Measurement, col.TagsCanonical, col.Timestamps, col.Values);
            if (ts.Count > 0) filtered.Add(new SegmentColumn(col.Measurement, col.TagsCanonical, col.Field, col.Kind, ts[0], ts[^1], ts, vals, col.Stats));
        }
        return filtered;
    }

    private (List<SegmentColumnMeta> Metas, bool UsedFooter) ReadSegmentMetadataCached(string path)
    {
        var info = new FileInfo(path);
        var key = info.FullName;
        var lastWriteUtc = info.LastWriteTimeUtc;
        var length = info.Length;

        if (_segmentMetadataCache.TryGetValue(key, out var cached)
            && cached.Length == length
            && cached.LastWriteUtc == lastWriteUtc)
            return (cached.Metas.ToList(), cached.UsedFooter);

        var read = SegmentReader.ReadMetadataWithInfo(path);
        _segmentMetadataCache[key] = (length, lastWriteUtc, read.Metadata, read.UsedFooter);
        return (read.Metadata.ToList(), read.UsedFooter);
    }

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

    private void FlushLocked(string db, string rp, List<BufferedPoint> l, bool updateCheckpoint = true)
    {
        if (l.Count == 0) return;
        var flushCount = l.Count;
        long flushBytes = 0;
        var byShard = new Dictionary<int, List<(Point Point, SeriesKey SeriesKey)>>();
        foreach (var buffered in l)
        {
            if (_maxBufferBytes > 0)
                flushBytes += EstimateBufferedPointBytes(buffered.Point);
            var (shardId, _) = _shards.GetOrCreateShard(db, rp, buffered.Point.TimestampNs);
            if (!byShard.TryGetValue(shardId, out var points))
            {
                points = [];
                byShard[shardId] = points;
            }
            points.Add((buffered.Point, buffered.SeriesKey));
        }

        foreach (var (shardId, points) in byShard)
        {
            var shardDir = _shards.ShardDir(db, rp, shardId);
            var segPath = Path.Combine(shardDir, $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}.seg");
            SegmentWriter.WriteSegment(segPath, points);
            _shards.RegisterSegment(db, rp, shardId, segPath);
        }
        _bufferedPointCount -= flushCount;
        if (_maxBufferBytes > 0)
            _bufferedByteCount -= flushBytes;
        l.Clear();
        var key = K(db, rp);
        _bufBySeries.TryRemove(key, out _);
        UpdateBufferReplayFloor(key, l);
        if (updateCheckpoint)
            UpdateWalCheckpoint();
    }

    private void FlushDatabase(string db)
    {
        var prefix = db + "|";
        foreach (var kv in _buf.Where(kv => kv.Key.StartsWith(prefix)).ToList())
        {
            var p = kv.Key.Split('|');
            var lk = GetLock(kv.Key);
            lk.EnterWriteLock();
            try { FlushLocked(p[0], p[1], kv.Value, updateCheckpoint: false); }
            finally { lk.ExitWriteLock(); }
        }
        UpdateWalCheckpoint();
    }

    public void FlushAll()
    {
        foreach (var kv in _buf.ToArray())
        {
            var p = kv.Key.Split('|');
            var lk = GetLock(kv.Key);
            lk.EnterWriteLock();
            try { FlushLocked(p[0], p[1], kv.Value, updateCheckpoint: false); }
            finally { lk.ExitWriteLock(); }
        }
        UpdateWalCheckpoint();
        _schema.SaveIfDirty();
        _manifest.SaveIfDirty();
    }

    /// <summary>
    /// Check series cardinality limit. Caller must hold the per-key write lock.
    /// _seriesKeys is a ConcurrentDictionary; per-db HashSet access is serialized by the per-key lock.
    /// </summary>
    private void CheckCardinalityLocked(string db, List<PendingPoint> pts)
    {
        if (!_seriesKeys.TryGetValue(db, out var existing))
            existing = [];
        var seen = new HashSet<SeriesKey>();
        var newSeries = 0;
        foreach (var p in pts)
            if (seen.Add(p.SeriesKey) && !existing.Contains(p.SeriesKey))
                newSeries++;
        var total = existing.Count + newSeries;
        if (total > _maxSeriesPerDb) throw new CardinalityLimitExceededException($"series cardinality limit exceeded for database '{db}': {total} > {_maxSeriesPerDb}");
    }

    private void CheckBufferLimit(List<Point> incomingPoints)
    {
        if (_maxBufferPoints > 0)
        {
            var bufferedPoints = GetBufferedPointCountInternal();
            if (bufferedPoints + incomingPoints.Count > _maxBufferPoints)
                throw new MemoryLimitExceededException($"memory buffer point limit exceeded: {bufferedPoints + incomingPoints.Count} > {_maxBufferPoints}");
        }

        if (_maxBufferBytes > 0)
        {
            var bufferedBytes = GetBufferedByteCountInternal();
            var incomingBytes = incomingPoints.Sum(EstimateBufferedPointBytes);
            if (bufferedBytes + incomingBytes > _maxBufferBytes)
                throw new MemoryLimitExceededException($"memory buffer byte limit exceeded: {bufferedBytes + incomingBytes} > {_maxBufferBytes}");
        }
    }

    private long GetBufferedPointCountInternal()
    {
        // Caller is expected to hold _globalLock (read or write). Counter maintained incrementally.
        return _bufferedPointCount;
    }

    private long GetBufferedByteCountInternal()
    {
        // Caller is expected to hold _globalLock (read or write). Counter maintained incrementally.
        return _bufferedByteCount;
    }

    private void RecalculateBufferedBytes()
    {
        // Called only on delete/drop paths (infrequent). Rebuilds the byte counter from scratch.
        long bytes = 0;
        foreach (var kv in _buf)
            foreach (var p in kv.Value)
                bytes += EstimateBufferedPointBytes(p.Point);
        _bufferedByteCount = bytes;
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

    private static Dictionary<string, FieldValue> SelectFields(Dictionary<string, FieldValue> fields, HashSet<string> requestedFields)
    {
        var selected = new Dictionary<string, FieldValue>(Math.Min(fields.Count, requestedFields.Count), StringComparer.Ordinal);
        foreach (var key in requestedFields)
            if (fields.TryGetValue(key, out var value))
                selected[key] = value;
        return selected;
    }

    private void AddBufferedPoints(string key, List<BufferedPoint> list, List<BufferedPoint> points)
    {
        list.AddRange(points);
        if (!_bufBySeries.TryGetValue(key, out var bySeries))
        {
            bySeries = new();
            _bufBySeries[key] = bySeries;
        }

        foreach (var point in points)
        {
            if (!bySeries.TryGetValue(point.SeriesKey, out var seriesPoints))
            {
                seriesPoints = [];
                bySeries[point.SeriesKey] = seriesPoints;
            }
            seriesPoints.Add(point);
        }

        _bufferedPointCount += points.Count;
        if (_maxBufferBytes > 0)
            foreach (var point in points)
                _bufferedByteCount += EstimateBufferedPointBytes(point.Point);
    }

    private void AddWrittenPoints(string db, string key, List<BufferedPoint> list, List<PendingPoint> points, IReadOnlyList<WalPosition> positions)
    {
        if (!_bufBySeries.TryGetValue(key, out var bySeries))
        {
            bySeries = new();
            _bufBySeries[key] = bySeries;
        }

        list.EnsureCapacity(list.Count + points.Count);
        var seenSeries = new HashSet<SeriesKey>();
        var indexPoints = new List<(string Measurement, string TagsCanonical, Dictionary<string, string> Tags)>();
        for (var i = 0; i < points.Count; i++)
        {
            var pending = points[i];
            var buffered = new BufferedPoint(pending.Point, positions[i], pending.SeriesKey);
            list.Add(buffered);
            if (!bySeries.TryGetValue(pending.SeriesKey, out var seriesPoints))
            {
                seriesPoints = [];
                bySeries[pending.SeriesKey] = seriesPoints;
            }
            seriesPoints.Add(buffered);

            if (seenSeries.Add(pending.SeriesKey))
                indexPoints.Add((pending.Point.Measurement, pending.SeriesKey.TagsCanonical, pending.Point.Tags));
        }

        if (!_seriesKeys.TryGetValue(db, out var keys)) { keys = []; _seriesKeys[db] = keys; }
        foreach (var series in seenSeries)
            keys.Add(series);

        _manifest.UpdateIndexes(db, indexPoints);

        _bufferedPointCount += points.Count;
        if (_maxBufferBytes > 0)
            for (var i = 0; i < points.Count; i++)
                _bufferedByteCount += EstimateBufferedPointBytes(points[i].Point);
    }

    private IEnumerable<BufferedPoint> BufferedCandidates(string key, List<BufferedPoint> list, string? measurement, HashSet<string>? allowedTagsCanonical)
    {
        if (measurement == null || allowedTagsCanonical == null || allowedTagsCanonical.Count == 0)
            return list;
        if (!_bufBySeries.TryGetValue(key, out var bySeries))
            return list;

        var candidates = new List<BufferedPoint>();
        foreach (var tags in allowedTagsCanonical)
            if (bySeries.TryGetValue(new SeriesKey(measurement, tags), out var points))
                candidates.AddRange(points);
        return candidates;
    }

    private void RebuildBufferSeriesIndex(string key, List<BufferedPoint> list)
    {
        if (list.Count == 0)
        {
            _bufBySeries.TryRemove(key, out _);
            return;
        }

        var bySeries = new Dictionary<SeriesKey, List<BufferedPoint>>();
        foreach (var point in list)
        {
            if (!bySeries.TryGetValue(point.SeriesKey, out var points))
            {
                points = [];
                bySeries[point.SeriesKey] = points;
            }
            points.Add(point);
        }
        _bufBySeries[key] = bySeries;
    }

    private void TrackSeriesKeys(string db, List<BufferedPoint> pts)
    {
        if (!_seriesKeys.TryGetValue(db, out var keys)) { keys = []; _seriesKeys[db] = keys; }
        var seen = new HashSet<SeriesKey>();
        foreach (var p in pts)
            if (seen.Add(p.SeriesKey))
                keys.Add(p.SeriesKey);
    }

    private void UpdateBufferReplayFloor(string key, List<BufferedPoint> list, bool forceRecalculate = false)
    {
        // _bufferReplayFloors is a ConcurrentDictionary; caller holds per-key write lock.
        if (list.Count == 0) _bufferReplayFloors.TryRemove(key, out _);
        else if (forceRecalculate || !_bufferReplayFloors.ContainsKey(key))
            _bufferReplayFloors[key] = FindReplayFloor(list);
    }

    private static WalPosition FindReplayFloor(List<BufferedPoint> list)
    {
        var floor = list[0].Position;
        for (var i = 1; i < list.Count; i++)
        {
            var position = list[i].Position;
            if (position.FileId < floor.FileId || (position.FileId == floor.FileId && position.Offset < floor.Offset))
                floor = position;
        }
        return floor;
    }

    private void UpdateWalCheckpoint()
    {
        // _bufferReplayFloors is a ConcurrentDictionary; snapshot values safely.
        var snapshot = _bufferReplayFloors.ToArray();
        if (snapshot.Length == 0)
        {
            _wal.Checkpoint(_wal.CurrentPosition);
            return;
        }
        var min = snapshot[0].Value;
        for (var i = 1; i < snapshot.Length; i++)
        {
            var pos = snapshot[i].Value;
            if (pos.FileId < min.FileId || (pos.FileId == min.FileId && pos.Offset < min.Offset))
                min = pos;
        }
        _wal.Checkpoint(min);
    }

    public int GetSeriesCardinality(string db) => _seriesKeys.TryGetValue(db, out var keys) ? keys.Count : 0;

private ReaderWriterLockSlim GetLock(string key, bool alreadyHoldingGlobalWrite = false)
    {
        // ConcurrentDictionary.GetOrAdd is lock-free for existing keys; the factory is only called
        // for new keys and ConcurrentDictionary guarantees exactly one instance per key.
        return _locks.GetOrAdd(key, _ => new ReaderWriterLockSlim());
    }

    private void CleanupExpiredShards()
    {
        try { _shards.CleanupExpiredShards(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000); }
        catch (Exception ex) { _health.RecordFailure("retention_cleanup", ex); }
    }

    private static string K(string db, string rp) => db + "|" + rp;

    public void Dispose()
    {
        _rpExpiryTimer?.Dispose(); _compactionTimer?.Dispose(); _flushTimer?.Dispose(); FlushAll(); _schema.SaveIfDirty(); _manifest.SaveIfDirty(); _wal.Dispose(); _globalLock.Dispose();
        foreach (var lk in _locks.Values) lk.Dispose();
    }

    private void PeriodicFlush()
    {
        try
        {
            foreach (var kv in _buf.ToArray())
            {
                if (kv.Value.Count == 0) continue;
                var p = kv.Key.Split('|');
                var lk = GetLock(kv.Key);
                lk.EnterWriteLock();
                try { FlushLocked(p[0], p[1], kv.Value, updateCheckpoint: false); }
                finally { lk.ExitWriteLock(); }
            }
            UpdateWalCheckpoint();
            _schema.SaveIfDirty();
            _manifest.SaveIfDirty();
        }
        catch (Exception ex) { _health.RecordFailure("periodic_flush", ex, blocksWrites: true); }
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

public readonly record struct BufferedStatsSnapshot(int MatchedPointCount, long MaxTime, IReadOnlyDictionary<string, BufferedFieldStats> Fields);

public readonly record struct BufferedFieldStats(int Count, double Sum, double Min, double Max)
{
    public static BufferedFieldStats Single(double value) => new(1, value, value, value);
    public BufferedFieldStats Add(double value) => new(Count + 1, Sum + value, Math.Min(Min, value), Math.Max(Max, value));
}

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

    private sealed record IndexedSegmentMetadata(List<SegmentColumnMeta> Metas, bool UsedFooter);

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
    // ponytail: global concurrency gate for slow read paths. Without it, N concurrent slow queries each
    // spawn up to 8 reader threads, exhausting the thread pool so *every* query times out together.
    private readonly SemaphoreSlim _queryGate;
    // ponytail: per-query materialization budget. ReadAllPoints enforces this *while* reading segments
    // (not just after, as the QueryExecutor layer does) so a huge LIMIT-less query cannot blow up the
    // process heap before the executor's post-hoc memory check ever runs.
    private readonly long _maxQueryMemoryBytes;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, (long Length, DateTime LastWriteUtc, List<SegmentColumnMeta> Metas, bool UsedFooter)> _segmentMetadataCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(string DbRp, string Measurement, string Tags), System.Collections.Concurrent.ConcurrentDictionary<string, IndexedSegmentMetadata>> _segmentMetadataBySeries = new();
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _segmentMetadataIndexReady = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, List<BufferedPoint>> _buf = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, Dictionary<SeriesKey, List<BufferedPoint>>> _bufBySeries = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, WalPosition> _bufferReplayFloors = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _lastBufferWriteTicks = new(StringComparer.Ordinal);
    private readonly int _threshold;
    private readonly long _flushColdTicks;
    private readonly long _maxSegmentFileBytes;
    private readonly long _minSegmentFileBytes;
    private readonly double _segmentFillRatio;
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
        int flushIntervalMs = 5000, long maxBufferPoints = 1_000_000, long maxBufferBytes = 0, int compactionIntervalMs = 30000,
        int flushColdDurationMs = 600_000, long compactionTargetBytes = 512L * 1024 * 1024,
        int maxConcurrentQueries = 0,
        long maxQueryMemoryBytes = 0,
        long maxSegmentFileBytes = 0,
        long minSegmentFileBytes = 0,
        double segmentFillRatio = 0.5)
    {
        _root = rootPath; _threshold = flushThreshold; _maxSeriesPerDb = maxSeriesPerDb; _maxBufferPoints = maxBufferPoints; _maxBufferBytes = maxBufferBytes;
        _queryGate = new SemaphoreSlim(maxConcurrentQueries > 0 ? maxConcurrentQueries : Math.Min(Environment.ProcessorCount, 8), int.MaxValue);
        _maxQueryMemoryBytes = maxQueryMemoryBytes > 0 ? maxQueryMemoryBytes : 512L * 1024 * 1024;
        _maxSegmentFileBytes = maxSegmentFileBytes > 0 ? maxSegmentFileBytes : 512L * 1024 * 1024;
        _minSegmentFileBytes = minSegmentFileBytes > 0 ? minSegmentFileBytes : 0;
        // ponytail: tail-merge ratio. When the remaining pending points fit under
        // (maxSegmentFileBytes * segmentFillRatio), they are merged into the current file instead of
        // being split off into a tiny trailing .seg. This caps the file count on HDD/network storage
        // without forcing exact size alignment. Clamped to (0,1]; 1 means "always split at the cap"
        // (old strict behavior).
        _segmentFillRatio = segmentFillRatio is > 0 and <= 1 ? segmentFillRatio : 0.5;
        _flushColdTicks = TimeSpan.FromMilliseconds(Math.Max(0, flushColdDurationMs)).Ticks;
        Directory.CreateDirectory(_root);
        _wal = new WalManager(Path.Combine(_root, "wal"), maxWalFileBytes, walFsync, walFsyncIntervalMs, _health);
        _manifest = new Manifest(_root);
        _schema = new SchemaRegistry(_root, maxFieldsPerMeasurement);
        _shards = new ShardManager(_root, _manifest);
        _tombstones = new TombstoneStore(_root);
        _compactor = new Compactor(_manifest, _shards, _tombstones, _schema,
            maxL0Segments: 6, maxL1Segments: 3,                    // 更积极的segment数量阈值
            maxL0Bytes: compactionTargetBytes, maxL1Bytes: compactionTargetBytes,
            minFilesPerCompaction: 2, maxPassesPerRun: 12,        // 更多合并轮次
            health: _health,
            maxSegmentFileBytes: _maxSegmentFileBytes,
            segmentFillRatio: _segmentFillRatio);
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
                    _lastBufferWriteTicks[key] = DateTime.UtcNow.Ticks;
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
                        var metas = ReadSegmentMetadataCached(segFile, db, shardRp).Metas;
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

            foreach (var rp in _manifest.ListRetentionPolicies(db))
                _segmentMetadataIndexReady[K(db, rp.Name)] = 0;
        }

        return result;
    }

    public Task WriteAsync(string db, string rp, List<Point> pts) => WriteInternalAsync(db, rp, pts);

    public Task WriteInternalAsync(string db, string rp, List<Point> pts)
    {
        if (!_health.WriteAvailable)
            throw new IOException("write path is unavailable after a WAL persistence failure");
        CreateDatabase(db); _manifest.EnsureRp(db, rp);
        
        // 提前过滤掉重复点，减少不必要的处理
        var pending = DeduplicateWritePoints(pts);
        if (pending.Count == 0) return Task.CompletedTask;
        
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
            
            // 优化的写入批处理：更大的写入批次减少WAL开销
            var walPositions = _wal.Append(db, rp, writePoints);
            if (!_buf.TryGetValue(key, out var list)) { list = []; _buf[key] = list; }
            
            // 批量添加写入点，减少锁持有时间内的操作
            AddWrittenPoints(db, key, list, pending, walPositions);
            _lastBufferWriteTicks[key] = DateTime.UtcNow.Ticks;
            UpdateBufferReplayFloor(key, list);
            
            // 基于大小而不仅仅是计数的flush触发器
            if (list.Count >= _threshold || 
                (_maxBufferBytes > 0 && list.Sum(p => EstimateBufferedPointBytes(p.Point)) >= _maxBufferBytes * 0.8))
            {
                FlushLocked(db, rp, list);
            }
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

    public void CreateDatabaseWithRp(string db, long durationNs, long shardDurationNs, int replication, string rpName)
    {
        if (_manifest.HasDatabase(db)) return;
        _manifest.EnsureDatabase(db);
        _manifest.EnsureRpWithDuration(db, rpName, durationNs, shardDurationNs, replication, isDefault: true);
        Directory.CreateDirectory(Path.Combine(_root, "db", db, rpName));
    }

    public IReadOnlyList<string> ListDatabases() => _manifest.ListDatabases();

    public string GetDefaultRpName(string db) => _manifest.GetDefaultRp(db).Name;
    public IReadOnlyList<string> ListSeries(string db, string? measurement) => _manifest.GetSeries(db, measurement);
    public int GetMeasurementCardinality(string db) => _manifest.ListMeasurements(db).Count;
    public int GetTagValueCardinality(string db, string? measurement, string? tagKey) => _manifest.GetTagValueCardinality(db, measurement, tagKey);

    /// <summary>
    /// Deduplicate and merge buffered + segment points by (measurement, seriesIdentity, timestamp),
    /// keeping buffered writes as the last-write-wins source. Returns points ordered by timestamp
    /// ascending. When <paramref name="limit"/> is provided, stops collecting once reached so a
    /// <c>LIMIT</c> no longer forces a full scan of every segment (ponytail).
    /// </summary>
    private List<Point> MergeAndDeduplicate(IEnumerable<Point> buffered, IEnumerable<List<Point>> segmentBatches, int? limit, long maxMemoryBytes)
    {
        // Series identity is cached per (measurement, canonical tags) so the canonical tag string is
        // not rebuilt for every point. Points with no tags are NOT cached: QuerySeriesIdentity falls
        // back to the legacy "tag" *field value* in that case, so identity then depends on Fields and
        // caching by tags alone would collapse distinct legacy series into one.
        var identityCache = new Dictionary<(string Meas, string Tags), string>();
        string IdentityOf(Point p)
        {
            // Mirror SeriesKey.From: it yields "" whenever Tags is empty (ignoring TagsCanonical),
            // which is exactly when the legacy "tag" field fallback kicks in.
            var tagsCanonical = p.Tags.Count == 0 ? null : p.TagsCanonical;
            if (string.IsNullOrEmpty(tagsCanonical))
                return QuerySeriesIdentity(p);

            var cacheKey = (p.Measurement, tagsCanonical);
            if (!identityCache.TryGetValue(cacheKey, out var id))
                identityCache[cacheKey] = id = QuerySeriesIdentity(p);
            return id;
        }

        // The points handed to us are the engine's live buffer/segment objects. Never store them
        // directly, because merging writes into Fields would mutate the write buffer itself.
        static Point Clone(Point p) => new()
        {
            Measurement = p.Measurement,
            Tags = p.Tags,
            TagsCanonical = p.TagsCanonical,
            TimestampNs = p.TimestampNs,
            Fields = new Dictionary<string, FieldValue>(p.Fields, StringComparer.Ordinal)
        };

        var map = new Dictionary<(string Meas, string Tags, long Ts), Point>(capacity: limit ?? 1024);

        // ponytail: materialization budget. We track (map bytes) + (sum of all segment batch list
        // bytes already in memory) so we reject a LIMIT-less or huge query *during* the scan instead
        // of only after the whole working set has been paged in (the QueryExecutor layer also checks
        // this, but only post-hoc). EstimatePointBytes mirrors the formula QueryExecutor uses so the
        // two layers see consistent numbers.
        long estimatedBytes = 0;
        long EstimatePointBytes(Point p)
        {
            long size = 96 + (p.Measurement?.Length ?? 0) * 2L + 8;
            foreach (var tag in p.Tags)
                size += 32 + (tag.Key.Length + tag.Value.Length) * 2L;
            foreach (var field in p.Fields)
            {
                size += 48 + field.Key.Length * 2L;
                size += field.Value.Kind switch
                {
                    FieldKind.Float => 24,
                    FieldKind.Integer => 24,
                    FieldKind.Boolean => 24,
                    FieldKind.String => 24 + (field.Value.String?.Length ?? 0) * 2L,
                    _ => 24
                };
            }
            return size;
        }
        void CheckMemory()
        {
            if (maxMemoryBytes > 0 && estimatedBytes > maxMemoryBytes)
                throw new InvalidOperationException(
                    $"query memory limit exceeded: {estimatedBytes} > {maxMemoryBytes} (reduce the time range, add a LIMIT, or raise Storage:MaxQueryMemoryBytes)");
        }

        void Merge(Point p)
        {
            var key = (p.Measurement, IdentityOf(p), p.TimestampNs);
            if (map.TryGetValue(key, out var existing))
            {
                // Last writer wins, matching InfluxDB field-merge semantics.
                foreach (var kv in p.Fields) existing.Fields[kv.Key] = kv.Value;
            }
            else
            {
                map[key] = Clone(p);
                estimatedBytes += EstimatePointBytes(p);
                CheckMemory();
            }
        }

        // Order matters for last-write-wins. Segments arrive from ListSegments ordered by level
        // descending, i.e. older compacted levels first and the newest L0 segments last; the write
        // buffer holds the very newest points and therefore must be merged last of all. Materialize
        // segmentBatches once so we can also count each batch list's own footprint in the memory
        // budget (those lists are live in memory for the whole merge, not just until iterated).
        var materializedBatches = segmentBatches as IList<List<Point>> ?? segmentBatches.ToList();
        foreach (var batch in materializedBatches)
        {
            // ponytail: include the segment batch list itself in the estimate, not just the points
            // it eventually moves into the map. A parallel path with N huge segments has them all
            // pinned in memory until this loop ends; we must account for them to enforce the limit.
            // Sample the first point (if any) as a per-point size estimate; this is conservative-ish
            // since FieldValue sizes vary little but string tags can vary. Empty batches contribute
            // only their list overhead which we model with the base constant.
            long perPoint = batch.Count > 0 ? EstimatePointBytes(batch[0]) : 96;
            estimatedBytes += batch.Count * perPoint;
            CheckMemory();
            foreach (var p in batch)
            {
                if (limit.HasValue && map.Count >= limit.Value) break;
                Merge(p);
            }
        }

        foreach (var p in buffered)
        {
            // Buffered points must always be applied so they can override stale segment values, even
            // once the limit is reached; only *new* keys are gated by the limit.
            if (limit.HasValue && map.Count >= limit.Value
                && !map.ContainsKey((p.Measurement, IdentityOf(p), p.TimestampNs))) continue;
            Merge(p);
        }

        // Points are already mostly time-ordered within each segment; a single stable sort is the
        // only ordering step (replaces the old DeduplicatePoints + ToList + OrderBy + spread copy).
        var result = map.Values.ToList();
        result.Sort(static (a, b) => a.TimestampNs.CompareTo(b.TimestampNs));
        return result;
    }

    public List<Point> ReadAllPoints(string db, string rp, string? meas, long? min, long? max,
        HashSet<string>? requestedFields = null, HashSet<string>? allowedTagsCanonical = null, List<FieldFilter>? fieldFilters = null,
        CancellationToken cancellationToken = default, int? limit = null)
    {
        // ponytail: throttle concurrent slow queries to protect the thread pool.
        _queryGate.Wait(cancellationToken);
        try
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

        // Fast path: single segment — read sequentially and merge with buffer (limit-aware).
        if (segments.Count <= 1)
        {
            var batch = new List<Point>();
            var perSegBudget = limit.HasValue ? Math.Max(0, limit.Value - buffered.Count) : int.MaxValue;
            foreach (var (segPath, _) in segments)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ReadSegmentInto(batch, db, segPath, requestedFields, meas, min, max, allowedTagsCanonical, fieldFilters, perSegBudget);
                if (limit.HasValue && batch.Count + buffered.Count >= limit.Value) break;
            }
            return MergeAndDeduplicate(buffered, new[] { batch }, limit, _maxQueryMemoryBytes);
        }

        // Parallel path: multiple segments — read concurrently, then merge with buffer (limit-aware).
        var segmentResults = new List<Point>[segments.Count];
        var options = new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, 8)
        };
        // Tracks how many points have been gathered so far so the remaining segments can be skipped
        // once a pushed-down limit is already satisfied. Uses an interlocked counter rather than
        // summing segmentResults, which other threads are concurrently writing to.
        var gathered = buffered.Count;
        Parallel.For(0, segments.Count, options, i =>
        {
            if (limit.HasValue && Volatile.Read(ref gathered) >= limit.Value) return;
            var segPath = segments[i].SegPath;
            cancellationToken.ThrowIfCancellationRequested();
            var list = new List<Point>();
            // ponytail: each segment stops at the remaining budget so a giant segment cannot alone
            // materialize the whole dataset; the budget is shared across segments via `gathered`.
            var budget = limit.HasValue
                ? Math.Max(0, limit.Value - Volatile.Read(ref gathered))
                : int.MaxValue;
            ReadSegmentInto(list, db, segPath, requestedFields, meas, min, max, allowedTagsCanonical, fieldFilters, budget);
            segmentResults[i] = list;
            if (limit.HasValue) Interlocked.Add(ref gathered, list.Count);
        });

        var merged = MergeAndDeduplicate(buffered, segmentResults.Where(b => b != null)!, limit, _maxQueryMemoryBytes);
        return merged;
        }
        finally { _queryGate.Release(); }
    }

    /// <summary>
    /// Read a single segment into the target list, applying metadata pushdown to skip irrelevant segments.
    /// </summary>
    private void ReadSegmentInto(List<Point> target, string db, string segPath,
        HashSet<string>? requestedFields, string? meas, long? min, long? max,
        HashSet<string>? allowedTagsCanonical, List<FieldFilter>? fieldFilters, int budget = int.MaxValue)
    {
        // ponytail: budget<=0 means the shared LIMIT has already been satisfied elsewhere, so
        // skip ReadSegmentColumns entirely — it would load every column's timestamps/values arrays
        // into memory (the dominant cost for big segments) just to throw them away.
        if (budget <= 0) return;
        try
        {
            if (meas != null || (min.HasValue && max.HasValue) || (fieldFilters != null && fieldFilters.Count > 0) || allowedTagsCanonical != null)
            {
                try
                {
                    var metas = ReadSegmentMetadataCached(segPath).Metas;

                    // Single pass over the metadata instead of four independent LINQ traversals
                    // (each of which used to walk the whole column list separately).
                    var hasMeas = meas == null;
                    var hasMinOk = !min.HasValue;
                    var hasMaxOk = !max.HasValue;
                    var hasTag = allowedTagsCanonical == null;
                    for (var mi = 0; mi < metas.Count; mi++)
                    {
                        var m = metas[mi];
                        if (!hasMeas && m.Measurement == meas) hasMeas = true;
                        if (!hasMinOk && m.MaxTime >= min!.Value) hasMinOk = true;
                        if (!hasMaxOk && m.MinTime <= max!.Value) hasMaxOk = true;
                        if (!hasTag && allowedTagsCanonical!.Contains(m.TagsCanonical)) hasTag = true;
                        if (hasMeas && hasMinOk && hasMaxOk && hasTag) break;
                    }
                    if (!hasMeas || !hasMinOk || !hasMaxOk || !hasTag) return;

                    if (fieldFilters != null && fieldFilters.Count > 0 && !CouldSegmentMatchFieldFilters(metas, meas, allowedTagsCanonical, fieldFilters))
                        return;
                }
                catch { /* fall through to full read */ }
            }

            // ponytail: Rebuild is lazy. Drain it but stop at `budget` matching points so a giant
            // segment costs memory proportional to LIMIT, not to its total matched size.
            foreach (var p in Rebuild(ReadSegmentColumns(db, segPath, requestedFields, meas, min, max, allowedTagsCanonical), min, max))
            {
                target.Add(p);
                if (target.Count >= budget) break;
            }
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
        foreach (var (segPath, indexedMetas) in EnumerateSeriesSegmentMetadata(db, rp, measurement, tagsCanonical, min, max, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var maxTime = indexedMetas.Metas
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
        foreach (var (segPath, indexedMetas) in EnumerateSeriesSegmentMetadata(db, rp, measurement, tagsCanonical, min, max, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var maxTime = indexedMetas.Metas
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
        foreach (var (segPath, indexedMetas) in EnumerateSeriesSegmentMetadata(db, rp, measurement, tagsCanonical, min, max, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var metas = indexedMetas.Metas
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

    /// <summary>
    /// Single-pass global descending read across all series in a measurement for
    /// "ORDER BY time DESC ... LIMIT n" queries. Uses cached segment metadata to walk segments
    /// newest-first and stops as soon as the limit is satisfied, rejecting points older than the
    /// current kth-newest cutoff. This avoids the per-series scan and the full-scan fallback that
    /// previously materialized far more points than needed on high-cardinality measurements with
    /// many segments or field-misaligned columns.
    /// Returns null when no bounded limit is provided so callers can fall back.
    /// </summary>
    public DescendingSeriesReadResult? TryReadGlobalDescending(
        string db, string rp, string measurement, long? min, long? max,
        HashSet<string>? requestedFields, HashSet<string>? allowedTagsCanonical,
        int? limit, CancellationToken cancellationToken)
    {
        if (!limit.HasValue || limit.Value <= 0)
            return null;

        var key = K(db, rp);
        var lk = GetLock(key);
        lk.EnterReadLock();
        List<(string Tags, long Ts, Point Point)> bufferRows;
        try
        {
            bufferRows = ReadGlobalBufferedPoints(key, measurement, min, max, requestedFields, allowedTagsCanonical);
        }
        finally { lk.ExitReadLock(); }

        // Buffer holds the newest writes and wins over segments for the same field.
        var result = new Dictionary<(string Tags, long Ts), Point>();
        foreach (var (tags, ts, point) in bufferRows)
        {
            if (result.TryGetValue((tags, ts), out var existing))
            {
                foreach (var field in point.Fields)
                    existing.Fields[field.Key] = field.Value;
            }
            else
            {
                result[(tags, ts)] = point;
            }
        }

        long cutoff = limit.HasValue && result.Count >= limit.Value ? KthLargestTimestamp(result, limit.Value) : long.MinValue;

        var candidates = new List<(string Path, long MaxTime)>();
        foreach (var (segPath, _) in _shards.ListSegments(db, rp, min, max))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var maxTime = long.MinValue;
            try
            {
                foreach (var m in ReadSegmentMetadataCached(segPath, db, rp).Metas)
                {
                    if (m.Measurement != measurement) continue;
                    if (allowedTagsCanonical != null && !allowedTagsCanonical.Contains(m.TagsCanonical)) continue;
                    if (requestedFields != null && !requestedFields.Contains(m.Field)) continue;
                    if (min.HasValue && m.MaxTime < min.Value) continue;
                    if (max.HasValue && m.MinTime > max.Value) continue;
                    if (m.MaxTime > maxTime) maxTime = m.MaxTime;
                }
            }
            catch (InvalidDataException) { }
            catch (FileNotFoundException) { }
            if (maxTime != long.MinValue) candidates.Add((segPath, maxTime));
        }
        candidates.Sort((a, b) => b.MaxTime.CompareTo(a.MaxTime));

        var segmentColumnsRead = 0;
        var stopReason = "segments-exhausted";
        foreach (var (segPath, segMaxTime) in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (result.Count >= limit.Value && segMaxTime <= cutoff)
            {
                stopReason = "segment-limit";
                break;
            }

            List<SegmentColumn> columns;
            try { columns = ReadSegmentColumns(db, segPath, requestedFields, measurement, min, max, allowedTagsCanonical); }
            catch (InvalidDataException) { continue; }
            catch (FileNotFoundException) { continue; }
            segmentColumnsRead += columns.Count;

            AddGlobalSegmentColumns(result, columns, cutoff);
            if (result.Count >= limit.Value)
            {
                cutoff = KthLargestTimestamp(result, limit.Value);
                TrimResultBelowCutoff(result, cutoff);
            }
        }

        if (result.Count >= limit.Value)
            stopReason = "segment-limit";

        var ordered = result.OrderByDescending(kv => kv.Key.Item2)
            .ThenBy(kv => kv.Key.Item1, StringComparer.Ordinal)
            .ToList();
        var points = new List<Point>(Math.Min(ordered.Count, limit.Value));
        for (var i = 0; i < ordered.Count && i < limit.Value; i++)
            points.Add(ordered[i].Value);

        return new DescendingSeriesReadResult(points, segmentColumnsRead, 0, stopReason);
    }

    /// <summary>
    /// Single-pass global ascending read across all series in a measurement for raw
    /// "SELECT ... FROM m LIMIT n" queries (no ORDER BY DESC). Uses cached segment metadata to walk
    /// segments oldest-first and stops as soon as the limit is satisfied, rejecting points newer
    /// than the current kth-smallest cutoff. This avoids the full-scan materialization that
    /// ReadAllPoints performs when a raw LIMIT query spans many segments.
    /// Returns null when no bounded limit is provided so callers can fall back.
    /// </summary>
    public DescendingSeriesReadResult? TryReadGlobalAscending(
        string db, string rp, string measurement, long? min, long? max,
        HashSet<string>? requestedFields, HashSet<string>? allowedTagsCanonical,
        int? limit, CancellationToken cancellationToken)
    {
        if (!limit.HasValue || limit.Value <= 0)
            return null;

        var key = K(db, rp);
        var lk = GetLock(key);
        List<(string Tags, long Ts, Point Point)> bufferRows;
        lk.EnterReadLock();
        try
        {
            bufferRows = ReadGlobalBufferedPoints(key, measurement, min, max, requestedFields, allowedTagsCanonical);
        }
        finally { lk.ExitReadLock(); }

        var result = new Dictionary<(string Tags, long Ts), Point>();
        foreach (var (tags, ts, point) in bufferRows)
        {
            if (result.TryGetValue((tags, ts), out var existing))
            {
                foreach (var field in point.Fields)
                    existing.Fields[field.Key] = field.Value;
            }
            else
            {
                result[(tags, ts)] = point;
            }
        }

        long cutoff = limit.HasValue && result.Count >= limit.Value ? KthSmallestTimestamp(result, limit.Value) : long.MaxValue;

        var candidates = new List<(string Path, long MinTime)>();
        foreach (var (segPath, _) in _shards.ListSegments(db, rp, min, max))
        {
            cancellationToken.ThrowIfCancellationRequested();
            long minTime = long.MaxValue;
            try
            {
                foreach (var m in ReadSegmentMetadataCached(segPath, db, rp).Metas)
                {
                    if (m.Measurement != measurement) continue;
                    if (allowedTagsCanonical != null && !allowedTagsCanonical.Contains(m.TagsCanonical)) continue;
                    if (requestedFields != null && !requestedFields.Contains(m.Field)) continue;
                    if (min.HasValue && m.MaxTime < min.Value) continue;
                    if (max.HasValue && m.MinTime > max.Value) continue;
                    if (m.MinTime < minTime) minTime = m.MinTime;
                }
            }
            catch (InvalidDataException) { }
            catch (FileNotFoundException) { }
            if (minTime != long.MaxValue) candidates.Add((segPath, minTime));
        }
        candidates.Sort((a, b) => a.MinTime.CompareTo(b.MinTime));

        var segmentColumnsRead = 0;
        var stopReason = "segments-exhausted";
        foreach (var (segPath, segMinTime) in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (result.Count >= limit.Value && segMinTime >= cutoff)
            {
                stopReason = "segment-limit";
                break;
            }

            List<SegmentColumn> columns;
            try { columns = ReadSegmentColumns(db, segPath, requestedFields, measurement, min, max, allowedTagsCanonical); }
            catch (InvalidDataException) { continue; }
            catch (FileNotFoundException) { continue; }
            segmentColumnsRead += columns.Count;

            AddGlobalSegmentColumnsAscending(result, columns, cutoff);
            if (result.Count >= limit.Value)
            {
                cutoff = KthSmallestTimestamp(result, limit.Value);
                TrimResultAboveCutoff(result, cutoff);
            }
        }

        if (result.Count >= limit.Value)
            stopReason = "segment-limit";

        var ordered = result.OrderBy(kv => kv.Key.Item2)
            .ThenBy(kv => kv.Key.Item1, StringComparer.Ordinal)
            .ToList();
        var points = new List<Point>(Math.Min(ordered.Count, limit.Value));
        for (var i = 0; i < ordered.Count && i < limit.Value; i++)
            points.Add(ordered[i].Value);

        return new DescendingSeriesReadResult(points, segmentColumnsRead, 0, stopReason);
    }

    private static void AddGlobalSegmentColumnsAscending(Dictionary<(string, long), Point> result, List<SegmentColumn> columns, long cutoff)
    {
        var tagCache = new Dictionary<string, KeyValuePair<string, string>[]>(StringComparer.Ordinal);
        foreach (var column in columns)
        {
            var tags = ParseTagsCached(column.TagsCanonical, tagCache);
            for (var i = 0; i < column.Timestamps.Count; i++)
            {
                var ts = column.Timestamps[i];
                if (ts > cutoff) break; // timestamps ascend within a column, so the rest are newer
                if (result.TryGetValue((column.TagsCanonical, ts), out var existing))
                {
                    if (!existing.Fields.ContainsKey(column.Field))
                        existing.Fields[column.Field] = column.Values[i];
                }
                else
                {
                    result[(column.TagsCanonical, ts)] = new Point
                    {
                        Measurement = column.Measurement,
                        Tags = tags,
                        TimestampNs = ts,
                        Fields = new Dictionary<string, FieldValue>(StringComparer.Ordinal) { [column.Field] = column.Values[i] }
                    };
                }
            }
        }
    }

    private static long KthSmallestTimestamp(Dictionary<(string, long), Point> result, int limit)
    {
        var ts = new long[result.Count];
        var i = 0;
        foreach (var kv in result) ts[i++] = kv.Key.Item2;
        Array.Sort(ts);
        return ts[limit - 1];
    }

    private static void TrimResultAboveCutoff(Dictionary<(string, long), Point> result, long cutoff)
    {
        List<(string, long)>? toRemove = null;
        foreach (var kv in result)
            if (kv.Key.Item2 > cutoff)
                (toRemove ??= []).Add(kv.Key);
        if (toRemove == null) return;
        foreach (var key in toRemove)
            result.Remove(key);
    }

    private List<(string Tags, long Ts, Point Point)> ReadGlobalBufferedPoints(
        string key, string measurement, long? min, long? max,
        HashSet<string>? requestedFields, HashSet<string>? allowedTagsCanonical)
    {
        var result = new List<(string, long, Point)>();
        if (!_buf.TryGetValue(key, out var list)) return result;
        foreach (var buffered in BufferedCandidates(key, list, measurement, allowedTagsCanonical))
        {
            var p = buffered.Point;
            if (!Match(p, measurement, min, max)) continue;
            var point = requestedFields == null ? p : new Point
            {
                Measurement = p.Measurement,
                Tags = p.Tags,
                Fields = SelectFields(p.Fields, requestedFields),
                TimestampNs = p.TimestampNs,
                TagsCanonical = p.TagsCanonical
            };
            if (requestedFields != null && point.Fields.Count == 0) continue;
            result.Add((buffered.SeriesKey.TagsCanonical, point.TimestampNs, point));
        }
        return result;
    }

    private static void AddGlobalSegmentColumns(Dictionary<(string, long), Point> result, List<SegmentColumn> columns, long cutoff)
    {
        var tagCache = new Dictionary<string, KeyValuePair<string, string>[]>(StringComparer.Ordinal);
        foreach (var column in columns)
        {
            var tags = ParseTagsCached(column.TagsCanonical, tagCache);
            for (var i = column.Timestamps.Count - 1; i >= 0; i--)
            {
                var ts = column.Timestamps[i];
                if (ts < cutoff) break; // timestamps ascend within a column, so the rest are older
                if (result.TryGetValue((column.TagsCanonical, ts), out var existing))
                {
                    if (!existing.Fields.ContainsKey(column.Field))
                        existing.Fields[column.Field] = column.Values[i];
                }
                else
                {
                    result[(column.TagsCanonical, ts)] = new Point
                    {
                        Measurement = column.Measurement,
                        Tags = tags,
                        TimestampNs = ts,
                        Fields = new Dictionary<string, FieldValue>(StringComparer.Ordinal) { [column.Field] = column.Values[i] }
                    };
                }
            }
        }
    }

    private static long KthLargestTimestamp(Dictionary<(string, long), Point> result, int limit)
    {
        var ts = new long[result.Count];
        var i = 0;
        foreach (var kv in result) ts[i++] = kv.Key.Item2;
        Array.Sort(ts);
        return ts[ts.Length - limit];
    }

    private static void TrimResultBelowCutoff(Dictionary<(string, long), Point> result, long cutoff)
    {
        List<(string, long)>? toRemove = null;
        foreach (var kv in result)
            if (kv.Key.Item2 < cutoff)
                (toRemove ??= []).Add(kv.Key);
        if (toRemove == null) return;
        foreach (var key in toRemove)
            result.Remove(key);
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
        long matchedPoints = 0;
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
        if (meas != null && allowedTagsCanonical != null)
        {
            var indexed = new Dictionary<string, IndexedSegmentMetadata>(StringComparer.OrdinalIgnoreCase);
            foreach (var tags in allowedTagsCanonical)
                foreach (var (path, metadata) in EnumerateSeriesSegmentMetadata(db, rp, meas, tags, min, max, cancellationToken))
                    indexed[path] = metadata;

            var indexedResult = indexed.Values
                .SelectMany(metadata => metadata.Metas)
                .Where(m => (!min.HasValue || m.MaxTime >= min.Value)
                    && (!max.HasValue || m.MinTime <= max.Value)
                    && (requestedFields == null || requestedFields.Contains(m.Field)))
                .ToList();
            return new SegmentMetadataQueryResult(indexedResult, indexed.Values.Count(metadata => metadata.UsedFooter), indexed.Values.Count(metadata => !metadata.UsedFooter));
        }

        var reads = new System.Collections.Concurrent.ConcurrentBag<(IReadOnlyList<SegmentColumnMeta> Metas, bool UsedFooter)>();
        Parallel.ForEach(
            _shards.ListSegments(db, rp, min, max),
            new ParallelOptions { CancellationToken = cancellationToken, MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, 8) },
            segment =>
            {
                try
                {
                    reads.Add(ReadSegmentMetadataCached(segment.SegPath, db, rp));
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

    public sealed record PointCountResult(Dictionary<string, long> FieldCounts, long MaxTimestampNs, long ScannedPoints);

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
        long scannedPoints = 0;

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
            result[group.Key] = group.Sum(kv => (long)kv.Value.Count);

        if (result.Count == 0) return null;

        return new PointCountResult(result, maxTime, scannedPoints);
    }

    public CompactionStatsSnapshot GetCompactionStats() => _compactor.GetStats();
    public StorageHealth Health => _health;
    public int CompactNow()
    {
        var merged = _compactor.CompactAll();
        if (merged > 0) InvalidateSegmentMetadataIndex();
        return merged;
    }

    private void RunCompaction()
    {
        try
        {
            if (_compactor.CompactAll() > 0) InvalidateSegmentMetadataIndex();
        }
        catch (Exception ex) { _health.RecordFailure("compaction", ex); }
    }

    public void DropDatabase(string db)
    {
        FlushDatabase(db);
        var dbDir = Path.Combine(_root, "db", db);
        if (Directory.Exists(dbDir)) try { Directory.Delete(dbDir, true); } catch { }
        InvalidateSegmentMetadataIndex();
        _manifest.DropDatabase(db); _manifest.SaveIfDirty(); _tombstones.DropDatabase(db);
        _globalLock.EnterWriteLock();
        try { foreach (var k in _buf.Keys.Where(k => k.StartsWith(db + "|")).ToList()) { if (_buf[k].Count > 0) { _bufferedPointCount -= _buf[k].Count; } _buf.TryRemove(k, out _); _bufBySeries.TryRemove(k, out _); _locks.TryRemove(k, out var lk); lk?.Dispose(); _bufferReplayFloors.TryRemove(k, out _); _lastBufferWriteTicks.TryRemove(k, out _); } _seriesKeys.TryRemove(db, out _); if (_maxBufferBytes > 0) RecalculateBufferedBytes(); }
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
                InvalidateSegmentMetadataIndex();
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
        var tagCache = new Dictionary<string, KeyValuePair<string, string>[]>(StringComparer.Ordinal);
        foreach (var it in map) yield return new Point { Measurement = it.Key.Item1, Tags = ParseTagsCached(it.Key.Item2, tagCache), TimestampNs = it.Key.Item3, Fields = it.Value };
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
        var tagCache = new Dictionary<string, KeyValuePair<string, string>[]>(StringComparer.Ordinal);
        foreach (var column in columns)
        {
            var tags = ParseTagsCached(column.TagsCanonical, tagCache);
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

    /// <summary>
    /// Parse a canonical tag string into key/value pairs, memoizing the split result per distinct
    /// canonical string. Callers previously re-parsed the identical string once per column (and once
    /// per rebuilt point), so the same <c>string.Split</c> ran thousands of times for one series.
    /// A fresh dictionary is still returned each time because <see cref="Point.Tags"/> is mutable.
    /// </summary>
    private static Dictionary<string, string> ParseTagsCached(string s, Dictionary<string, KeyValuePair<string, string>[]> cache)
    {
        if (string.IsNullOrEmpty(s)) return new Dictionary<string, string>(StringComparer.Ordinal);
        if (!cache.TryGetValue(s, out var pairs))
        {
            var parsed = ParseTags(s);
            pairs = new KeyValuePair<string, string>[parsed.Count];
            var idx = 0;
            foreach (var kv in parsed) pairs[idx++] = kv;
            cache[s] = pairs;
        }

        var d = new Dictionary<string, string>(pairs.Length, StringComparer.Ordinal);
        for (var i = 0; i < pairs.Length; i++) d[pairs[i].Key] = pairs[i].Value;
        return d;
    }

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

    private long _metadataCacheHits;
    private long _metadataCacheMisses;

    /// <summary>
    /// Returns cached segment column metadata. The returned list is shared and MUST be treated as
    /// read-only by callers: segment files are immutable once written, so copying the list on every
    /// cache hit was pure overhead (a "hit" used to allocate a full list copy, which for
    /// high-cardinality segments meant tens of thousands of entries per query).
    /// </summary>
    private (IReadOnlyList<SegmentColumnMeta> Metas, bool UsedFooter) ReadSegmentMetadataCached(string path, string? db = null, string? rp = null)
    {
        // Segment files are immutable once written - cache by path only, no FileInfo needed.
        if (_segmentMetadataCache.TryGetValue(path, out var cached))
        {
            Interlocked.Increment(ref _metadataCacheHits);
            if (db != null && rp != null) IndexSegmentMetadata(db, rp, path, cached.Metas, cached.UsedFooter);
            return (cached.Metas, cached.UsedFooter);
        }

        Interlocked.Increment(ref _metadataCacheMisses);
        var read = SegmentReader.ReadMetadataWithInfo(path);
        var info = new FileInfo(path);
        _segmentMetadataCache[path] = (info.Length, info.LastWriteTimeUtc, read.Metadata, read.UsedFooter);
        if (db != null && rp != null) IndexSegmentMetadata(db, rp, path, read.Metadata, read.UsedFooter);
        return (read.Metadata, read.UsedFooter);
    }

    private IEnumerable<(string Path, IndexedSegmentMetadata Metadata)> EnumerateSeriesSegmentMetadata(
        string db, string rp, string measurement, string tagsCanonical, long? min, long? max, CancellationToken cancellationToken)
    {
        var dbRp = K(db, rp);
        var key = (dbRp, measurement, tagsCanonical);
        if (_segmentMetadataIndexReady.ContainsKey(dbRp))
        {
            if (_segmentMetadataBySeries.TryGetValue(key, out var indexed))
                foreach (var item in indexed)
                    yield return (item.Key, item.Value);
            yield break;
        }

        foreach (var (path, _) in _shards.ListSegments(db, rp, min, max))
        {
            cancellationToken.ThrowIfCancellationRequested();
            IndexedSegmentMetadata metadata;
            try
            {
                var read = ReadSegmentMetadataCached(path, db, rp);
                metadata = new IndexedSegmentMetadata(read.Metas.Where(m => m.Measurement == measurement && m.TagsCanonical == tagsCanonical).ToList(), read.UsedFooter);
            }
            catch (InvalidDataException) { continue; }
            catch (FileNotFoundException) { continue; }
            if (metadata.Metas.Count > 0) yield return (path, metadata);
        }

        if (!min.HasValue && !max.HasValue)
            _segmentMetadataIndexReady[dbRp] = 0;
    }

    private void IndexSegmentMetadata(string db, string rp, string path, List<SegmentColumnMeta> metas, bool usedFooter)
    {
        foreach (var group in metas.GroupBy(meta => (meta.Measurement, meta.TagsCanonical)))
        {
            var segments = _segmentMetadataBySeries.GetOrAdd((K(db, rp), group.Key.Measurement, group.Key.TagsCanonical), _ => new(StringComparer.OrdinalIgnoreCase));
            segments[path] = new IndexedSegmentMetadata(group.ToList(), usedFooter);
        }
    }

    private void InvalidateSegmentMetadataIndex()
    {
        _segmentMetadataCache.Clear();
        _segmentMetadataBySeries.Clear();
        _segmentMetadataIndexReady.Clear();
    }

    /// <summary>
    /// Get cache statistics for diagnostics.
    /// </summary>
    public (long Hits, long Misses, int CachedCount) GetMetadataCacheStats() =>
        (Interlocked.Read(ref _metadataCacheHits), Interlocked.Read(ref _metadataCacheMisses), _segmentMetadataCache.Count);

    /// <pre>
    /// Register segment metadata in the cache after a new segment is written.
    /// This avoids the first query having to read the metadata from disk.
    /// </pre>
    internal void RegisterSegmentMetadata(string db, string rp, string path)
    {
        var read = SegmentReader.ReadMetadataWithInfo(path);
        var info = new FileInfo(path);
        _segmentMetadataCache[path] = (info.Length, info.LastWriteTimeUtc, read.Metadata, read.UsedFooter);
        IndexSegmentMetadata(db, rp, path, read.Metadata, read.UsedFooter);
    }

    /// <pre>
    /// Remove segment metadata from the cache when a segment is deleted (compaction).
    /// </pre>
    internal void UnregisterSegmentMetadata(string path)
    {
        _segmentMetadataCache.TryRemove(path, out _);
    }

    private static bool Match(Point p, string? m, long? min, long? max) => (m == null || p.Measurement == m) && (!min.HasValue || p.TimestampNs >= min) && (!max.HasValue || p.TimestampNs <= max);

    private static bool CouldSegmentMatchFieldFilters(
        IReadOnlyList<SegmentColumnMeta> metas,
        string? measurement,
        HashSet<string>? allowedTagsCanonical,
        List<FieldFilter> fieldFilters)
    {
        // Avoid materializing intermediate lists (previously one for relevant metas plus one per
        // filter); iterate the shared metadata list directly instead.
        static bool IsRelevant(SegmentColumnMeta m, string? measurement, HashSet<string>? allowedTagsCanonical) =>
            (measurement == null || m.Measurement == measurement)
            && (allowedTagsCanonical == null || allowedTagsCanonical.Contains(m.TagsCanonical));

        var anyRelevant = false;
        for (var i = 0; i < metas.Count; i++)
        {
            if (!IsRelevant(metas[i], measurement, allowedTagsCanonical)) continue;
            anyRelevant = true;
            break;
        }
        if (!anyRelevant)
            return false;

        foreach (var filter in fieldFilters)
        {
            var anyCandidate = false;
            var anyMatch = false;
            for (var i = 0; i < metas.Count; i++)
            {
                var m = metas[i];
                if (!IsRelevant(m, measurement, allowedTagsCanonical)) continue;
                if (!string.Equals(m.Field, filter.Field, StringComparison.Ordinal)) continue;
                anyCandidate = true;
                if (CouldColumnMatch(m, filter)) { anyMatch = true; break; }
            }
            if (!anyCandidate || !anyMatch)
                return false;
        }

        return true;
    }

    private static bool CouldColumnMatch(SegmentColumnMeta meta, FieldFilter filter)
    {
        var stats = meta.Stats;
        if (stats == null)
            return true;

        // 增强的统计信息过滤，更精确地判断segment是否可能匹配查询条件
        switch (filter.Op)
        {
            case FieldOp.Eq:
                // 对于等于操作，只有当filter值在min-max范围内且count>0时才可能匹配
                if (stats.Count == 0) return false;
                if (filter.Value < stats.Min || filter.Value > stats.Max) return false;
                return true;
                
            case FieldOp.Neq:
                // 对于不等于，只要不是所有值都等于filter值就可能匹配
                if (stats.Count == 0) return true;
                if (Math.Abs(stats.Min - filter.Value) < 1e-9 && 
                    Math.Abs(stats.Max - filter.Value) < 1e-9 && 
                    Math.Abs(stats.Min - stats.Max) < 1e-9) return false;
                return true;
                
            case FieldOp.Gt:
                // 对于大于，max必须大于filter值且count > 0
                if (stats.Count == 0 || stats.Max <= filter.Value) return false;
                return true;
                
            case FieldOp.Gte:
                // 对于大于等于，max必须大于等于filter值且count > 0
                if (stats.Count == 0 || stats.Max < filter.Value) return false;
                return true;
                
            case FieldOp.Lt:
                // 对于小于，min必须小于filter值且count > 0
                if (stats.Count == 0 || stats.Min >= filter.Value) return false;
                return true;
                
            case FieldOp.Lte:
                // 对于小于等于，min必须小于等于filter值且count > 0
                if (stats.Count == 0 || stats.Min > filter.Value) return false;
                return true;
                
            default:
                return true;
        }
    }

    private void FlushLocked(string db, string rp, List<BufferedPoint> l, bool updateCheckpoint = true, bool force = false)
    {
        if (l.Count == 0) return;

        // ponytail: MinSegmentFileBytes floor. Defer flushing a tiny buffer so cold/low-volume shards
        // don't scatter many small .seg files; the points merge into larger files as they accumulate.
        // We only skip when below the floor AND not already at the hard buffer cap (skipping there
        // would risk OOM). WAL already persists the points, so deferring only delays WAL truncation,
        // never loses data. `force` (set for a shard that has been cold past 2x FlushColdDurationMs)
        // bypasses the floor so data is eventually durable.
        if (!force && _minSegmentFileBytes > 0 && l.Count < _maxBufferPoints)
        {
            long est = 0;
            foreach (var buffered in l) est += EstimateBufferedPointBytes(buffered.Point);
            if (est < _minSegmentFileBytes) return;
        }

        var flushCount = l.Count;
        long flushBytes = 0;
        
        // 优化的shard分组，使用预分配列表减少内存分配
        var byShard = new Dictionary<int, List<(Point Point, SeriesKey SeriesKey)>>();
        foreach (var buffered in l)
        {
            if (_maxBufferBytes > 0)
                flushBytes += EstimateBufferedPointBytes(buffered.Point);
            var (shardId, _) = _shards.GetOrCreateShard(db, rp, buffered.Point.TimestampNs);
            if (!byShard.TryGetValue(shardId, out var points))
            {
                // 预分配足够大的容量减少列表扩容
                points = new List<(Point, SeriesKey)>(Math.Max(1000, l.Count / 10));
                byShard[shardId] = points;
            }
            points.Add((buffered.Point, buffered.SeriesKey));
        }

        // 并行flush到不同shard（优化的并行写入）
        if (byShard.Count > 1)
        {
            var shardTasks = new Task[byShard.Count];
            var shardArray = byShard.ToArray();
            for (int i = 0; i < shardArray.Length; i++)
            {
                var (shardId, points) = shardArray[i];
                shardTasks[i] = Task.Run(() => WriteShardSegments(db, rp, shardId, points));
            }
            Task.WaitAll(shardTasks);
        }
        else
        {
            // 单个shard时直接处理，避免任务调度开销
            foreach (var (shardId, points) in byShard)
                WriteShardSegments(db, rp, shardId, points);
        }
        
        _bufferedPointCount -= flushCount;
        if (_maxBufferBytes > 0)
            _bufferedByteCount -= flushBytes;
        l.Clear();
        var key = K(db, rp);
        _lastBufferWriteTicks.TryRemove(key, out _);
        _bufBySeries.TryRemove(key, out _);
        UpdateBufferReplayFloor(key, l);
        if (updateCheckpoint)
            UpdateWalCheckpoint();
    }

    // ponytail: write one shard's pending points to .seg file(s), honoring the configured max segment
    // size. A single flush of a hot shard can exceed MaxSegmentFileBytes, so we split the points into
    // chunks of at most that many (estimated) bytes — each chunk becomes one file. Small flushes that
    // fit under the cap land in a single file, so data is merged into big files rather than scattered
    // across many tiny ones. Per-point size is estimated up front; the very last point of a chunk is
    // always emitted even if it nudges the chunk slightly over the cap (a single point can't be split).
    private void WriteShardSegments(string db, string rp, int shardId, List<(Point Point, SeriesKey SeriesKey)> points)
    {
        if (points.Count == 0) return;
        var shardDir = _shards.ShardDir(db, rp, shardId);
        if (!Directory.Exists(shardDir)) Directory.CreateDirectory(shardDir);

        if (_maxSegmentFileBytes <= 0 || points.Count == 1)
        {
            var segPath = Path.Combine(shardDir, $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}.seg");
            SegmentWriter.WriteSegment(segPath, points);
            _shards.RegisterSegment(db, rp, shardId, segPath);
            RegisterSegmentMetadata(db, rp, segPath);
            return;
        }

        // ponytail: tail-merge. We never split a chunk once it has reached the fill floor
        // (maxSegmentFileBytes * segmentFillRatio), and once the *remaining* unwritten points are
        // smaller than that floor we stop opening new files entirely and pack them into the current
        // chunk. This avoids a tiny trailing .seg that would otherwise inflate the file count on
        // HDD/network storage. Files may end up anywhere from floor..cap in size — we deliberately
        // do not force exact size alignment.
        long remaining = 0;
        foreach (var p in points) remaining += EstimateSegmentPointBytes(p.Point);
        var fillFloor = (long)(_maxSegmentFileBytes * _segmentFillRatio);
        // If the whole batch is below the floor, a single file is fine (MinSegmentFileBytes handles
        // the cold-shard deferral case before we get here).
        var chunk = new List<(Point, SeriesKey)>(Math.Min(points.Count, 1 << 16));
        long chunkBytes = 0;
        int fileCount = 0;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        for (int i = 0; i < points.Count; i++)
        {
            var p = points[i];
            var pBytes = EstimateSegmentPointBytes(p.Point);
            // Amortize the series header (stored once per file) onto the first point of each chunk.
            if (chunk.Count == 0) pBytes += SeriesHeaderBytes(p.Point);
            remaining -= pBytes;
            // Open a new file only when (a) the current chunk already cleared the fill floor AND
            // (b) adding this point would breach the hard cap AND (c) there is still enough
            // remaining data to justify another file (>= floor). Otherwise keep packing.
            if (chunk.Count > 0 && chunkBytes >= fillFloor && chunkBytes + pBytes > _maxSegmentFileBytes
                && remaining >= fillFloor)
            {
                FlushChunk(shardDir, db, rp, shardId, chunk, nowMs);
                fileCount++;
                chunk.Clear();
                chunkBytes = 0;
            }
            chunk.Add(p);
            chunkBytes += pBytes;
        }
        if (chunk.Count > 0)
        {
            FlushChunk(shardDir, db, rp, shardId, chunk, nowMs);
            fileCount++;
        }
    }

    private void FlushChunk(string shardDir, string db, string rp, int shardId, List<(Point, SeriesKey)> chunk, long nowMs)
    {
        var segPath = Path.Combine(shardDir, $"{nowMs}-{Guid.NewGuid():N}.seg");
        SegmentWriter.WriteSegment(segPath, chunk);
        _shards.RegisterSegment(db, rp, shardId, segPath);
        RegisterSegmentMetadata(db, rp, segPath);
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

    // ponytail: columnar on-disk estimate used for .seg file *size* splitting. Unlike
    // EstimateBufferedPointBytes (which models the in-memory buffered cost where measurement/tags are
    // repeated per point), the segment format stores the series header (measurement, tags, field names)
    // once and delta/column-encodes the data, so per-point cost is just the timestamp plus each field
    // value. Using the in-memory estimate here over-estimated by ~100x and caused the splitter to emit
    // many tiny files on HDD/network storage. The series header is amortized once per chunk.
    private static long EstimateSegmentPointBytes(Point point)
    {
        long size = 8; // timestamp
        foreach (var field in point.Fields)
            size += field.Value.Kind == FieldKind.String ? EstimateStringBytes(field.Value.String) : 8;
        return size;
    }

    private static long SeriesHeaderBytes(Point point)
    {
        long size = 64 + EstimateStringBytes(point.Measurement);
        if (point.Tags != null)
            foreach (var tag in point.Tags)
                size += EstimateStringBytes(tag.Key) + EstimateStringBytes(tag.Value);
        if (point.Fields != null)
            foreach (var field in point.Fields)
                size += EstimateStringBytes(field.Key);
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
        try
        {
            if (_shards.CleanupExpiredShards(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000) > 0)
                InvalidateSegmentMetadataIndex();
        }
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
            var coldBefore = DateTime.UtcNow.Ticks - _flushColdTicks;
            foreach (var kv in _buf.ToArray())
            {
                var p = kv.Key.Split('|');
                var lk = GetLock(kv.Key);
                lk.EnterWriteLock();
                try
                {
                    if (kv.Value.Count == 0) continue;
                    if (kv.Value.Count < _threshold
                        && (_flushColdTicks <= 0 || !_lastBufferWriteTicks.TryGetValue(kv.Key, out var lastWrite) || lastWrite > coldBefore))
                        continue;
                    // ponytail: a shard colder than 2x FlushColdDurationMs bypasses the MinSegmentFileBytes
                    // floor so its buffered points become durable instead of lingering in the buffer forever.
                    var force = _flushColdTicks > 0
                        && _lastBufferWriteTicks.TryGetValue(kv.Key, out var lastWrite2)
                        && lastWrite2 <= coldBefore - _flushColdTicks;
                    FlushLocked(p[0], p[1], kv.Value, updateCheckpoint: false, force: force);
                }
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

public readonly record struct BufferedStatsSnapshot(long MatchedPointCount, long MaxTime, IReadOnlyDictionary<string, BufferedFieldStats> Fields);

public readonly record struct BufferedFieldStats(long Count, double Sum, double Min, double Max)
{
    public static BufferedFieldStats Single(double value) => new(1, value, value, value);
    public BufferedFieldStats Add(double value) => new(Count + 1, Sum + value, Math.Min(Min, value), Math.Max(Max, value));
}

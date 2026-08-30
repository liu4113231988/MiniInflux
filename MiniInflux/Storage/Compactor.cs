using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Multi-level shard compactor. Uses filename-based levels:
/// no prefix / l0-* => L0, l1-* => L1, l2-* => L2.
/// </summary>
public sealed class Compactor
{
    private readonly Manifest _manifest;
    private readonly ShardManager _shardManager;
    private readonly TombstoneStore _tombstones;
    private readonly SchemaRegistry _schema;
    private readonly StorageHealth? _health;
    // ponytail: min timestamp held by an in-flight flush snapshot, per db (long.MaxValue when none).
    // Data at or above it may still land in a new segment after this task's snapshot was taken, so
    // tombstone coverage is only retired below it (see TryRetireTombstones).
    private readonly Func<string, long>? _inFlightFlushMinTs;
    private readonly int _maxL0Segments;
    private readonly int _maxL1Segments;
    private readonly long _maxL0Bytes;
    private readonly long _maxL1Bytes;
    private readonly int _minFilesPerCompaction;
    private readonly int _maxPassesPerRun;
    private readonly long _maxSegmentFileBytes;
    private readonly double _segmentFillRatio;
    // ponytail: optional I/O budget for compaction output writes (bytes/second; 0 = unlimited).
    private readonly long _maxWriteBytesPerSecond;
    private long _throttleWindowStartTicks;
    private long _throttleWindowBytes;
    private readonly object _compactionLock = new();
    private long _totalRuns;
    private long _totalTasks;
    private long _totalSegmentsMerged;
    private int _running;
    private int _queuedTasks;
    private DateTimeOffset? _lastRunUtc;

    public Compactor(Manifest manifest, ShardManager shardManager, TombstoneStore tombstones,
        SchemaRegistry schema, int maxL0Segments = 10, int maxL1Segments = 4,
        long maxL0Bytes = 512 * 1024 * 1024, long maxL1Bytes = 512 * 1024 * 1024,
        int minFilesPerCompaction = 2, int maxPassesPerRun = 8, StorageHealth? health = null,
        long maxSegmentFileBytes = 0, double segmentFillRatio = 0.5,
        long maxWriteBytesPerSecond = 0, Func<string, long>? inFlightFlushMinTs = null)
    {
        _manifest = manifest;
        _shardManager = shardManager;
        _tombstones = tombstones;
        _schema = schema;
        _health = health;
        _inFlightFlushMinTs = inFlightFlushMinTs;
        _maxL0Segments = maxL0Segments;
        _maxL1Segments = maxL1Segments;
        _maxL0Bytes = maxL0Bytes;
        _maxL1Bytes = maxL1Bytes;
        _minFilesPerCompaction = Math.Max(2, minFilesPerCompaction);
        _maxPassesPerRun = Math.Max(1, maxPassesPerRun);
        _maxSegmentFileBytes = maxSegmentFileBytes > 0 ? maxSegmentFileBytes : 512L * 1024 * 1024;
        _segmentFillRatio = segmentFillRatio is > 0 and <= 1 ? segmentFillRatio : 0.5;
        _maxWriteBytesPerSecond = Math.Max(0, maxWriteBytesPerSecond);
    }

    public int CompactAll()
    {
        if (!Monitor.TryEnter(_compactionLock)) return 0;
        Interlocked.Exchange(ref _running, 1);
        try
        {
            int merged = 0;
            for (int pass = 0; pass < _maxPassesPerRun; pass++)
            {
                var tasks = BuildTasks();
                Interlocked.Exchange(ref _queuedTasks, tasks.Count);
                if (tasks.Count == 0)
                    break;

                int mergedThisPass = 0;
                foreach (var task in tasks)
                {
                    if (CompactShard(task.Db, task.Rp, task.Shard, task.Level, task.Files))
                    {
                        merged++;
                        mergedThisPass++;
                    }
                }

                if (mergedThisPass == 0)
                    break;
            }

            if (merged > 0)
            {
                Interlocked.Increment(ref _totalRuns);
                _lastRunUtc = DateTimeOffset.UtcNow;
            }
            return merged;
        }
        finally
        {
            Interlocked.Exchange(ref _queuedTasks, 0);
            Interlocked.Exchange(ref _running, 0);
            Monitor.Exit(_compactionLock);
        }
    }

    public CompactionStatsSnapshot GetStats()
    {
        var running = Interlocked.CompareExchange(ref _running, 0, 0) == 1;
        var queued = Interlocked.CompareExchange(ref _queuedTasks, 0, 0);
        return new CompactionStatsSnapshot
        {
            TotalRuns = Interlocked.Read(ref _totalRuns),
            TotalTasks = Interlocked.Read(ref _totalTasks),
            TotalSegmentsMerged = Interlocked.Read(ref _totalSegmentsMerged),
            Running = running,
            QueuedTasks = queued,
            BacklogTasks = running ? queued : BuildTasks().Count,
            LastRunUtc = _lastRunUtc
        };
    }

    private List<CompactionTask> BuildTasks()
    {
        var tasks = new List<CompactionTask>();
        foreach (var db in _manifest.ListDatabases())
        {
            foreach (var rp in _manifest.ListRetentionPolicies(db))
            {
                foreach (var shard in _manifest.GetShards(db, rp.Name))
                {
                    var shardDir = _shardManager.ShardDir(db, rp.Name, shard.Id);
                    var segFiles = shard.SegmentFiles
                        .Select(file => Path.Combine(shardDir, file))
                        .Where(File.Exists)
                        .ToList();

                    var described = DescribeFiles(segFiles);
                    var claimed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                    var l0Task = BuildLevelTask(
                        db, rp.Name, shard, 1,
                        described.Where(f => f.Level == 0 && !claimed.Contains(f.Path)).ToList(),
                        described.Where(f => f.Level == 1 && !claimed.Contains(f.Path)).ToList(),
                        _maxL0Segments, _maxL0Bytes);
                    if (l0Task != null)
                    {
                        tasks.Add(l0Task);
                        foreach (var file in l0Task.Files)
                            claimed.Add(file.Path);
                    }

                    var l1Task = BuildLevelTask(
                        db, rp.Name, shard, 2,
                        described.Where(f => f.Level == 1 && !claimed.Contains(f.Path)).ToList(),
                        described.Where(f => f.Level == 2 && !claimed.Contains(f.Path)).ToList(),
                        _maxL1Segments, _maxL1Bytes);
                    if (l1Task != null)
                    {
                        tasks.Add(l1Task);
                        foreach (var file in l1Task.Files)
                            claimed.Add(file.Path);
                    }
                }
            }
        }
        return tasks;
    }

    private List<FileCandidate> DescribeFiles(List<string> segFiles) =>
        segFiles.Select(DescribeFile)
            .OrderBy(x => x.LastWriteUtc)
            .ThenBy(x => x.Path, StringComparer.OrdinalIgnoreCase)
            .ToList();

    private FileCandidate DescribeFile(string path)
        => new(path, SafeLength(path), SafeWriteTime(path), InferLevel(path), null, null);

    private FileCandidate ReadTimeRange(FileCandidate file)
    {
        long? minTimeNs = null;
        long? maxTimeNs = null;
        try
        {
            var metadata = SegmentReader.ReadMetadata(file.Path);
            if (metadata.Count > 0)
            {
                minTimeNs = metadata.Min(m => m.MinTime);
                maxTimeNs = metadata.Max(m => m.MaxTime);
            }
        }
        catch (Exception ex) { _health?.RecordFailure("compaction_metadata", ex); }

        return file with { MinTimeNs = minTimeNs, MaxTimeNs = maxTimeNs };
    }

    private CompactionTask? BuildLevelTask(
        string db,
        string rp,
        ShardGroupInfo shard,
        int outputLevel,
        List<FileCandidate> currentLevelFiles,
        List<FileCandidate> overlapLevelFiles,
        int segmentThreshold,
        long byteThreshold)
    {
        var requiredFiles = Math.Min(_minFilesPerCompaction, Math.Max(1, segmentThreshold));
        if (currentLevelFiles.Count < requiredFiles)
            return null;

        var totalBytes = currentLevelFiles.Sum(f => f.Length);
        var triggerByCount = currentLevelFiles.Count >= segmentThreshold;
        var triggerByBytes = byteThreshold > 0 && totalBytes >= byteThreshold;
        if (!triggerByCount && !triggerByBytes)
            return null;

        var selected = new List<FileCandidate>();
        long selectedBytes = 0;
        foreach (var file in currentLevelFiles)
        {
            selected.Add(file);
            selectedBytes += file.Length;

            if (selected.Count >= requiredFiles && byteThreshold > 0 && selectedBytes >= byteThreshold)
                break;
            if (selected.Count >= segmentThreshold && byteThreshold <= 0)
                break;
        }

        if (selected.Count < requiredFiles)
            selected = currentLevelFiles.Take(requiredFiles).ToList();

        selected = selected.Select(ReadTimeRange).ToList();
        var describedOverlaps = overlapLevelFiles.Select(ReadTimeRange).ToList();
        var includeAllOverlaps = selected.Any(file => !file.MinTimeNs.HasValue || !file.MaxTimeNs.HasValue);
        if (describedOverlaps.Count > 0)
        {
            var selectedMinTime = selected.Where(f => f.MinTimeNs.HasValue).Select(f => f.MinTimeNs!.Value).DefaultIfEmpty(long.MinValue).Min();
            var selectedMaxTime = selected.Where(f => f.MaxTimeNs.HasValue).Select(f => f.MaxTimeNs!.Value).DefaultIfEmpty(long.MaxValue).Max();
            foreach (var overlap in describedOverlaps)
            {
                if (includeAllOverlaps || Overlaps(overlap, selectedMinTime, selectedMaxTime))
                    selected.Add(overlap);
            }
        }

        return new CompactionTask(db, rp, shard, outputLevel, selected
            .GroupBy(f => f.Path, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .ToList());
    }

    private static bool Overlaps(FileCandidate candidate, long minTimeNs, long maxTimeNs)
    {
        if (!candidate.MinTimeNs.HasValue || !candidate.MaxTimeNs.HasValue)
            return true;
        return candidate.MaxTimeNs.Value >= minTimeNs && candidate.MinTimeNs.Value <= maxTimeNs;
    }

    private bool CompactShard(string db, string rp, ShardGroupInfo shard, int outputLevel, List<FileCandidate> segFiles)
    {
        if (segFiles.Count == 0) return false;

        // Capture before rewriting: a flush snapshot in flight now may write pre-delete points to a
        // new segment after this task's snapshot was taken, so coverage is only retired below it.
        long gcFloor = _inFlightFlushMinTs != null && _tombstones.HasTombstones(db)
            ? _inFlightFlushMinTs(db)
            : long.MaxValue;
        // A delete that lands after the inputs were read cannot have been applied to the rewritten
        // output, so retirement is skipped whenever the store mutated during the rewrite.
        long tombstoneVersionAtRead = _tombstones.CurrentVersion;

        var orderedInputs = segFiles
            .OrderByDescending(f => f.Level)
            .ThenBy(f => f.LastWriteUtc)
            .ThenBy(f => f.Path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        // Footer metadata per input, read once: drives group enumeration, batch sizing, and the
        // legacy-format detection.
        var inputMetas = new List<(string Path, List<SegmentColumnMeta> Metas)>(orderedInputs.Count);
        foreach (var file in orderedInputs)
        {
            try { inputMetas.Add((file.Path, SegmentReader.ReadMetadata(file.Path))); }
            catch (Exception ex) { _health?.RecordFailure("compaction_metadata", ex); return false; }
        }

        // Legacy v2 files store tags in a synthetic column and need whole-file materialization to
        // normalize; fall back to the original path for them (rare, old data only).
        var hasLegacyTagColumns = inputMetas.Any(x => x.Metas.Any(m =>
            m.TagsCanonical.Length == 0 && m.Field == "tag" && m.Kind == FieldKind.String));
        if (hasLegacyTagColumns)
            return CompactShardMaterialized(db, rp, shard, outputLevel, orderedInputs, gcFloor, tombstoneVersionAtRead);

        var shardDir = _shardManager.ShardDir(db, rp, shard.Id);
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var retentionCutoffNs = GetRetentionCutoffNs(db, rp);

        // Enumerate distinct (measurement, tags, field) groups across all inputs with their
        // per-input columns.
        var groups = new Dictionary<(string Meas, string Tags, string Field), List<(int FileIdx, SegmentColumnMeta Meta)>>();
        for (var fi = 0; fi < inputMetas.Count; fi++)
            foreach (var meta in inputMetas[fi].Metas)
            {
                var key = (meta.Measurement, meta.TagsCanonical, meta.Field);
                if (!groups.TryGetValue(key, out var list)) groups[key] = list = [];
                list.Add((fi, meta));
            }

        if (groups.Count == 0) return false;

        var orderedGroups = groups.Keys
            .OrderBy(k => k.Meas, StringComparer.Ordinal)
            .ThenBy(k => k.Tags, StringComparer.Ordinal)
            .ThenBy(k => k.Field, StringComparer.Ordinal)
            .ToList();

        // Pre-estimate every group's output bytes from metadata so the chunker's remaining-bytes
        // check (no tiny trailing file) still works with lazy merging.
        long totalEstimate = 0;
        var groupEstimate = new Dictionary<(string Meas, string Tags, string Field), long>();
        foreach (var g in orderedGroups)
        {
            long est = 0;
            foreach (var (_, meta) in groups[g]) est += EstimateGroupBytes(meta);
            groupEstimate[g] = est;
            totalEstimate += est;
        }

        // ponytail: honor MaxSegmentFileBytes on the *output* too. Merged columns are packed into
        // chunks of at most the cap (column-aligned so each output file stays a valid segment), with
        // the same tail-merge fill rule as the flush path.
        var mergedPaths = new List<string>();
        var chunk = new List<SegmentColumn>(Math.Min(orderedGroups.Count, 1 << 14));
        long chunkBytes = 0;
        var fillFloor = (long)(_maxSegmentFileBytes * _segmentFillRatio);
        var remaining = totalEstimate;

        foreach (var col in MergeColumnsStreaming(db, inputMetas, groups, orderedGroups, retentionCutoffNs))
        {
            var colBytes = EstimateColumnBytes(col);
            remaining -= groupEstimate[(col.Measurement, col.TagsCanonical, col.Field)];
            if (chunk.Count > 0 && chunkBytes >= fillFloor && chunkBytes + colBytes > _maxSegmentFileBytes
                && remaining >= fillFloor)
            {
                WriteMergedChunk(shardDir, outputLevel, nowMs, chunk, mergedPaths);
                chunk.Clear();
                chunkBytes = 0;
            }
            chunk.Add(col);
            chunkBytes += colBytes;
        }
        if (chunk.Count > 0)
            WriteMergedChunk(shardDir, outputLevel, nowMs, chunk, mergedPaths);

        var finalized = FinalizeCompaction(db, rp, shard.Id, orderedInputs, mergedPaths);
        if (finalized) TryRetireTombstones(db, rp, shard, orderedInputs, gcFloor, tombstoneVersionAtRead);
        return finalized;
    }

    private const long CompactionBatchPointBudget = 2_000_000;

    /// <summary>
    /// ponytail: streaming merge. Previously every input segment was fully decoded into memory
    /// before merging, so a byte-triggered compaction of large files pinned multiple GB on the heap.
    /// Now groups are processed in batches sized by metadata point counts; each batch decodes only
    /// its own columns (one pass per input file), merges them, and the resulting columns are written
    /// out and dropped before the next batch. Peak memory is one batch plus one output chunk.
    /// </summary>
    private IEnumerable<SegmentColumn> MergeColumnsStreaming(
        string db,
        List<(string Path, List<SegmentColumnMeta> Metas)> inputMetas,
        Dictionary<(string Meas, string Tags, string Field), List<(int FileIdx, SegmentColumnMeta Meta)>> groups,
        List<(string Meas, string Tags, string Field)> orderedGroups,
        long? retentionCutoffNs)
    {
        var batch = new List<(string Meas, string Tags, string Field)>();
        long batchPoints = 0;
        foreach (var group in orderedGroups)
        {
            batch.Add(group);
            foreach (var (_, meta) in groups[group]) batchPoints += meta.PointCount;
            if (batchPoints < CompactionBatchPointBudget) continue;

            foreach (var col in MergeBatch(db, batch, groups, inputMetas, retentionCutoffNs))
                yield return col;
            batch = [];
            batchPoints = 0;
        }
        if (batch.Count > 0)
            foreach (var col in MergeBatch(db, batch, groups, inputMetas, retentionCutoffNs))
                yield return col;
    }

    private IEnumerable<SegmentColumn> MergeBatch(
        string db,
        List<(string Meas, string Tags, string Field)> batch,
        Dictionary<(string Meas, string Tags, string Field), List<(int FileIdx, SegmentColumnMeta Meta)>> groups,
        List<(string Path, List<SegmentColumnMeta> Metas)> inputMetas,
        long? retentionCutoffNs)
    {
        var batchSet = new HashSet<(string Meas, string Tags, string Field)>(batch);

        // One pass per input file, decoding only the batch's columns; columns arrive in input
        // order, which preserves the existing last-write-wins tie-break (newest input wins).
        var decoded = new Dictionary<(string Meas, string Tags, string Field), List<SegmentColumn>>();
        for (var fi = 0; fi < inputMetas.Count; fi++)
        {
            List<SegmentColumn> cols;
            try { cols = SegmentReader.ReadSegmentSelected(inputMetas[fi].Path, (m, t, f) => batchSet.Contains((m, t, f))); }
            catch (Exception ex)
            {
                _health?.RecordFailure("compaction_read", ex);
                throw;
            }
            foreach (var col in cols)
            {
                var key = (col.Measurement, col.TagsCanonical, col.Field);
                if (!decoded.TryGetValue(key, out var list)) decoded[key] = list = [];
                list.Add(col);
            }
        }

        foreach (var group in batch)
        {
            if (!decoded.TryGetValue(group, out var cols)) continue;

            // Tombstones are applied per column before the merge, matching the previous behavior.
            var filtered = new List<SegmentColumn>(cols.Count);
            foreach (var col in cols)
            {
                if (_tombstones.IsColumnDeleted(db, col.Measurement, col.TagsCanonical, col.MinTime, col.MaxTime))
                    continue;
                var (ts, vals) = _tombstones.FilterColumnDeleted(db, col.Measurement, col.TagsCanonical, col.Timestamps, col.Values);
                if (retentionCutoffNs.HasValue)
                    (ts, vals) = ApplyRetentionCutoff(ts, vals, retentionCutoffNs.Value);
                if (ts.Count == 0) continue;
                filtered.Add(col with { Timestamps = ts, Values = vals, MinTime = ts[0], MaxTime = ts[^1] });
            }
            if (filtered.Count == 0) continue;

            var merged = MergeGroupColumns(group, filtered);
            _schema.ValidateAndRegisterColumns(db, [merged]);
            _manifest.UpdateIndexes(db, [(group.Meas, group.Tags, ParseTags(group.Tags))]);
            yield return merged;
        }
    }

    /// <summary>
    /// K-way merge of sorted, deduplicated columns. On equal timestamps the column from the latest
    /// input (highest position in the ordered input list) wins, matching the previous
    /// SortedDictionary-based merge semantics.
    /// </summary>
    private static SegmentColumn MergeGroupColumns((string Meas, string Tags, string Field) group, List<SegmentColumn> cols)
    {
        var heap = new PriorityQueue<(int Col, int Idx, long Ts), long>();
        for (var c = 0; c < cols.Count; c++)
        {
            var timestamps = cols[c].Timestamps;
            if (timestamps.Count > 0) heap.Enqueue((c, 0, timestamps[0]), timestamps[0]);
        }

        var ts = new List<long>();
        var vals = new List<FieldValue>();

        while (heap.TryDequeue(out var item, out var priority))
        {
            var (col, idx, timestamp) = item;
            var value = cols[col].Values[idx];

            // Resolve equal-timestamp duplicates: the highest input position wins. Every losing
            // cursor must advance here; the final winner advances exactly once below.
            while (heap.TryPeek(out _, out var nextPriority) && nextPriority == priority)
            {
                var (col2, idx2, _) = heap.Dequeue();
                if (col2 > col)
                {
                    EnqueueNext(heap, cols, col, idx);
                    col = col2; idx = idx2;
                    value = cols[col].Values[idx];
                }
                else
                {
                    EnqueueNext(heap, cols, col2, idx2);
                }
            }

            ts.Add(timestamp);
            vals.Add(value);
            EnqueueNext(heap, cols, col, idx);
        }

        return new SegmentColumn(group.Meas, group.Tags, group.Field, cols[0].Kind, ts[0], ts[^1], ts, vals);
    }

    private static void EnqueueNext(PriorityQueue<(int Col, int Idx, long Ts), long> heap, List<SegmentColumn> cols, int col, int idx)
    {
        var next = idx + 1;
        if (next < cols[col].Timestamps.Count)
        {
            var ts = cols[col].Timestamps[next];
            heap.Enqueue((col, next, ts), ts);
        }
    }

    /// <summary>
    /// Retention cutoff (now - duration) for the db/rp, or null for infinite retention. Points
    /// older than the cutoff are dropped during compaction so data inside a long-lived shard
    /// does not survive until the whole shard ages out.
    /// </summary>
    private long? GetRetentionCutoffNs(string db, string rp)
    {
        var rpInfo = _manifest.GetRp(db, rp);
        if (rpInfo == null || rpInfo.DurationNs <= 0) return null;
        var nowNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000;
        return nowNs - rpInfo.DurationNs;
    }

    /// <summary>Drop timestamps below the retention cutoff (sorted input; binary search the split).</summary>
    private static (List<long> Ts, List<FieldValue> Vals) ApplyRetentionCutoff(
        List<long> ts, List<FieldValue> vals, long cutoffNs)
    {
        int lo = 0, hi = ts.Count - 1, first = ts.Count;
        while (lo <= hi)
        {
            var mid = (lo + hi) >> 1;
            if (ts[mid] >= cutoffNs) { first = mid; hi = mid - 1; }
            else lo = mid + 1;
        }
        if (first == 0) return (ts, vals);
        if (first == ts.Count) return (new List<long>(), new List<FieldValue>());
        return (ts.GetRange(first, ts.Count - first), vals.GetRange(first, vals.Count - first));
    }

    private static long EstimateGroupBytes(SegmentColumnMeta meta)
    {
        long perPoint = meta.Kind == FieldKind.String ? 8 + 24 : 8 + 8;
        long header = 128 + (meta.Measurement.Length + meta.TagsCanonical.Length + meta.Field.Length) * 2L;
        return meta.PointCount * perPoint + header;
    }

    /// <summary>
    /// Original whole-file materialization path, kept for legacy v2 tag-column normalization where
    /// points must be rebuilt before the merge.
    /// </summary>
    private bool CompactShardMaterialized(string db, string rp, ShardGroupInfo shard, int outputLevel,
        List<FileCandidate> orderedInputs, long gcFloor, long tombstoneVersionAtRead)
    {
        var allColumns = new List<SegmentColumn>();
        foreach (var file in orderedInputs)
        {
            try { allColumns.AddRange(NormalizeLegacyTagColumns(SegmentReader.ReadSegment(file.Path))); }
            catch (Exception ex) { _health?.RecordFailure("compaction_read", ex); return false; }
        }

        if (allColumns.Count == 0) return false;

        var retentionCutoffNs = GetRetentionCutoffNs(db, rp);
        var filtered = new List<SegmentColumn>();
        foreach (var col in allColumns)
        {
            if (_tombstones.IsColumnDeleted(db, col.Measurement, col.TagsCanonical, col.MinTime, col.MaxTime))
                continue;

            var (ts, vals) = _tombstones.FilterColumnDeleted(db, col.Measurement, col.TagsCanonical, col.Timestamps, col.Values);
            if (retentionCutoffNs.HasValue)
                (ts, vals) = ApplyRetentionCutoff(ts, vals, retentionCutoffNs.Value);
            if (ts.Count == 0) continue;

            filtered.Add(new SegmentColumn(
                col.Measurement, col.TagsCanonical, col.Field, col.Kind,
                ts[0], ts[^1], ts, vals, col.Stats));
        }

        if (filtered.Count == 0)
        {
            var emptied = FinalizeCompaction(db, rp, shard.Id, orderedInputs, []);
            if (emptied) TryRetireTombstones(db, rp, shard, orderedInputs, gcFloor, tombstoneVersionAtRead);
            return emptied;
        }

        var merged = MergeColumns(filtered);
        if (merged.Count == 0) return false;

        _schema.ValidateAndRegisterColumns(db, merged);
        var shardDir = _shardManager.ShardDir(db, rp, shard.Id);
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        // ponytail: honor MaxSegmentFileBytes on the *output* too. A merge of N 512MB inputs could
        // otherwise produce a multi-GB L1/L2 file that violates the configured size cap. Split the
        // merged columns (kept column-aligned so each output file stays a valid segment) into chunks
        // of at most the cap. We apply the same tail-merge rule as the flush path: once the current
        // chunk clears the fill floor and the *remaining* columns are smaller than that floor, we
        // stop opening new files and pack them in, so compaction never emits a tiny trailing .seg.
        var mergedPaths = new List<string>();
        var chunk = new List<SegmentColumn>(Math.Min(merged.Count, 1 << 14));
        long chunkBytes = 0;
        long remaining = 0;
        foreach (var c in merged) remaining += EstimateColumnBytes(c);
        var fillFloor = (long)(_maxSegmentFileBytes * _segmentFillRatio);
        for (int i = 0; i < merged.Count; i++)
        {
            var col = merged[i];
            var colBytes = EstimateColumnBytes(col);
            remaining -= colBytes;
            if (chunk.Count > 0 && chunkBytes >= fillFloor && chunkBytes + colBytes > _maxSegmentFileBytes
                && remaining >= fillFloor)
            {
                WriteMergedChunk(shardDir, outputLevel, nowMs, chunk, mergedPaths);
                chunk.Clear();
                chunkBytes = 0;
            }
            chunk.Add(col);
            chunkBytes += colBytes;
        }
        if (chunk.Count > 0)
            WriteMergedChunk(shardDir, outputLevel, nowMs, chunk, mergedPaths);

        _manifest.UpdateIndexes(db, merged
            .GroupBy(c => (c.Measurement, c.TagsCanonical))
            .Select(g => (g.Key.Measurement, g.Key.TagsCanonical, ParseTags(g.Key.TagsCanonical))));
        var finalized = FinalizeCompaction(db, rp, shard.Id, orderedInputs, mergedPaths);
        if (finalized) TryRetireTombstones(db, rp, shard, orderedInputs, gcFloor, tombstoneVersionAtRead);
        return finalized;
    }

    /// <summary>
    /// Tombstone GC: when a pass rewrites EVERY segment file currently registered for the shard,
    /// the deletes inside the rewritten range were physically applied and the corresponding
    /// coverage can be retired, ending the permanent read-time filtering cost after a DELETE.
    /// Coverage at or above the in-flight-flush floor is kept — that data may still land in a
    /// segment written after this task's snapshot was taken.
    /// </summary>
    private void TryRetireTombstones(string db, string rp, ShardGroupInfo shard, List<FileCandidate> inputs, long gcFloor, long tombstoneVersionAtRead)
    {
        if (_tombstones.CurrentVersion != tombstoneVersionAtRead) return;
        if (!FullyRewroteShard(db, rp, shard, inputs)) return;
        if (inputs.Any(f => !f.MinTimeNs.HasValue || !f.MaxTimeNs.HasValue)) return;

        var rewrittenMin = inputs.Min(f => f.MinTimeNs!.Value);
        var rewrittenMax = inputs.Max(f => f.MaxTimeNs!.Value);
        var retireMax = Math.Min(rewrittenMax, gcFloor - 1);
        if (retireMax < rewrittenMin) return;

        _tombstones.RemoveCoveredRange(db, rewrittenMin, retireMax);
    }

    private bool FullyRewroteShard(string db, string rp, ShardGroupInfo shard, List<FileCandidate> inputs)
    {
        var inputPaths = new HashSet<string>(inputs.Select(f => f.Path), StringComparer.OrdinalIgnoreCase);
        var shardDir = _shardManager.ShardDir(db, rp, shard.Id);
        return shard.SegmentFiles
            .Select(name => Path.Combine(shardDir, name))
            .All(path => !File.Exists(path) || inputPaths.Contains(path));
    }

    private static long EstimateColumnBytes(SegmentColumn col)
    {
        // ponytail: columnar on-disk estimate (matches SegmentWriter's delta/column encoding). Per
        // point we store an 8-byte timestamp plus an 8-byte value (non-string field; string fields
        // are rarer and we overestimate them slightly to stay safe). The series header (measurement,
        // tags, field name) is stored once for the whole column, so it is amortized here. Using the
        // old per-point 64-byte estimate over-estimated ~8x and could emit many tiny merged files.
        long points = col.Timestamps.Count;
        long perPoint = col.Kind == FieldKind.String
            ? 8 + 24 + (col.Values.Count > 0 ? (col.Values[0].String?.Length ?? 0) * 2L : 0)
            : 8 + 8;
        // ponytail: the on-disk layout stores each column TWICE — once in the data section (strings,
        // kind, min/max ts, count, codec bytes, block length prefixes, block stats) and again in the
        // metadata footer (strings, kind, min/max, count, codec bytes, stats) — plus shared per-file
        // magic/version/footer/CRC bytes. The old 64-byte base ignored the metadata copy and file
        // overhead, underestimating small columns ~1.7x, which let tail-merged output exceed
        // MaxSegmentFileBytes. 128 base + UTF-8-length strings x2 (data + metadata) tracks reality
        // closely for tiny columns while staying negligible for payload-dominated large ones.
        long header = 128 + (col.Measurement.Length + col.TagsCanonical.Length + col.Field.Length) * 2L;
        return points * perPoint + header;
    }

    private void WriteMergedChunk(string shardDir, int outputLevel, long nowMs, List<SegmentColumn> chunk, List<string> mergedPaths)
    {
        var path = Path.Combine(shardDir, $"l{outputLevel}-{nowMs}-{Guid.NewGuid():N}.seg");
        SegmentWriter.WriteColumns(path, chunk);
        mergedPaths.Add(path);

        if (_maxWriteBytesPerSecond > 0)
            ThrottleAfterWrite(new FileInfo(path).Length);
    }

    /// <summary>
    /// ponytail: fixed-window I/O budget. After each merged chunk write, account its size against a
    /// one-second window; when the window budget is exhausted, sleep out the remainder so background
    /// compaction cannot saturate disk at the expense of foreground reads/writes.
    /// </summary>
    private void ThrottleAfterWrite(long bytesWritten)
    {
        var now = Environment.TickCount64;
        if (_throttleWindowStartTicks == 0 || now - _throttleWindowStartTicks >= 1000)
        {
            _throttleWindowStartTicks = now;
            _throttleWindowBytes = 0;
        }

        _throttleWindowBytes += bytesWritten;
        if (_throttleWindowBytes <= _maxWriteBytesPerSecond)
            return;

        var elapsed = now - _throttleWindowStartTicks;
        if (elapsed < 1000)
            Thread.Sleep((int)(1000 - elapsed));

        // Start a fresh window after the pause.
        _throttleWindowStartTicks = Environment.TickCount64;
        _throttleWindowBytes = 0;
    }

    private bool FinalizeCompaction(string db, string rp, int shardId, List<FileCandidate> sourceFiles, List<string> mergedPaths)
    {
        try
        {
            _manifest.ReplaceSegmentsInShard(
                db,
                rp,
                shardId,
                sourceFiles.Select(f => f.Path),
                mergedPaths);
        }
        catch (Exception ex)
        {
            _health?.RecordFailure("compaction_manifest", ex);
            foreach (var p in mergedPaths)
                TryDelete(p);
            return false;
        }

        foreach (var source in sourceFiles)
            TryDelete(source.Path);

        Interlocked.Increment(ref _totalTasks);
        Interlocked.Add(ref _totalSegmentsMerged, sourceFiles.Count);
        return true;
    }

    private static int InferLevel(string segmentPath)
    {
        var name = Path.GetFileName(segmentPath);
        if (name.StartsWith("l2-", StringComparison.OrdinalIgnoreCase)) return 2;
        if (name.StartsWith("l1-", StringComparison.OrdinalIgnoreCase)) return 1;
        return 0;
    }

    private void TryDelete(string path)
    {
        try { File.Delete(path); }
        catch (Exception ex) { _health?.RecordFailure("compaction_delete", ex); }
    }

    private long SafeLength(string path)
    {
        try { return new FileInfo(path).Length; }
        catch (Exception ex) { _health?.RecordFailure("compaction_file_length", ex); return 0; }
    }

    private DateTime SafeWriteTime(string path)
    {
        try { return File.GetLastWriteTimeUtc(path); }
        catch (Exception ex) { _health?.RecordFailure("compaction_file_timestamp", ex); return DateTime.MinValue; }
    }

    private static List<SegmentColumn> MergeColumns(List<SegmentColumn> columns)
    {
        var groups = columns.GroupBy(c => (c.Measurement, c.TagsCanonical, c.Field));
        var result = new List<SegmentColumn>();

        foreach (var g in groups)
        {
            var tsMap = new SortedDictionary<long, FieldValue>();
            var kind = g.First().Kind;

            foreach (var col in g)
                for (int i = 0; i < col.Timestamps.Count; i++)
                    tsMap[col.Timestamps[i]] = col.Values[i];

            var ts = tsMap.Keys.ToList();
            var vals = tsMap.Values.ToList();

            result.Add(new SegmentColumn(
                g.Key.Measurement, g.Key.TagsCanonical, g.Key.Field, kind,
                ts[0], ts[^1], ts, vals));
        }

        return result;
    }

    private static List<SegmentColumn> NormalizeLegacyTagColumns(List<SegmentColumn> columns)
    {
        if (!columns.Any(column => column.TagsCanonical.Length == 0
            && column.Field == "tag"
            && column.Kind == FieldKind.String))
            return columns;

        var points = ColumnsToPoints(columns);
        var normalized = false;
        foreach (var point in points)
        {
            if (point.Tags.Count != 0
                || !point.Fields.Remove("tag", out var tag)
                || tag.Kind != FieldKind.String
                || string.IsNullOrEmpty(tag.String))
                continue;
            point.Tags["tag"] = tag.String;
            normalized = true;
        }

        return normalized
            ? SegmentWriter.BuildColumns(points.Select(point => (point, SeriesKey.From(point))))
            : columns;
    }

    private static List<Point> ColumnsToPoints(List<SegmentColumn> columns)
    {
        var map = new Dictionary<(string Measurement, string Tags, long Timestamp), Dictionary<string, FieldValue>>();

        foreach (var col in columns)
        {
            for (int i = 0; i < col.Timestamps.Count; i++)
            {
                var key = (col.Measurement, col.TagsCanonical, col.Timestamps[i]);
                if (!map.TryGetValue(key, out var fields))
                {
                    fields = new(StringComparer.Ordinal);
                    map[key] = fields;
                }
                fields[col.Field] = col.Values[i];
            }
        }

        return map.Select(kv => new Point
        {
            Measurement = kv.Key.Measurement,
            Tags = ParseTags(kv.Key.Tags),
            TimestampNs = kv.Key.Timestamp,
            Fields = kv.Value
        }).ToList();
    }

    private static Dictionary<string, string> ParseTags(string s)
    {
        var d = new Dictionary<string, string>(StringComparer.Ordinal);
        if (string.IsNullOrEmpty(s)) return d;
        foreach (var p in s.Split(','))
        {
            var i = p.IndexOf('=');
            if (i > 0) d[p[..i]] = p[(i + 1)..];
        }
        return d;
    }

    private sealed record CompactionTask(string Db, string Rp, ShardGroupInfo Shard, int Level, List<FileCandidate> Files);
    private sealed record FileCandidate(string Path, long Length, DateTime LastWriteUtc, int Level, long? MinTimeNs, long? MaxTimeNs);
}

public sealed class CompactionStatsSnapshot
{
    public long TotalRuns { get; set; }
    public long TotalTasks { get; set; }
    public long TotalSegmentsMerged { get; set; }
    public bool Running { get; set; }
    public int QueuedTasks { get; set; }
    public int BacklogTasks { get; set; }
    public DateTimeOffset? LastRunUtc { get; set; }
}

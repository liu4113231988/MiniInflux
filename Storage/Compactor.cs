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
    private readonly int _maxL0Segments;
    private readonly int _maxL1Segments;
    private readonly long _maxL0Bytes;
    private readonly long _maxL1Bytes;
    private readonly int _minFilesPerCompaction;
    private readonly int _maxPassesPerRun;
    private readonly object _compactionLock = new();
    private long _totalRuns;
    private long _totalTasks;
    private long _totalSegmentsMerged;
    private int _running;
    private int _queuedTasks;
    private DateTimeOffset? _lastRunUtc;

    public Compactor(Manifest manifest, ShardManager shardManager, TombstoneStore tombstones,
        SchemaRegistry schema, int maxL0Segments = 10, int maxL1Segments = 4,
        long maxL0Bytes = 32 * 1024 * 1024, long maxL1Bytes = 128 * 1024 * 1024,
        int minFilesPerCompaction = 2, int maxPassesPerRun = 8)
    {
        _manifest = manifest;
        _shardManager = shardManager;
        _tombstones = tombstones;
        _schema = schema;
        _maxL0Segments = maxL0Segments;
        _maxL1Segments = maxL1Segments;
        _maxL0Bytes = maxL0Bytes;
        _maxL1Bytes = maxL1Bytes;
        _minFilesPerCompaction = Math.Max(2, minFilesPerCompaction);
        _maxPassesPerRun = Math.Max(1, maxPassesPerRun);
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

    public CompactionStatsSnapshot GetStats() => new()
    {
        TotalRuns = Interlocked.Read(ref _totalRuns),
        TotalTasks = Interlocked.Read(ref _totalTasks),
        TotalSegmentsMerged = Interlocked.Read(ref _totalSegmentsMerged),
        Running = Interlocked.CompareExchange(ref _running, 0, 0) == 1,
        QueuedTasks = Interlocked.CompareExchange(ref _queuedTasks, 0, 0),
        LastRunUtc = _lastRunUtc
    };

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

    private static List<FileCandidate> DescribeFiles(List<string> segFiles) =>
        segFiles.Select(DescribeFile)
            .OrderBy(x => x.LastWriteUtc)
            .ThenBy(x => x.Path, StringComparer.OrdinalIgnoreCase)
            .ToList();

    private static FileCandidate DescribeFile(string path)
    {
        long? minTimeNs = null;
        long? maxTimeNs = null;
        try
        {
            var metadata = SegmentReader.ReadMetadata(path);
            if (metadata.Count > 0)
            {
                minTimeNs = metadata.Min(m => m.MinTime);
                maxTimeNs = metadata.Max(m => m.MaxTime);
            }
        }
        catch { }

        return new FileCandidate(path, SafeLength(path), SafeWriteTime(path), InferLevel(path), minTimeNs, maxTimeNs);
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

            if (selected.Count >= requiredFiles && (selected.Count >= segmentThreshold || (byteThreshold > 0 && selectedBytes >= byteThreshold)))
                break;
        }

        if (selected.Count < requiredFiles)
            selected = currentLevelFiles.Take(requiredFiles).ToList();

        var includeAllOverlaps = selected.Any(file => !file.MinTimeNs.HasValue || !file.MaxTimeNs.HasValue);
        if (overlapLevelFiles.Count > 0)
        {
            var selectedMinTime = selected.Where(f => f.MinTimeNs.HasValue).Select(f => f.MinTimeNs!.Value).DefaultIfEmpty(long.MinValue).Min();
            var selectedMaxTime = selected.Where(f => f.MaxTimeNs.HasValue).Select(f => f.MaxTimeNs!.Value).DefaultIfEmpty(long.MaxValue).Max();
            foreach (var overlap in overlapLevelFiles)
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
        Interlocked.Increment(ref _totalTasks);
        Interlocked.Add(ref _totalSegmentsMerged, segFiles.Count);

        var orderedInputs = segFiles
            .OrderByDescending(f => f.Level)
            .ThenBy(f => f.LastWriteUtc)
            .ThenBy(f => f.Path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var allColumns = new List<SegmentColumn>();
        foreach (var file in orderedInputs)
        {
            try { allColumns.AddRange(SegmentReader.ReadSegment(file.Path)); }
            catch { }
        }

        if (allColumns.Count == 0) return false;

        var filtered = new List<SegmentColumn>();
        foreach (var col in allColumns)
        {
            if (_tombstones.IsColumnDeleted(db, col.Measurement, col.TagsCanonical, col.MinTime, col.MaxTime))
                continue;

            var (ts, vals) = _tombstones.FilterColumnDeleted(db, col.Measurement, col.TagsCanonical, col.Timestamps, col.Values);
            if (ts.Count == 0) continue;

            filtered.Add(new SegmentColumn(
                col.Measurement, col.TagsCanonical, col.Field, col.Kind,
                ts[0], ts[^1], ts, vals, col.Stats));
        }

        if (filtered.Count == 0)
        {
            return FinalizeCompaction(db, rp, shard.Id, orderedInputs, null);
        }

        var merged = MergeColumns(filtered);
        if (merged.Count == 0) return false;

        _schema.ValidateAndRegisterColumns(db, merged);
        var shardDir = _shardManager.ShardDir(db, rp, shard.Id);
        var mergedPath = Path.Combine(shardDir, $"l{outputLevel}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}.seg");
        SegmentWriter.WriteColumns(mergedPath, merged);
        _manifest.UpdateIndexes(db, merged
            .GroupBy(c => (c.Measurement, c.TagsCanonical))
            .Select(g => (g.Key.Measurement, g.Key.TagsCanonical, ParseTags(g.Key.TagsCanonical))));
        return FinalizeCompaction(db, rp, shard.Id, orderedInputs, mergedPath);
    }

    private bool FinalizeCompaction(string db, string rp, int shardId, List<FileCandidate> sourceFiles, string? mergedPath)
    {
        if (!TryStageSourceFiles(sourceFiles, out var stagedMoves))
        {
            if (!string.IsNullOrWhiteSpace(mergedPath))
                TryDelete(mergedPath);
            return false;
        }

        try
        {
            _manifest.ReplaceSegmentsInShard(
                db,
                rp,
                shardId,
                sourceFiles.Select(f => f.Path),
                string.IsNullOrWhiteSpace(mergedPath) ? [] : [mergedPath]);
        }
        catch
        {
            RollbackStageMoves(stagedMoves);
            if (!string.IsNullOrWhiteSpace(mergedPath))
                TryDelete(mergedPath);
            return false;
        }

        foreach (var move in stagedMoves)
            TryDelete(move.StagedPath);

        return true;
    }

    private static int InferLevel(string segmentPath)
    {
        var name = Path.GetFileName(segmentPath);
        if (name.StartsWith("l2-", StringComparison.OrdinalIgnoreCase)) return 2;
        if (name.StartsWith("l1-", StringComparison.OrdinalIgnoreCase)) return 1;
        return 0;
    }

    private static void TryDelete(string path)
    {
        try { File.Delete(path); } catch { }
    }

    private static bool TryStageSourceFiles(List<FileCandidate> sourceFiles, out List<StagedMove> stagedMoves)
    {
        stagedMoves = [];
        foreach (var source in sourceFiles)
        {
            var stagedPath = source.Path + $".compacted-{Guid.NewGuid():N}";
            try
            {
                File.Move(source.Path, stagedPath);
                stagedMoves.Add(new StagedMove(source.Path, stagedPath));
            }
            catch
            {
                RollbackStageMoves(stagedMoves);
                stagedMoves = [];
                return false;
            }
        }

        return true;
    }

    private static void RollbackStageMoves(List<StagedMove> stagedMoves)
    {
        for (int i = stagedMoves.Count - 1; i >= 0; i--)
        {
            try
            {
                if (File.Exists(stagedMoves[i].StagedPath))
                    File.Move(stagedMoves[i].StagedPath, stagedMoves[i].OriginalPath);
            }
            catch { }
        }
    }

    private static long SafeLength(string path)
    {
        try { return new FileInfo(path).Length; }
        catch { return 0; }
    }

    private static DateTime SafeWriteTime(string path)
    {
        try { return File.GetLastWriteTimeUtc(path); }
        catch { return DateTime.MinValue; }
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
    private sealed record StagedMove(string OriginalPath, string StagedPath);
}

public sealed class CompactionStatsSnapshot
{
    public long TotalRuns { get; set; }
    public long TotalTasks { get; set; }
    public long TotalSegmentsMerged { get; set; }
    public bool Running { get; set; }
    public int QueuedTasks { get; set; }
    public DateTimeOffset? LastRunUtc { get; set; }
}

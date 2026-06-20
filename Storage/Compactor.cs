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
    private readonly object _compactionLock = new();
    private long _totalRuns;
    private long _totalTasks;
    private long _totalSegmentsMerged;
    private int _running;
    private int _queuedTasks;
    private DateTimeOffset? _lastRunUtc;

    public Compactor(Manifest manifest, ShardManager shardManager, TombstoneStore tombstones,
        SchemaRegistry schema, int maxL0Segments = 10, int maxL1Segments = 4,
        long maxL0Bytes = 32 * 1024 * 1024, long maxL1Bytes = 128 * 1024 * 1024, int minFilesPerCompaction = 2)
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
    }

    public int CompactAll()
    {
        if (!Monitor.TryEnter(_compactionLock)) return 0;
        Interlocked.Exchange(ref _running, 1);
        try
        {
            var tasks = BuildTasks();
            Interlocked.Exchange(ref _queuedTasks, tasks.Count);
            if (tasks.Count == 0) return 0;

            int merged = 0;
            foreach (var task in tasks)
            {
                if (CompactShard(task.Db, task.Rp, task.Shard, task.Level, task.Files))
                    merged++;
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

                    var l0 = segFiles.Where(f => InferLevel(f) == 0).ToList();
                    var l0Task = BuildLevelTask(db, rp.Name, shard, 1, l0, _maxL0Segments, _maxL0Bytes);
                    if (l0Task != null)
                        tasks.Add(l0Task);

                    var l1 = segFiles.Where(f => InferLevel(f) == 1).ToList();
                    var l1Task = BuildLevelTask(db, rp.Name, shard, 2, l1, _maxL1Segments, _maxL1Bytes);
                    if (l1Task != null)
                        tasks.Add(l1Task);
                }
            }
        }
        return tasks;
    }

    private CompactionTask? BuildLevelTask(string db, string rp, ShardGroupInfo shard, int outputLevel, List<string> segFiles, int segmentThreshold, long byteThreshold)
    {
        var requiredFiles = Math.Min(_minFilesPerCompaction, Math.Max(1, segmentThreshold));
        if (segFiles.Count < requiredFiles)
            return null;

        var files = segFiles
            .Select(path => new FileCandidate(path, SafeLength(path), SafeWriteTime(path)))
            .OrderBy(x => x.LastWriteUtc)
            .ThenBy(x => x.Path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var totalBytes = files.Sum(f => f.Length);
        var triggerByCount = files.Count >= segmentThreshold;
        var triggerByBytes = byteThreshold > 0 && totalBytes >= byteThreshold;
        if (!triggerByCount && !triggerByBytes)
            return null;

        var selected = new List<string>();
        long selectedBytes = 0;
        foreach (var file in files)
        {
            selected.Add(file.Path);
            selectedBytes += file.Length;

            if (selected.Count >= requiredFiles && (selected.Count >= segmentThreshold || (byteThreshold > 0 && selectedBytes >= byteThreshold)))
                break;
        }

        if (selected.Count < requiredFiles)
            selected = files.Take(requiredFiles).Select(f => f.Path).ToList();

        return new CompactionTask(db, rp, shard, outputLevel, selected);
    }

    private bool CompactShard(string db, string rp, ShardGroupInfo shard, int outputLevel, List<string> segFiles)
    {
        if (segFiles.Count == 0) return false;
        Interlocked.Increment(ref _totalTasks);
        Interlocked.Add(ref _totalSegmentsMerged, segFiles.Count);

        var allColumns = new List<SegmentColumn>();
        foreach (var f in segFiles)
        {
            try { allColumns.AddRange(SegmentReader.ReadSegment(f)); }
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
            foreach (var f in segFiles) TryDelete(f);
            _manifest.RemoveSegmentsFromShard(db, rp, shard.Id, segFiles);
            return true;
        }

        var merged = MergeColumns(filtered);
        var points = ColumnsToPoints(merged);
        if (points.Count == 0) return false;

        var shardDir = _shardManager.ShardDir(db, rp, shard.Id);
        var mergedPath = Path.Combine(shardDir, $"l{outputLevel}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}.seg");
        SegmentWriter.WriteSegment(mergedPath, points);

        foreach (var f in segFiles) TryDelete(f);
        _manifest.RemoveSegmentsFromShard(db, rp, shard.Id, segFiles);
        _manifest.AddSegmentToShard(db, rp, shard.Id, mergedPath);
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

    private sealed record CompactionTask(string Db, string Rp, ShardGroupInfo Shard, int Level, List<string> Files);
    private sealed record FileCandidate(string Path, long Length, DateTime LastWriteUtc);
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

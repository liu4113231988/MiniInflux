using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Manages shard groups: routes writes to correct shard based on timestamp,
/// provides shard-aware reads, and handles shard lifecycle.
///
/// Directory layout:
///   data/db/{db}/{rp}/shards/{shardId}/
///     000001.seg
///     000002.seg
/// </summary>
public sealed class ShardManager
{
    private readonly string _dataPath;
    private readonly Manifest _manifest;
    private int _nextShardId;

    public ShardManager(string dataPath, Manifest manifest)
    {
        _dataPath = dataPath;
        _manifest = manifest;
        InitNextShardId();
    }

    private void InitNextShardId()
    {
        var max = 0;
        foreach (var db in _manifest.ListDatabases())
        {
            foreach (var (_, shard) in _manifest.GetAllShards(db))
                if (shard.Id > max) max = shard.Id;
        }
        _nextShardId = max + 1;
    }

    /// <summary>
    /// Get or create a shard for the given db/rp/timestampNs.
    /// Returns (shardId, shardDirPath).
    /// </summary>
    public (int ShardId, string ShardDir) GetOrCreateShard(string db, string rp, long timestampNs)
    {
        var (shardId, shardDir, _, _) = GetOrCreateShardWithRange(db, rp, timestampNs);
        return (shardId, shardDir);
    }

    /// <summary>
    /// Get or create a shard, also returning its time range so callers routing many points can
    /// cache the last-resolved shard instead of re-scanning the manifest per point.
    /// </summary>
    public (int ShardId, string ShardDir, long StartTimeNs, long EndTimeNs) GetOrCreateShardWithRange(string db, string rp, long timestampNs)
    {
        // Look for existing shard that covers this timestamp
        var shards = _manifest.GetShards(db, rp);
        foreach (var s in shards)
        {
            if (timestampNs >= s.StartTimeNs && timestampNs < s.EndTimeNs)
            {
                var dir = ShardDir(db, rp, s.Id);
                Directory.CreateDirectory(dir);
                return (s.Id, dir, s.StartTimeNs, s.EndTimeNs);
            }
        }

        // Create new shard
        var shardDurationNs = GetShardDurationNs(db, rp);
        var shardStart = timestampNs / shardDurationNs * shardDurationNs;
        var shardEnd = shardStart + shardDurationNs;
        var id = Interlocked.Increment(ref _nextShardId);

        var shard = new ShardGroupInfo
        {
            Id = id,
            StartTimeNs = shardStart,
            EndTimeNs = shardEnd
        };
        _manifest.AddShardGroup(db, rp, shard);

        var shardDir = ShardDir(db, rp, id);
        Directory.CreateDirectory(shardDir);
        return (id, shardDir, shardStart, shardEnd);
    }

    /// <summary>
    /// Get the shard directory for a given db/rp/shardId.
    /// </summary>
    public string ShardDir(string db, string rp, int shardId) =>
        Path.Combine(_dataPath, "db", db, rp, "shards", shardId.ToString("D6"));

    /// <summary>
    /// List all segment files across relevant shards for a db/rp, filtered by time range.
    /// Returns (segmentFilePath, shardInfo) pairs.
    /// </summary>
    public IReadOnlyList<(string SegPath, ShardGroupInfo Shard)> ListSegments(
        string db, string rp, long? minTimeNs = null, long? maxTimeNs = null)
    {
        // Precompute the sort keys once per segment. The previous LINQ chain re-parsed the file name
        // (GetFileName / IndexOf / long.TryParse) inside the comparator, so parsing ran O(n log n)
        // times per query instead of O(n).
        var keyed = new List<(int Level, long Sequence, string SegPath, ShardGroupInfo Shard)>();
        var shards = _manifest.GetShards(db, rp, minTimeNs, maxTimeNs);
        foreach (var shard in shards)
        {
            var dir = ShardDir(db, rp, shard.Id);
            if (!Directory.Exists(dir)) continue;

            foreach (var file in shard.SegmentFiles)
            {
                var seg = Path.Combine(dir, file);
                keyed.Add((InferSegmentLevel(seg), InferSegmentSequence(seg), seg, shard));
            }
        }

        keyed.Sort(static (a, b) =>
        {
            var cmp = b.Level.CompareTo(a.Level); // level descending
            if (cmp != 0) return cmp;
            cmp = a.Sequence.CompareTo(b.Sequence); // sequence ascending
            if (cmp != 0) return cmp;
            return string.Compare(a.SegPath, b.SegPath, StringComparison.OrdinalIgnoreCase);
        });

        var result = new List<(string SegPath, ShardGroupInfo Shard)>(keyed.Count);
        foreach (var item in keyed)
            result.Add((item.SegPath, item.Shard));
        return result;
    }

    /// <summary>
    /// Register a new segment file in the manifest after a successful flush.
    /// </summary>
    public void RegisterSegment(string db, string rp, int shardId, string segmentPath)
    {
        _manifest.AddSegmentToShard(db, rp, shardId, segmentPath);
    }

    /// <summary>
    /// Delete expired shards based on retention policies.
    /// Returns count of deleted shards.
    /// </summary>
    public int CleanupExpiredShards(long nowNs)
    {
        var expired = _manifest.GetExpiredShards(_dataPath, nowNs);
        int count = 0;
        foreach (var (db, rp, shardId, dir) in expired)
        {
            try
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                _manifest.RemoveShardGroup(db, rp, shardId);
                count++;
            }
            catch { /* best effort */ }
        }
        return count;
    }

    /// <summary>
    /// Get the shard duration in ns based on the RP duration.
    /// </summary>
    private long GetShardDurationNs(string db, string rp)
    {
        var rpInfo = _manifest.GetRp(db, rp);
        if (rpInfo?.ShardDurationNs > 0) return rpInfo.ShardDurationNs;
        var rpDuration = rpInfo?.DurationNs ?? 0;

        if (rpDuration <= 0) return 604_800_000_000_000L;          // infinite RP -> 7d shards
        if (rpDuration < 172_800_000_000_000L) return 3_600_000_000_000L; // < 2d -> 1h
        if (rpDuration <= 604_800_000_000_000L) return 86_400_000_000_000L; // <= 7d -> 1d
        if (rpDuration <= 7_776_000_000_000_000L) return 604_800_000_000_000L; // <= 90d -> 7d
        return 2_592_000_000_000_000L; // > 90d -> 30d
    }

    /// <summary>
    /// Get all shard directories for a db/rp (for compaction or full scans).
    /// </summary>
    public IReadOnlyList<ShardGroupInfo> GetAllShards(string db, string rp) =>
        _manifest.GetShards(db, rp);

    private static int InferSegmentLevel(string segmentPath)
    {
        var name = Path.GetFileName(segmentPath);
        if (name.StartsWith("l2-", StringComparison.OrdinalIgnoreCase)) return 2;
        if (name.StartsWith("l1-", StringComparison.OrdinalIgnoreCase)) return 1;
        return 0;
    }

    private static long InferSegmentSequence(string segmentPath)
    {
        var name = Path.GetFileNameWithoutExtension(segmentPath);
        var start = name.StartsWith('l') ? name.IndexOf('-') + 1 : 0;
        var end = name.IndexOf('-', start);
        return end > start && long.TryParse(name.AsSpan(start, end - start), out var sequence)
            ? sequence
            : long.MinValue;
    }
}

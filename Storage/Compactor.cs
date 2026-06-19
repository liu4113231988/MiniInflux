using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Simple L0->L1 compactor. Merges small segments within a shard to reduce file count.
/// Also applies tombstones during compaction to physically remove deleted data.
/// </summary>
public sealed class Compactor
{
    private readonly Manifest _manifest;
    private readonly ShardManager _shardManager;
    private readonly TombstoneStore _tombstones;
    private readonly SchemaRegistry _schema;
    private readonly int _maxSegmentsPerShard;
    private readonly object _compactionLock = new();

    public Compactor(Manifest manifest, ShardManager shardManager, TombstoneStore tombstones,
        SchemaRegistry schema, int maxSegmentsPerShard = 10)
    {
        _manifest = manifest;
        _shardManager = shardManager;
        _tombstones = tombstones;
        _schema = schema;
        _maxSegmentsPerShard = maxSegmentsPerShard;
    }

    /// <summary>
    /// Run compaction across all databases and shards.
    /// Call periodically from a background timer.
    /// </summary>
    public int CompactAll()
    {
        if (!Monitor.TryEnter(_compactionLock)) return 0;
        try
        {
            int totalMerged = 0;
            foreach (var db in _manifest.ListDatabases())
            {
                foreach (var rp in _manifest.ListRetentionPolicies(db))
                {
                    var shards = _manifest.GetShards(db, rp.Name);
                    foreach (var shard in shards)
                    {
                        var segFiles = shard.SegmentFiles
                            .Select(f => Path.Combine(_shardManager.ShardDir(db, rp.Name, shard.Id), f))
                            .Where(File.Exists)
                            .ToList();

                        if (segFiles.Count >= _maxSegmentsPerShard)
                        {
                            CompactShard(db, rp.Name, shard, segFiles);
                            totalMerged++;
                        }
                    }
                }
            }
            return totalMerged;
        }
        finally { Monitor.Exit(_compactionLock); }
    }

    /// <summary>
    /// Compact a single shard: merge all segments into one, applying tombstones.
    /// </summary>
    private void CompactShard(string db, string rp, ShardGroupInfo shard, List<string> segFiles)
    {
        var allColumns = new List<SegmentColumn>();

        // Read all segments
        foreach (var f in segFiles)
        {
            try { allColumns.AddRange(SegmentReader.ReadSegment(f)); }
            catch { /* skip corrupted */ }
        }

        if (allColumns.Count == 0) return;

        // Apply tombstones: filter out deleted columns/data
        var filtered = new List<SegmentColumn>();
        foreach (var col in allColumns)
        {
            if (_tombstones.IsColumnDeleted(db, col.Measurement, col.TagsCanonical, col.MinTime, col.MaxTime))
                continue;

            var (ts, vals) = _tombstones.FilterColumnDeleted(
                db, col.Measurement, col.TagsCanonical, col.Timestamps, col.Values);

            if (ts.Count == 0) continue;

            filtered.Add(new SegmentColumn(
                col.Measurement, col.TagsCanonical, col.Field, col.Kind,
                ts[0], ts[^1], ts, vals));
        }

        if (filtered.Count == 0)
        {
            // All data deleted, remove segment files and clear shard from manifest
            foreach (var f in segFiles) try { File.Delete(f); } catch { }
            foreach (var f in shard.SegmentFiles)
                _manifest.RemoveShardGroup(db, rp, shard.Id); // will be re-added if needed
            return;
        }

        // Deduplicate: same measurement+tags+field+timestamp -> last write wins
        var merged = MergeColumns(filtered);

        // Convert columns back to points for writing
        var points = ColumnsToPoints(merged);

        if (points.Count == 0) return;

        // Write merged segment atomically
        var shardDir = _shardManager.ShardDir(db, rp, shard.Id);
        var mergedPath = Path.Combine(shardDir,
            $"compacted-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}.seg");

        SegmentWriter.WriteSegment(mergedPath, points);

        // Delete old segment files
        foreach (var f in segFiles)
        {
            try { File.Delete(f); } catch { /* best effort */ }
        }

        // Update manifest: clear old segments, add new one
        shard.SegmentFiles.Clear();
        _manifest.AddSegmentToShard(db, rp, shard.Id, mergedPath);
    }

    /// <summary>
    /// Merge columns with duplicate keys (measurement+tags+field+timestamp).
    /// Last-write-wins semantics: later values override earlier ones.
    /// </summary>
    private static List<SegmentColumn> MergeColumns(List<SegmentColumn> columns)
    {
        // Group by series+field
        var groups = columns.GroupBy(c => (c.Measurement, c.TagsCanonical, c.Field));
        var result = new List<SegmentColumn>();

        foreach (var g in groups)
        {
            // Merge all timestamps and values from columns in this group
            var tsMap = new SortedDictionary<long, FieldValue>();
            var kind = g.First().Kind;

            foreach (var col in g)
            {
                for (int i = 0; i < col.Timestamps.Count; i++)
                    tsMap[col.Timestamps[i]] = col.Values[i]; // last write wins
            }

            var ts = tsMap.Keys.ToList();
            var vals = tsMap.Values.ToList();

            result.Add(new SegmentColumn(
                g.Key.Measurement, g.Key.TagsCanonical, g.Key.Field, kind,
                ts[0], ts[^1], ts, vals));
        }

        return result;
    }

    /// <summary>
    /// Convert segment columns back to points for re-writing.
    /// </summary>
    private static List<Point> ColumnsToPoints(List<SegmentColumn> columns)
    {
        var map = new Dictionary<(string Measurement, string Tags, long Timestamp),
            Dictionary<string, FieldValue>>();

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
}

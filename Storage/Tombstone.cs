using System.Text.Json;
using System.Text.Json.Serialization;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// A delete marker: measurement-level, series-level, or time-range-level.
/// </summary>
public sealed class Tombstone
{
    public string Measurement { get; set; } = "";
    public string? TagsCanonical { get; set; } // null = all series in measurement
    public long? MinTimeNs { get; set; }       // null = from beginning
    public long? MaxTimeNs { get; set; }       // null = to end
    public long CreatedAtNs { get; set; }
}

/// <summary>
/// Manages tombstone (delete markers) per database.
/// Tombstones are persisted to data/tombstones/{db}.json.
/// During reads, tombstones filter out deleted data.
/// During compaction, tombstones physically remove data.
/// </summary>
public sealed class TombstoneStore
{
    private readonly string _dir;
    private readonly object _lock = new();
    private readonly Dictionary<string, List<Tombstone>> _tombstones = new(StringComparer.Ordinal);

    public TombstoneStore(string dataPath)
    {
        _dir = Path.Combine(dataPath, "tombstones");
        LoadAll();
    }

    /// <summary>
    /// Add a measurement-level delete (all series in measurement within optional time range).
    /// </summary>
    public void AddMeasurementDelete(string db, string measurement, long? minTime = null, long? maxTime = null)
    {
        lock (_lock)
        {
            if (AddLocked(db, new Tombstone
            {
                Measurement = measurement,
                MinTimeNs = minTime,
                MaxTimeNs = maxTime,
                CreatedAtNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000
            }))
                Save(db);
        }
    }

    /// <summary>
    /// Add a series-level delete (specific tag set within optional time range).
    /// </summary>
    public void AddSeriesDelete(string db, string measurement, string tagsCanonical,
        long? minTime = null, long? maxTime = null)
    {
        lock (_lock)
        {
            if (AddLocked(db, new Tombstone
            {
                Measurement = measurement,
                TagsCanonical = tagsCanonical,
                MinTimeNs = minTime,
                MaxTimeNs = maxTime,
                CreatedAtNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000
            }))
                Save(db);
        }
    }

    public void AddSeriesDeletes(string db, string measurement, IEnumerable<(string TagsCanonical, long? MinTime, long? MaxTime)> deletes)
    {
        lock (_lock)
        {
            var changed = false;
            var createdAtNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000;
            foreach (var delete in deletes)
            {
                changed |= AddLocked(db, new Tombstone
                {
                    Measurement = measurement,
                    TagsCanonical = delete.TagsCanonical,
                    MinTimeNs = delete.MinTime,
                    MaxTimeNs = delete.MaxTime,
                    CreatedAtNs = createdAtNs
                });
            }

            if (changed)
                Save(db);
        }
    }

    /// <summary>
    /// Drop all tombstones for a database (used when DROP DATABASE is called).
    /// </summary>
    public void DropDatabase(string db)
    {
        lock (_lock)
        {
            _tombstones.Remove(db);
            var path = Path.Combine(_dir, $"{db}.json");
            if (File.Exists(path)) try { File.Delete(path); } catch { /* best effort */ }
        }
    }

    /// <summary>
    /// Check if a specific column is fully covered by a tombstone (can be skipped during compaction).
    /// </summary>
    public bool IsColumnDeleted(string db, string measurement, string tagsCanonical,
        long minTime, long maxTime)
    {
        lock (_lock)
        {
            if (!_tombstones.TryGetValue(db, out var list) || list.Count == 0) return false;
            return list.Any(t => t.Measurement == measurement
                && (t.TagsCanonical == null || t.TagsCanonical == tagsCanonical)
                && (!t.MinTimeNs.HasValue || t.MinTimeNs.Value <= minTime)
                && (!t.MaxTimeNs.HasValue || t.MaxTimeNs.Value >= maxTime));
        }
    }

    /// <summary>
    /// Filter points against active tombstones. Returns only non-deleted points.
    /// </summary>
    public List<Point> FilterDeleted(string db, List<Point> points)
    {
        lock (_lock)
        {
            if (!_tombstones.TryGetValue(db, out var list) || list.Count == 0) return points;
            return points.Where(p => !IsDeletedLocked(p, list)).ToList();
        }
    }

    /// <summary>
    /// Filter column timestamps against tombstones. Returns filtered timestamps and values.
    /// </summary>
    public (List<long> Timestamps, List<FieldValue> Values) FilterColumnDeleted(
        string db, string measurement, string tagsCanonical,
        List<long> timestamps, List<FieldValue> values)
    {
        lock (_lock)
        {
            if (!_tombstones.TryGetValue(db, out var list) || list.Count == 0)
                return (timestamps, values);

            var ranges = BuildMergedRanges(list, measurement, tagsCanonical);

            if (ranges.Count == 0) return (timestamps, values);

            var filteredTs = new List<long>();
            var filteredVals = new List<FieldValue>();
            var rangeIndex = 0;
            for (int i = 0; i < timestamps.Count; i++)
            {
                var ts = timestamps[i];
                while (rangeIndex < ranges.Count && ranges[rangeIndex].Max < ts)
                    rangeIndex++;
                var deleted = rangeIndex < ranges.Count
                    && ranges[rangeIndex].Min <= ts
                    && ranges[rangeIndex].Max >= ts;
                if (!deleted)
                {
                    filteredTs.Add(ts);
                    filteredVals.Add(values[i]);
                }
            }
            return (filteredTs, filteredVals);
        }
    }

    /// <summary>
    /// Clear all tombstones for a shard after compaction has applied them.
    /// </summary>
    public void ClearApplied(string db, int shardId)
    {
        // In a full implementation, we'd track which tombstones have been applied to which shards.
        // For simplicity, we keep all tombstones and let compaction check each time.
    }

    private bool IsDeletedLocked(Point p, List<Tombstone> list)
    {
        var sk = SeriesKey.From(p);
        return list.Any(t =>
            t.Measurement == p.Measurement &&
            (t.TagsCanonical == null || t.TagsCanonical == sk.TagsCanonical) &&
            (!t.MinTimeNs.HasValue || t.MinTimeNs.Value <= p.TimestampNs) &&
            (!t.MaxTimeNs.HasValue || t.MaxTimeNs.Value >= p.TimestampNs));
    }

    private static List<(long Min, long Max)> BuildMergedRanges(List<Tombstone> tombstones, string measurement, string tagsCanonical)
    {
        var ranges = tombstones
            .Where(t => t.Measurement == measurement && (t.TagsCanonical == null || t.TagsCanonical == tagsCanonical))
            .Select(t => (Min: t.MinTimeNs ?? long.MinValue, Max: t.MaxTimeNs ?? long.MaxValue))
            .OrderBy(t => t.Min)
            .ToList();
        if (ranges.Count <= 1)
            return ranges;

        var merged = new List<(long Min, long Max)> { ranges[0] };
        for (var i = 1; i < ranges.Count; i++)
        {
            var last = merged[^1];
            var current = ranges[i];
            if (current.Min <= last.Max)
                merged[^1] = (last.Min, Math.Max(last.Max, current.Max));
            else
                merged.Add(current);
        }
        return merged;
    }

    private List<Tombstone> GetList(string db)
    {
        if (!_tombstones.TryGetValue(db, out var list))
        {
            list = [];
            _tombstones[db] = list;
        }
        return list;
    }

    private bool AddLocked(string db, Tombstone tombstone)
    {
        var list = GetList(db);
        var min = tombstone.MinTimeNs;
        var max = tombstone.MaxTimeNs;

        if (list.Any(t => t.Measurement == tombstone.Measurement
            && t.TagsCanonical == tombstone.TagsCanonical
            && Covers(t, tombstone)))
            return false;

        for (var i = list.Count - 1; i >= 0; i--)
        {
            var existing = list[i];
            if (existing.Measurement != tombstone.Measurement || existing.TagsCanonical != tombstone.TagsCanonical)
                continue;

            var existingMin = existing.MinTimeNs ?? long.MinValue;
            var existingMax = existing.MaxTimeNs ?? long.MaxValue;
            var newMin = min ?? long.MinValue;
            var newMax = max ?? long.MaxValue;
            if (newMin > existingMax || newMax < existingMin)
                continue;

            min = MinNullable(existing.MinTimeNs, min);
            max = MaxNullable(existing.MaxTimeNs, max);
            list.RemoveAt(i);
        }

        tombstone.MinTimeNs = min;
        tombstone.MaxTimeNs = max;
        if (tombstone.TagsCanonical != null && list.Any(t => t.Measurement == tombstone.Measurement
            && t.TagsCanonical == null
            && Covers(t, tombstone)))
            return false;

        if (tombstone.TagsCanonical == null)
            list.RemoveAll(t => t.Measurement == tombstone.Measurement
                && t.TagsCanonical != null
                && Covers(tombstone, t));

        list.Add(tombstone);
        return true;
    }

    private static bool Covers(Tombstone cover, Tombstone covered) =>
        (cover.MinTimeNs ?? long.MinValue) <= (covered.MinTimeNs ?? long.MinValue)
        && (cover.MaxTimeNs ?? long.MaxValue) >= (covered.MaxTimeNs ?? long.MaxValue);

    private static long? MinNullable(long? a, long? b) =>
        !a.HasValue || !b.HasValue ? null : Math.Min(a.Value, b.Value);

    private static long? MaxNullable(long? a, long? b) =>
        !a.HasValue || !b.HasValue ? null : Math.Max(a.Value, b.Value);

    private void LoadAll()
    {
        if (!Directory.Exists(_dir)) return;
        foreach (var f in Directory.GetFiles(_dir, "*.json"))
        {
            try
            {
                var db = Path.GetFileNameWithoutExtension(f);
                var json = File.ReadAllText(f);
                var list = JsonSerializer.Deserialize(json, TombstoneJsonContext.Default.ListTombstone);
                if (list != null) _tombstones[db] = list;
            }
            catch { /* corrupted tombstone file, skip */ }
        }
    }

    private void Save(string db)
    {
        if (!_tombstones.TryGetValue(db, out var list)) return;
        Directory.CreateDirectory(_dir);
        var path = Path.Combine(_dir, $"{db}.json");
        var tmp = path + ".tmp";
        var json = JsonSerializer.Serialize(list, TombstoneJsonContext.Default.ListTombstone);
        File.WriteAllText(tmp, json);
        File.Move(tmp, path, overwrite: true);
    }
}

[JsonSerializable(typeof(List<Tombstone>))]
internal partial class TombstoneJsonContext : JsonSerializerContext { }

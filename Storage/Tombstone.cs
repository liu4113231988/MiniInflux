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
        Directory.CreateDirectory(_dir);
        LoadAll();
    }

    /// <summary>
    /// Add a measurement-level delete (all series in measurement within optional time range).
    /// </summary>
    public void AddMeasurementDelete(string db, string measurement, long? minTime = null, long? maxTime = null)
    {
        lock (_lock)
        {
            var t = new Tombstone
            {
                Measurement = measurement,
                MinTimeNs = minTime,
                MaxTimeNs = maxTime,
                CreatedAtNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000
            };
            GetList(db).Add(t);
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
            var t = new Tombstone
            {
                Measurement = measurement,
                TagsCanonical = tagsCanonical,
                MinTimeNs = minTime,
                MaxTimeNs = maxTime,
                CreatedAtNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000
            };
            GetList(db).Add(t);
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

            var matching = list.Where(t =>
                t.Measurement == measurement &&
                (t.TagsCanonical == null || t.TagsCanonical == tagsCanonical)).ToList();

            if (matching.Count == 0) return (timestamps, values);

            var filteredTs = new List<long>();
            var filteredVals = new List<FieldValue>();
            for (int i = 0; i < timestamps.Count; i++)
            {
                var ts = timestamps[i];
                var deleted = matching.Any(t =>
                    (!t.MinTimeNs.HasValue || t.MinTimeNs.Value <= ts) &&
                    (!t.MaxTimeNs.HasValue || t.MaxTimeNs.Value >= ts));
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

    private List<Tombstone> GetList(string db)
    {
        if (!_tombstones.TryGetValue(db, out var list))
        {
            list = [];
            _tombstones[db] = list;
        }
        return list;
    }

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
        var path = Path.Combine(_dir, $"{db}.json");
        var tmp = path + ".tmp";
        var json = JsonSerializer.Serialize(list, TombstoneJsonContext.Default.ListTombstone);
        File.WriteAllText(tmp, json);
        File.Move(tmp, path, overwrite: true);
    }
}

[JsonSerializable(typeof(List<Tombstone>))]
internal partial class TombstoneJsonContext : JsonSerializerContext { }

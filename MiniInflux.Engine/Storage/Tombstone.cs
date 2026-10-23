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
    // null = every retention policy (legacy tombstone files without the property also match any RP)
    public string? RetentionPolicy { get; set; }
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
    private readonly StorageHealth? _health;
    private readonly object _lock = new();
    private readonly Dictionary<string, List<Tombstone>> _tombstones = new(StringComparer.Ordinal);
    private long _version;
    private RangeCache? _rangeCache;

    private sealed record RangeCache(long Version, Dictionary<string, List<(long Min, long Max)>> ByKey);

    public TombstoneStore(string dataPath, StorageHealth? health = null)
    {
        _dir = Path.Combine(dataPath, "tombstones");
        _health = health;
        LoadAll();
    }

    /// <summary>
    /// Add a measurement-level delete (all series in measurement within optional time range).
    /// </summary>
    public void AddMeasurementDelete(string db, string measurement, long? minTime = null, long? maxTime = null,
        string? retentionPolicy = null)
    {
        lock (_lock)
        {
            if (AddLocked(db, new Tombstone
            {
                Measurement = measurement,
                RetentionPolicy = retentionPolicy,
                MinTimeNs = minTime,
                MaxTimeNs = maxTime,
                CreatedAtNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000
            }))
            {
                _version++;
                Save(db);
            }
        }
    }

    /// <summary>
    /// Add a series-level delete (specific tag set within optional time range).
    /// </summary>
    public void AddSeriesDelete(string db, string measurement, string tagsCanonical,
        long? minTime = null, long? maxTime = null, string? retentionPolicy = null)
    {
        lock (_lock)
        {
            if (AddLocked(db, new Tombstone
            {
                Measurement = measurement,
                TagsCanonical = tagsCanonical,
                RetentionPolicy = retentionPolicy,
                MinTimeNs = minTime,
                MaxTimeNs = maxTime,
                CreatedAtNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000
            }))
            {
                _version++;
                Save(db);
            }
        }
    }

    public void AddSeriesDeletes(string db, string measurement, IEnumerable<(string TagsCanonical, long? MinTime, long? MaxTime)> deletes,
        string? retentionPolicy = null)
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
                    RetentionPolicy = retentionPolicy,
                    MinTimeNs = delete.MinTime,
                    MaxTimeNs = delete.MaxTime,
                    CreatedAtNs = createdAtNs
                });
            }

            if (changed)
            {
                _version++;
                Save(db);
            }
        }
    }

    /// <summary>
    /// Drop all tombstones for a database (used when DROP DATABASE is called).
    /// </summary>
    public void DropDatabase(string db)
    {
        IdentifierValidator.ValidateDatabaseName(db);
        lock (_lock)
        {
            _tombstones.Remove(db);
            _version++;
            _rangeCache = null;
            var path = Path.Combine(_dir, $"{db}.json");
            if (File.Exists(path)) try { File.Delete(path); } catch { /* best effort */ }
        }
    }

    /// <summary>
    /// Remove tombstones scoped to a retention policy. Legacy tombstones with no RP scope are
    /// retained because they still apply to every retention policy.
    /// </summary>
    public void DropRetentionPolicy(string db, string retentionPolicy)
    {
        IdentifierValidator.ValidateDatabaseName(db);
        IdentifierValidator.ValidateRetentionPolicyName(retentionPolicy);
        lock (_lock)
        {
            if (!_tombstones.TryGetValue(db, out var list)) return;

            var retained = list.Where(t => t.RetentionPolicy != retentionPolicy).ToList();
            if (retained.Count == list.Count) return;

            if (retained.Count == 0)
            {
                var path = Path.Combine(_dir, $"{db}.json");
                if (File.Exists(path)) File.Delete(path);
                _tombstones.Remove(db);
            }
            else
            {
                _tombstones[db] = retained;
                try
                {
                    Save(db);
                }
                catch
                {
                    _tombstones[db] = list;
                    throw;
                }
            }

            _version++;
            _rangeCache = null;
        }
    }

    /// <summary>
    /// Check if a specific column is fully covered by a tombstone (can be skipped during compaction).
    /// </summary>
    public bool IsColumnDeleted(string db, string measurement, string tagsCanonical,
        long minTime, long maxTime, string? retentionPolicy = null)
    {
        lock (_lock)
        {
            var ranges = GetMergedRangesLocked(db, measurement, tagsCanonical, retentionPolicy);
            if (ranges.Count == 0) return false;

            // Merged ranges are disjoint and sorted by Min, so the only candidate that can cover
            // minTime is the rightmost range starting at or before it.
            int lo = 0, hi = ranges.Count - 1, idx = -1;
            while (lo <= hi)
            {
                var mid = (lo + hi) >> 1;
                if (ranges[mid].Min <= minTime) { idx = mid; lo = mid + 1; }
                else hi = mid - 1;
            }
            return idx >= 0 && ranges[idx].Max >= maxTime;
        }
    }

    /// <summary>
    /// Monotonic store version, bumped on every mutation. Compaction captures it before reading
    /// data and re-checks before retiring coverage: a delete that lands in between must keep its
    /// tombstone (the rewritten output can contain the deleted point).
    /// </summary>
    public long CurrentVersion { get { lock (_lock) return _version; } }

    /// <summary>
    /// Check if any tombstones exist for the given database (fast path to skip filtering when clean).
    /// </summary>
    public bool HasTombstones(string db)
    {
        lock (_lock)
        {
            return _tombstones.TryGetValue(db, out var list) && list.Count > 0;
        }
    }

    /// <summary>
    /// Filter a list of timestamps against active tombstones, returning only non-deleted timestamps.
    /// Used by the fast count path which reads timestamps without field values.
    /// </summary>
    public List<long> FilterTimestampsDeleted(string db, string measurement, string tagsCanonical, List<long> timestamps,
        string? retentionPolicy = null)
    {
        lock (_lock)
        {
            var ranges = GetMergedRangesLocked(db, measurement, tagsCanonical, retentionPolicy);
            if (ranges.Count == 0)
                return timestamps;

            var filteredTs = new List<long>(timestamps.Count);
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
                    filteredTs.Add(ts);
            }
            return filteredTs;
        }
    }

    /// <summary>
    /// Filter points against active tombstones. Returns only non-deleted points.
    /// </summary>
    public List<Point> FilterDeleted(string db, List<Point> points, string? retentionPolicy = null)
    {
        lock (_lock)
        {
            if (!_tombstones.TryGetValue(db, out var list) || list.Count == 0) return points;
            return points.Where(p => !IsDeletedLocked(p, list, retentionPolicy)).ToList();
        }
    }

    /// <summary>
    /// Filter column timestamps against tombstones. Returns filtered timestamps and values.
    /// </summary>
    public (List<long> Timestamps, List<FieldValue> Values) FilterColumnDeleted(
        string db, string measurement, string tagsCanonical,
        List<long> timestamps, List<FieldValue> values, string? retentionPolicy = null)
    {
        lock (_lock)
        {
            var ranges = GetMergedRangesLocked(db, measurement, tagsCanonical, retentionPolicy);
            if (ranges.Count == 0)
                return (timestamps, values);

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
    /// Retire tombstone coverage for [minNs, maxNs] after compaction physically rewrote every
    /// segment of a shard containing that range: the deleted data no longer exists, so filtering
    /// on read would be dead weight. Fully covered tombstones are dropped; partially overlapping
    /// ones are split so only the still-unapplied ranges remain.
    /// </summary>
    public void RemoveCoveredRange(string db, long minNs, long maxNs, string? retentionPolicy = null)
    {
        lock (_lock)
        {
            if (!_tombstones.TryGetValue(db, out var list) || list.Count == 0) return;

            var changed = false;
            for (var i = list.Count - 1; i >= 0; i--)
            {
                var t = list[i];
                // Only retire tombstones whose scope was fully rewritten. A db-wide (null RP)
                // tombstone still applies to the other retention policies.
                if (t.RetentionPolicy != retentionPolicy) continue;
                var tMin = t.MinTimeNs ?? long.MinValue;
                var tMax = t.MaxTimeNs ?? long.MaxValue;
                if (tMax < minNs || tMin > maxNs) continue;

                var coveredMin = Math.Max(tMin, minNs);
                var coveredMax = Math.Min(tMax, maxNs);
                changed = true;
                list.RemoveAt(i);

                // Keep the unapplied remainders; null bounds stay unbounded on their side.
                if (tMin < coveredMin)
                    list.Add(new Tombstone
                    {
                        Measurement = t.Measurement,
                        TagsCanonical = t.TagsCanonical,
                        RetentionPolicy = t.RetentionPolicy,
                        MinTimeNs = t.MinTimeNs,
                        MaxTimeNs = coveredMin - 1,
                        CreatedAtNs = t.CreatedAtNs
                    });
                if (tMax > coveredMax)
                    list.Add(new Tombstone
                    {
                        Measurement = t.Measurement,
                        TagsCanonical = t.TagsCanonical,
                        RetentionPolicy = t.RetentionPolicy,
                        MinTimeNs = coveredMax + 1,
                        MaxTimeNs = t.MaxTimeNs,
                        CreatedAtNs = t.CreatedAtNs
                    });
            }

            if (changed)
            {
                _version++;
                Save(db);
            }
        }
    }

    private bool IsDeletedLocked(Point p, List<Tombstone> list, string? retentionPolicy)
    {
        var sk = SeriesKey.From(p);
        return list.Any(t =>
            t.Measurement == p.Measurement &&
            (t.TagsCanonical == null || t.TagsCanonical == sk.TagsCanonical) &&
            (t.RetentionPolicy == null || retentionPolicy == null || t.RetentionPolicy == retentionPolicy) &&
            (!t.MinTimeNs.HasValue || t.MinTimeNs.Value <= p.TimestampNs) &&
            (!t.MaxTimeNs.HasValue || t.MaxTimeNs.Value >= p.TimestampNs));
    }

    private static bool RpMatches(Tombstone tombstone, string? retentionPolicy) =>
        tombstone.RetentionPolicy == null || retentionPolicy == null || tombstone.RetentionPolicy == retentionPolicy;

    private static List<(long Min, long Max)> BuildMergedRanges(List<Tombstone> tombstones, string measurement, string? tagsCanonical,
        string? retentionPolicy)
    {
        var ranges = tombstones
            .Where(t => t.Measurement == measurement
                && (t.TagsCanonical == null || t.TagsCanonical == tagsCanonical)
                && RpMatches(t, retentionPolicy))
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

    /// <summary>
    /// Merged, sorted, disjoint ranges for one (db, measurement, tags) key, cached until the store
    /// mutates. Building per query per column is the hot read-path cost once deletes exist.
    /// </summary>
    private List<(long Min, long Max)> GetMergedRangesLocked(string db, string measurement, string tagsCanonical,
        string? retentionPolicy)
    {
        if (!_tombstones.TryGetValue(db, out var list) || list.Count == 0) return [];

        Dictionary<string, List<(long Min, long Max)>> byKey;
        var cache = _rangeCache;
        if (cache == null || cache.Version != _version)
        {
            byKey = new Dictionary<string, List<(long Min, long Max)>>(StringComparer.Ordinal);
            _rangeCache = new RangeCache(_version, byKey);
        }
        else
        {
            byKey = cache.ByKey;
        }

        var key = measurement + "\n" + (tagsCanonical ?? "*") + "\n" + (retentionPolicy ?? "*");
        if (byKey.TryGetValue(key, out var ranges)) return ranges;

        ranges = BuildMergedRanges(list, measurement, tagsCanonical, retentionPolicy);
        byKey[key] = ranges;
        return ranges;
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
            && RpCovers(t, tombstone)
            && Covers(t, tombstone)))
            return false;

        for (var i = list.Count - 1; i >= 0; i--)
        {
            var existing = list[i];
            if (existing.Measurement != tombstone.Measurement
                || existing.TagsCanonical != tombstone.TagsCanonical
                || existing.RetentionPolicy != tombstone.RetentionPolicy)
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
            && RpCovers(t, tombstone)
            && Covers(t, tombstone)))
            return false;

        if (tombstone.TagsCanonical == null)
            list.RemoveAll(t => t.Measurement == tombstone.Measurement
                && t.TagsCanonical != null
                && RpCovers(tombstone, t)
                && Covers(tombstone, t));

        list.Add(tombstone);
        return true;
    }

    private static bool RpCovers(Tombstone cover, Tombstone covered) =>
        cover.RetentionPolicy == null || cover.RetentionPolicy == covered.RetentionPolicy;

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
            catch (Exception ex)
            {
                // Reporting the failure keeps deleted data from silently reappearing after a
                // corrupt tombstone file: operators see it in health stats instead of a clean start.
                _health?.RecordFailure("tombstone_load", ex);
            }
        }
    }

    private void Save(string db)
    {
        IdentifierValidator.ValidateDatabaseName(db);
        if (!_tombstones.TryGetValue(db, out var list)) return;
        Directory.CreateDirectory(_dir);
        var path = Path.Combine(_dir, $"{db}.json");
        var tmp = path + ".tmp";
        try
        {
            using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(fs, list, TombstoneJsonContext.Default.ListTombstone);
                fs.Flush(true);
            }
            File.Move(tmp, path, overwrite: true);
        }
        catch
        {
            try { if (File.Exists(tmp)) File.Delete(tmp); } catch (IOException) { }
            throw;
        }
    }
}

[JsonSerializable(typeof(List<Tombstone>))]
public partial class TombstoneJsonContext : JsonSerializerContext { }

using System.Collections.Concurrent;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Per-series last point cache (P1). Keyed by K(db,rp) + SeriesKey. Updated on write path,
/// validated from segment footer after flush, used for last()/current-value queries (&lt;10ms).
/// AOT-friendly: no reflection, ConcurrentDictionary only.
/// </summary>
public sealed class LastValueCache
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<SeriesKey, Point>> _store = new(StringComparer.Ordinal);

    private static string K(string db, string rp) => db + "|" + rp;

    private static Point ClonePoint(Point p)
    {
        // Shallow clone Tags dict (tags are small) and Fields dict (last-write-wins merge needs copy)
        return new Point
        {
            Measurement = p.Measurement,
            Tags = new Dictionary<string, string>(p.Tags, StringComparer.Ordinal),
            Fields = new Dictionary<string, FieldValue>(p.Fields, StringComparer.Ordinal),
            TimestampNs = p.TimestampNs,
            TagsCanonical = p.TagsCanonical
        };
    }

    /// <summary>Insert or merge a single point into cache (LWW on same timestamp).</summary>
    public void Update(string db, string rp, Point point)
    {
        var outer = _store.GetOrAdd(K(db, rp), _ => new ConcurrentDictionary<SeriesKey, Point>());
        var sk = SeriesKey.From(point);
        // normalize TagsCanonical for stable identity — Point is init-only, so create normalized copy if missing
        var normalized = point;
        if (string.IsNullOrEmpty(point.TagsCanonical))
            normalized = new Point { Measurement = point.Measurement, Tags = point.Tags, Fields = point.Fields, TimestampNs = point.TimestampNs, TagsCanonical = sk.TagsCanonical };
        var toStore = normalized;
        outer.AddOrUpdate(sk,
            _ => ClonePoint(toStore),
            (_, existing) =>
            {
                if (toStore.TimestampNs > existing.TimestampNs)
                    return ClonePoint(toStore);
                if (toStore.TimestampNs == existing.TimestampNs)
                {
                    // merge fields: new overwrites old on same timestamp (duplicates LWW)
                    var merged = new Dictionary<string, FieldValue>(existing.Fields, StringComparer.Ordinal);
                    foreach (var kv in toStore.Fields) merged[kv.Key] = kv.Value;
                    // keep existing Measurement/Tags but adopt timestamp equality merged fields
                    return new Point
                    {
                        Measurement = existing.Measurement,
                        Tags = new Dictionary<string, string>(existing.Tags, StringComparer.Ordinal),
                        Fields = merged,
                        TimestampNs = existing.TimestampNs,
                        TagsCanonical = existing.TagsCanonical
                    };
                }
                return existing;
            });
    }

    public void UpdateMany(string db, string rp, IEnumerable<Point> points)
    {
        foreach (var p in points) Update(db, rp, p);
    }

    public bool TryGet(string db, string rp, SeriesKey key, out Point point)
    {
        if (_store.TryGetValue(K(db, rp), out var inner) && inner.TryGetValue(key, out point!))
            return true;
        point = default!;
        return false;
    }

    public bool TryGet(string db, string rp, string measurement, string tagsCanonical, out Point point)
        => TryGet(db, rp, new SeriesKey(measurement, tagsCanonical), out point);

    /// <summary>All cached points for measurement filtered by allowedTagsCanonical (null = all).</summary>
    public IReadOnlyList<Point> GetForMeasurement(string db, string rp, string measurement, HashSet<string>? allowedTags = null)
    {
        if (!_store.TryGetValue(K(db, rp), out var inner)) return Array.Empty<Point>();
        var res = new List<Point>();
        foreach (var kv in inner)
        {
            if (!string.Equals(kv.Key.Measurement, measurement, StringComparison.Ordinal)) continue;
            if (allowedTags != null && !allowedTags.Contains(kv.Key.TagsCanonical)) continue;
            res.Add(kv.Value);
        }
        return res;
    }

    public IReadOnlyList<Point> GetAll(string db, string rp, HashSet<string>? allowedTags = null)
    {
        if (!_store.TryGetValue(K(db, rp), out var inner)) return Array.Empty<Point>();
        if (allowedTags == null) return inner.Values.ToList();
        var res = new List<Point>();
        foreach (var kv in inner)
            if (allowedTags.Contains(kv.Key.TagsCanonical))
                res.Add(kv.Value);
        return res;
    }

    /// <summary>Validate after flush: ensure cached Timestamp matches flushed maxTime; missing is backfilled.</summary>
    public void ValidateAfterFlush(string db, string rp, IReadOnlyList<Point> flushedPoints, IReadOnlyDictionary<SeriesKey, long> flushedMaxTimeBySeries)
    {
        var key = K(db, rp);
        if (!_store.TryGetValue(key, out var inner)) return;
        foreach (var kv in flushedMaxTimeBySeries)
        {
            if (inner.TryGetValue(kv.Key, out var cached))
            {
                if (cached.TimestampNs == kv.Value) continue;
                // stale or missing newer? pick newest flushed point for that series
                var newest = flushedPoints.Where(p => SeriesKey.From(p).Equals(kv.Key)).MaxBy(p => p.TimestampNs);
                if (newest != null && newest.TimestampNs == kv.Value)
                    Update(db, rp, newest);
            }
            else
            {
                var newest = flushedPoints.Where(p => SeriesKey.From(p).Equals(kv.Key)).MaxBy(p => p.TimestampNs);
                if (newest != null) Update(db, rp, newest);
            }
        }
    }

    public void RemoveSeries(string db, string rp, SeriesKey key)
    {
        if (_store.TryGetValue(K(db, rp), out var inner)) inner.TryRemove(key, out _);
    }

    public void RemoveSeriesBatch(string db, string measurement, IReadOnlySet<string> tagsSet)
    {
        foreach (var outerKvp in _store)
        {
            if (!outerKvp.Key.StartsWith(db + "|", StringComparison.Ordinal)) continue;
            foreach (var tags in tagsSet)
            {
                outerKvp.Value.TryRemove(new SeriesKey(measurement, tags), out _);
            }
        }
    }

    public void RemoveMeasurement(string db, string measurement)
    {
        foreach (var outerKvp in _store)
        {
            if (!outerKvp.Key.StartsWith(db + "|", StringComparison.Ordinal)) continue;
            var toRemove = outerKvp.Value.Keys.Where(k => string.Equals(k.Measurement, measurement, StringComparison.Ordinal)).ToList();
            foreach (var k in toRemove) outerKvp.Value.TryRemove(k, out _);
        }
    }

    public void ClearDb(string db)
    {
        var prefix = db + "|";
        foreach (var key in _store.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToList())
            _store.TryRemove(key, out _);
    }

    public void ClearMeasurementFromDbRp(string db, string rp, string measurement)
    {
        if (_store.TryGetValue(K(db, rp), out var inner))
        {
            var toRemove = inner.Keys.Where(k => string.Equals(k.Measurement, measurement, StringComparison.Ordinal)).ToList();
            foreach (var k in toRemove) inner.TryRemove(k, out _);
        }
    }

    // generic predicate-based eviction for DeleteBuffered where we know exact tags but not all fields
    public void EvictWhere(string db, string rp, Func<SeriesKey, Point, bool> predicate)
    {
        if (!_store.TryGetValue(K(db, rp), out var inner)) return;
        foreach (var kv in inner.ToArray())
            if (predicate(kv.Key, kv.Value))
                inner.TryRemove(kv.Key, out _);
    }

    public int Count => _store.Values.Sum(v => v.Count);

    // for diagnostics
    public IReadOnlyDictionary<string, int> CountByDbRp
        => _store.ToDictionary(k => k.Key, v => v.Value.Count, StringComparer.Ordinal);
}

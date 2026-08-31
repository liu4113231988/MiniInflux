using System.Collections.Concurrent;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// P3 Distinct Value Cache：缓存 manifest 标签索引结果（SHOW TAG VALUES / KEYS 等），写入时失效.
/// AOT 友好，ConcurrentDictionary + 惰性加载.
/// </summary>
public sealed class DistinctValueCache
{
    private readonly ConcurrentDictionary<string, CacheEntry> _cache = new(StringComparer.Ordinal);
    private long _hits, _misses;

    private sealed class CacheEntry
    {
        public required IReadOnlyList<string> Values { get; init; }
        public long CreatedAtTicks { get; init; }
    }

    private static string KMeasurements(string db) => $"m|{db}";
    private static string KTagKeys(string db, string? measurement) => $"tk|{db}|{measurement ?? "*"}";
    private static string KTagValues(string db, string? measurement, string tagKey) => $"tv|{db}|{measurement ?? "*"}|{tagKey}";
    private static string KFieldKeys(string db, string? measurement) => $"fk|{db}|{measurement ?? "*"}";
    private static string KSeries(string db, string measurement) => $"s|{db}|{measurement}";
    private static string KSeriesAll(string db) => $"s|{db}|*";

    public IReadOnlyList<string> GetMeasurements(string db, Func<IReadOnlyList<string>> loader)
        => GetOrAdd(KMeasurements(db), loader);

    public IReadOnlyList<string> GetTagKeys(string db, string? measurement, Func<IReadOnlyList<string>> loader)
        => GetOrAdd(KTagKeys(db, measurement), loader);

    public IReadOnlyList<string> GetTagValues(string db, string? measurement, string tagKey, Func<IReadOnlyList<string>> loader)
        => GetOrAdd(KTagValues(db, measurement, tagKey), loader);

    public IReadOnlyList<string> GetFieldKeys(string db, string? measurement, Func<IReadOnlyList<string>> loader)
        => GetOrAdd(KFieldKeys(db, measurement), loader);

    public IReadOnlyList<string> GetSeries(string db, string measurement, Func<IReadOnlyList<string>> loader)
        => GetOrAdd(KSeries(db, measurement), loader);

    private IReadOnlyList<string> GetOrAdd(string key, Func<IReadOnlyList<string>> loader)
    {
        if (_cache.TryGetValue(key, out var entry))
        {
            Interlocked.Increment(ref _hits);
            return entry.Values;
        }
        Interlocked.Increment(ref _misses);
        var values = loader();
        // store copy to avoid external mutation
        var copy = values.ToArray();
        _cache[key] = new CacheEntry { Values = copy, CreatedAtTicks = DateTime.UtcNow.Ticks };
        return copy;
    }

    public void InvalidateDb(string db)
    {
        var prefixM = $"m|{db}";
        var prefixTk = $"tk|{db}|";
        var prefixTv = $"tv|{db}|";
        var prefixFk = $"fk|{db}|";
        var prefixS = $"s|{db}|";
        foreach (var k in _cache.Keys.ToArray())
            if (k == prefixM || k.StartsWith(prefixTk, StringComparison.Ordinal) || k.StartsWith(prefixTv, StringComparison.Ordinal) || k.StartsWith(prefixFk, StringComparison.Ordinal) || k.StartsWith(prefixS, StringComparison.Ordinal) || k == KSeriesAll(db))
                _cache.TryRemove(k, out _);
    }

    public void InvalidateMeasurement(string db, string measurement)
    {
        var keys = new[]
        {
            KTagKeys(db, measurement),
            KTagKeys(db, null),
            KFieldKeys(db, measurement),
            KFieldKeys(db, null),
            KSeries(db, measurement),
            KSeriesAll(db),
            KMeasurements(db)
        };
        foreach (var k in keys) _cache.TryRemove(k, out _);
        // tag values for all keys of this measurement
        var prefix = $"tv|{db}|{measurement}|";
        foreach (var k in _cache.Keys.ToArray())
            if (k.StartsWith(prefix, StringComparison.Ordinal))
                _cache.TryRemove(k, out _);
    }

    public void InvalidateTagKey(string db, string? measurement, string tagKey)
    {
        _cache.TryRemove(KTagValues(db, measurement, tagKey), out _);
        _cache.TryRemove(KTagKeys(db, measurement), out _);
        _cache.TryRemove(KTagKeys(db, null), out _);
    }

    public void ClearAll() => _cache.Clear();

    public (long Hits, long Misses, int Count) Stats => (Interlocked.Read(ref _hits), Interlocked.Read(ref _misses), _cache.Count);
}

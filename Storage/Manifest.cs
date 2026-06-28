using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Retention policy metadata.
/// </summary>
public sealed class RetentionPolicyInfo
{
    public string Name { get; set; } = "";
    public long DurationNs { get; set; } // 0 = infinite
    public int Replication { get; set; } = 1;
    public bool IsDefault { get; set; }
    public List<ShardGroupInfo> ShardGroups { get; set; } = [];

    public long DurationMs => DurationNs / 1_000_000;
}

public sealed class ContinuousQueryInfo
{
    public string Name { get; set; } = "";
    public string Database { get; set; } = "";
    public string QueryText { get; set; } = "";
    public long EveryNs { get; set; }
    public long ForNs { get; set; }
    public int RecomputeRecentBuckets { get; set; }
    public long LastCompletedBucketStartNs { get; set; } = long.MinValue;
}

/// <summary>
/// Shard group metadata. Each shard covers a specific time range within a db/rp.
/// </summary>
public sealed class ShardGroupInfo
{
    public int Id { get; set; }
    public long StartTimeNs { get; set; }
    public long EndTimeNs { get; set; }
    public List<string> SegmentFiles { get; set; } = [];
}

/// <summary>
/// Central metadata manifest for the database engine.
/// Tracks databases, retention policies, shard groups, and segment files.
/// Persisted to data/meta/manifest.json.
/// </summary>
public sealed class Manifest
{
    private readonly string _path;
    private readonly object _lock = new();
    private ManifestData _data = new();
    private bool _dirty;

    public Manifest(string dataPath)
    {
        var metaDir = Path.Combine(dataPath, "meta");
        Directory.CreateDirectory(metaDir);
        _path = Path.Combine(metaDir, "manifest.json");
        Load();
    }

    internal ManifestData Data => _data;

    #region Database operations

    public void EnsureDatabase(string db)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out _))
            {
                _data.Databases[db] = new DatabaseInfo();
                Save();
            }
        }
    }

    public IReadOnlyList<string> ListDatabases()
    {
        lock (_lock) return _data.Databases.Keys.Order().ToArray();
    }

    public bool HasDatabase(string db)
    {
        lock (_lock) return _data.Databases.ContainsKey(db);
    }

    public void DropDatabase(string db)
    {
        lock (_lock)
        {
            if (_data.Databases.Remove(db))
                Save();
        }
    }

    #endregion

    #region Retention policy operations

    public RetentionPolicyInfo GetDefaultRp(string db)
    {
        lock (_lock)
        {
            if (_data.Databases.TryGetValue(db, out var dbInfo))
            {
                var def = dbInfo.RetentionPolicies.Values.FirstOrDefault(r => r.IsDefault);
                if (def != null) return def;
                if (dbInfo.RetentionPolicies.Count > 0)
                    return dbInfo.RetentionPolicies.Values.First();
            }
            return new RetentionPolicyInfo { Name = "autogen", DurationNs = 0, Replication = 1, IsDefault = true };
        }
    }

    public RetentionPolicyInfo? GetRp(string db, string rpName)
    {
        lock (_lock)
        {
            if (_data.Databases.TryGetValue(db, out var dbInfo) &&
                dbInfo.RetentionPolicies.TryGetValue(rpName, out var rp))
                return rp;
            return null;
        }
    }

    public void EnsureRp(string db, string rpName)
    {
        lock (_lock)
        {
            EnsureDatabase(db);
            var dbInfo = _data.Databases[db];
            if (!dbInfo.RetentionPolicies.ContainsKey(rpName))
            {
                var isFirst = dbInfo.RetentionPolicies.Count == 0;
                dbInfo.RetentionPolicies[rpName] = new RetentionPolicyInfo
                {
                    Name = rpName, DurationNs = 0, Replication = 1, IsDefault = isFirst
                };
                Save();
            }
        }
    }

    public void CreateRetentionPolicy(string db, string rpName, long durationNs, bool isDefault)
    {
        lock (_lock)
        {
            EnsureDatabase(db);
            var dbInfo = _data.Databases[db];
            if (isDefault)
                foreach (var rp in dbInfo.RetentionPolicies.Values) rp.IsDefault = false;

            dbInfo.RetentionPolicies[rpName] = new RetentionPolicyInfo
            {
                Name = rpName, DurationNs = durationNs, Replication = 1, IsDefault = isDefault
            };
            Save();
        }
    }

    public void AlterRetentionPolicy(string db, string rpName, long? durationNs, bool? isDefault)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return;
            if (!dbInfo.RetentionPolicies.TryGetValue(rpName, out var rp)) return;

            if (durationNs.HasValue) rp.DurationNs = durationNs.Value;
            if (isDefault == true)
            {
                foreach (var r in dbInfo.RetentionPolicies.Values) r.IsDefault = false;
                rp.IsDefault = true;
            }
            Save();
        }
    }

    public void DropRetentionPolicy(string db, string rpName)
    {
        lock (_lock)
        {
            if (_data.Databases.TryGetValue(db, out var dbInfo) &&
                dbInfo.RetentionPolicies.Remove(rpName))
                Save();
        }
    }

    public IReadOnlyList<RetentionPolicyInfo> ListRetentionPolicies(string db)
    {
        lock (_lock)
        {
            if (_data.Databases.TryGetValue(db, out var dbInfo))
                return dbInfo.RetentionPolicies.Values.ToList();
            return [];
        }
    }

    public void SaveContinuousQuery(string db, ContinuousQueryInfo query)
    {
        lock (_lock)
        {
            EnsureDatabase(db);
            var dbInfo = _data.Databases[db];
            dbInfo.ContinuousQueries[query.Name] = new ContinuousQueryInfo
            {
                Name = query.Name,
                Database = db,
                QueryText = query.QueryText,
                EveryNs = query.EveryNs,
                ForNs = query.ForNs,
                RecomputeRecentBuckets = query.RecomputeRecentBuckets,
                LastCompletedBucketStartNs = query.LastCompletedBucketStartNs
            };
            Save();
        }
    }

    public bool RemoveContinuousQuery(string db, string name)
    {
        lock (_lock)
        {
            if (_data.Databases.TryGetValue(db, out var dbInfo) && dbInfo.ContinuousQueries.Remove(name))
            {
                Save();
                return true;
            }
            return false;
        }
    }

    public IReadOnlyList<ContinuousQueryInfo> ListContinuousQueries(string? db = null)
    {
        lock (_lock)
        {
            if (!string.IsNullOrWhiteSpace(db))
            {
                if (_data.Databases.TryGetValue(db!, out var dbInfo))
                    return dbInfo.ContinuousQueries.Values
                        .OrderBy(q => q.Name, StringComparer.Ordinal)
                        .ToList();
                return [];
            }

            return _data.Databases
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .SelectMany(kv => kv.Value.ContinuousQueries.Values.OrderBy(q => q.Name, StringComparer.Ordinal))
                .ToList();
        }
    }

    public ContinuousQueryInfo? GetContinuousQuery(string db, string name)
    {
        lock (_lock)
        {
            if (_data.Databases.TryGetValue(db, out var dbInfo) && dbInfo.ContinuousQueries.TryGetValue(name, out var query))
                return query;
            return null;
        }
    }

    public void UpdateContinuousQueryProgress(string db, string name, long lastCompletedBucketStartNs)
    {
        lock (_lock)
        {
            if (_data.Databases.TryGetValue(db, out var dbInfo) && dbInfo.ContinuousQueries.TryGetValue(name, out var query))
            {
                query.LastCompletedBucketStartNs = lastCompletedBucketStartNs;
                Save();
            }
        }
    }

    #endregion

    #region Shard operations

    public void AddShardGroup(string db, string rp, ShardGroupInfo shard)
    {
        lock (_lock)
        {
            EnsureRp(db, rp);
            _data.Databases[db].RetentionPolicies[rp].ShardGroups.Add(shard);
            Save();
        }
    }

    public void RemoveShardGroup(string db, string rp, int shardId)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return;
            if (!dbInfo.RetentionPolicies.TryGetValue(rp, out var rpInfo)) return;
            rpInfo.ShardGroups.RemoveAll(s => s.Id == shardId);
            Save();
        }
    }

    /// <summary>
    /// Get all shards for a db/rp, optionally filtered by time range.
    /// </summary>
    public IReadOnlyList<ShardGroupInfo> GetShards(string db, string rp, long? minTimeNs = null, long? maxTimeNs = null)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return [];
            if (!dbInfo.RetentionPolicies.TryGetValue(rp, out var rpInfo)) return [];
            return rpInfo.ShardGroups
                .Where(s => (!maxTimeNs.HasValue || s.EndTimeNs >= maxTimeNs.Value)
                         && (!minTimeNs.HasValue || s.StartTimeNs <= minTimeNs.Value))
                .ToList();
        }
    }

    /// <summary>
    /// Get all shards across all RPs for a database.
    /// </summary>
    public IReadOnlyList<(string Rp, ShardGroupInfo Shard)> GetAllShards(string db)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return [];
            return dbInfo.RetentionPolicies
                .SelectMany(kv => kv.Value.ShardGroups.Select(s => (kv.Key, s)))
                .ToList();
        }
    }

    public void AddSegmentToShard(string db, string rp, int shardId, string segmentFile)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return;
            if (!dbInfo.RetentionPolicies.TryGetValue(rp, out var rpInfo)) return;
            var shard = rpInfo.ShardGroups.FirstOrDefault(s => s.Id == shardId);
            if (shard == null) return;
            var fileName = Path.GetFileName(segmentFile);
            if (!shard.SegmentFiles.Contains(fileName))
            {
                shard.SegmentFiles.Add(fileName);
                Save();
            }
        }
    }

    public void RemoveSegmentsFromShard(string db, string rp, int shardId, IEnumerable<string> segmentFiles)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return;
            if (!dbInfo.RetentionPolicies.TryGetValue(rp, out var rpInfo)) return;
            var shard = rpInfo.ShardGroups.FirstOrDefault(s => s.Id == shardId);
            if (shard == null) return;

            var names = new HashSet<string>(segmentFiles.Select(Path.GetFileName).Where(name => !string.IsNullOrWhiteSpace(name))!, StringComparer.OrdinalIgnoreCase);
            shard.SegmentFiles.RemoveAll(file => names.Contains(file));
            Save();
        }
    }

    public void ReplaceSegmentsInShard(string db, string rp, int shardId, IEnumerable<string> oldSegmentFiles, IEnumerable<string> newSegmentFiles)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return;
            if (!dbInfo.RetentionPolicies.TryGetValue(rp, out var rpInfo)) return;
            var shard = rpInfo.ShardGroups.FirstOrDefault(s => s.Id == shardId);
            if (shard == null) return;

            var removeNames = new HashSet<string>(
                oldSegmentFiles.Select(Path.GetFileName).Where(name => !string.IsNullOrWhiteSpace(name))!,
                StringComparer.OrdinalIgnoreCase);
            shard.SegmentFiles.RemoveAll(file => removeNames.Contains(file));

            foreach (var file in newSegmentFiles.Select(Path.GetFileName).Where(name => !string.IsNullOrWhiteSpace(name)).Select(name => name!))
            {
                if (!shard.SegmentFiles.Contains(file, StringComparer.OrdinalIgnoreCase))
                    shard.SegmentFiles.Add(file);
            }

            Save();
        }
    }

    #endregion

    #region Indexes

    /// <summary>
    /// Update series and tag indexes for a batch of points.
    /// </summary>
    public void UpdateIndexes(string db, IEnumerable<(string Measurement, string TagsCanonical, Dictionary<string, string> Tags)> points)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return;
            var changed = false;
            foreach (var (meas, tagsCanon, tags) in points)
            {
                // Series index
                if (!dbInfo.SeriesIndex.TryGetValue(meas, out var seriesSet))
                { seriesSet = new(StringComparer.Ordinal); dbInfo.SeriesIndex[meas] = seriesSet; }
                changed |= seriesSet.Add(tagsCanon);

                // Tag inverted index
                if (!dbInfo.TagIndex.TryGetValue(meas, out var tagMap))
                { tagMap = new(StringComparer.Ordinal); dbInfo.TagIndex[meas] = tagMap; }
                if (!dbInfo.TagSeriesIndex.TryGetValue(meas, out var tagSeriesMap))
                { tagSeriesMap = new(StringComparer.Ordinal); dbInfo.TagSeriesIndex[meas] = tagSeriesMap; }
                foreach (var (k, v) in tags)
                {
                    if (!tagMap.TryGetValue(k, out var valSet))
                    { valSet = new(StringComparer.Ordinal); tagMap[k] = valSet; }
                    changed |= valSet.Add(v);

                    if (!tagSeriesMap.TryGetValue(k, out var valueMap))
                    { valueMap = new(StringComparer.Ordinal); tagSeriesMap[k] = valueMap; }
                    if (!valueMap.TryGetValue(v, out var tagValueSeries))
                    { tagValueSeries = new(StringComparer.Ordinal); valueMap[v] = tagValueSeries; }
                    changed |= tagValueSeries.Add(tagsCanon);
                }
            }
            if (changed) _dirty = true;
        }
    }

    /// <summary>
    /// Get all series keys for a measurement (or all measurements if null).
    /// </summary>
    public IReadOnlyList<string> GetSeries(string db, string? measurement)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return [];
            if (measurement != null)
                return dbInfo.SeriesIndex.TryGetValue(measurement, out var s) ? s.Order().ToArray() : [];
            return dbInfo.SeriesIndex.Values.SelectMany(s => s).Distinct().Order().ToArray();
        }
    }

    public int GetTagValueCardinality(string db, string? measurement, string? tagKey)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return 0;
            var values = new HashSet<string>(StringComparer.Ordinal);
            var measurements = measurement != null
                ? (dbInfo.TagIndex.ContainsKey(measurement) ? new[] { measurement } : Array.Empty<string>())
                : dbInfo.TagIndex.Keys.ToArray();
            foreach (var m in measurements)
            {
                if (!dbInfo.TagIndex.TryGetValue(m, out var tagMap)) continue;
                var keys = tagKey != null
                    ? (tagMap.ContainsKey(tagKey) ? new[] { tagKey } : Array.Empty<string>())
                    : tagMap.Keys.ToArray();
                foreach (var key in keys)
                    if (tagMap.TryGetValue(key, out var vals))
                        foreach (var value in vals) values.Add($"{key}={value}");
            }
            return values.Count;
        }
    }

    public IReadOnlyList<string> ListMeasurements(string db)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return [];
            return dbInfo.SeriesIndex.Keys.Order().ToArray();
        }
    }

    /// <summary>
    /// Get tag values for a measurement/tagKey (or all measurements if null).
    /// </summary>
    public IReadOnlyList<(string Key, string Value)> GetTagValues(string db, string? measurement, string? tagKey)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return [];
            var result = new List<(string, string)>();
            var measurements = measurement != null
                ? (dbInfo.TagIndex.ContainsKey(measurement) ? new[] { measurement } : Array.Empty<string>())
                : dbInfo.TagIndex.Keys.ToArray();
            foreach (var m in measurements)
            {
                if (!dbInfo.TagIndex.TryGetValue(m, out var tagMap)) continue;
                var keys = tagKey != null
                    ? (tagMap.ContainsKey(tagKey) ? new[] { tagKey } : Array.Empty<string>())
                    : tagMap.Keys.ToArray();
                foreach (var k in keys)
                {
                    if (!tagMap.TryGetValue(k, out var vals)) continue;
                    foreach (var v in vals.Order()) result.Add((k, v));
                }
            }
            return result.Distinct().OrderBy(x => x.Item2).ToArray();
        }
    }

    public IReadOnlyList<string> GetSeriesForTagValue(string db, string measurement, string tagKey, string tagValue)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return [];
            if (!dbInfo.TagSeriesIndex.TryGetValue(measurement, out var tagMap)) return [];
            if (!tagMap.TryGetValue(tagKey, out var valueMap)) return [];
            return valueMap.TryGetValue(tagValue, out var series) ? series.Order().ToArray() : [];
        }
    }

    public IReadOnlyList<string> GetSeriesForTagKey(string db, string measurement, string tagKey)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return [];
            if (!dbInfo.TagSeriesIndex.TryGetValue(measurement, out var tagMap)) return [];
            if (!tagMap.TryGetValue(tagKey, out var valueMap)) return [];
            return valueMap.Values.SelectMany(v => v).Distinct(StringComparer.Ordinal).Order().ToArray();
        }
    }

    public IReadOnlyList<string> GetSeriesForTagRegex(string db, string measurement, string tagKey, string pattern, bool negate)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return [];
            if (!dbInfo.TagSeriesIndex.TryGetValue(measurement, out var tagMap)) return [];
            if (!tagMap.TryGetValue(tagKey, out var valueMap)) return [];

            var regex = new Regex(pattern, RegexOptions.CultureInvariant);
            return valueMap
                .Where(kv => negate ? !regex.IsMatch(kv.Key) : regex.IsMatch(kv.Key))
                .SelectMany(kv => kv.Value)
                .Distinct(StringComparer.Ordinal)
                .Order()
                .ToArray();
        }
    }

    /// <summary>
    /// Remove series/tag index entries for a measurement (on DROP MEASUREMENT).
    /// </summary>
    public void RemoveMeasurementIndex(string db, string measurement)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return;
            dbInfo.SeriesIndex.Remove(measurement);
            dbInfo.TagIndex.Remove(measurement);
            dbInfo.TagSeriesIndex.Remove(measurement);
            Save();
        }
    }

    public void RemoveSeriesIndex(string db, string measurement, IReadOnlySet<string> tagsCanonicalSet)
    {
        lock (_lock)
        {
            if (!_data.Databases.TryGetValue(db, out var dbInfo)) return;
            if (dbInfo.SeriesIndex.TryGetValue(measurement, out var series))
                series.RemoveWhere(tagsCanonicalSet.Contains);

            // Rebuild tag indexes for the measurement from remaining series keys.
            dbInfo.TagIndex.Remove(measurement);
            dbInfo.TagSeriesIndex.Remove(measurement);
            if (!dbInfo.SeriesIndex.TryGetValue(measurement, out var remaining) || remaining.Count == 0)
            {
                dbInfo.SeriesIndex.Remove(measurement);
                Save();
                return;
            }

            var tagMap = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            var tagSeriesMap = new Dictionary<string, Dictionary<string, HashSet<string>>>(StringComparer.Ordinal);
            foreach (var tagsCanonical in remaining)
            {
                foreach (var (key, value) in ParseTags(tagsCanonical))
                {
                    if (!tagMap.TryGetValue(key, out var vals))
                    { vals = new(StringComparer.Ordinal); tagMap[key] = vals; }
                    vals.Add(value);

                    if (!tagSeriesMap.TryGetValue(key, out var valueMap))
                    { valueMap = new(StringComparer.Ordinal); tagSeriesMap[key] = valueMap; }
                    if (!valueMap.TryGetValue(value, out var seriesSet))
                    { seriesSet = new(StringComparer.Ordinal); valueMap[value] = seriesSet; }
                    seriesSet.Add(tagsCanonical);
                }
            }
            dbInfo.TagIndex[measurement] = tagMap;
            dbInfo.TagSeriesIndex[measurement] = tagSeriesMap;
            Save();
        }
    }

    private static IEnumerable<(string Key, string Value)> ParseTags(string tagsCanonical)
    {
        if (string.IsNullOrWhiteSpace(tagsCanonical)) yield break;
        foreach (var part in tagsCanonical.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var i = part.IndexOf('=');
            if (i > 0) yield return (part[..i], part[(i + 1)..]);
        }
    }

    #endregion

    #region Expiry

    /// <summary>
    /// Returns list of expired (db, rp, shardId, shardDir) tuples that should be deleted.
    /// </summary>
    public List<(string Db, string Rp, int ShardId, string ShardDir)> GetExpiredShards(string dataPath, long nowNs)
    {
        var expired = new List<(string, string, int, string)>();
        lock (_lock)
        {
            foreach (var (db, dbInfo) in _data.Databases)
            {
                foreach (var (rpName, rp) in dbInfo.RetentionPolicies)
                {
                    if (rp.DurationNs <= 0) continue;
                    var cutoff = nowNs - rp.DurationNs;
                    foreach (var shard in rp.ShardGroups.Where(s => s.EndTimeNs < cutoff))
                    {
                        var dir = Path.Combine(dataPath, "db", db, rpName, "shards", shard.Id.ToString("D6"));
                        expired.Add((db, rpName, shard.Id, dir));
                    }
                }
            }
        }
        return expired;
    }

    #endregion

    #region Persistence

    private void Load()
    {
        if (!File.Exists(_path)) return;
        try
        {
            var json = File.ReadAllText(_path);
            var data = JsonSerializer.Deserialize(json, ManifestJsonContext.Default.ManifestData);
            if (data != null) _data = data;
        }
        catch { /* corrupted manifest, start fresh */ }
    }

    public void Save()
    {
        var tmp = _path + ".tmp";
        var json = JsonSerializer.Serialize(_data, ManifestJsonContext.Default.ManifestData);
        File.WriteAllText(tmp, json);
        File.Move(tmp, _path, overwrite: true);
        _dirty = false;
    }

    public void SaveIfDirty()
    {
        lock (_lock)
        {
            if (_dirty) Save();
        }
    }

    #endregion
}

internal sealed class DatabaseInfo
{
    public Dictionary<string, RetentionPolicyInfo> RetentionPolicies { get; set; } = new(StringComparer.Ordinal);
    public Dictionary<string, ContinuousQueryInfo> ContinuousQueries { get; set; } = new(StringComparer.Ordinal);
    public Dictionary<string, HashSet<string>> SeriesIndex { get; set; } = new(StringComparer.Ordinal);
    public Dictionary<string, Dictionary<string, HashSet<string>>> TagIndex { get; set; } = new(StringComparer.Ordinal);
    public Dictionary<string, Dictionary<string, Dictionary<string, HashSet<string>>>> TagSeriesIndex { get; set; } = new(StringComparer.Ordinal);
}

internal sealed class ManifestData
{
    public Dictionary<string, DatabaseInfo> Databases { get; set; } = new(StringComparer.Ordinal);
}

[JsonSerializable(typeof(ManifestData))]
[JsonSerializable(typeof(DatabaseInfo))]
[JsonSerializable(typeof(RetentionPolicyInfo))]
[JsonSerializable(typeof(ContinuousQueryInfo))]
[JsonSerializable(typeof(ShardGroupInfo))]
[JsonSerializable(typeof(List<ContinuousQueryInfo>))]
[JsonSerializable(typeof(List<ShardGroupInfo>))]
[JsonSerializable(typeof(Dictionary<string, DatabaseInfo>))]
[JsonSerializable(typeof(Dictionary<string, RetentionPolicyInfo>))]
[JsonSerializable(typeof(Dictionary<string, ContinuousQueryInfo>))]
[JsonSerializable(typeof(Dictionary<string, HashSet<string>>))]
[JsonSerializable(typeof(Dictionary<string, Dictionary<string, HashSet<string>>>))]
[JsonSerializable(typeof(Dictionary<string, Dictionary<string, Dictionary<string, HashSet<string>>>>))]
[JsonSerializable(typeof(HashSet<string>))]
internal partial class ManifestJsonContext : JsonSerializerContext { }

using System.Text.Json;
using System.Text.Json.Serialization;

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
            foreach (var (meas, tagsCanon, tags) in points)
            {
                // Series index
                if (!dbInfo.SeriesIndex.TryGetValue(meas, out var seriesSet))
                { seriesSet = new(StringComparer.Ordinal); dbInfo.SeriesIndex[meas] = seriesSet; }
                seriesSet.Add(tagsCanon);

                // Tag inverted index
                if (!dbInfo.TagIndex.TryGetValue(meas, out var tagMap))
                { tagMap = new(StringComparer.Ordinal); dbInfo.TagIndex[meas] = tagMap; }
                foreach (var (k, v) in tags)
                {
                    if (!tagMap.TryGetValue(k, out var valSet))
                    { valSet = new(StringComparer.Ordinal); tagMap[k] = valSet; }
                    valSet.Add(v);
                }
            }
            Save();
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
            Save();
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
    }

    #endregion
}

internal sealed class DatabaseInfo
{
    public Dictionary<string, RetentionPolicyInfo> RetentionPolicies { get; set; } = new(StringComparer.Ordinal);
    public Dictionary<string, HashSet<string>> SeriesIndex { get; set; } = new(StringComparer.Ordinal);
    public Dictionary<string, Dictionary<string, HashSet<string>>> TagIndex { get; set; } = new(StringComparer.Ordinal);
}

internal sealed class ManifestData
{
    public Dictionary<string, DatabaseInfo> Databases { get; set; } = new(StringComparer.Ordinal);
}

[JsonSerializable(typeof(ManifestData))]
[JsonSerializable(typeof(DatabaseInfo))]
[JsonSerializable(typeof(RetentionPolicyInfo))]
[JsonSerializable(typeof(ShardGroupInfo))]
[JsonSerializable(typeof(List<ShardGroupInfo>))]
[JsonSerializable(typeof(Dictionary<string, DatabaseInfo>))]
[JsonSerializable(typeof(Dictionary<string, RetentionPolicyInfo>))]
[JsonSerializable(typeof(Dictionary<string, HashSet<string>>))]
[JsonSerializable(typeof(Dictionary<string, Dictionary<string, HashSet<string>>>))]
[JsonSerializable(typeof(HashSet<string>))]
internal partial class ManifestJsonContext : JsonSerializerContext { }

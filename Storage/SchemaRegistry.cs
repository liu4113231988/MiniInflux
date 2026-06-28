using System.Text.Json;
using System.Text.Json.Serialization;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Tracks field types per measurement to detect and prevent type conflicts.
/// Persisted to data/meta/schema.json.
/// </summary>
public sealed class SchemaRegistry
{
    private readonly string _schemaPath;
    private readonly object _lock = new();
    private readonly int _maxFieldsPerMeasurement;
    // Key: "db|measurement|fieldKey" -> FieldKind
    private readonly Dictionary<string, FieldKind> _schema = new(StringComparer.Ordinal);

    public SchemaRegistry(string dataPath, int maxFieldsPerMeasurement = 1024)
    {
        var metaDir = Path.Combine(dataPath, "meta");
        Directory.CreateDirectory(metaDir);
        _schemaPath = Path.Combine(metaDir, "schema.json");
        _maxFieldsPerMeasurement = maxFieldsPerMeasurement;
        Load();
    }

    private string MakeKey(string db, string measurement, string fieldKey) =>
        $"{db}|{measurement}|{fieldKey}";

    /// <summary>
    /// Validate and register field types for a batch of points.
    /// Throws FieldConflictException if a type conflict is detected.
    /// </summary>
    public void ValidateAndRegister(string db, string measurement, IEnumerable<Point> points)
    {
        var batchFields = new Dictionary<string, FieldKind>(StringComparer.Ordinal);
        foreach (var p in points)
        {
            foreach (var field in p.Fields)
            {
                if (batchFields.TryGetValue(field.Key, out var batchKind))
                {
                    if (batchKind != field.Value.Kind)
                        throw new FieldConflictException(
                            $"field type conflict: {measurement}.{field.Key} is {batchKind}, got {field.Value.Kind}");
                }
                else
                {
                    batchFields[field.Key] = field.Value.Kind;
                }
            }
        }

        lock (_lock)
        {
            var changed = false;
            foreach (var field in batchFields)
            {
                var key = MakeKey(db, measurement, field.Key);
                if (_schema.TryGetValue(key, out var existing))
                {
                    if (existing != field.Value)
                        throw new FieldConflictException(
                            $"field type conflict: {measurement}.{field.Key} is {existing}, got {field.Value}");
                }
                else
                {
                    _schema[key] = field.Value;
                    changed = true;
                }
            }

            // Enforce max fields per measurement
            if (_maxFieldsPerMeasurement > 0)
            {
                var prefix = $"{db}|{measurement}|";
                var fieldCount = _schema.Count(k => k.Key.StartsWith(prefix, StringComparison.Ordinal));
                if (fieldCount > _maxFieldsPerMeasurement)
                    throw new FieldConflictException(
                        $"max fields per measurement exceeded: {measurement} has {fieldCount} fields (limit: {_maxFieldsPerMeasurement})");
            }

            if (changed)
                Save();
        }
    }

    /// <summary>
    /// Get the registered field type for a specific field, or null if not registered.
    /// </summary>
    public FieldKind? GetFieldType(string db, string measurement, string fieldKey)
    {
        lock (_lock)
        {
            return _schema.TryGetValue(MakeKey(db, measurement, fieldKey), out var kind) ? kind : null;
        }
    }

    /// <summary>
    /// Get all registered fields for a measurement.
    /// </summary>
    public IReadOnlyList<(string FieldKey, FieldKind Kind)> GetFields(string db, string? measurement)
    {
        lock (_lock)
        {
            var prefix = measurement != null ? $"{db}|{measurement}|" : $"{db}|";
            return _schema
                .Where(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(kv =>
                {
                    var parts = kv.Key.Split('|');
                    return (parts[2], kv.Value);
                })
                .ToList();
        }
    }

    public IReadOnlyList<string> ListMeasurements(string db)
    {
        lock (_lock)
        {
            var prefix = $"{db}|";
            return _schema.Keys
                .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
                .Select(k => k.Split('|')[1])
                .Distinct(StringComparer.Ordinal)
                .Order()
                .ToArray();
        }
    }

    private void Load()
    {
        if (!File.Exists(_schemaPath)) return;
        try
        {
            var json = File.ReadAllText(_schemaPath);
            var entries = JsonSerializer.Deserialize(json, SchemaJsonContext.Default.ListSchemaEntry);
            if (entries == null) return;
            foreach (var e in entries)
                _schema[MakeKey(e.Db, e.Measurement, e.Field)] = (FieldKind)e.Kind;
        }
        catch { /* corrupted schema, start fresh */ }
    }

    private void Save()
    {
        var entries = _schema.Select(kv =>
        {
            var parts = kv.Key.Split('|');
            return new SchemaEntry(parts[0], parts[1], parts[2], (byte)kv.Value);
        }).ToList();

        var json = JsonSerializer.Serialize(entries, SchemaJsonContext.Default.ListSchemaEntry);
        File.WriteAllText(_schemaPath, json);
    }
}

public sealed record SchemaEntry(string Db, string Measurement, string Field, byte Kind);

public sealed class FieldConflictException : Exception
{
    public FieldConflictException(string message) : base(message) { }
}

[JsonSerializable(typeof(List<SchemaEntry>))]
internal partial class SchemaJsonContext : JsonSerializerContext { }

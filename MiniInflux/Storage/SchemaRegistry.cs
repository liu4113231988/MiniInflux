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
    private readonly Dictionary<string, Dictionary<string, Dictionary<string, FieldKind>>> _schema = new(StringComparer.Ordinal);
    private bool _dirty;
    private volatile bool _dirtyVol;

    public SchemaRegistry(string dataPath, int maxFieldsPerMeasurement = 1024)
    {
        var metaDir = Path.Combine(dataPath, "meta");
        Directory.CreateDirectory(metaDir);
        _schemaPath = Path.Combine(metaDir, "schema.json");
        _maxFieldsPerMeasurement = maxFieldsPerMeasurement;
        Load();
    }

    /// <summary>
    /// Persist schema if changes have been made since the last save.
    /// Called by the engine on a periodic timer to batch schema writes.
    /// </summary>
    public void SaveIfDirty()
    {
        if (!_dirtyVol) return;
        lock (_lock)
        {
            if (!_dirty) return;
            Save();
        }
    }

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
            var changed = RegisterFieldsLocked(db, measurement, batchFields);

            if (changed)
            {
                _dirty = true;
                _dirtyVol = true;
            }
        }
    }

    public void ValidateAndRegisterColumns(string db, IEnumerable<SegmentColumn> columns)
    {
        var byMeasurement = new Dictionary<string, Dictionary<string, FieldKind>>(StringComparer.Ordinal);
        foreach (var column in columns)
        {
            if (!byMeasurement.TryGetValue(column.Measurement, out var fields))
            {
                fields = new(StringComparer.Ordinal);
                byMeasurement[column.Measurement] = fields;
            }

            if (fields.TryGetValue(column.Field, out var existing) && existing != column.Kind)
                throw new FieldConflictException(
                    $"field type conflict: {column.Measurement}.{column.Field} is {existing}, got {column.Kind}");
            fields[column.Field] = column.Kind;
        }

        lock (_lock)
        {
            var changed = false;
            foreach (var (measurement, fields) in byMeasurement)
                changed |= RegisterFieldsLocked(db, measurement, fields);

            if (changed)
            {
                _dirty = true;
                _dirtyVol = true;
            }
        }
    }

    /// <summary>
    /// Get the registered field type for a specific field, or null if not registered.
    /// </summary>
    public FieldKind? GetFieldType(string db, string measurement, string fieldKey)
    {
        lock (_lock)
        {
            return _schema.TryGetValue(db, out var measurements)
                && measurements.TryGetValue(measurement, out var fields)
                && fields.TryGetValue(fieldKey, out var kind)
                ? kind
                : null;
        }
    }

    /// <summary>
    /// Get all registered fields for a measurement.
    /// </summary>
    public IReadOnlyList<(string FieldKey, FieldKind Kind)> GetFields(string db, string? measurement)
    {
        lock (_lock)
        {
            if (!_schema.TryGetValue(db, out var measurements))
                return [];
            if (measurement != null)
                return measurements.TryGetValue(measurement, out var fields)
                    ? fields.Select(kv => (kv.Key, kv.Value)).ToList()
                    : [];
            return measurements.Values.SelectMany(fields => fields.Select(kv => (kv.Key, kv.Value))).ToList();
        }
    }

    public IReadOnlyList<string> ListMeasurements(string db)
    {
        lock (_lock)
        {
            return _schema.TryGetValue(db, out var measurements)
                ? measurements.Keys.Order().ToArray()
                : [];
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
                GetFieldsMap(e.Db, e.Measurement)[e.Field] = (FieldKind)e.Kind;
        }
        catch { /* corrupted schema, start fresh */ }
    }

    private void Save()
    {
        var entries = _schema.SelectMany(db =>
            db.Value.SelectMany(measurement =>
                measurement.Value.Select(field =>
                    new SchemaEntry(db.Key, measurement.Key, field.Key, (byte)field.Value)))).ToList();

        var json = JsonSerializer.Serialize(entries, SchemaJsonContext.Default.ListSchemaEntry);
        var tmp = _schemaPath + ".tmp";
        File.WriteAllText(tmp, json);
        File.Move(tmp, _schemaPath, overwrite: true);
        _dirty = false;
        _dirtyVol = false;
    }

    private bool RegisterFieldsLocked(string db, string measurement, Dictionary<string, FieldKind> batchFields)
    {
        var fields = GetFieldsMap(db, measurement);
        var changed = false;
        foreach (var field in batchFields)
        {
            if (fields.TryGetValue(field.Key, out var existing))
            {
                if (existing != field.Value)
                    throw new FieldConflictException(
                        $"field type conflict: {measurement}.{field.Key} is {existing}, got {field.Value}");
            }
            else
            {
                fields[field.Key] = field.Value;
                changed = true;
            }
        }

        if (_maxFieldsPerMeasurement > 0 && fields.Count > _maxFieldsPerMeasurement)
            throw new FieldConflictException(
                $"max fields per measurement exceeded: {measurement} has {fields.Count} fields (limit: {_maxFieldsPerMeasurement})");

        return changed;
    }

    private Dictionary<string, FieldKind> GetFieldsMap(string db, string measurement)
    {
        if (!_schema.TryGetValue(db, out var measurements))
        {
            measurements = new(StringComparer.Ordinal);
            _schema[db] = measurements;
        }
        if (!measurements.TryGetValue(measurement, out var fields))
        {
            fields = new(StringComparer.Ordinal);
            measurements[measurement] = fields;
        }
        return fields;
    }
}

public sealed record SchemaEntry(string Db, string Measurement, string Field, byte Kind);

public sealed class FieldConflictException : Exception
{
    public FieldConflictException(string message) : base(message) { }
}

[JsonSerializable(typeof(List<SchemaEntry>))]
internal partial class SchemaJsonContext : JsonSerializerContext { }

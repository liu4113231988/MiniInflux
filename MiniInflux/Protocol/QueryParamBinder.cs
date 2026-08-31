using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;

namespace MiniInflux.Net10.Protocol;

/// <summary>
/// P3 参数化查询：`$name` 占位符在 WHERE 谓词中解析为 ParamFilter，请求时经
/// ApplyParams 在解析树级别绑定值。绑定值永不拼回 SQL 文本，因此不存在注入面；
/// 模板文本不变，解析结果可跨不同参数值复用（ParseCached）。
/// </summary>
public static class QueryParamBinder
{
    private static readonly ConcurrentDictionary<string, ParsedQuery> _parseCache = new(StringComparer.Ordinal);
    private const int MaxCacheSize = 1024;

    public static ParsedQuery ParseCached(string query)
    {
        if (_parseCache.TryGetValue(query, out var cached)) return cached;
        var parsed = InfluxQlParser.Parse(query);
        // Bounded cache: TryAdd is atomic; eviction is best-effort under contention (overshoot by at most a few entries).
        if (_parseCache.Count >= MaxCacheSize)
        {
            var first = _parseCache.Keys.FirstOrDefault();
            if (first != null) _parseCache.TryRemove(first, out _);
        }
        _parseCache.TryAdd(query, parsed);
        // If another thread raced, return the existing entry to keep single cached instance
        if (_parseCache.TryGetValue(query, out var winner) && !ReferenceEquals(winner, parsed))
            return winner;
        return parsed;
    }

    public static bool HasUnboundParams(ParsedQuery q) =>
        q.ParamFilters.Count > 0
        || (q.Subquery != null && HasUnboundParams(q.Subquery))
        || (q.ExplainedQuery != null && HasUnboundParams(q.ExplainedQuery));

    /// <summary>
    /// Bind `$name` placeholders in a parsed template. Throws FormatException when a
    /// placeholder has no value, when the value type does not fit the operator, or when
    /// parameters appear outside SELECT (incl. subquery/EXPLAIN) statements.
    /// Never mutates the template: resolved filters go into fresh lists so cached
    /// templates stay reusable across requests.
    /// </summary>
    public static ParsedQuery ApplyParams(ParsedQuery template, IReadOnlyDictionary<string, JsonElement>? map)
    {
        if (template.Kind == QueryKind.Explain && template.ExplainedQuery != null)
            return template with { ExplainedQuery = ApplyParams(template.ExplainedQuery, map) };

        if (template.Kind != QueryKind.Select)
            throw new FormatException($"parameters are not supported in {template.Kind} statements");

        var boundSubquery = template.Subquery != null ? ApplyParams(template.Subquery, map) : null;
        if (template.ParamFilters.Count == 0)
            return boundSubquery == template.Subquery ? template : template with { Subquery = boundSubquery };

        var tagFilters = new List<TagFilter>(template.TagFilters);
        var fieldFilters = new List<FieldFilter>(template.FieldFilters);
        long? min = template.MinTimeNs, max = template.MaxTimeNs;
        foreach (var param in template.ParamFilters)
        {
            if (map == null || !map.TryGetValue(param.Name, out var value))
                throw new FormatException($"missing parameter: ${param.Name}");

            if (string.Equals(param.Key, "time", StringComparison.OrdinalIgnoreCase))
            {
                var (pMin, pMax) = ResolveTimeParam(param, value);
                if (pMin.HasValue) min = min.HasValue ? Math.Max(min.Value, pMin.Value) : pMin;
                if (pMax.HasValue) max = max.HasValue ? Math.Min(max.Value, pMax.Value) : pMax;
                continue;
            }

            ResolveValueParam(param, value, tagFilters, fieldFilters);
        }

        return template with
        {
            TagFilters = tagFilters,
            FieldFilters = fieldFilters,
            ParamFilters = [],
            MinTimeNs = min,
            MaxTimeNs = max,
            Subquery = boundSubquery
        };
    }

    static (long? Min, long? Max) ResolveTimeParam(ParamFilter param, JsonElement value)
    {
        string text;
        if (value.ValueKind == JsonValueKind.String)
            text = value.GetString()!;
        else if (value.ValueKind == JsonValueKind.Number)
        {
            // Only integer epoch values are accepted for numeric time params; fractional/scientific would be mis-parsed as dates
            if (!value.TryGetInt64(out var intNs))
                throw new FormatException($"parameter ${param.Name} must be an integer timestamp for time comparison: {value.GetRawText()}");
            text = intNs.ToString(CultureInfo.InvariantCulture);
        }
        else
            throw new FormatException($"parameter ${param.Name} must be a timestamp string for time comparison");

        long ns;
        try { ns = InfluxQlParser.ParseTime(text); }
        catch (Exception ex) { throw new FormatException($"parameter ${param.Name} is not a valid timestamp: {text}", ex); }

        return param.Op switch
        {
            ">=" => (ns, null),
            ">" => (checked(ns + 1), null),
            "<=" => (null, ns),
            "<" => (null, checked(ns - 1)),
            "=" => (ns, ns),
            _ => throw new FormatException($"operator {param.Op} is not supported for time parameters")
        };
    }

    // Value binding mirrors the literal-parsing semantics of InfluxQlParser.TryParseFilter:
    // quoted strings and booleans/nulls become tag filters, numbers become field filters,
    // and range operators require numeric values.
    static void ResolveValueParam(ParamFilter param, JsonElement value, List<TagFilter> tagFilters, List<FieldFilter> fieldFilters)
    {
        switch (param.Op)
        {
            case "=~":
            case "!~":
                if (value.ValueKind != JsonValueKind.String)
                    throw new FormatException($"parameter ${param.Name} must be a string for regex comparison");
                tagFilters.Add(new TagFilter(param.Key, value.GetString()!, param.Op == "=~" ? TagOp.Regex : TagOp.NotRegex));
                return;
            case "=":
            case "!=":
            case "<>":
                var eqOp = param.Op == "=" ? FieldOp.Eq : FieldOp.Neq;
                switch (value.ValueKind)
                {
                    case JsonValueKind.String:
                        tagFilters.Add(new TagFilter(param.Key, value.GetString()!, param.Op == "=" ? TagOp.Eq : TagOp.Neq));
                        return;
                    case JsonValueKind.Number:
                        fieldFilters.Add(new FieldFilter(param.Key, value.GetDouble(), eqOp));
                        return;
                    case JsonValueKind.True:
                    case JsonValueKind.False:
                        tagFilters.Add(new TagFilter(param.Key, value.GetBoolean() ? "true" : "false", param.Op == "=" ? TagOp.Eq : TagOp.Neq));
                        return;
                    case JsonValueKind.Null:
                        tagFilters.Add(new TagFilter(param.Key, "null", param.Op == "=" ? TagOp.Eq : TagOp.Neq));
                        return;
                    default:
                        throw new FormatException($"parameter ${param.Name} has unsupported type {value.ValueKind}");
                }
            default:
                var rangeOp = param.Op switch
                {
                    ">=" => FieldOp.Gte,
                    "<=" => FieldOp.Lte,
                    ">" => FieldOp.Gt,
                    "<" => FieldOp.Lt,
                    _ => throw new FormatException($"unsupported parameter operator: {param.Op}")
                };
                double numeric;
                if (value.ValueKind == JsonValueKind.Number)
                    numeric = value.GetDouble();
                else if (value.ValueKind == JsonValueKind.String
                    && double.TryParse(value.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
                    numeric = parsed;
                else
                    throw new FormatException($"parameter ${param.Name} must be numeric for {param.Op} comparison");
                fieldFilters.Add(new FieldFilter(param.Key, numeric, rangeOp));
                return;
        }
    }

    public static bool TryParseParamsJson(string? json, out Dictionary<string, JsonElement> map)
    {
        map = new(StringComparer.Ordinal);
        if (string.IsNullOrWhiteSpace(json)) return false;
        try
        {
            var dict = JsonSerializer.Deserialize(json, AppJsonContext.Default.DictionaryStringJsonElement);
            if (dict == null) return false;
            map = dict;
            return true;
        }
        catch { return false; }
    }
}

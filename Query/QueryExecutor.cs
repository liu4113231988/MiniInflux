using System.Text.RegularExpressions;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Net10.Query;

public sealed class QueryResponse
{
    public List<QueryResult> Results { get; set; } = [];
}

public sealed class QueryResult
{
    public int StatementId { get; set; }
    public List<QuerySeries>? Series { get; set; }
    public string? Error { get; set; }
}

public sealed class QuerySeries
{
    public string Name { get; set; } = "";
    public Dictionary<string, string>? Tags { get; set; }
    public List<string> Columns { get; set; } = [];
    public List<List<object?>> Values { get; set; } = [];
}

public sealed class QueryExecutor
{
    public Task<QueryResponse> ExecuteAsync(TsdbEngine e, string? db, string q)
    {
        try
        {
            return Task.FromResult(new QueryResponse
            {
                Results = [new QueryResult { StatementId = 0, Series = Run(e, db, InfluxQlParser.Parse(q)) }]
            });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new QueryResponse
            {
                Results = [new QueryResult { StatementId = 0, Error = ex.Message }]
            });
        }
    }

    static List<QuerySeries>? Run(TsdbEngine e, string? db, ParsedQuery q)
    {
        switch (q.Kind)
        {
            case QueryKind.CreateDatabase:
                e.CreateDatabase(q.Database!);
                return null;
            case QueryKind.ShowDatabases:
                return [new() { Name = "databases", Columns = ["name"],
                    Values = e.ListDatabases().Select(x => new List<object?> { x }).ToList() }];
            case QueryKind.ShowMeasurements:
                Req(db);
                return [new() { Name = "measurements", Columns = ["name"],
                    Values = e.ListMeasurements(db!).Select(x => new List<object?> { x }).ToList() }];
            case QueryKind.ShowFieldKeys:
                Req(db);
                return [new() { Name = q.Measurement ?? "fieldKeys", Columns = ["fieldKey", "fieldType"],
                    Values = e.ListFieldKeys(db!, q.Measurement).Select(x => new List<object?> { x.Field, x.Kind.ToString().ToLowerInvariant() }).ToList() }];
            case QueryKind.ShowTagKeys:
                Req(db);
                return [new() { Name = q.Measurement ?? "tagKeys", Columns = ["tagKey"],
                    Values = e.ListTagKeys(db!, q.Measurement).Select(x => new List<object?> { x }).ToList() }];
            case QueryKind.ShowTagValues:
                Req(db);
                return [new() { Name = q.Measurement ?? "tagValues", Columns = ["key", "value"],
                    Values = e.ListTagValues(db!, q.Measurement, q.TagKey ?? "").Select(x => new List<object?> { x.Key, x.Value }).ToList() }];
            case QueryKind.CreateRetentionPolicy:
                Req(q.Database);
                e.CreateDatabase(q.Database!);
                e.Meta.CreateRetentionPolicy(q.Database!, q.RpName!, q.RpDurationNs ?? 0, q.RpDefault ?? false);
                return null;
            case QueryKind.AlterRetentionPolicy:
                Req(q.Database);
                e.Meta.AlterRetentionPolicy(q.Database!, q.RpName!, q.RpDurationNs, q.RpDefault);
                return null;
            case QueryKind.DropRetentionPolicy:
                Req(q.Database);
                e.Meta.DropRetentionPolicy(q.Database!, q.RpName!);
                return null;
            case QueryKind.ShowRetentionPolicies:
                Req(q.Database ?? db);
                var rpList = e.Meta.ListRetentionPolicies(q.Database ?? db!);
                return [new() { Name = "retention policies", Columns = ["name", "duration", "replicaN", "default"],
                    Values = rpList.Select(r => new List<object?> { r.Name, FormatDuration(r.DurationNs), r.Replication, r.IsDefault }).ToList() }];
            case QueryKind.DropDatabase:
                Req(q.Database);
                e.DropDatabase(q.Database!);
                return null;
            case QueryKind.DropMeasurement:
                Req(db);
                e.DropMeasurement(db!, q.Measurement!);
                return null;
            case QueryKind.Delete:
                Req(db);
                e.DeleteFromMeasurement(db!, q.Measurement!, q.MinTimeNs, q.MaxTimeNs);
                return null;
            case QueryKind.Select:
                Req(db);
                return Select(e, db!, q);
            default:
                throw new NotSupportedException();
        }
    }

    static List<QuerySeries> Select(TsdbEngine e, string db, ParsedQuery q)
    {
        // Projection pushdown: only read needed fields from segments
        HashSet<string>? requestedFields = null;
        if (q.Select.Count > 0 && !(q.Select.Count == 1 && q.Select[0].Field == "*"))
            requestedFields = new HashSet<string>(q.Select.Select(x => x.Field), StringComparer.Ordinal);

        var pts = e.ReadAllPoints(db, "autogen", q.Measurement, q.MinTimeNs, q.MaxTimeNs, requestedFields);
        pts = ApplyTagFilters(pts, q.TagFilters);
        pts = ApplyFieldFilters(pts, q.FieldFilters);
        if (q.Desc) pts = pts.OrderByDescending(x => x.TimestampNs).ToList();

        if (q.GroupByNs.HasValue || q.GroupByTags.Count > 0)
            return AggGroupBy(pts, q);

        if (q.Limit.HasValue) pts = pts.Take(q.Limit.Value).ToList();

        var fields = q.Select.Count == 1 && q.Select[0].Field == "*"
            ? pts.SelectMany(p => p.Fields.Keys).Distinct().Order().ToList()
            : q.Select.Select(x => x.Field).ToList();
        var tags = pts.SelectMany(p => p.Tags.Keys).Distinct().Order().ToList();
        var cols = new List<string> { "time" };
        cols.AddRange(tags);
        cols.AddRange(fields);

        var vals = new List<List<object?>>();
        foreach (var p in pts)
        {
            var row = new List<object?> { Time(p.TimestampNs) };
            foreach (var t in tags) row.Add(p.Tags.TryGetValue(t, out var v) ? v : null);
            foreach (var f in fields) row.Add(p.Fields.TryGetValue(f, out var v) ? v.ToObject() : null);
            vals.Add(row);
        }
        return [new() { Name = q.Measurement ?? "", Columns = cols, Values = vals }];
    }

    static List<QuerySeries> AggGroupBy(List<Point> pts, ParsedQuery q)
    {
        long? step = q.GroupByNs;
        var tagNames = q.GroupByTags;
        var items = q.Select.Where(x => x.Func != "").ToList();
        if (items.Count == 0) items = [new("count", "*", "count")];

        var groups = new Dictionary<(string TagKey, long? BucketTime), List<Point>>();
        foreach (var p in pts)
        {
            var tagVals = tagNames.Select(tn => p.Tags.TryGetValue(tn, out var v) ? v : "").ToArray();
            var tagKey = string.Join("|", tagVals);
            long? bucketTime = step.HasValue ? p.TimestampNs / step.Value * step.Value : null;
            var key = (tagKey, bucketTime);
            if (!groups.TryGetValue(key, out var list)) { list = []; groups[key] = list; }
            list.Add(p);
        }

        var seriesMap = new Dictionary<string, QuerySeries>();
        foreach (var (key, groupPts) in groups.OrderBy(g => g.Key.BucketTime ?? 0))
        {
            var tagVals = key.TagKey.Split('|');
            var tagsDict = tagNames.Count > 0
                ? tagNames.Zip(tagVals, (n, v) => (n, v)).ToDictionary(x => x.n, x => x.v)
                : null;
            var seriesKey = key.TagKey;
            if (!seriesMap.TryGetValue(seriesKey, out var series))
            {
                var cols = new List<string> { "time" };
                cols.AddRange(items.Select(x => x.Alias));
                series = new QuerySeries { Name = q.Measurement ?? "", Tags = tagsDict, Columns = cols, Values = [] };
                seriesMap[seriesKey] = series;
            }
            var row = new List<object?> { Time(key.BucketTime ?? 0) };
            foreach (var it in items)
            {
                var groupValues = groupPts
                    .Select(p => it.Field == "*"
                        ? FieldValue.FromInteger(p.Fields.Count)
                        : p.Fields.TryGetValue(it.Field, out var v) ? v : (FieldValue?)null)
                    .Where(v => v.HasValue).Select(v => v!.Value).ToList();
                var groupTimestamps = groupPts.Select(p => p.TimestampNs).ToList();
                row.Add(Calc(groupValues, it.Func, it.Param, groupTimestamps));
            }
            series.Values.Add(row);
        }

        if (step.HasValue && q.Fill != FillMode.None && q.MinTimeNs.HasValue && q.MaxTimeNs.HasValue)
            ApplyFill(seriesMap, q, step.Value, items);

        if (q.Limit.HasValue)
            foreach (var s in seriesMap.Values) s.Values = s.Values.Take(q.Limit.Value).ToList();

        return seriesMap.Values.ToList();
    }

    static void ApplyFill(Dictionary<string, QuerySeries> seriesMap, ParsedQuery q, long step, List<SelectItem> items)
    {
        var minBucket = q.MinTimeNs!.Value / step * step;
        var maxBucket = q.MaxTimeNs!.Value / step * step;
        foreach (var series in seriesMap.Values)
        {
            var timeSet = new HashSet<long>();
            foreach (var row in series.Values)
            {
                if (row[0] is string ts && DateTimeOffset.TryParse(ts, out var dto))
                    timeSet.Add(dto.ToUnixTimeMilliseconds() * 1_000_000);
            }
            var filledRows = new List<List<object?>>();
            object? prevValue = null;
            for (long t = minBucket; t <= maxBucket; t += step)
            {
                if (timeSet.Contains(t))
                {
                    var existing = series.Values.FirstOrDefault(v =>
                        v[0] is string ts2 && DateTimeOffset.TryParse(ts2, out var dto2)
                        && dto2.ToUnixTimeMilliseconds() * 1_000_000 == t);
                    if (existing != null) { filledRows.Add(existing); if (q.Fill == FillMode.Previous && existing.Count > 1) prevValue = existing[1]; continue; }
                }
                var row = new List<object?> { Time(t) };
                for (int i = 0; i < items.Count; i++)
                {
                    row.Add(q.Fill switch { FillMode.Zero => (object?)0, FillMode.Previous => prevValue, _ => null });
                }
                filledRows.Add(row);
            }
            series.Values = filledRows.OrderBy(r => r[0]?.ToString()).ToList();
        }
    }

    static List<Point> ApplyTagFilters(List<Point> pts, List<TagFilter> filters)
    {
        if (filters.Count == 0) return pts;
        return pts.Where(p =>
        {
            foreach (var f in filters)
            {
                var tagVal = p.Tags.TryGetValue(f.Key, out var v) ? v : null;
                switch (f.Op)
                {
                    case TagOp.Eq: if (tagVal != f.Value) return false; break;
                    case TagOp.Neq: if (tagVal == f.Value) return false; break;
                    case TagOp.Regex: if (tagVal == null || !Regex.IsMatch(tagVal, f.Value)) return false; break;
                    case TagOp.NotRegex: if (tagVal != null && Regex.IsMatch(tagVal, f.Value)) return false; break;
                }
            }
            return true;
        }).ToList();
    }

    static List<Point> ApplyFieldFilters(List<Point> pts, List<FieldFilter> filters)
    {
        if (filters.Count == 0) return pts;
        return pts.Where(p =>
        {
            foreach (var f in filters)
            {
                if (!p.Fields.TryGetValue(f.Field, out var fv)) return false;
                var numVal = fv.AsDouble();
                if (!numVal.HasValue) return false;
                var ok = f.Op switch
                {
                    FieldOp.Eq => Math.Abs(numVal.Value - f.Value) < 1e-9,
                    FieldOp.Neq => Math.Abs(numVal.Value - f.Value) >= 1e-9,
                    FieldOp.Gt => numVal.Value > f.Value, FieldOp.Gte => numVal.Value >= f.Value,
                    FieldOp.Lt => numVal.Value < f.Value, FieldOp.Lte => numVal.Value <= f.Value,
                    _ => false
                };
                if (!ok) return false;
            }
            return true;
        }).ToList();
    }

    static object? Calc(List<FieldValue> vs, string fn, double param = 0, List<long>? timestamps = null)
    {
        if (fn == "count") return vs.Count;
        if (vs.Count == 0) return null;
        var nums = vs.Select(v => v.AsDouble()).Where(x => x.HasValue).Select(x => x!.Value).ToList();
        return fn switch
        {
            "sum" => nums.Count == 0 ? null : nums.Sum(),
            "mean" => nums.Count == 0 ? null : nums.Average(),
            "min" => nums.Count == 0 ? null : nums.Min(),
            "max" => nums.Count == 0 ? null : nums.Max(),
            "first" => vs[0].ToObject(),
            "last" => vs[^1].ToObject(),
            "spread" => nums.Count == 0 ? null : nums.Max() - nums.Min(),
            "stddev" => nums.Count < 2 ? null : StdDev(nums),
            "median" => nums.Count == 0 ? null : Percentile(nums, 50),
            "percentile" => nums.Count == 0 ? null : Percentile(nums, param),
            "derivative" => CalcDerivative(timestamps, nums, false),
            "non_negative_derivative" => CalcDerivative(timestamps, nums, true),
            "difference" => nums.Count < 2 ? null : nums[^1] - nums[0],
            "cumulative_sum" => nums.Count == 0 ? null : nums.Sum(),
            "moving_average" => nums.Count == 0 ? null : nums.Average(),
            _ => null
        };
    }

    static double StdDev(List<double> nums)
    {
        if (nums.Count < 2) return 0;
        var mean = nums.Average();
        var sumSq = nums.Sum(x => (x - mean) * (x - mean));
        return Math.Sqrt(sumSq / (nums.Count - 1));
    }

    static double Percentile(List<double> nums, double p)
    {
        if (nums.Count == 0) return 0;
        var sorted = nums.Order().ToList();
        var rank = (p / 100.0) * (sorted.Count - 1);
        var lower = (int)Math.Floor(rank);
        var upper = (int)Math.Ceiling(rank);
        if (lower == upper) return sorted[lower];
        return sorted[lower] + (rank - lower) * (sorted[upper] - sorted[lower]);
    }

    static double? CalcDerivative(List<long>? timestamps, List<double> nums, bool nonNegative)
    {
        if (timestamps == null || timestamps.Count < 2 || nums.Count < 2) return null;
        var valDiff = nums[^1] - nums[0];
        var timeDiff = (timestamps[^1] - timestamps[0]) / 1_000_000_000.0; // convert ns to seconds
        if (timeDiff <= 0) return null;
        var rate = valDiff / timeDiff;
        if (nonNegative && rate < 0) return 0;
        return rate;
    }

    static void Req(string? db) { if (string.IsNullOrWhiteSpace(db)) throw new InvalidOperationException("missing required parameter db"); }

    static string Time(long ns) => DateTimeOffset.FromUnixTimeMilliseconds(ns / 1_000_000).UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");

    static string FormatDuration(long ns)
    {
        if (ns <= 0) return "0s";
        var s = ns / 1_000_000_000;
        if (s % 86400 == 0 && s >= 86400) return $"{s / 86400}d";
        if (s % 3600 == 0 && s >= 3600) return $"{s / 3600}h";
        if (s % 60 == 0 && s >= 60) return $"{s / 60}m";
        return $"{s}s";
    }
}

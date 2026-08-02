using System.Globalization;

namespace MiniInflux.Net10.Protocol;

public enum QueryKind
{
    CreateDatabase, ShowDatabases, ShowMeasurements, ShowFieldKeys, ShowTagKeys, ShowTagValues, Select,
    CreateRetentionPolicy, AlterRetentionPolicy, DropRetentionPolicy, ShowRetentionPolicies,
    CreateContinuousQuery, ShowContinuousQueries, DropContinuousQuery,
    DropDatabase, DropMeasurement, DropSeries, DropShard, Delete,
    ShowSeries, ShowSeriesCardinality, ShowMeasurementCardinality, ShowTagValuesCardinality,
    Explain, ShowQueries, KillQuery
}

public enum TagOp { Eq, Neq, Regex, NotRegex }
public enum FieldOp { Gt, Gte, Lt, Lte, Eq, Neq }
public enum FillMode { None, Null, Zero, Previous, Linear }

public sealed record SelectItem(string Func, string Field, string Alias, double Param = 0, long? UnitNs = null)
{
    public bool IsDistinct { get; init; }
    public bool IsCountDistinct { get; init; }
}
public sealed record TagFilter(string Key, string Value, TagOp Op);
public sealed record FieldFilter(string Field, double Value, FieldOp Op);
public sealed class ParsedQuery
{
    public required QueryKind Kind { get; set; }
    public string? Database { get; init; }
    public string? Measurement { get; init; }
    public List<string> Measurements { get; init; } = [];
    public ParsedQuery? Subquery { get; init; }
    public string? SourceDatabase { get; init; }
    public string? SourceRpName { get; init; }
    public List<SelectItem> Select { get; init; } = [];
    public long? MinTimeNs { get; init; }
    public long? MaxTimeNs { get; init; }
    public int? Limit { get; init; }
    public int? Offset { get; init; }
    public int? SeriesLimit { get; init; }
    public int? SeriesOffset { get; init; }
    public bool Desc { get; init; }
    public long? GroupByNs { get; init; }
    public List<string> GroupByTags { get; init; } = [];
    public bool GroupByAllTags { get; init; }
    public FillMode Fill { get; init; } = FillMode.None;
    public string? TagKey { get; init; }
    public string? MeasurementFilter { get; init; }
    public List<TagFilter> ShowTagFilters { get; init; } = [];
    public List<TagFilter> TagFilters { get; init; } = [];
    public List<FieldFilter> FieldFilters { get; init; } = [];

    /// <summary>
    /// Groups of tag filters connected by OR logic.
    /// Each group is a list of AND-connected filters.
    /// If a point matches ANY group, it passes the OR filter.
    /// Example: tag='a' OR tag='b' AND tag2='c' becomes:
    ///   Group 1: [tag='a']
    ///   Group 2: [tag='b', tag2='c']
    /// </summary>
    public List<List<TagFilter>> OrTagFilterGroups { get; init; } = [];
    public bool HasOrFilters { get; init; }
    public string? RpName { get; init; }
    public long? RpDurationNs { get; init; }
    public bool? RpDefault { get; init; }
    public string? ContinuousQueryName { get; init; }
    public string? ContinuousQueryText { get; init; }
    public long? ContinuousQueryEveryNs { get; init; }
    public long? ContinuousQueryForNs { get; init; }
    public int? ContinuousQueryRecomputeRecentBuckets { get; init; }
    public string? IntoTarget { get; init; }
    public bool ExplainAnalyze { get; set; }
    public long? KillQueryId { get; init; }
}

public static class InfluxQlParser
{
    public static ParsedQuery Parse(string q)
    {
        q = q.Trim().TrimEnd(';');
        if (q.StartsWith("CREATE RETENTION POLICY ", StringComparison.OrdinalIgnoreCase)) return ParseCreateRp(q);
        if (q.StartsWith("ALTER RETENTION POLICY ", StringComparison.OrdinalIgnoreCase)) return ParseAlterRp(q);
        if (q.StartsWith("DROP RETENTION POLICY ", StringComparison.OrdinalIgnoreCase)) return ParseDropRp(q);
        if (q.StartsWith("CREATE CONTINUOUS QUERY ", StringComparison.OrdinalIgnoreCase)) return ParseCreateContinuousQuery(q);
        if (q.StartsWith("DROP CONTINUOUS QUERY ", StringComparison.OrdinalIgnoreCase)) return ParseDropContinuousQuery(q);
        if (q.Equals("SHOW CONTINUOUS QUERIES", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowContinuousQueries };
        if (q.StartsWith("SHOW RETENTION POLICIES", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowRetentionPolicies, Database = AfterOn(q) };
        if (q.StartsWith("DROP DATABASE ", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.DropDatabase, Database = Unq(q[14..].Trim()) };
        if (q.StartsWith("DROP MEASUREMENT ", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.DropMeasurement, Measurement = Unq(q[17..].Trim()) };
        if (q.StartsWith("DROP SERIES", StringComparison.OrdinalIgnoreCase)) return ParseDropSeries(q);
        if (q.StartsWith("DROP SHARD ", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.DropShard, Limit = int.Parse(q["DROP SHARD ".Length..].Trim(), CultureInfo.InvariantCulture) };
        if (q.StartsWith("DELETE FROM ", StringComparison.OrdinalIgnoreCase)) return ParseDelete(q);
        if (q.StartsWith("CREATE DATABASE ", StringComparison.OrdinalIgnoreCase))
        {
            var database = q[16..].Trim();
            if (database.StartsWith("IF NOT EXISTS ", StringComparison.OrdinalIgnoreCase)) database = database[14..].Trim();
            return new() { Kind = QueryKind.CreateDatabase, Database = Unq(database) };
        }
        if (q.Equals("SHOW DATABASES", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowDatabases };
        if (q.Equals("SHOW MEASUREMENTS", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowMeasurements };
        if (q.StartsWith("SHOW MEASUREMENTS", StringComparison.OrdinalIgnoreCase))
            return ParseShowMeasurements(q);
        if (q.StartsWith("SHOW SERIES CARDINALITY", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowSeriesCardinality, Measurement = AfterFrom(q) };
        if (q.StartsWith("SHOW SERIES", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowSeries, Measurement = AfterFrom(q) };
        if (q.StartsWith("SHOW MEASUREMENT CARDINALITY", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowMeasurementCardinality };
        if (q.StartsWith("SHOW TAG VALUES CARDINALITY", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowTagValuesCardinality, Measurement = AfterFrom(q), TagKey = AfterKey(q) };
        if (q.StartsWith("SHOW FIELD KEYS", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowFieldKeys, Measurement = AfterFrom(q) };
        if (q.StartsWith("SHOW TAG KEYS", StringComparison.OrdinalIgnoreCase))
            return ParseShowTagKeys(q);
        if (q.StartsWith("SHOW TAG VALUES", StringComparison.OrdinalIgnoreCase))
            return ParseShowTagValues(q);
        if (q.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase)) return ParseSelect(q);
        if (q.StartsWith("EXPLAIN ", StringComparison.OrdinalIgnoreCase))
        {
            var inner = q["EXPLAIN ".Length..].Trim();
            var analyze = inner.StartsWith("ANALYZE ", StringComparison.OrdinalIgnoreCase);
            if (analyze) inner = inner["ANALYZE ".Length..].Trim();
            var parsed = Parse(inner);
            parsed.Kind = QueryKind.Explain;
            parsed.ExplainAnalyze = analyze;
            return parsed;
        }
        if (q.StartsWith("SHOW QUERIES", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowQueries };
        if (q.StartsWith("KILL QUERY ", StringComparison.OrdinalIgnoreCase))
        {
            var idStr = q["KILL QUERY ".Length..].Trim();
            return new() { Kind = QueryKind.KillQuery, KillQueryId = long.Parse(idStr, CultureInfo.InvariantCulture) };
        }
        throw new NotSupportedException($"unsupported query: {q}");
    }

    static ParsedQuery ParseCreateRp(string q)
    {
        var rest = q["CREATE RETENTION POLICY ".Length..].Trim();
        var name = ReadToken(ref rest); ConsumeKeyword(ref rest, "ON");
        var db = ReadToken(ref rest); ConsumeKeyword(ref rest, "DURATION");
        var duration = ReadToken(ref rest); ConsumeKeyword(ref rest, "REPLICATION");
        ReadToken(ref rest);
        var isDefault = rest.TrimStart().StartsWith("DEFAULT", StringComparison.OrdinalIgnoreCase);
        return new() { Kind = QueryKind.CreateRetentionPolicy, RpName = Unq(name),
            Database = Unq(db), RpDurationNs = DurationToNs(duration), RpDefault = isDefault };
    }

    static ParsedQuery ParseAlterRp(string q)
    {
        var rest = q["ALTER RETENTION POLICY ".Length..].Trim();
        var name = ReadToken(ref rest); ConsumeKeyword(ref rest, "ON");
        var db = ReadToken(ref rest);
        long? durationNs = null; bool? isDefault = null;
        rest = rest.TrimStart();
        while (rest.Length > 0)
        {
            if (rest.StartsWith("DURATION ", StringComparison.OrdinalIgnoreCase))
            { rest = rest["DURATION ".Length..].TrimStart(); durationNs = DurationToNs(ReadToken(ref rest)); }
            else if (rest.StartsWith("DEFAULT", StringComparison.OrdinalIgnoreCase))
            { isDefault = true; rest = rest["DEFAULT".Length..].TrimStart(); }
            else ReadToken(ref rest);
            rest = rest.TrimStart();
        }
        return new() { Kind = QueryKind.AlterRetentionPolicy, RpName = Unq(name),
            Database = Unq(db), RpDurationNs = durationNs, RpDefault = isDefault };
    }

    static ParsedQuery ParseDropRp(string q)
    {
        var rest = q["DROP RETENTION POLICY ".Length..].Trim();
        var name = ReadToken(ref rest); ConsumeKeyword(ref rest, "ON"); var db = ReadToken(ref rest);
        return new() { Kind = QueryKind.DropRetentionPolicy, RpName = Unq(name), Database = Unq(db) };
    }

    static ParsedQuery ParseCreateContinuousQuery(string q)
    {
        var rest = q["CREATE CONTINUOUS QUERY ".Length..].Trim();
        var name = ReadToken(ref rest);
        ConsumeKeyword(ref rest, "ON");
        var db = ReadToken(ref rest);
        rest = rest.TrimStart();

        long? everyNs = null;
        long? forNs = null;
        int? recomputeRecentBuckets = null;
        if (rest.StartsWith("RESAMPLE ", StringComparison.OrdinalIgnoreCase))
        {
            rest = rest["RESAMPLE ".Length..].TrimStart();
            while (!rest.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase))
            {
                if (rest.StartsWith("EVERY ", StringComparison.OrdinalIgnoreCase))
                {
                    rest = rest["EVERY ".Length..];
                    everyNs = DurationToNs(ReadToken(ref rest));
                }
                else if (rest.StartsWith("FOR ", StringComparison.OrdinalIgnoreCase))
                {
                    rest = rest["FOR ".Length..];
                    forNs = DurationToNs(ReadToken(ref rest));
                }
                else if (rest.StartsWith("RECOMPUTE ", StringComparison.OrdinalIgnoreCase))
                {
                    rest = rest["RECOMPUTE ".Length..];
                    recomputeRecentBuckets = int.Parse(ReadToken(ref rest), CultureInfo.InvariantCulture);
                }
                else
                {
                    break;
                }

                rest = rest.TrimStart();
            }
        }

        var beginIndex = rest.IndexOf("BEGIN", StringComparison.OrdinalIgnoreCase);
        var endIndex = rest.LastIndexOf("END", StringComparison.OrdinalIgnoreCase);
        if (beginIndex < 0 || endIndex < 0 || endIndex <= beginIndex)
            throw new FormatException("CREATE CONTINUOUS QUERY requires BEGIN ... END");

        var queryText = rest[(beginIndex + "BEGIN".Length)..endIndex].Trim();
        var parsed = Parse(queryText);
        if (parsed.Kind != QueryKind.Select)
            throw new NotSupportedException("continuous query body must be a SELECT statement");

        return new()
        {
            Kind = QueryKind.CreateContinuousQuery,
            Database = Unq(db),
            ContinuousQueryName = Unq(name),
            ContinuousQueryText = queryText,
            ContinuousQueryEveryNs = everyNs,
            ContinuousQueryForNs = forNs,
            ContinuousQueryRecomputeRecentBuckets = recomputeRecentBuckets
        };
    }

    static ParsedQuery ParseDropContinuousQuery(string q)
    {
        var rest = q["DROP CONTINUOUS QUERY ".Length..].Trim();
        var name = ReadToken(ref rest);
        ConsumeKeyword(ref rest, "ON");
        var db = ReadToken(ref rest);
        return new()
        {
            Kind = QueryKind.DropContinuousQuery,
            Database = Unq(db),
            ContinuousQueryName = Unq(name)
        };
    }

    static ParsedQuery ParseDelete(string q)
    {
        var rest = q["DELETE FROM ".Length..].Trim();
        var target = ParseQualifiedMeasurement(ReadToken(ref rest));
        long? min = null, max = null;
var tagFilters = new List<TagFilter>(); var fieldFilters = new List<FieldFilter>();
var orGroups = new List<List<TagFilter>>(); var hasOr = false;
var upper = rest.ToUpperInvariant(); var wi = upper.IndexOf(" WHERE ");
if (wi >= 0) ParseWhere(rest[(wi + 7)..], out min, out max, tagFilters, fieldFilters, out orGroups, out hasOr);
return new() { Kind = QueryKind.Delete, Measurement = target.Measurement,
SourceDatabase = target.Database, SourceRpName = target.RetentionPolicy,
MinTimeNs = min, MaxTimeNs = max, TagFilters = tagFilters, FieldFilters = fieldFilters, OrTagFilterGroups = orGroups, HasOrFilters = hasOr };
    }

    static ParsedQuery ParseDropSeries(string q)
    {
        var rest = q["DROP SERIES".Length..].Trim();
        var measurements = new List<string>();
        long? min = null, max = null;
        var tagFilters = new List<TagFilter>(); var fieldFilters = new List<FieldFilter>();
        var orGroups = new List<List<TagFilter>>(); var hasOr = false;

        if (rest.StartsWith("FROM ", StringComparison.OrdinalIgnoreCase))
        {
            rest = rest["FROM ".Length..].Trim();
            var upperWhere = rest.ToUpperInvariant();
            var whereIndex = upperWhere.IndexOf(" WHERE ");
            var measurementText = whereIndex >= 0 ? rest[..whereIndex] : rest;
            rest = whereIndex >= 0 ? rest[whereIndex..] : string.Empty;
            measurements = SplitMeasurementList(measurementText);
        }

var upper = rest.ToUpperInvariant(); var wi = upper.IndexOf(" WHERE ");
if (wi >= 0) ParseWhere(rest[(wi + 7)..], out min, out max, tagFilters, fieldFilters, out orGroups, out hasOr);
return new()
{
Kind = QueryKind.DropSeries,
            Measurement = measurements.FirstOrDefault(),
            Measurements = measurements,
            MinTimeNs = min,
            MaxTimeNs = max,
            TagFilters = tagFilters,
            FieldFilters = fieldFilters,
            OrTagFilterGroups = orGroups,
            HasOrFilters = hasOr
        };
    }

    static ParsedQuery ParseShowMeasurements(string q)
    {
        var u = q.ToUpperInvariant();
        string? measurementFilter = null;
        List<TagFilter> tagFilters = [];

        // Parse WITH MEASUREMENT =~ /regex/
        var withIdx = u.IndexOf(" WITH MEASUREMENT ");
        if (withIdx >= 0)
        {
            var rest = q[(withIdx + 18)..].Trim();
            if (rest.Contains("=~"))
            {
                var parts = rest.Split("=~", 2);
                measurementFilter = ExtractRegex(parts[1].Trim());
            }
            else
            {
                measurementFilter = Unq(rest.Trim().Trim('\''));
            }
        }

        // Parse WHERE clause
        var whereIdx = u.IndexOf(" WHERE ");
        if (whereIdx >= 0)
        {
            var whereEnd = EndClause(u, whereIdx + 7);
            var whereClause = q[(whereIdx + 7)..whereEnd];
            long? min, max;
            List<FieldFilter> fieldFilters = [];
            List<List<TagFilter>> orGroups;
            bool hasOr;
            ParseWhere(whereClause, out min, out max, tagFilters, fieldFilters, out orGroups, out hasOr);
        }

        return new() { Kind = QueryKind.ShowMeasurements, MeasurementFilter = measurementFilter, ShowTagFilters = tagFilters };
    }

    static ParsedQuery ParseShowTagKeys(string q)
    {
        var m = AfterFrom(q);
        var u = q.ToUpperInvariant();
        var whereIdx = u.IndexOf(" WHERE ");
        var tagFilters = new List<TagFilter>();
        if (whereIdx >= 0)
        {
            var whereEnd = EndClause(u, whereIdx + 7);
            var whereClause = q[(whereIdx + 7)..whereEnd];
            long? min, max;
            List<FieldFilter> fieldFilters = [];
            List<List<TagFilter>> orGroups;
            bool hasOr;
            ParseWhere(whereClause, out min, out max, tagFilters, fieldFilters, out orGroups, out hasOr);
        }
        return new() { Kind = QueryKind.ShowTagKeys, Measurement = m, ShowTagFilters = tagFilters };
    }

    static ParsedQuery ParseShowTagValues(string q)
    {
        var m = AfterFrom(q);
        var key = AfterKey(q);
        var u = q.ToUpperInvariant();
        var tagFilters = new List<TagFilter>();

        // Parse WHERE clause for tag value filtering
        var whereIdx = u.IndexOf(" WHERE ");
        if (whereIdx >= 0)
        {
            var whereEnd = EndClause(u, whereIdx + 7);
            var whereClause = q[(whereIdx + 7)..whereEnd];
            long? min, max;
            List<FieldFilter> fieldFilters = [];
            List<List<TagFilter>> orGroups;
            bool hasOr;
            ParseWhere(whereClause, out min, out max, tagFilters, fieldFilters, out orGroups, out hasOr);
        }

        return new() { Kind = QueryKind.ShowTagValues, Measurement = m, TagKey = key, ShowTagFilters = tagFilters };
    }

    static ParsedQuery ParseSelect(string q)
    {
        int fi = IndexOfTopLevelKeyword(q, " FROM ");
        if (fi < 0) throw new FormatException("SELECT requires FROM");
        var fieldText = q[7..fi].Trim();
        string? intoTarget = null;
        var intoIndex = fieldText.ToUpperInvariant().LastIndexOf(" INTO ", StringComparison.Ordinal);
        if (intoIndex >= 0)
        {
            intoTarget = fieldText[(intoIndex + 6)..].Trim();
            fieldText = fieldText[..intoIndex].Trim();
        }
        var rest = q[(fi + 6)..].Trim();
        ParsedQuery? subquery = null;
        string? measurement = null;
        string? sourceDb = null;
        string? sourceRp = null;
        string tail;

        if (rest.StartsWith('('))
        {
            var closeIndex = FindMatchingParen(rest, 0);
            if (closeIndex < 0)
                throw new FormatException("subquery requires closing ')'");

            var inner = rest[1..closeIndex].Trim();
            subquery = Parse(inner);
            if (subquery.Kind != QueryKind.Select)
                throw new NotSupportedException("subquery must be a SELECT statement");
            tail = rest.Length > closeIndex + 1 ? rest[(closeIndex + 1)..] : "";
        }
        else
        {
            string sourceToken = ReadSourceToken(rest);
            var source = ParseQualifiedMeasurement(sourceToken);
            measurement = source.Measurement;
            sourceDb = source.Database;
            sourceRp = source.RetentionPolicy;
            tail = rest.Length > sourceToken.Length ? rest[sourceToken.Length..] : "";
        }

        long? min = null, max = null, gb = null; int? limit = null, offset = null;
        int? slimit = null, soffset = null;
        var tu = tail.ToUpperInvariant();
        bool desc = tu.Contains(" ORDER BY TIME DESC");
        var tagFilters = new List<TagFilter>(); var fieldFilters = new List<FieldFilter>();
        var orGroups = new List<List<TagFilter>>(); var hasOr = false;
        var groupByTags = new List<string>(); var groupByAllTags = false; var fill = FillMode.None;
        int wi = tu.IndexOf(" WHERE ");
        if (wi >= 0) { var end = EndClause(tu, wi + 7); ParseWhere(tail[(wi + 7)..end], out min, out max, tagFilters, fieldFilters, out orGroups, out hasOr); }
        var gu = tu.IndexOf(" GROUP BY ");
        if (gu >= 0) { var gs = gu + 10; var ge = EndClause(tu, gs); ParseGroupBy(tail[gs..ge].Trim(), out gb, groupByTags, out groupByAllTags); }
        var fu = tu.IndexOf(" FILL(");
        if (fu >= 0) { var fe = tu.IndexOf(')', fu); if (fe >= 0) { var fv = tail[(fu + 6)..fe].Trim().ToLowerInvariant();
            fill = fv switch { "null" => FillMode.Null, "0" => FillMode.Zero, "previous" => FillMode.Previous, "linear" => FillMode.Linear, _ => FillMode.None }; } }
        var lu = tu.LastIndexOf(" LIMIT ");
        if (lu >= 0) limit = int.Parse(tail[(lu + 7)..].Trim().Split(' ')[0], CultureInfo.InvariantCulture);
        var ou = tu.LastIndexOf(" OFFSET ");
        if (ou >= 0) offset = int.Parse(tail[(ou + 8)..].Trim().Split(' ')[0], CultureInfo.InvariantCulture);
        var slu = tu.LastIndexOf(" SLIMIT ");
        if (slu >= 0) slimit = int.Parse(tail[(slu + 8)..].Trim().Split(' ')[0], CultureInfo.InvariantCulture);
        var sou = tu.LastIndexOf(" SOFFSET ");
        if (sou >= 0) soffset = int.Parse(tail[(sou + 9)..].Trim().Split(' ')[0], CultureInfo.InvariantCulture);
        return new() { Kind = QueryKind.Select, Measurement = measurement, Subquery = subquery, SourceDatabase = sourceDb, SourceRpName = sourceRp, Select = ParseItems(fieldText),
            MinTimeNs = min, MaxTimeNs = max, Limit = limit, Desc = desc, GroupByNs = gb,
            Offset = offset, SeriesLimit = slimit, SeriesOffset = soffset,
            GroupByTags = groupByTags, GroupByAllTags = groupByAllTags, Fill = fill, TagFilters = tagFilters, FieldFilters = fieldFilters, OrTagFilterGroups = orGroups, HasOrFilters = hasOr, IntoTarget = intoTarget };
    }

    static void ParseGroupBy(string text, out long? gbNs, List<string> tags, out bool groupByAllTags)
    {
        gbNs = null;
        groupByAllTags = false;
        foreach (var part in text.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var p = part.Trim(); var pu = p.ToUpperInvariant();
            if (pu.StartsWith("TIME(") && p.EndsWith(')')) gbNs = DurationToNs(p[5..^1]);
            else if (p == "*") groupByAllTags = true;
            else tags.Add(Unq(p));
        }
    }

    static void ParseWhere(string where, out long? min, out long? max, List<TagFilter> tagFilters, List<FieldFilter> fieldFilters, out List<List<TagFilter>> orGroups, out bool hasOr)
    {
        min = null; max = null;
        orGroups = [];
        hasOr = false;

        // Split by OR at top level first
        var orBranches = SplitTopLevelOrClauses(where);

        if (orBranches.Count > 1)
        {
            hasOr = true;
            // Each branch becomes a separate AND-group
            foreach (var branch in orBranches)
            {
                var group = new List<TagFilter>();
                foreach (var raw in SplitTopLevelAndClauses(branch))
                {
                    var p = raw.Trim();
                    if (TryParseTimeFilter(p, out var newMin, out var newMax))
                    {
                        if (newMin.HasValue) min = newMin;
                        if (newMax.HasValue) max = newMax;
                        continue;
                    }
                    TryParseFilter(p, group, fieldFilters);
                }
                if (group.Count > 0)
                    orGroups.Add(group);
            }
            return;
        }

        // No OR - use original AND-only parsing
        foreach (var raw in SplitTopLevelAndClauses(where))
        {
            var p = raw.Trim();
            if (TryParseTimeFilter(p, out var newMin, out var newMax))
            {
                if (newMin.HasValue) min = newMin;
                if (newMax.HasValue) max = newMax;
                continue;
            }
            TryParseFilter(p, tagFilters, fieldFilters);
        }
    }

    static bool TryParseTimeFilter(string p, out long? min, out long? max)
    {
        min = null; max = null;
        if (!p.StartsWith("time", StringComparison.OrdinalIgnoreCase))
            return false;

        if (p.Contains(">=")) min = ParseTime(p.Split(">=")[1]);
        else if (p.Contains("<=")) max = ParseTime(p.Split("<=")[1]);
        else if (p.Contains('>')) min = ParseTime(p.Split('>')[1]) + 1;
        else if (p.Contains('<')) max = ParseTime(p.Split('<')[1]) - 1;
        return true;
    }

    static void TryParseFilter(string p, List<TagFilter> tagFilters, List<FieldFilter> fieldFilters)
    {
        if (p.Contains("=~")) { var parts = p.Split("=~", 2); tagFilters.Add(new TagFilter(Unq(parts[0].Trim()), ExtractRegex(parts[1].Trim()), TagOp.Regex)); return; }
        if (p.Contains("!~")) { var parts = p.Split("!~", 2); tagFilters.Add(new TagFilter(Unq(parts[0].Trim()), ExtractRegex(parts[1].Trim()), TagOp.NotRegex)); return; }
        if (p.Contains("!="))
        {
            var parts = p.Split("!=", 2); var key = Unq(parts[0].Trim()); var val = Unq(parts[1].Trim().Trim('\''));
            if (double.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(key, nv, FieldOp.Neq));
            else tagFilters.Add(new TagFilter(key, val, TagOp.Neq)); return;
        }
        if (p.Contains(">=")) { var parts = p.Split(">=", 2); if (double.TryParse(Unq(parts[1].Trim().Trim('\'')), NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(Unq(parts[0].Trim()), nv, FieldOp.Gte)); return; }
        if (p.Contains("<=")) { var parts = p.Split("<=", 2); if (double.TryParse(Unq(parts[1].Trim().Trim('\'')), NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(Unq(parts[0].Trim()), nv, FieldOp.Lte)); return; }
        if (p.Contains('>')) { var parts = p.Split('>', 2); if (double.TryParse(Unq(parts[1].Trim().Trim('\'')), NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(Unq(parts[0].Trim()), nv, FieldOp.Gt)); return; }
        if (p.Contains('<')) { var parts = p.Split('<', 2); if (double.TryParse(Unq(parts[1].Trim().Trim('\'')), NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(Unq(parts[0].Trim()), nv, FieldOp.Lt)); return; }
        if (p.Contains('='))
        {
            var parts = p.Split('=', 2); var key = Unq(parts[0].Trim()); var val = Unq(parts[1].Trim().Trim('\''));
            if (double.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(key, nv, FieldOp.Eq));
            else tagFilters.Add(new TagFilter(key, val, TagOp.Eq));
        }
    }

    static string ExtractRegex(string s) { s = s.Trim(); return s.Length >= 2 && s[0] == '/' && s[^1] == '/' ? s[1..^1] : s; }
    static int EndClause(string u, int st) => new[] { u.IndexOf(" GROUP BY ", st), u.IndexOf(" ORDER BY ", st), u.IndexOf(" LIMIT ", st), u.IndexOf(" OFFSET ", st), u.IndexOf(" SLIMIT ", st), u.IndexOf(" SOFFSET ", st), u.IndexOf(" FILL(", st) }.Where(x => x >= 0).DefaultIfEmpty(u.Length).Min();
    static List<SelectItem> ParseItems(string s)
    {
        if (s == "*") return [new("", "*", "*")];
        var items = new List<SelectItem>();
        var parts = SplitOutsideParens(s);
        foreach (var raw in parts)
        {
            var x = raw.Trim();
            if (string.IsNullOrEmpty(x)) continue;

            // Handle DISTINCT: SELECT DISTINCT field FROM ...
            if (x.StartsWith("DISTINCT ", StringComparison.OrdinalIgnoreCase))
            {
                var fld = Unq(x["DISTINCT ".Length..].Trim());
                items.Add(new SelectItem("distinct", fld, "distinct_" + fld) { IsDistinct = true });
                continue;
            }

            var p = x.IndexOf('(');
            if (p > 0 && x.EndsWith(')'))
            {
                var f = x[..p].Trim().ToLowerInvariant();
                var inner = x[(p + 1)..^1].Trim();

                // Handle COUNT(DISTINCT field)
                if (f == "count" && inner.StartsWith("DISTINCT ", StringComparison.OrdinalIgnoreCase))
                {
                    var fld = Unq(inner["DISTINCT ".Length..].Trim());
                    items.Add(new SelectItem("count", fld, "count_distinct_" + fld) { IsCountDistinct = true });
                    continue;
                }

                var args = inner.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                var fld2 = Unq(args[0]);
                double param = 0;
                long? unitNs = null;
                if (args.Length > 1)
                {
                    var second = args[1].Trim();
                    if (double.TryParse(second, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var numeric))
                        param = numeric;
                    else
                        unitNs = DurationToNs(second);
                }
                var aliasSuffix = args.Length > 1 ? "_" + args[1].Trim().Replace("\"", "").Replace("'", "") : "";
                var alias = $"{f}_{fld2}{aliasSuffix}";
                items.Add(new SelectItem(f, fld2, alias, param, unitNs));
            }
            else
            {
                items.Add(new SelectItem("", Unq(x), Unq(x)));
            }
        }
        return items;
    }

    static List<string> SplitOutsideParens(string s)
    {
        var result = new List<string>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.Length; i++)
        {
            if (s[i] == '(') depth++;
            else if (s[i] == ')') depth--;
            else if (s[i] == ',' && depth == 0) { result.Add(s[start..i]); start = i + 1; }
        }
        result.Add(s[start..]);
        return result;
    }
    static List<string> SplitMeasurementList(string text)
    {
        var parts = new List<string>();
        int start = 0;
        bool inDoubleQuote = false;
        for (int i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (ch == '"')
                inDoubleQuote = !inDoubleQuote;
            else if (ch == ',' && !inDoubleQuote)
            {
                var part = text[start..i].Trim();
                if (part.Length > 0)
                    parts.Add(ParseQualifiedMeasurement(part).Measurement);
                start = i + 1;
            }
        }

        var tail = text[start..].Trim();
        if (tail.Length > 0)
            parts.Add(ParseQualifiedMeasurement(tail).Measurement);
        return parts;
    }
    static string? AfterFrom(string q) { var u = q.ToUpperInvariant(); var i = u.IndexOf(" FROM "); return i < 0 ? null : Unq(ReadToken(q[(i + 6)..].Trim())); }
    static string? AfterKey(string q) { var u = q.ToUpperInvariant(); var i = u.IndexOf(" KEY "); if (i < 0) return null; var part = q[(i + 5)..].Trim(); if (part.StartsWith('=')) part = part[1..].Trim(); return Unq(ReadToken(part)); }
    static string? AfterOn(string q) { var u = q.ToUpperInvariant(); var i = u.IndexOf(" ON "); return i < 0 ? null : Unq(ReadToken(q[(i + 4)..].Trim())); }
    static string ReadToken(ref string rest)
    {
        rest = rest.TrimStart();
        var token = ReadToken(rest);
        rest = rest[token.Length..];
        return token;
    }
    static string ReadToken(string text)
    {
        text = text.TrimStart();
        int i = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;
        while (i < text.Length)
        {
            var ch = text[i];
            if (ch == '\'' && !inDoubleQuote)
                inSingleQuote = !inSingleQuote;
            else if (ch == '"' && !inSingleQuote)
                inDoubleQuote = !inDoubleQuote;
            else if (!inSingleQuote && !inDoubleQuote && char.IsWhiteSpace(ch))
                break;
            i++;
        }
        return text[..i];
    }
    static void ConsumeKeyword(ref string rest, string kw) { rest = rest.TrimStart(); if (rest.StartsWith(kw, StringComparison.OrdinalIgnoreCase)) rest = rest[kw.Length..]; }
    static string Unq(string s) { s = s.Trim(); return s.Length >= 2 && s[0] == '"' && s[^1] == '"' ? s[1..^1] : s; }
    static (string? Database, string? RetentionPolicy, string Measurement) ParseQualifiedMeasurement(string token)
    {
        var parts = SplitQualifiedIdentifier(token);
        return parts.Length switch
        {
            3 => (parts[0], parts[1], parts[2]),
            2 => (null, parts[0], parts[1]),
            _ => (null, null, parts[0])
        };
    }
    public static string[] SplitQualifiedIdentifier(string text)
    {
        var parts = new List<string>();
        int start = 0;
        bool inDoubleQuote = false;
        for (int i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (ch == '"')
                inDoubleQuote = !inDoubleQuote;
            else if (ch == '.' && !inDoubleQuote)
            {
                var segment = text[start..i].Trim();
                if (segment.Length > 0)
                    parts.Add(Unq(segment));
                start = i + 1;
            }
        }

        var tail = text[start..].Trim();
        if (tail.Length > 0)
            parts.Add(Unq(tail));
        return parts.ToArray();
    }
    static long ParseTime(string s)
    {
        s = s.Trim().Trim('\'');
        if (long.TryParse(s, out var n)) return n;
        // Support epoch with unit suffix: 1234567890s, 1234567890ms, 1234567890u, 1234567890ns
        if (s.Length > 2)
        {
            var suffix = s[^2..].ToLowerInvariant();
            var numPart = s[..^2];
            if (long.TryParse(numPart, out var epoch) && suffix is "ns" or "us" or "ms" or ".s")
                return suffix switch { "ns" => epoch, "us" => epoch * 1000, "ms" => epoch * 1_000_000, _ => 0 };
        }
        if (s.Length > 1)
        {
            var last = s[^1];
            var numPart = s[..^1];
            if (long.TryParse(numPart, out var epoch))
            {
                return last switch
                {
                    's' => epoch * 1_000_000_000,
                    'u' => epoch * 1000,
                    'm' => epoch * 60_000_000_000,
                    'h' => epoch * 3600_000_000_000,
                    _ => 0
                };
            }
        }
        if (s.StartsWith("now()", StringComparison.OrdinalIgnoreCase)) return ParseNowTime(s);
        return DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUnixTimeMilliseconds() * 1_000_000;
    }
    static long ParseNowTime(string s)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000;
        var rest = s["now()".Length..].Trim();
        if (rest.Length == 0) return now;
        var op = rest[0];
        if (op != '+' && op != '-') throw new FormatException($"bad time expression: {s}");
        var duration = DurationToNs(rest[1..]);
        return op == '-' ? now - duration : now + duration;
    }
    public static long DurationToNs(string s)
    {
        s = new string(s.Trim().ToLowerInvariant().Where(ch => !char.IsWhiteSpace(ch)).ToArray());
        long num = long.Parse(new string(s.TakeWhile(char.IsDigit).ToArray()));
        string unit = new string(s.SkipWhile(char.IsDigit).ToArray());
        return unit switch { "ns" => num, "u" or "us" => num * 1000, "ms" => num * 1_000_000, "s" => num * 1_000_000_000,
            "m" => num * 60_000_000_000, "h" => num * 3600_000_000_000, "d" => num * 86400_000_000_000, "w" => num * 7 * 86400_000_000_000,
            _ => throw new FormatException($"bad duration: {s}") };
    }

    static int IndexOfTopLevelKeyword(string text, string keyword)
    {
        int depth = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;
        for (int i = 0; i <= text.Length - keyword.Length; i++)
        {
            var ch = text[i];
            if (ch == '\'' && !inDoubleQuote) inSingleQuote = !inSingleQuote;
            else if (ch == '"' && !inSingleQuote) inDoubleQuote = !inDoubleQuote;
            else if (!inSingleQuote && !inDoubleQuote)
            {
                if (ch == '(') depth++;
                else if (ch == ')') depth--;
            }

            if (depth == 0 && !inSingleQuote && !inDoubleQuote &&
                text.AsSpan(i, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    static string ReadSourceToken(string text)
    {
        return ReadToken(text);
    }

    static int FindMatchingParen(string text, int openIndex)
    {
        int depth = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;
        for (int i = openIndex; i < text.Length; i++)
        {
            var ch = text[i];
            if (ch == '\'' && !inDoubleQuote) inSingleQuote = !inSingleQuote;
            else if (ch == '"' && !inSingleQuote) inDoubleQuote = !inDoubleQuote;
            else if (!inSingleQuote && !inDoubleQuote)
            {
                if (ch == '(') depth++;
                else if (ch == ')')
                {
                    depth--;
                    if (depth == 0)
                        return i;
                }
            }
        }

        return -1;
    }

    static List<string> SplitTopLevelAndClauses(string text)
    {
        var clauses = new List<string>();
        int depth = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;
        bool inRegex = false;
        int start = 0;
        for (int i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (inRegex)
            {
                if (ch == '/' && (i == 0 || text[i - 1] != '\\'))
                    inRegex = false;
                continue;
            }

            if (ch == '\'' && !inDoubleQuote)
            {
                inSingleQuote = !inSingleQuote;
                continue;
            }

            if (ch == '"' && !inSingleQuote)
            {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }

            if (inSingleQuote || inDoubleQuote)
                continue;

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                continue;
            }

            if (ch == '/' && i > 0 && (text[i - 1] == '~' || text[i - 1] == '!'))
            {
                inRegex = true;
                continue;
            }

            if (depth == 0 && i + 3 <= text.Length && text.AsSpan(i, 3).Equals("AND".AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
                var leftBoundary = i == 0 || char.IsWhiteSpace(text[i - 1]);
                var rightBoundary = i + 3 == text.Length || char.IsWhiteSpace(text[i + 3]);
                if (leftBoundary && rightBoundary)
                {
                    var clause = text[start..i].Trim();
                    if (clause.Length > 0)
                        clauses.Add(clause);
                    start = i + 3;
                    i += 2;
                }
            }
        }

        var tail = text[start..].Trim();
        if (tail.Length > 0)
            clauses.Add(tail);
        return clauses;
    }

    /// <summary>
    /// Split WHERE clause by OR at top level (not inside quotes, parens, or regex).
    /// </summary>
    static List<string> SplitTopLevelOrClauses(string text)
    {
        var clauses = new List<string>();
        int depth = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;
        bool inRegex = false;
        int start = 0;
        for (int i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (inRegex)
            {
                if (ch == '/' && (i == 0 || text[i - 1] != '\\'))
                    inRegex = false;
                continue;
            }

            if (ch == '\'' && !inDoubleQuote)
            {
                inSingleQuote = !inSingleQuote;
                continue;
            }

            if (ch == '"' && !inSingleQuote)
            {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }

            if (inSingleQuote || inDoubleQuote)
                continue;

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                continue;
            }

            if (ch == '/' && i > 0 && (text[i - 1] == '~' || text[i - 1] == '!'))
            {
                inRegex = true;
                continue;
            }

            // Check for OR at top level
            if (depth == 0 && i + 2 <= text.Length && text.AsSpan(i, 2).Equals("OR".AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
                var leftBoundary = i == 0 || char.IsWhiteSpace(text[i - 1]);
                var rightBoundary = i + 2 == text.Length || char.IsWhiteSpace(text[i + 2]);
                if (leftBoundary && rightBoundary)
                {
                    var clause = text[start..i].Trim();
                    if (clause.Length > 0)
                        clauses.Add(clause);
                    start = i + 2;
                    i += 1;
                }
            }
        }

        var tail = text[start..].Trim();
        if (tail.Length > 0)
            clauses.Add(tail);
        return clauses;
    }
}

using System.Globalization;

namespace MiniInflux.Net10.Protocol;

public enum QueryKind
{
    CreateDatabase, ShowDatabases, ShowMeasurements, ShowFieldKeys, ShowTagKeys, ShowTagValues, Select,
    CreateRetentionPolicy, AlterRetentionPolicy, DropRetentionPolicy, ShowRetentionPolicies,
    CreateContinuousQuery, ShowContinuousQueries, DropContinuousQuery,
    DropDatabase, DropMeasurement, DropSeries, DropShard, Delete,
    ShowSeries, ShowSeriesCardinality, ShowMeasurementCardinality, ShowTagValuesCardinality
}

public enum TagOp { Eq, Neq, Regex, NotRegex }
public enum FieldOp { Gt, Gte, Lt, Lte, Eq, Neq }
public enum FillMode { None, Null, Zero, Previous, Linear }

public sealed record SelectItem(string Func, string Field, string Alias, double Param = 0, long? UnitNs = null);
public sealed record TagFilter(string Key, string Value, TagOp Op);
public sealed record FieldFilter(string Field, double Value, FieldOp Op);
public sealed class ParsedQuery
{
    public required QueryKind Kind { get; init; }
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
    public List<TagFilter> TagFilters { get; init; } = [];
    public List<FieldFilter> FieldFilters { get; init; } = [];
    public string? RpName { get; init; }
    public long? RpDurationNs { get; init; }
    public bool? RpDefault { get; init; }
    public string? ContinuousQueryName { get; init; }
    public string? ContinuousQueryText { get; init; }
    public long? ContinuousQueryEveryNs { get; init; }
    public long? ContinuousQueryForNs { get; init; }
    public int? ContinuousQueryRecomputeRecentBuckets { get; init; }
    public string? IntoTarget { get; init; }
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
            return new() { Kind = QueryKind.CreateDatabase, Database = Unq(q[16..].Trim()) };
        if (q.Equals("SHOW DATABASES", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowDatabases };
        if (q.Equals("SHOW MEASUREMENTS", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowMeasurements };
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
            return new() { Kind = QueryKind.ShowTagKeys, Measurement = AfterFrom(q) };
        if (q.StartsWith("SHOW TAG VALUES", StringComparison.OrdinalIgnoreCase))
        {
            var m = AfterFrom(q); var key = AfterKey(q);
            return new() { Kind = QueryKind.ShowTagValues, Measurement = m, TagKey = key };
        }
        if (q.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase)) return ParseSelect(q);
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
        var upper = rest.ToUpperInvariant(); var wi = upper.IndexOf(" WHERE ");
        if (wi >= 0) ParseWhere(rest[(wi + 7)..], out min, out max, tagFilters, fieldFilters);
        return new() { Kind = QueryKind.Delete, Measurement = target.Measurement,
            SourceDatabase = target.Database, SourceRpName = target.RetentionPolicy,
            MinTimeNs = min, MaxTimeNs = max, TagFilters = tagFilters, FieldFilters = fieldFilters };
    }

    static ParsedQuery ParseDropSeries(string q)
    {
        var rest = q["DROP SERIES".Length..].Trim();
        var measurements = new List<string>();
        long? min = null, max = null;
        var tagFilters = new List<TagFilter>(); var fieldFilters = new List<FieldFilter>();

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
        if (wi >= 0) ParseWhere(rest[(wi + 7)..], out min, out max, tagFilters, fieldFilters);
        return new()
        {
            Kind = QueryKind.DropSeries,
            Measurement = measurements.FirstOrDefault(),
            Measurements = measurements,
            MinTimeNs = min,
            MaxTimeNs = max,
            TagFilters = tagFilters,
            FieldFilters = fieldFilters
        };
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
        var groupByTags = new List<string>(); var groupByAllTags = false; var fill = FillMode.None;
        int wi = tu.IndexOf(" WHERE ");
        if (wi >= 0) { var end = EndClause(tu, wi + 7); ParseWhere(tail[(wi + 7)..end], out min, out max, tagFilters, fieldFilters); }
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
            GroupByTags = groupByTags, GroupByAllTags = groupByAllTags, Fill = fill, TagFilters = tagFilters, FieldFilters = fieldFilters, IntoTarget = intoTarget };
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

    static void ParseWhere(string where, out long? min, out long? max, List<TagFilter> tagFilters, List<FieldFilter> fieldFilters)
    {
        min = null; max = null;
        foreach (var raw in SplitTopLevelAndClauses(where))
        {
            var p = raw.Trim();
            if (p.StartsWith("time", StringComparison.OrdinalIgnoreCase))
            {
                if (p.Contains(">=")) min = ParseTime(p.Split(">=")[1]);
                else if (p.Contains("<=")) max = ParseTime(p.Split("<=")[1]);
                else if (p.Contains('>')) min = ParseTime(p.Split('>')[1]) + 1;
                else if (p.Contains('<')) max = ParseTime(p.Split('<')[1]) - 1;
                continue;
            }
            if (p.Contains("=~")) { var parts = p.Split("=~", 2); tagFilters.Add(new TagFilter(Unq(parts[0].Trim()), ExtractRegex(parts[1].Trim()), TagOp.Regex)); continue; }
            if (p.Contains("!~")) { var parts = p.Split("!~", 2); tagFilters.Add(new TagFilter(Unq(parts[0].Trim()), ExtractRegex(parts[1].Trim()), TagOp.NotRegex)); continue; }
            if (p.Contains("!="))
            {
                var parts = p.Split("!=", 2); var key = Unq(parts[0].Trim()); var val = Unq(parts[1].Trim().Trim('\''));
                if (double.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(key, nv, FieldOp.Neq));
                else tagFilters.Add(new TagFilter(key, val, TagOp.Neq)); continue;
            }
            if (p.Contains(">=")) { var parts = p.Split(">=", 2); if (double.TryParse(Unq(parts[1].Trim().Trim('\'')), NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(Unq(parts[0].Trim()), nv, FieldOp.Gte)); continue; }
            if (p.Contains("<=")) { var parts = p.Split("<=", 2); if (double.TryParse(Unq(parts[1].Trim().Trim('\'')), NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(Unq(parts[0].Trim()), nv, FieldOp.Lte)); continue; }
            if (p.Contains('>')) { var parts = p.Split('>', 2); if (double.TryParse(Unq(parts[1].Trim().Trim('\'')), NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(Unq(parts[0].Trim()), nv, FieldOp.Gt)); continue; }
            if (p.Contains('<')) { var parts = p.Split('<', 2); if (double.TryParse(Unq(parts[1].Trim().Trim('\'')), NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(Unq(parts[0].Trim()), nv, FieldOp.Lt)); continue; }
            if (p.Contains('='))
            {
                var parts = p.Split('=', 2); var key = Unq(parts[0].Trim()); var val = Unq(parts[1].Trim().Trim('\''));
                if (double.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var nv)) fieldFilters.Add(new FieldFilter(key, nv, FieldOp.Eq));
                else tagFilters.Add(new TagFilter(key, val, TagOp.Eq));
            }
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
            var p = x.IndexOf('(');
            if (p > 0 && x.EndsWith(')'))
            {
                var f = x[..p].Trim().ToLowerInvariant();
                var inner = x[(p + 1)..^1].Trim();
                var args = inner.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                var fld = Unq(args[0]);
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
                var alias = $"{f}_{fld}{aliasSuffix}";
                items.Add(new SelectItem(f, fld, alias, param, unitNs));
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
    static long ParseTime(string s) { s = s.Trim().Trim('\''); if (long.TryParse(s, out var n)) return n;
        return DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUnixTimeMilliseconds() * 1_000_000; }
    public static long DurationToNs(string s)
    {
        s = s.Trim().ToLowerInvariant();
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
}

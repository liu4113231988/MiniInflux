using System.Globalization;

namespace MiniInflux.Net10.Protocol;

public enum QueryKind
{
    CreateDatabase, ShowDatabases, ShowMeasurements, ShowFieldKeys, ShowTagKeys, ShowTagValues, Select,
    CreateRetentionPolicy, AlterRetentionPolicy, DropRetentionPolicy, ShowRetentionPolicies,
    DropDatabase, DropMeasurement, Delete
}

public enum TagOp { Eq, Neq, Regex, NotRegex }
public enum FieldOp { Gt, Gte, Lt, Lte, Eq, Neq }
public enum FillMode { None, Null, Zero, Previous, Linear }

public sealed record SelectItem(string Func, string Field, string Alias, double Param = 0);
public sealed record TagFilter(string Key, string Value, TagOp Op);
public sealed record FieldFilter(string Field, double Value, FieldOp Op);

public sealed class ParsedQuery
{
    public required QueryKind Kind { get; init; }
    public string? Database { get; init; }
    public string? Measurement { get; init; }
    public List<SelectItem> Select { get; init; } = [];
    public long? MinTimeNs { get; init; }
    public long? MaxTimeNs { get; init; }
    public int? Limit { get; init; }
    public bool Desc { get; init; }
    public long? GroupByNs { get; init; }
    public List<string> GroupByTags { get; init; } = [];
    public FillMode Fill { get; init; } = FillMode.None;
    public string? TagKey { get; init; }
    public List<TagFilter> TagFilters { get; init; } = [];
    public List<FieldFilter> FieldFilters { get; init; } = [];
    public string? RpName { get; init; }
    public long? RpDurationNs { get; init; }
    public bool? RpDefault { get; init; }
}

public static class InfluxQlParser
{
    public static ParsedQuery Parse(string q)
    {
        q = q.Trim().TrimEnd(';');
        if (q.StartsWith("CREATE RETENTION POLICY ", StringComparison.OrdinalIgnoreCase)) return ParseCreateRp(q);
        if (q.StartsWith("ALTER RETENTION POLICY ", StringComparison.OrdinalIgnoreCase)) return ParseAlterRp(q);
        if (q.StartsWith("DROP RETENTION POLICY ", StringComparison.OrdinalIgnoreCase)) return ParseDropRp(q);
        if (q.StartsWith("SHOW RETENTION POLICIES", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowRetentionPolicies, Database = AfterOn(q) };
        if (q.StartsWith("DROP DATABASE ", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.DropDatabase, Database = Unq(q[14..].Trim()) };
        if (q.StartsWith("DROP MEASUREMENT ", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.DropMeasurement, Measurement = Unq(q[17..].Trim()) };
        if (q.StartsWith("DELETE FROM ", StringComparison.OrdinalIgnoreCase)) return ParseDelete(q);
        if (q.StartsWith("CREATE DATABASE ", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.CreateDatabase, Database = Unq(q[16..].Trim()) };
        if (q.Equals("SHOW DATABASES", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowDatabases };
        if (q.Equals("SHOW MEASUREMENTS", StringComparison.OrdinalIgnoreCase))
            return new() { Kind = QueryKind.ShowMeasurements };
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

    static ParsedQuery ParseDelete(string q)
    {
        var rest = q["DELETE FROM ".Length..].Trim();
        var meas = ReadToken(ref rest);
        long? min = null, max = null;
        var tagFilters = new List<TagFilter>(); var fieldFilters = new List<FieldFilter>();
        var upper = rest.ToUpperInvariant(); var wi = upper.IndexOf(" WHERE ");
        if (wi >= 0) ParseWhere(rest[(wi + 7)..], out min, out max, tagFilters, fieldFilters);
        return new() { Kind = QueryKind.Delete, Measurement = Unq(meas),
            MinTimeNs = min, MaxTimeNs = max, TagFilters = tagFilters, FieldFilters = fieldFilters };
    }

    static ParsedQuery ParseSelect(string q)
    {
        var u = q.ToUpperInvariant();
        int fi = u.IndexOf(" FROM ");
        if (fi < 0) throw new FormatException("SELECT requires FROM");
        var fieldText = q[7..fi].Trim();
        var rest = q[(fi + 6)..].Trim();
        string meas = rest.Split(' ', StringSplitOptions.RemoveEmptyEntries)[0];
        string tail = rest.Length > meas.Length ? rest[meas.Length..] : "";
        long? min = null, max = null, gb = null; int? limit = null;
        bool desc = u.Contains(" ORDER BY TIME DESC");
        var tagFilters = new List<TagFilter>(); var fieldFilters = new List<FieldFilter>();
        var groupByTags = new List<string>(); var fill = FillMode.None;
        var tu = tail.ToUpperInvariant();
        int wi = tu.IndexOf(" WHERE ");
        if (wi >= 0) { var end = EndClause(tu, wi + 7); ParseWhere(tail[(wi + 7)..end], out min, out max, tagFilters, fieldFilters); }
        var gu = tu.IndexOf(" GROUP BY ");
        if (gu >= 0) { var gs = gu + 10; var ge = EndClause(tu, gs); ParseGroupBy(tail[gs..ge].Trim(), out gb, groupByTags); }
        var fu = tu.IndexOf(" FILL(");
        if (fu >= 0) { var fe = tu.IndexOf(')', fu); if (fe >= 0) { var fv = tail[(fu + 6)..fe].Trim().ToLowerInvariant();
            fill = fv switch { "null" => FillMode.Null, "0" => FillMode.Zero, "previous" => FillMode.Previous, "linear" => FillMode.Linear, _ => FillMode.None }; } }
        var lu = tu.LastIndexOf(" LIMIT ");
        if (lu >= 0) limit = int.Parse(tail[(lu + 7)..].Trim().Split(' ')[0], CultureInfo.InvariantCulture);
        return new() { Kind = QueryKind.Select, Measurement = Unq(meas), Select = ParseItems(fieldText),
            MinTimeNs = min, MaxTimeNs = max, Limit = limit, Desc = desc, GroupByNs = gb,
            GroupByTags = groupByTags, Fill = fill, TagFilters = tagFilters, FieldFilters = fieldFilters };
    }

    static void ParseGroupBy(string text, out long? gbNs, List<string> tags)
    {
        gbNs = null;
        foreach (var part in text.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var p = part.Trim(); var pu = p.ToUpperInvariant();
            if (pu.StartsWith("TIME(") && p.EndsWith(')')) gbNs = DurationToNs(p[5..^1]);
            else tags.Add(Unq(p));
        }
    }

    static void ParseWhere(string where, out long? min, out long? max, List<TagFilter> tagFilters, List<FieldFilter> fieldFilters)
    {
        min = null; max = null;
        foreach (var raw in where.Split("AND", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
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
    static int EndClause(string u, int st) => new[] { u.IndexOf(" GROUP BY ", st), u.IndexOf(" ORDER BY ", st), u.IndexOf(" LIMIT ", st), u.IndexOf(" FILL(", st) }.Where(x => x >= 0).DefaultIfEmpty(u.Length).Min();
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
                if (args.Length > 1) double.TryParse(args[1], System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out param);
                var alias = param != 0 ? $"{f}_{fld}_{(int)param}" : $"{f}_{fld}";
                items.Add(new SelectItem(f, fld, alias, param));
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
    static string? AfterFrom(string q) { var u = q.ToUpperInvariant(); var i = u.IndexOf(" FROM "); return i < 0 ? null : Unq(q[(i + 6)..].Trim().Split(' ')[0]); }
    static string? AfterKey(string q) { var u = q.ToUpperInvariant(); var i = u.IndexOf(" KEY "); if (i < 0) return null; var part = q[(i + 5)..].Trim(); if (part.StartsWith('=')) part = part[1..].Trim(); return Unq(part.Trim().Split(' ')[0]); }
    static string? AfterOn(string q) { var u = q.ToUpperInvariant(); var i = u.IndexOf(" ON "); return i < 0 ? null : Unq(q[(i + 4)..].Trim().Split(' ')[0]); }
    static string ReadToken(ref string rest) { rest = rest.TrimStart(); int i = 0; while (i < rest.Length && !char.IsWhiteSpace(rest[i])) i++; var t = rest[..i]; rest = rest[i..]; return t; }
    static void ConsumeKeyword(ref string rest, string kw) { rest = rest.TrimStart(); if (rest.StartsWith(kw, StringComparison.OrdinalIgnoreCase)) rest = rest[kw.Length..]; }
    static string Unq(string s) { s = s.Trim(); return s.Length >= 2 && s[0] == '"' && s[^1] == '"' ? s[1..^1] : s; }
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
}

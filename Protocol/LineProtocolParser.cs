using System.Globalization;
using MiniInflux.Net10.Model;
namespace MiniInflux.Net10.Protocol;

public readonly record struct TimestampPrecision(long Multiplier)
{
    public static TimestampPrecision Parse(string? v) => v switch
    { null or "" or "n" or "ns" => new(1), "u" or "us" => new(1_000), "ms" => new(1_000_000), "s" => new(1_000_000_000), "m" => new(60L*1_000_000_000), "h" => new(3600L*1_000_000_000), _ => throw new FormatException($"invalid precision: {v}") };
}

public static class LineProtocolParser
{
    public static List<Point> ParseMany(string text, TimestampPrecision precision)
    {
        var res = new List<Point>(EstimatePointCapacity(text));
        var stringPool = new StringPool();
        var start = 0;
        while (start < text.Length)
        {
            var end = text.IndexOf('\n', start);
            if (end < 0)
                end = text.Length;

            var lineEnd = end > start && text[end - 1] == '\r' ? end - 1 : end;
            if (HasContent(text, start, lineEnd) && text[start] != '#')
            {
                res.Add(HasSpecial(text, start, lineEnd)
                    ? ParseOne(text[start..lineEnd], precision)
                    : ParseSimple(text, start, lineEnd, precision, stringPool));
            }

            start = end + 1;
        }
        return res;
    }

    private static bool HasContent(string text, int start, int end)
    {
        for (var i = start; i < end; i++)
            if (!char.IsWhiteSpace(text[i]))
                return true;
        return false;
    }

    private static int EstimatePointCapacity(string text) => Math.Min(100_000, text.Length / 64);

    private static bool HasSpecial(string text, int start, int end)
    {
        for (var i = start; i < end; i++)
            if (text[i] is '\\' or '"')
                return true;
        return false;
    }

    public static Point ParseOne(string line, TimestampPrecision precision)
    {
        if (line.IndexOf('\\') < 0 && line.IndexOf('"') < 0)
            return ParseSimple(line, precision);

        var first = FindUnescaped(line, ' ', 0); if (first <= 0) throw new FormatException("invalid line protocol: missing field set");
        var second = FindUnescaped(line, ' ', first + 1);
        var seriesPart = line[..first]; var fieldPart = second < 0 ? line[(first+1)..] : line[(first+1)..second]; var timePart = second < 0 ? null : line[(second+1)..].Trim();
        var mt = SplitUnescaped(seriesPart, ','); var measurement = UnescapeKey(mt[0]);
        var tags = new Dictionary<string,string>(StringComparer.Ordinal);
        for (int i=1;i<mt.Count;i++){var kv=SplitFirstUnescaped(mt[i],'='); tags[UnescapeKey(kv.Key)]=UnescapeKey(kv.Value);}        
        var fields = new Dictionary<string,FieldValue>(StringComparer.Ordinal);
        foreach(var f in SplitUnescaped(fieldPart, ',')){var kv=SplitFirstUnescaped(f,'='); fields[UnescapeKey(kv.Key)]=ParseFieldValue(kv.Value);}        
        var ts = string.IsNullOrEmpty(timePart) ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()*1_000_000 : checked(long.Parse(timePart, CultureInfo.InvariantCulture)*precision.Multiplier);
        return new Point{Measurement=measurement, Tags=tags, Fields=fields, TimestampNs=ts};
    }

    private static Point ParseSimple(string line, TimestampPrecision precision)
    {
        return ParseSimple(line, 0, line.Length, precision, new StringPool());
    }

    private static Point ParseSimple(string line, int start, int end, TimestampPrecision precision, StringPool stringPool)
    {
        var first = line.IndexOf(' ', start, end - start);
        if (first <= start) throw new FormatException("invalid line protocol: missing field set");
        var second = line.IndexOf(' ', first + 1, end - first - 1);
        var seriesEnd = first;
        var fieldsStart = first + 1;
        var fieldsEnd = second < 0 ? end : second;
        var timeStart = second < 0 ? -1 : second + 1;

        var measurementEnd = line.IndexOf(',', start, seriesEnd - start);
        if (measurementEnd < 0) measurementEnd = seriesEnd;
        Dictionary<string, string> tags;
        var tagStart = measurementEnd + 1;
        var tagsSorted = true;
        string? tagsCanonical = "";
        if (measurementEnd >= seriesEnd)
        {
            tags = new Dictionary<string, string>(0, StringComparer.Ordinal);
        }
        else if (stringPool.TryGetTags(line, measurementEnd + 1, seriesEnd - measurementEnd - 1, out tagsCanonical, out tags))
        {
            // Repeated series in the same batch: reuse the parsed tag dictionary.
        }
        else
        {
            tags = new Dictionary<string, string>(4, StringComparer.Ordinal);
            string? previousTag = null;
            while (tagStart < seriesEnd)
            {
                var tagEnd = line.IndexOf(',', tagStart, seriesEnd - tagStart);
                if (tagEnd < 0) tagEnd = seriesEnd;
                var eq = line.IndexOf('=', tagStart, tagEnd - tagStart);
                if (eq <= tagStart) throw new FormatException($"invalid key-value: {line[tagStart..tagEnd]}");
                var tagKey = stringPool.Get(line, tagStart, eq - tagStart);
                if (previousTag != null && string.CompareOrdinal(previousTag, tagKey) > 0)
                    tagsSorted = false;
                previousTag = tagKey;
                tags[tagKey] = stringPool.Get(line, eq + 1, tagEnd - eq - 1);
                tagStart = tagEnd + 1;
            }

            tagsCanonical = tagsSorted ? stringPool.Get(line, measurementEnd + 1, seriesEnd - measurementEnd - 1) : null;
            if (tagsCanonical != null)
                stringPool.AddTags(tagsCanonical, tags);
        }

        var fields = new Dictionary<string, FieldValue>(2, StringComparer.Ordinal);
        var fieldStart = fieldsStart;
        while (fieldStart < fieldsEnd)
        {
            var fieldEnd = line.IndexOf(',', fieldStart, fieldsEnd - fieldStart);
            if (fieldEnd < 0) fieldEnd = fieldsEnd;
            var eq = line.IndexOf('=', fieldStart, fieldEnd - fieldStart);
            if (eq <= fieldStart) throw new FormatException($"invalid key-value: {line[fieldStart..fieldEnd]}");
            fields[stringPool.Get(line, fieldStart, eq - fieldStart)] = ParseSimpleFieldValue(line, eq + 1, fieldEnd);
            fieldStart = fieldEnd + 1;
        }

        var hasTime = false;
        if (timeStart >= 0)
        {
            while (timeStart < end && char.IsWhiteSpace(line[timeStart])) timeStart++;
            hasTime = timeStart < end;
        }
        var ts = !hasTime
            ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000
            : checked(long.Parse(line.AsSpan(timeStart, end - timeStart), CultureInfo.InvariantCulture) * precision.Multiplier);

        return new Point
        {
            Measurement = stringPool.Get(line, start, measurementEnd - start),
            Tags = tags,
            Fields = fields,
            TimestampNs = ts,
            TagsCanonical = tags.Count == 0 ? "" : tagsSorted ? tagsCanonical : null
        };
    }

    private sealed class StringPool
    {
        private readonly Dictionary<int, List<string>> _buckets = [];
        private readonly Dictionary<string, Dictionary<string, string>> _tagsByCanonical = new(StringComparer.Ordinal);

        public string Get(string text, int start, int length)
        {
            var span = text.AsSpan(start, length);
            var hash = string.GetHashCode(span, StringComparison.Ordinal);
            if (_buckets.TryGetValue(hash, out var values))
            {
                foreach (var value in values)
                    if (value.AsSpan().SequenceEqual(span))
                        return value;
            }
            else
            {
                values = [];
                _buckets[hash] = values;
            }

            var created = text.Substring(start, length);
            values.Add(created);
            return created;
        }

        public bool TryGetTags(string text, int start, int length, out string canonical, out Dictionary<string, string> tags)
        {
            canonical = Get(text, start, length);
            if (_tagsByCanonical.TryGetValue(canonical, out tags!))
            {
                return true;
            }

            tags = null!;
            return false;
        }

        public void AddTags(string canonical, Dictionary<string, string> tags) => _tagsByCanonical.TryAdd(canonical, tags);
    }

    private static FieldValue ParseSimpleFieldValue(string text, int start, int end)
    {
        if (end > start && text[end - 1] == 'i')
            return FieldValue.FromInteger(long.Parse(text.AsSpan(start, end - start - 1), CultureInfo.InvariantCulture));
        if (IsBool(text, start, end, out var boolean))
            return FieldValue.FromBoolean(boolean);
        return FieldValue.FromDouble(double.Parse(text.AsSpan(start, end - start), CultureInfo.InvariantCulture));
    }

    private static FieldValue ParseFieldValue(string v)
    {
        if (v.Length>=2 && v[0]=='"' && v[^1]=='"') return FieldValue.FromString(UnescapeString(v[1..^1]));
        if (v.EndsWith('i')) return FieldValue.FromInteger(long.Parse(v[..^1], CultureInfo.InvariantCulture));
        if (IsBool(v, out var b)) return FieldValue.FromBoolean(b);
        return FieldValue.FromDouble(double.Parse(v, CultureInfo.InvariantCulture));
    }
    private static bool IsBool(string s, int start, int end, out bool v)
    {
        var len = end - start;
        if (len == 1)
        {
            switch (s[start])
            {
                case 't':
                case 'T':
                    v = true;
                    return true;
                case 'f':
                case 'F':
                    v = false;
                    return true;
            }
        }
        if (len == 4 && string.Compare(s, start, "true", 0, 4, ignoreCase: true, CultureInfo.InvariantCulture) == 0)
        {
            v = true;
            return true;
        }
        if (len == 5 && string.Compare(s, start, "false", 0, 5, ignoreCase: true, CultureInfo.InvariantCulture) == 0)
        {
            v = false;
            return true;
        }
        v = false;
        return false;
    }
    private static bool IsBool(string s,out bool v){switch(s){case "t":case "T":case "true":case "True":case "TRUE":v=true;return true;case "f":case "F":case "false":case "False":case "FALSE":v=false;return true;default:v=false;return false;}}
    private static int FindUnescaped(string s,char ch,int start){bool esc=false,inStr=false; for(int i=start;i<s.Length;i++){var c=s[i]; if(esc){esc=false;continue;} if(c=='\\'){esc=true;continue;} if(c=='"') inStr=!inStr; if(!inStr && c==ch) return i;} return -1;}
    private static List<string> SplitUnescaped(string s,char sep){var r=new List<string>(); int st=0; bool esc=false,inStr=false; for(int i=0;i<s.Length;i++){var c=s[i]; if(esc){esc=false;continue;} if(c=='\\'){esc=true;continue;} if(c=='"'){inStr=!inStr;continue;} if(!inStr&&c==sep){r.Add(s[st..i]);st=i+1;}} r.Add(s[st..]); return r;}
    private static (string Key,string Value) SplitFirstUnescaped(string s,char sep){var p=FindUnescaped(s,sep,0); if(p<=0) throw new FormatException($"invalid key-value: {s}"); return (s[..p],s[(p+1)..]);}
    private static string UnescapeKey(string s)=>s.Replace("\\ "," ").Replace("\\,",",").Replace("\\=","=").Replace("\\","");
    private static string UnescapeString(string s)=>s.Replace("\\\"","\"").Replace("\\","");
}

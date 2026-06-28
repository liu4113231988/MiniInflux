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
        var res = new List<Point>();
        var start = 0;
        while (start < text.Length)
        {
            var end = text.IndexOf('\n', start);
            if (end < 0)
                end = text.Length;

            var line = text[start..end].TrimEnd('\r');
            if (!string.IsNullOrWhiteSpace(line) && !line.StartsWith('#'))
                res.Add(ParseOne(line, precision));

            start = end + 1;
        }
        return res;
    }
    public static Point ParseOne(string line, TimestampPrecision precision)
    {
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
    private static FieldValue ParseFieldValue(string v)
    {
        if (v.Length>=2 && v[0]=='"' && v[^1]=='"') return FieldValue.FromString(UnescapeString(v[1..^1]));
        if (v.EndsWith('i')) return FieldValue.FromInteger(long.Parse(v[..^1], CultureInfo.InvariantCulture));
        if (IsBool(v, out var b)) return FieldValue.FromBoolean(b);
        return FieldValue.FromDouble(double.Parse(v, CultureInfo.InvariantCulture));
    }
    private static bool IsBool(string s,out bool v){switch(s){case "t":case "T":case "true":case "True":case "TRUE":v=true;return true;case "f":case "F":case "false":case "False":case "FALSE":v=false;return true;default:v=false;return false;}}
    private static int FindUnescaped(string s,char ch,int start){bool esc=false,inStr=false; for(int i=start;i<s.Length;i++){var c=s[i]; if(esc){esc=false;continue;} if(c=='\\'){esc=true;continue;} if(c=='"') inStr=!inStr; if(!inStr && c==ch) return i;} return -1;}
    private static List<string> SplitUnescaped(string s,char sep){var r=new List<string>(); int st=0; bool esc=false,inStr=false; for(int i=0;i<s.Length;i++){var c=s[i]; if(esc){esc=false;continue;} if(c=='\\'){esc=true;continue;} if(c=='"'){inStr=!inStr;continue;} if(!inStr&&c==sep){r.Add(s[st..i]);st=i+1;}} r.Add(s[st..]); return r;}
    private static (string Key,string Value) SplitFirstUnescaped(string s,char sep){var p=FindUnescaped(s,sep,0); if(p<=0) throw new FormatException($"invalid key-value: {s}"); return (s[..p],s[(p+1)..]);}
    private static string UnescapeKey(string s)=>s.Replace("\\ "," ").Replace("\\,",",").Replace("\\=","=").Replace("\\","");
    private static string UnescapeString(string s)=>s.Replace("\\\"","\"").Replace("\\","");
}

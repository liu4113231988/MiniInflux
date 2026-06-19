using System.Text;
namespace MiniInflux.Net10.Model;

public readonly record struct SeriesKey(string Measurement, string TagsCanonical)
{
    public static SeriesKey From(Point p)
    {
        if (p.Tags.Count == 0) return new(p.Measurement, "");
        var sb = new StringBuilder();
        foreach (var kv in p.Tags.OrderBy(x => x.Key, StringComparer.Ordinal))
        { if (sb.Length > 0) sb.Append(','); sb.Append(kv.Key).Append('=').Append(kv.Value); }
        return new(p.Measurement, sb.ToString());
    }
}

using System.Text;
namespace MiniInflux.Net10.Model;

public readonly record struct SeriesKey(string Measurement, string TagsCanonical)
{
    public static SeriesKey From(Point p)
    {
        if (p.Tags.Count == 0) return new(p.Measurement, "");
        if (p.TagsCanonical != null) return new(p.Measurement, p.TagsCanonical);
        var sb = new StringBuilder(p.Tags.Count * 16);
        string? previous = null;
        var sorted = true;
        foreach (var kv in p.Tags)
        {
            if (previous != null)
            {
                if (string.CompareOrdinal(previous, kv.Key) > 0)
                {
                    sorted = false;
                    break;
                }
                sb.Append(',');
            }

            sb.Append(kv.Key).Append('=').Append(kv.Value);
            previous = kv.Key;
        }

        if (!sorted)
        {
            sb.Clear();
            foreach (var kv in p.Tags.OrderBy(x => x.Key, StringComparer.Ordinal))
            {
                if (sb.Length > 0) sb.Append(',');
                sb.Append(kv.Key).Append('=').Append(kv.Value);
            }
        }

        return new(p.Measurement, sb.ToString());
    }
}

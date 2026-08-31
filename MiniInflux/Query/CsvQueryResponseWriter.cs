using System.Globalization;
using System.Text;

namespace MiniInflux.Net10.Query;

/// <summary>
/// Server-side CSV rendering of v1-style query responses (format=csv on /query and the v3
/// query endpoint). One block per series: a "name,tags,&lt;columns&gt;" header followed by data
/// rows. Tags render as key=value pairs joined with ';' so the CSV delimiter stays unambiguous.
/// </summary>
public static class CsvQueryResponseWriter
{
    public static string Write(QueryResponse response)
    {
        var sb = new StringBuilder(4096);
        foreach (var result in response.Results)
        {
            if (result.Error != null || result.Series == null) continue;
            foreach (var series in result.Series)
            {
                // Per-column escaping so "time,value" stays two columns (InfluxDB v1 CSV compatible)
                sb.Append("name,tags,");
                sb.AppendLine(string.Join(",", series.Columns.Select(Escape)));

                var tagText = series.Tags == null || series.Tags.Count == 0
                    ? ""
                    : string.Join(";",
                        series.Tags.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                            .Select(kv => $"{EscapeTagComponent(kv.Key)}={EscapeTagComponent(kv.Value)}"));

                foreach (var row in series.Values)
                {
                    sb.Append(Escape(series.Name)).Append(',').Append(Escape(tagText));
                    foreach (var cell in row)
                    {
                        sb.Append(',');
                        sb.Append(Render(cell));
                    }
                    sb.AppendLine();
                }
            }
        }
        return sb.ToString();
    }

    private static string Render(object? value) => value switch
    {
        null => "",
        double d => d.ToString("R", CultureInfo.InvariantCulture),
        float f => f.ToString("R", CultureInfo.InvariantCulture),
        long l => l.ToString(CultureInfo.InvariantCulture),
        int i => i.ToString(CultureInfo.InvariantCulture),
        bool b => b ? "true" : "false",
        string s => Escape(s),
        _ => Escape(value.ToString() ?? "")
    };

    private static string EscapeTagComponent(string value)
    {
        // Escape characters that conflict with the "k=v; k=v" encoding: % ; = , newlines.
        // Percent-encode to keep round-tripping distinct from literal values containing ';'/'='.
        if (value.IndexOfAny(['%', ';', '=', ',', '\n', '\r', '\\']) < 0) return value;
        return value.Replace("%", "%25").Replace(";", "%3B").Replace("=", "%3D").Replace(",", "%2C").Replace("\n", "%0A").Replace("\r", "%0D").Replace("\\", "%5C");
    }

    private static string Escape(string value)
    {
        // CSV formula injection: leading = + - @ | or tab can trigger spreadsheet execution.
        var needsForceQuote = value.Length > 0 && value[0] is '=' or '+' or '-' or '@' or '|' or '\t';
        if (needsForceQuote)
        {
            return "\"'" + value.Replace("\"", "\"\"") + "\"";
        }
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }
}

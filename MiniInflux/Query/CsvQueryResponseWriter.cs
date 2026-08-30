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
                sb.Append("name,tags,");
                sb.AppendLine(EscapeJoin(series.Columns, ','));

                var tagText = series.Tags == null || series.Tags.Count == 0
                    ? ""
                    : string.Join(";",
                        series.Tags.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                            .Select(kv => $"{kv.Key}={kv.Value}"));

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

    private static string EscapeJoin(IEnumerable<string> values, char delimiter)
    {
        var joined = string.Join(delimiter, values);
        return Escape(joined);
    }

    private static string Escape(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }
}

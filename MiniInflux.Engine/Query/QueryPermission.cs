using MiniInflux.Net10.Protocol;

namespace MiniInflux.Net10.Query;

/// <summary>
/// Maps a parsed statement to the coarse read/write permission tier used by the HTTP API.
/// SELECT INTO and all DDL/DML require write; ordinary SELECT and SHOW require read.
/// </summary>
public static class QueryPermission
{
    public static string RequiredPermission(ParsedQuery query) =>
        IsReadOnly(query) ? "read" : "write";

    public static bool IsReadOnly(ParsedQuery query)
    {
        switch (query.Kind)
        {
            case QueryKind.Select:
                return string.IsNullOrWhiteSpace(query.IntoTarget)
                    && (query.Subquery == null || IsReadOnly(query.Subquery));
            case QueryKind.Explain:
                return query.ExplainedQuery == null || IsReadOnly(query.ExplainedQuery);
            case QueryKind.ShowDatabases:
            case QueryKind.ShowMeasurements:
            case QueryKind.ShowFieldKeys:
            case QueryKind.ShowTagKeys:
            case QueryKind.ShowTagValues:
            case QueryKind.ShowSeries:
            case QueryKind.ShowSeriesCardinality:
            case QueryKind.ShowMeasurementCardinality:
            case QueryKind.ShowTagValuesCardinality:
            case QueryKind.ShowTagKeyCardinality:
            case QueryKind.ShowFieldKeyCardinality:
            case QueryKind.ShowRetentionPolicies:
            case QueryKind.ShowContinuousQueries:
            case QueryKind.ShowShards:
            case QueryKind.ShowShardGroups:
            case QueryKind.ShowStats:
            case QueryKind.ShowDiagnostics:
            case QueryKind.ShowQueries:
                return true;
            default:
                return false;
        }
    }
}

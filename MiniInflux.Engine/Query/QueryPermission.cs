using MiniInflux.Net10.Protocol;

namespace MiniInflux.Net10.Query;

/// <summary>
/// Maps a parsed statement to the coarse read/write permission tier used by the HTTP API.
/// SELECT INTO and all DDL/DML require write; ordinary SELECT and SHOW require read.
/// </summary>
public static class QueryPermission
{
    /// <summary>
    /// Maximum subquery nesting depth. Guards both permission classification and execution
    /// against unbounded recursion (StackOverflowException cannot be caught and would crash
    /// the process); deeper queries are rejected with an explicit error instead.
    /// </summary>
    public const int MaxSubqueryDepth = 16;

    public static string RequiredPermission(ParsedQuery query) =>
        IsReadOnly(query) ? "read" : "write";

    public static bool IsReadOnly(ParsedQuery query, int depth = 0)
    {
        if (depth > MaxSubqueryDepth)
            throw new NotSupportedException($"subquery nesting exceeds the maximum depth of {MaxSubqueryDepth}");
        switch (query.Kind)
        {
            case QueryKind.Select:
                return string.IsNullOrWhiteSpace(query.IntoTarget)
                    && (query.Subquery == null || IsReadOnly(query.Subquery, depth + 1));
            case QueryKind.Explain:
                return query.ExplainedQuery == null || IsReadOnly(query.ExplainedQuery, depth + 1);
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
